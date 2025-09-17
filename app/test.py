# Dans votre fichier de routes

import random
from typing import List, Optional
from collections import defaultdict
from sqlalchemy import select, func, and_
import models
import schemas2
from models import RessourceModel, CagnottePostModel, CagnotteModel, UserModel, CategorieModel
from schemas2 import CagnottePostFeedResponse, RessourceResponse, Author, CagnotteSimpleResponse, TypeRessource, Ressource
import asyncio
from collections import defaultdict
import logging
import json
from dotenv import load_dotenv
import os
from typing import List, Dict, Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, Session
from sqlalchemy.orm import selectinload, aliased
import uuid as uuid_pkg
from sqlalchemy import select, func, union_all, literal_column
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import case

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Query, status, Depends, APIRouter
from contextlib import asynccontextmanager
from datetime import datetime
import random
from decimal import Decimal
from pydantic import BaseModel, Field


import redis.asyncio as redis
import redis.asyncio as aioredis
from db.redis_client import get_redis_client, redis_connection_pool
from db.postgres_config import get_db, AsyncSession, engine, create_db_and_tables


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC_USER_EVENT = os.getenv("KAFKA_TOPIC_USER_EVENT")


# ===================================================================
#OPTIMISATION 1: Gérer le producteur Kafka via le cycle de vie de l'app
# ===================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Démarrage de l'application FastAPI...")
    
    # --- Initialisation ---
    # 1. Connexion à Redis
    await redis_connection_pool.ping()
    logger.info("Connexion à Redis réussie !")

    # 2. Créer le producteur Kafka et le maintenir en vie
    logger.info("Démarrage du producteur Kafka...")
    app.state.kafka_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await app.state.kafka_producer.start()
    logger.info("Producteur Kafka démarré.")
    
    yield
    
    logger.info("🛑 Arrêt de l'application...")
    # --- Nettoyage ---
    # 1. Arrêter le producteur Kafka
    logger.info("Arrêt du producteur Kafka...")
    await app.state.kafka_producer.stop()
    logger.info("Producteur Kafka arrêté.")

    # 2. Fermer le pool Redis
    await redis_connection_pool.close()
    logger.info("Pool de connexions Redis fermé.")
    
    # 3. Fermer le pool de connexions de la base de données
    await engine.dispose()
    logger.info("Pool de connexions de la base de données fermé.")


app = FastAPI(
    title="Event Ingestion API",
    description="An ultra-fast API to receive event batches and push them to Kafka.",
    lifespan=lifespan
)

#===============================Methodes==========================
# Dépendance pour injecter le producteur Kafka dans les endpoints
async def get_kafka_producer() -> AIOKafkaProducer:
    return app.state.kafka_producer
#===============================Methodes==========================




def build_feed_response(
    post: models.CagnottePostModel, 
    ressources: List[models.RessourceModel],
    likes_count: Optional[int] = 0,
    comments_count: Optional[int] = 0
) -> schemas2.CagnottePostFeedResponse:
    
    # Filtrer les ressources par type
    video_resources = [r for r in ressources if r.type == schemas2.TypeRessource.POST_VIDEO and r.alt_text and r.alt_text.startswith("READY_")]
    thumbnails = [r for r in ressources if r.type == schemas2.TypeRessource.POST_THUMBNAIL]
    images = [r for r in ressources if r.type == schemas2.TypeRessource.POST_IMAGE]

    # Calculer les métadonnées des médias
    has_video = bool(video_resources)
    total_medias = len(ressources)
    has_multiple_medias = total_medias > 1
    
    # Extraire les URLs des formats vidéo
    def get_video_url(alt_text_prefix):
        for r in video_resources:
            if r.alt_text == alt_text_prefix:
                return r.file
        return None

    mobile_video_url = get_video_url("READY_MOBILE")
    standard_video_url = get_video_url("READY_STANDARD")
    hd_video_url = get_video_url("READY_HD")

    # Déterminer l'URL principale
    main_media_url = None
    if has_video:
        main_media_url = standard_video_url or mobile_video_url or hd_video_url
    elif images:
        main_media_url = images[0].file

    # Déterminer le thumbnail
    thumbnail_url = thumbnails[0].file if thumbnails else (images[0].file if images else None)

    # Durée de la vidéo
    video_duration = None
    if has_video:
        for r in video_resources:
            if r.duration is not None:
                video_duration = r.duration
                break

    # Création de l'objet de réponse final
    feed_item = schemas2.CagnottePostFeedResponse(
        id=post.id,
        cagnotte=schemas2.CagnotteSimpleResponse.from_orm(post.cagnotte),
        author=schemas2.Author.from_orm(post.author),
        type=post.type,
        title=post.title,
        content=post.content,
        likesCount=post.likes_count,
        commentsCount=post.comments_count,
        viewsCount=post.views_count,
        sharesCount=post.shares_count,
        createdDate=post.created_date,
        isPinned=post.is_pinned,
        medias=[schemas2.RessourceResponse.from_orm(r) for r in ressources],
        mainMediaUrl=main_media_url,
        thumbnailUrl=thumbnail_url,
        previewGifUrl=None, # TODO
        hasVideo=has_video,
        hasMultipleMedias=has_multiple_medias,
        totalMedias=total_medias,
        aspectRatio="16:9", # TODO
        videoDuration=video_duration,
        estimatedDataUsage=0, # TODO
        mobileVideoUrl=mobile_video_url,
        standardVideoUrl=standard_video_url,
        hdVideoUrl=hd_video_url,
    )
    return feed_item



# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Analytiques & Recommandations
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
@app.get("/recommendations/feed/", response_model=List[schemas2.CagnottePostFeedResponse])
async def get_recommendations_feed(
    user_id: Optional[str] = None,
    session_id: Optional[str] = None,
    total_recommendations: int = 10,
    db: AsyncSession = Depends(get_db),
    redis_client: aioredis.Redis = Depends(get_redis_client)
):
    """
    Génère des recommandations de cagnottes enrichies avec le post le plus récent
    et TOUTES les ressources associées à ce post.
    """
    # =================================================================
    # ÉTAPE 1: OBTENIR LES RECOMMANDATIONS DE CAGNOTTES (logique initiale)
    # =================================================================
    if not user_id and not session_id:
        raise HTTPException(status_code=400, detail="Un 'user_id' ou un 'session_id' doit être fourni.")
    if user_id and session_id:
        raise HTTPException(status_code=400, detail="Fournissez 'user_id' OU 'session_id', pas les deux.")
        
    redis_key = f"profile:user:{user_id}" if user_id else f"profile:session:{session_id}"

    user_profile = await redis_client.hgetall(redis_key)
    # if not user_profile:
    #     return []

    sorted_categories = sorted(user_profile.items(), key=lambda item: float(item[1]), reverse=True)
    
    recommendations_plan = []
    # ... (logique de distribution inchangée)
    if len(sorted_categories) >= 3:
        top_cat_1, _ = sorted_categories[0]; top_cat_2, _ = sorted_categories[1]; top_cat_3, _ = sorted_categories[2]
        num_cat_1 = round(total_recommendations * 0.5); num_cat_2 = round(total_recommendations * 0.3)
        num_cat_3 = total_recommendations - num_cat_1 - num_cat_2
        recommendations_plan = [(top_cat_1, num_cat_1), (top_cat_2, num_cat_2), (top_cat_3, num_cat_3)]
    elif len(sorted_categories) == 2:
        top_cat_1, _ = sorted_categories[0]; top_cat_2, _ = sorted_categories[1]
        num_cat_1 = round(total_recommendations * 0.6); num_cat_2 = total_recommendations - num_cat_1
        recommendations_plan = [(top_cat_1, num_cat_1), (top_cat_2, num_cat_2)]
    elif len(sorted_categories) == 1:
        top_cat_1, _ = sorted_categories[0]
        recommendations_plan = [(top_cat_1, total_recommendations)]

    if not recommendations_plan:
        return []

    union_queries = []
    for category_id, num_to_fetch in recommendations_plan:
        if num_to_fetch > 0:
            sq = select(models.CagnotteModel).where(models.CagnotteModel.id_categorie == category_id).order_by(func.random()).limit(num_to_fetch)
            union_queries.append(sq)

    if not union_queries:
        return []   

    full_query = union_all(*union_queries).alias("all_cagnottes")
    final_stmt = select(models.CagnotteModel).options(selectinload(models.CagnotteModel.categorie), selectinload(models.CagnotteModel.admin)).join(full_query, models.CagnotteModel.id == full_query.c.id)
    result = await db.execute(final_stmt)
    recommended_cagnottes = result.scalars().all()


    if not recommended_cagnottes:
        return []

    cagnotte_ids = [c.id for c in recommended_cagnottes]

    # ÉTAPE 2: RÉCUPÉRER LE POST LE PLUS RÉCENT DE CHAQUE CAGNOTTE
    Post = models.CagnottePostModel
    post_subquery = select(Post.id, func.row_number().over(partition_by=Post.id_cagnotte, order_by=Post.created_date.desc()).label("rn")).where(Post.id_cagnotte.in_(cagnotte_ids)).subquery()
    stmt_posts = select(Post).options(selectinload(Post.cagnotte), selectinload(Post.author)).join(post_subquery, Post.id == post_subquery.c.id).where(post_subquery.c.rn == 1)
    
    result_posts = await db.execute(stmt_posts)
    relevant_posts = result_posts.scalars().unique().all()

    if not relevant_posts:
        return []

    post_ids = [p.id for p in relevant_posts]
    post_cagnotte_ids = [p.cagnotte.id for p in relevant_posts]

    # ÉTAPE 3: RÉCUPÉRER EN MASSE TOUTES LES DONNÉES ADDITIONNELLES
    # 3.1 - Toutes les ressources, triées par `order_index`
    Ressource = models.RessourceModel
    stmt_resources = select(Ressource).where(Ressource.reference.in_(post_ids)).order_by(Ressource.order_index.asc())
    result_resources = await db.execute(stmt_resources)
    resources_by_post_id = defaultdict(list)
    for res in result_resources.scalars().all():
        resources_by_post_id[res.reference].append(res)

    # 3.2 - Tous les comptes de likes (sur la cagnotte)
    # Favorite = models.FavoriteModel
    # stmt_likes = select(Favorite.id_cagnotte, func.count(Favorite.id).label("count")).where(Favorite.id_cagnotte.in_(post_cagnotte_ids)).group_by(Favorite.id_cagnotte)
    # result_likes = await db.execute(stmt_likes)
    # likes_by_cagnotte_id = {cagnotte_id: count for cagnotte_id, count in result_likes.all()}

    # # 3.3 - Tous les comptes de commentaires (sur le post)
    # Commentaire = models.CommentaireModel
    # stmt_comments = select(Commentaire.id_post, func.count(Commentaire.id).label("count")).where(and_(Commentaire.id_post.in_(post_ids), Commentaire.deleted == False)).group_by(Commentaire.id_post)
    # result_comments = await db.execute(stmt_comments)
    # comments_by_post_id = {post_id: count for post_id, count in result_comments.all()}

    # ÉTAPE 4: ASSEMBLER LA RÉPONSE FINALE EN UTILISANT LA FONCTION HELPER
    final_response = []
    for post in relevant_posts:
        ressources = resources_by_post_id.get(post.id, [])
        # likes_count = likes_by_cagnotte_id.get(post.cagnotte.id, 0)
        # comments_count = comments_by_post_id.get(post.id, 0)
        
        # feed_item = build_feed_response(post, ressources, likes_count, comments_count)
        feed_item = build_feed_response(post, ressources)
        final_response.append(feed_item)
        
    random.shuffle(final_response)
    return final_response


@app.get("/countries/popularity/{country}")
async def get_country_popularity(
    country: str,
    redis: aioredis.Redis = Depends(get_redis_client)
):
    """
    Récupère les détails de popularité pour un pays spécifique
    """
    try:
        data = await redis.get("analytics_popularity_by_country")
        if not data:
            raise HTTPException(status_code=404, detail="Données de popularité non trouvées")
        
        countries_data = json.loads(data)
        
        country_data = countries_data.get(country)
        if not country_data:
            raise HTTPException(status_code=404, detail=f"Pays '{country}' non trouvé")
        
        return {
            "status": "success",
            "data": country_data,
            "timestamp": datetime.now().isoformat()
        }
        
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Erreur de décodage des données Redis")
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des données pour {country}: {e}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")


@app.get("/countries/popularity/{country}/feed/", response_model=List[schemas2.CagnottePostFeedResponse])
async def get_country_popularity_feed(
    country: str,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis_client)
):
    """
    Récupère un feed des posts les plus populaires pour un pays,
    basé sur les analyses stockées dans Redis.
    """
    # ===================================================================
    # ÉTAPE 1: RÉCUPÉRER LES IDs DES VIDÉOS POPULAIRES DEPUIS REDIS
    # ===================================================================
    try:
        data = await redis.get("analytics_popularity_by_country")
        if not data:
            return [] # Pas de données, on retourne une liste vide
        
        countries_data = json.loads(data)
        country_data = countries_data.get(country)
        
        if not country_data:
            return [] # Pays non trouvé, on retourne une liste vide

        top_videos = country_data.get("top_videos", [])
        if not top_videos:
            return []

        # On extrait les IDs et on gère la conversion en int si nécessaire
        video_ids = [int(item["video_id"]) for item in top_videos]

    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Erreur de données de popularité pour {country}: {e}")
        raise HTTPException(status_code=500, detail="Données de popularité invalides ou malformées")

    # ===================================================================
    # ÉTAPE 2: TROUVER LES POSTS CORRESPONDANTS DANS L'ORDRE DE POPULARITÉ
    # ===================================================================
    
    # On crée une map pour pouvoir trier les résultats de la BDD dans le même ordre que Redis
    order_map = {video_id: index for index, video_id in enumerate(video_ids)}
    order_logic = case(order_map, value=models.RessourceModel.id)

    # Requête pour trouver les posts en joignant sur les ressources
    # et en triant selon l'ordre de popularité de Redis
    Post = models.CagnottePostModel
    Ressource = models.RessourceModel
    stmt_posts = (
        select(Post)
        .options(selectinload(Post.cagnotte).selectinload(models.CagnotteModel.categorie), selectinload(Post.author))
        .join(Ressource, Post.id == Ressource.reference)
        .where(Ressource.id.in_(video_ids))
        .order_by(order_logic) # <-- TRI MAGIQUE ICI
    )
    
    result_posts = await db.execute(stmt_posts)
    relevant_posts = result_posts.scalars().unique().all()

    if not relevant_posts:
        return []

    # ===================================================================
    # ÉTAPE 3: APPLIQUER L'ENRICHISSEMENT (LOGIQUE EXISTANTE)
    # ===================================================================
    post_ids = [p.id for p in relevant_posts]
    post_cagnotte_ids = [p.cagnotte.id for p in relevant_posts]

    # 3.1 - Toutes les ressources, triées par `order_index`
    stmt_resources = select(Ressource).where(Ressource.reference.in_(post_ids)).order_by(Ressource.order_index.asc())
    result_resources = await db.execute(stmt_resources)
    resources_by_post_id = defaultdict(list)
    for res in result_resources.scalars().all():
        resources_by_post_id[res.reference].append(res)

    # # 3.2 - Tous les comptes de likes (sur la cagnotte)
    # # ... (la même requête que précédemment)
    # likes_by_cagnotte_id = { ... }

    # # 3.3 - Tous les comptes de commentaires (sur le post)
    # # ... (la même requête que précédemment)
    # comments_by_post_id = { ... }

    # ===================================================================
    # ÉTAPE 4: ASSEMBLER LA RÉPONSE FINALE
    # ===================================================================
    final_response = []
    for post in relevant_posts:
        ressources = resources_by_post_id.get(post.id, [])
        # likes_count = likes_by_cagnotte_id.get(post.cagnotte.id, 0)
        # comments_count = comments_by_post_id.get(post.id, 0)
        
        # feed_item = build_feed_response(post, ressources, likes_count, comments_count)
        feed_item = build_feed_response(post, ressources)
        final_response.append(feed_item)
        
    return final_response





# =========================POPULARITY==========================================

#Liste des catégories par popularité
@app.get("/categories/popularity")
async def get_categories_popularity(
    redis: aioredis.Redis = Depends(get_redis_client),
    limit: Optional[int] = Query(None, ge=1, le=100, description="Nombre maximum de catégories à retourner"),
    sort_by: str = Query("popularity_score", description="Critère de tri"),
    order: str = Query("desc", description="Ordre: asc ou desc")
):
    """
    Récupère la popularité par catégorie depuis Redis
    """
    try:
        data = await redis.get("analytics_popularity_by_category")
        if not data:
            raise HTTPException(status_code=404, detail="Données de popularité par catégorie non trouvées")
        
        categories_data = json.loads(data)
        categories_list = list(categories_data.values())
        
        # Tri
        reverse = order.lower() == "desc"
        try:
            categories_list.sort(key=lambda x: x.get(sort_by, 0), reverse=reverse)
        except (KeyError, TypeError):
            raise HTTPException(status_code=400, detail=f"Critère de tri '{sort_by}' invalide")
        
        # Limitation
        if limit:
            categories_list = categories_list[:limit]
        
        return {
            "status": "success",
            "data": categories_list,
            "total_categories": len(categories_data),
            "returned_categories": len(categories_list),
            "sorted_by": sort_by,
            "order": order,
            "timestamp": datetime.now().isoformat()
        }
        
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Erreur de décodage des données Redis")
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des données catégories: {e}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")

#étails pour une catégorie spécifique
@app.get("/analytics/categories/popularity/{category_id}")
async def get_category_popularity(
    category_id: str,
    redis: aioredis.Redis = Depends(get_redis_client)
):
    """
    Récupère les détails de popularité pour une catégorie spécifique
    """
    try:
        data = await redis.get("analytics_popularity_by_category")
        if not data:
            raise HTTPException(status_code=404, detail="Données de popularité non trouvées")
        
        categories_data = json.loads(data)
        category_data = categories_data.get(category_id)
        if not category_data:
            raise HTTPException(status_code=404, detail=f"Catégorie '{category_id}' non trouvée")
        
        return {
            "status": "success",
            "data": category_data,
            "timestamp": datetime.now().isoformat()
        }
        
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Erreur de décodage des données Redis")
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des données pour la catégorie {category_id}: {e}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")
# =========================POPULARITY==========================================

