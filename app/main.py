import random
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
import asyncio
from collections import defaultdict
import logging
import json
from dotenv import load_dotenv
import os
from typing import List, Dict, Any, Optional, Set
from fastapi import FastAPI, HTTPException, Query, status, Depends, APIRouter, Header, Path, Request
from typing import Annotated
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from pydentics import models
from pydentics import schemas2
from pydentics import schemas3

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, Session
from sqlalchemy.orm import selectinload, aliased
from sqlalchemy import select, func, union_all, literal_column, join
from sqlalchemy.dialects.postgresql import UUID, array
from sqlalchemy import select, func, and_
import sqlalchemy as sa
from sqlalchemy import case, cast, TEXT

from aiokafka import AIOKafkaProducer
from pydantic import BaseModel, Field
import redis
import redis.asyncio as aioredis

from db.redis_client import get_redis_client, redis_connection_pool
from db.postgres_config import get_db, AsyncSession, engine, create_db_and_tables
from utils.auxiliaires_fonctions import build_feed_response, _get_popular_ids_from_redis, _get_trending_data_from_redis, _get_new_arrivals_ids_from_db, enrich_feed_with_presigned_urls, check_cache_freshness, select_feed_items
from worker.feed.feed_popular_trending_new import schedule_feed_generation, generate_and_cache_feed_for_country

from utils import config


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC_USER_EVENT = os.getenv("KAFKA_TOPIC_USER_EVENT")
SPRING_BOOT_API_URL = os.getenv("SPRING_BOOT_API_URL")


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
async def get_kafka_producer(request: Request) -> AIOKafkaProducer:
    return request.app.state.kafka_producer
#===============================Methodes==========================



#=========================RECEVELY-EVENT-USER_ENDPOINT===========================
@app.post(
    "/events/collect",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Accept a batch of user events"
)
async def accept_events(
    batch: schemas2.EventBatchModel,
    producer: AIOKafkaProducer = Depends(get_kafka_producer) # Injection de dépendance
):
    """
    Accepte un lot d'événements, les envoie à Kafka et répond immédiatement.
    Utilise le producteur Kafka partagé pour une performance maximale.
    """
    try:
        logger.info(f"Received a batch of {len(batch.events)} events. Sending to Kafka topic '{KAFKA_TOPIC_USER_EVENT}'.")

        tasks = []
        for event in batch.events:
            message = event.model_dump_json().encode("utf-8")
            tasks.append(producer.send(KAFKA_TOPIC_USER_EVENT, message))
        
        await asyncio.gather(*tasks)

        logger.info("Batch successfully sent to Kafka.")
        return {"status": "accepted", "message": f"{len(batch.events)} events queued."}

    except Exception as e:
        logger.error(f"Error sending to Kafka: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not send events to the processing queue. Please try again later."
        )

#=========================RECEVELY-EVENT-USER_ENDPOINT===========================



# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Analytiques & Recommandations
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

#=========================COLD-START-ENDPOINT===================================
@app.get(
    "/cold-start/combined/feed/v1/{country}",
    response_model=List[schemas3.CombinedFeedItem],
    summary="Feed combiné et intelligent (ultra-optimisé)",
    description="""
    Récupère un feed pré-calculé et mis en cache pour une performance maximale.
    
    - ⚡ Latence cible: <50ms
    - 🔄 Cache mis à jour toutes les 2 minutes
    - 🎲 Ordre aléatoire pour chaque requête
    - 🌍 Pays supportés: Mali, Cameroun, Senegal
    """,
    tags=["Feed"]
)
async def get_combined_feed_optimized(
    country: str,
    redis: aioredis.Redis = Depends(get_redis_client),
    limit: int = Query(
        config.EndpointConfig.DEFAULT_LIMIT,
        ge=config.EndpointConfig.MIN_LIMIT,
        le=config.EndpointConfig.MAX_LIMIT,
        description="Nombre d'éléments à retourner"
    ),
    randomize: bool = Query(
        True,
        description="Mélanger les résultats aléatoirement"
    ),
    seed: Optional[int] = Query(
        None,
        description="Seed pour randomisation (tests uniquement)"
    )
):
    """
    Endpoint ultra-optimisé pour récupérer le feed combiné.
    
    Performance optimizations:
    - Lecture directe depuis Redis (pas de DB)
    - Enrichissement des URLs à la volée uniquement
    - Sélection aléatoire après enrichissement
    - Gestion d'erreurs granulaire
    """
    
    # ===================================================================
    # ÉTAPE 2: RÉCUPÉRATION DEPUIS LE CACHE
    # ===================================================================
    cache_key = config.EndpointConfig.CACHE_KEY_TEMPLATE.format(country=country)
    
    try:
        # Vérifier la fraîcheur du cache
        ttl = await check_cache_freshness(redis, cache_key)
        
        if ttl is None:
            # Cache expiré ou inexistant
            if config.EndpointConfig.ENABLE_FALLBACK:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail={
                        "message": f"Le feed pour '{country}' est en cours de génération.",
                        "retry_after": f"{config.EndpointConfig.FALLBACK_RETRY_DELAY} secondes",
                        "country": country
                    }
                )
            else:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Aucun feed disponible pour '{country}'"
                )
        
        # Récupérer les données du cache
        cached_data = await redis.get(cache_key)
        
        if not cached_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Feed vide pour '{country}'"
            )
        
        # Désérialisation
        feed_items_raw = json.loads(cached_data)
        
        if not feed_items_raw:
            return []
        
        print(f"✅ Cache hit pour {country}: {len(feed_items_raw)} éléments (TTL: {ttl}s)")
    
    except HTTPException:
        raise
    except json.JSONDecodeError as e:
        print(f"❌ Erreur de désérialisation JSON pour {country}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur de format des données en cache"
        )
    except Exception as e:
        print(f"❌ Erreur Redis pour {country}: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service de cache temporairement indisponible"
        )
    
    # ===================================================================
    # ÉTAPE 3: SÉLECTION DES ÉLÉMENTS (AVANT ENRICHISSEMENT)
    # ===================================================================
    # OPTIMISATION: Sélectionner AVANT d'enrichir pour limiter les appels de présignature
    selected_items = select_feed_items(
        feed_items_raw,
        limit=limit,
        randomize=randomize,
        seed=seed
    )
    
    if not selected_items:
        return []
    
    # ===================================================================
    # ÉTAPE 4: ENRICHISSEMENT DES URLS PRÉSIGNÉES
    # ===================================================================
    try:
        enriched_feed = await enrich_feed_with_presigned_urls(selected_items)
        
        print(f"🎬 Feed enrichi pour {country}: {len(enriched_feed)} éléments")
        
        return enriched_feed
    
    except Exception as e:
        print(f"❌ Erreur lors de l'enrichissement des URLs pour {country}: {e}")
        
        # FALLBACK: Retourner les données sans URLs présignées
        # plutôt que de tout faire échouer
        print(f"⚠️ Fallback: retour des données sans URLs présignées")
        
        # Retourner les données brutes (sans enrichissement)
        # Attention: vérifier que votre modèle accepte des URLs vides/None
        return selected_items



# @app.get(
#     "/feed/v1/combined2/{country}",
#     response_model=List[schemas3.CombinedFeedItem],
#     summary="Génère un feed combiné et intelligent",
#     description="Mélange les posts populaires, tendance et les nouveautés pour créer un feed dynamique."
# )
# async def get_combined_feed(
#     country: str,
#     db: AsyncSession = Depends(get_db),
#     redis: aioredis.Redis = Depends(get_redis_client),
#     limit: int = Query(20, ge=5, le=100, description="Nombre total d'éléments à retourner."),
#     popular_ratio: int = Query(3, ge=1, description="Ratio d'éléments populaires dans le mix."),
#     trending_ratio: int = Query(2, ge=1, description="Ratio d'éléments tendance dans le mix."),
#     new_arrivals_count: int = Query(2, ge=0, le=10, description="Nombre de nouveautés à injecter au début du feed.")
# ):
#     #ge=5, le=100: Ce sont des contraintes de validation. La valeur doit être supérieure ou égale à 5 (ge pour "greater or equal") et inférieure ou égale à 100 (le pour "less or equal").
#     """
#     Cet endpoint crée un feed personnalisé en combinant plusieurs sources :
#     1.  **Nouveautés** : Les X derniers posts pour assurer la visibilité des nouveaux contenus.
#     2.  **Populaires** : Les posts les plus vus/likés.
#     3.  **Tendance** : Les posts avec une forte vélocité de popularité.

#     Le résultat est un mélange intelligent pour maximiser l'engagement utilisateur.
#     """
#     # ===================================================================
#     # ÉTAPE 1: RÉCUPÉRATION PARALLÈLE DES DONNÉES SOURCES
#     # ===================================================================
#     try:
#         popular_ids_task = _get_popular_ids_from_redis(redis, country)
#         trending_data_task = _get_trending_data_from_redis(redis, country)
#         new_arrivals_ids_task = _get_new_arrivals_ids_from_db(db, new_arrivals_count)

#         popular_ids, trending_videos_raw, new_arrivals_ids = await asyncio.gather(
#             popular_ids_task, trending_data_task, new_arrivals_ids_task
#         )
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Erreur lors de la récupération des données sources: {e}")

#     # ===================================================================
#     # ÉTAPE 2: TRAITEMENT & FILTRAGE DES DONNÉES TENDANCE
#     # ===================================================================
#     # On applique les filtres business sur les données tendance (comme dans l'ancien endpoint)
#     # Pour cet exemple, on trie simplement par score
#     trending_videos_up_only = [
#         v for v in trending_videos_raw if v.get('trend_direction') == 'up'
#     ]
    
#     # On utilise la liste filtrée pour la suite
#     trending_ids = [int(v['video_id']) for v in trending_videos_up_only]
#     trending_data_map = {int(v['video_id']): v for v in trending_videos_up_only}


#     # ===================================================================
#     # ÉTAPE 3: MÉLANGE INTELLIGENT ET DÉDOUBLONNAGE
#     # ===================================================================
#     final_ordered_ids = []
#     reasons_map = {}
#     seen_ids = set()

#     # 3.1 - Injecter les nouveautés en priorité
#     for video_id in new_arrivals_ids:
#         if video_id not in seen_ids:
#             final_ordered_ids.append(video_id)
#             reasons_map[video_id] = schemas3.CombinedFeedReason.NEW_ARRIVAL
#             seen_ids.add(video_id)

#     # 3.2 - Mélanger populaires et tendance selon le ratio
#     popular_iter = iter(popular_ids)
#     trending_iter = iter(trending_ids)
    
#     while len(final_ordered_ids) < limit:
#         added_in_cycle = 0
#         # Ajouter des posts populaires
#         for _ in range(popular_ratio):
#             try:
#                 video_id = next(popular_iter)
#                 if video_id not in seen_ids:
#                     final_ordered_ids.append(video_id)
#                     reasons_map[video_id] = schemas3.CombinedFeedReason.POPULAR
#                     seen_ids.add(video_id)
#                     added_in_cycle += 1
#                     if len(final_ordered_ids) >= limit: break
#             except StopIteration:
#                 break # Plus de posts populaires

#         if len(final_ordered_ids) >= limit: break

#         # Ajouter des posts tendance
#         for _ in range(trending_ratio):
#             try:
#                 video_id = next(trending_iter)
#                 if video_id not in seen_ids:
#                     final_ordered_ids.append(video_id)
#                     reasons_map[video_id] = schemas3.CombinedFeedReason.TRENDING
#                     seen_ids.add(video_id)
#                     added_in_cycle += 1
#                     if len(final_ordered_ids) >= limit: break
#             except StopIteration:
#                 break # Plus de posts tendance

#         if added_in_cycle == 0:
#             break # Si on n'a plus rien à ajouter, on sort

#     if not final_ordered_ids:
#         return []

#     # ===================================================================
#     # ÉTAPE 4: ENRICHISSEMENT UNIFIÉ VIA LA BASE DE DONNÉES
#     # ===================================================================
#     Post = models.CagnottePostModel
#     Ressource = models.RessourceModel

#     # Map pour préserver l'ordre du mélange
#     order_map = {video_id: index for index, video_id in enumerate(final_ordered_ids)}
#     order_logic = case(order_map, value=Ressource.id)

#     # Requête unique pour récupérer tous les posts nécessaires
#     stmt_posts = (
#         select(Post)
#         .options(
#             selectinload(Post.cagnotte).selectinload(models.CagnotteModel.categorie),
#             selectinload(Post.cagnotte).selectinload(models.CagnotteModel.admin),
#             selectinload(Post.author)
#         )
#         .join(Ressource, Post.id == Ressource.reference)
#         .where(Ressource.id.in_(final_ordered_ids))
#         .order_by(order_logic)
#     )
#     result_posts = await db.execute(stmt_posts)
#     # .unique() est crucial car un post peut matcher plusieurs ressources (vidéos)
#     relevant_posts = result_posts.scalars().unique().all()
    
#     post_ids = [p.id for p in relevant_posts]
    
#     # Requête pour récupérer TOUTES les ressources de ces posts
#     stmt_resources = select(Ressource).where(Ressource.reference.in_(post_ids)).order_by(Ressource.order_index.asc())
#     result_resources = await db.execute(stmt_resources)
#     resources_by_post_id = defaultdict(list)
#     for res in result_resources.scalars().all():
#         resources_by_post_id[res.reference].append(res)

#     # ===================================================================
#     # ÉTAPE 5: ASSEMBLAGE DE LA RÉPONSE FINALE
#     # ===================================================================
#     final_response_models = []
#     for post in relevant_posts:
#         ressources = resources_by_post_id.get(post.id, [])
        
#         # Trouver quel ID de ressource a mis ce post dans notre liste
#         # C'est un peu complexe, mais nécessaire si un post a plusieurs vidéos
#         trigger_ressource_id = None
#         for r in ressources:
#             if r.id in reasons_map:
#                 trigger_ressource_id = r.id
#                 break
        
#         if trigger_ressource_id is None:
#             continue # Ne devrait pas arriver, mais sécurité

#         # Construire l'objet de base
#         base_feed_item = build_feed_response(post, ressources)

#         # Créer l'objet final enrichi
#         reason = reasons_map[trigger_ressource_id]
        
#         # Créer un dictionnaire de données pour l'objet final
#         response_data = base_feed_item.model_dump()
#         response_data['reason'] = reason

#         if reason == schemas3.CombinedFeedReason.TRENDING:
#             trending_info = trending_data_map.get(trigger_ressource_id, {})
#             response_data['trendingScore'] = trending_info.get('trending_score')
#             response_data['velocityPercent'] = trending_info.get('velocity_percent')
#             response_data['trendDirection'] = trending_info.get('trend_direction')
        
#         final_response_models.append(schemas3.CombinedFeedItem.model_validate(response_data))

#     # ===================================================================
#     # NOUVELLE ÉTAPE 6: ENRICHISSEMENT AVEC LES URLS PRÉSIGNÉES
#     # ===================================================================
#     if not final_response_models:
#         return []

#     # On convertit nos modèles Pydantic en dictionnaires pour pouvoir les modifier
#     feed_items_as_dicts = [item.model_dump() for item in final_response_models]

#     # On appelle notre nouvelle fonction qui va tout convertir en parallèle
#     enriched_feed = await enrich_feed_with_presigned_urls(feed_items_as_dicts)
    
#     # ===================================================================
#     # ÉTAPE 7: FINALISATION
#     # ===================================================================
#     random.shuffle(enriched_feed)
    
#     # FastAPI s'occupera de valider que la liste de dictionnaires correspond au `response_model`
#     return enriched_feed

#=========================COLD-START-ENDPOINT===================================


#===============================Preferences==========================

@app.get(
    "/users/preferences/categories/{user_id}",
    response_model=List[schemas2.Categorie],
    summary="Récupérer les catégories de préférences d'un utilisateur",
    tags=["Users"]
)
async def get_user_preference_categories(user_id: uuid.UUID, db: Session = Depends(get_db)):
    """
    Récupère une liste unique de toutes les catégories associées aux préférences
    d'un utilisateur donné.

    - **user_id**: L'identifiant UUID de l'utilisateur.
    - **Retourne**: Une liste d'objets catégorie.
    """
    # Vérifier si l'utilisateur existe (bonne pratique)
    user = await db.get(models.UserModel, user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User with id {user_id} not found"
        )

    # Construction de la requête avec SQLAlchemy
    # C'est la partie la plus importante.
    # On sélectionne les catégories (models.CategorieModel)
    # On fait la jointure avec la table preference_categories
    # Puis la jointure avec la table des préférences (models.PreferenceModel)
    # On filtre pour ne garder que les préférences de l'utilisateur concerné
    # On utilise distinct() pour s'assurer que chaque catégorie n'apparaît qu'une seule fois
    query = (
        select(models.CategorieModel)
        .join(models.preference_categories_table)
        .join(models.PreferenceModel)
        .where(models.PreferenceModel.user_id == user_id)
        .distinct()
    )

    # Exécution de la requête et récupération des résultats
    # .scalars().all() retourne une liste d'objets CategorieModel
    result = await db.execute(query)
    result_categories = result.scalars().all()

    return result_categories


@app.get(
    "/users/preferences/recommendations/feed/v1/{user_id}",
    response_model=List[schemas2.CagnottePostFeedResponse],
    summary="Générer un feed basé sur les préférences initiales de l'utilisateur",
    tags=["Recommendations"]
)
async def get_user_preferences_feed(
    user_id: uuid.UUID,
    total_recommendations: int = 15,
    db: AsyncSession = Depends(get_db)
):
    """
    Génère un feed de recommandations pour un utilisateur basé sur les catégories
    qu'il a sélectionnées comme préférences. C'est la stratégie de démarrage
    avant que le profil utilisateur (Redis) ne soit construit.
    La récupération des données est ultra-optimisée avec une seule requête à la BDD.
    """
    # =================================================================
    # ÉTAPE 1: OBTENIR LES CATÉGORIES DE PRÉFÉRENCES DE L'UTILISATEUR
    # =================================================================
    
    # On récupère les catégories de préférences directement depuis la base de données
    stmt_categories = (
        select(models.CategorieModel)
        .join(models.preference_categories_table)
        .join(models.PreferenceModel)
        .where(models.PreferenceModel.user_id == user_id)
        .distinct()
    )
    result_categories = await db.execute(stmt_categories)
    preference_categories = result_categories.scalars().all()

    if not preference_categories:
        # Si l'utilisateur n'a pas de préférences, on ne peut rien recommander
        return []

    # =================================================================
    # ÉTAPE 2: CRÉER LE PLAN DE RECOMMANDATION
    # =================================================================
    
    # Contrairement au profil Redis, les préférences n'ont pas de score.
    # Nous allons donc distribuer les recommandations de manière équitable.
    recommendations_plan = []
    num_categories = len(preference_categories)
    
    base_count = total_recommendations // num_categories
    remainder = total_recommendations % num_categories

    for i, category in enumerate(preference_categories):
        num_to_fetch = base_count
        if i < remainder:
            num_to_fetch += 1  # Distribuer le reste sur les premières catégories
        
        if num_to_fetch > 0:
            recommendations_plan.append((str(category.id), num_to_fetch))

    if not recommendations_plan:
        return []

    # =================================================================
    # ÉTAPE 3: LA REQUÊTE UNIQUE ET OPTIMISÉE (LOGIQUE IDENTIQUE)
    # =================================================================
    
    # --- CTE 1: Sélection des cagnottes recommandées ---
    union_queries = []
    for category_id, num_to_fetch in recommendations_plan:
        if num_to_fetch > 0:
            sq = select(models.CagnotteModel.id).where(models.CagnotteModel.id_categorie == category_id).order_by(func.random()).limit(num_to_fetch)
            union_queries.append(sq)

    if not union_queries:
        return []

    recommended_cagnottes_cte = union_all(*union_queries).cte("recommended_cagnottes")

    # --- CTE 2: Identification du post le plus récent pour chaque cagnotte ---
    Post = models.CagnottePostModel
    post_subquery = (
        select(
            Post.id,
            Post.id_cagnotte,
            func.row_number().over(
                partition_by=Post.id_cagnotte,
                order_by=Post.created_date.desc()
            ).label("rn")
        )
        .where(Post.id_cagnotte.in_(select(recommended_cagnottes_cte.c.id)))
        .subquery("ranked_posts")
    )
    latest_posts_cte = select(post_subquery.c.id, post_subquery.c.id_cagnotte).where(post_subquery.c.rn == 1).cte("latest_posts")

    # --- Requête finale: Assemblage de tout ---
    Ressource = models.RessourceModel
    final_stmt = (
        select(Post, Ressource)
        .join(latest_posts_cte, Post.id == latest_posts_cte.c.id)
        .outerjoin(Ressource, Ressource.reference == Post.id)
        .options(
            selectinload(Post.cagnotte).selectinload(models.CagnotteModel.categorie),
            selectinload(Post.cagnotte).selectinload(models.CagnotteModel.admin),
            selectinload(Post.cagnotte).selectinload(models.CagnotteModel.sponsors).selectinload(models.SponsorModel.organisation),
            selectinload(Post.author)
        )
        .order_by(latest_posts_cte.c.id_cagnotte, Ressource.order_index.asc())
    )

    result = await db.execute(final_stmt)
    
    # =================================================================
    # ÉTAPE 4: ASSEMBLAGE EN PYTHON (LOGIQUE IDENTIQUE)
    # =================================================================
    
    posts_with_resources = defaultdict(lambda: {"post": None, "resources": []})
    for post, resource in result.all():
        if not posts_with_resources[post.id]["post"]:
            posts_with_resources[post.id]["post"] = post
        if resource:
            posts_with_resources[post.id]["resources"].append(resource)

    final_response = []
    for item in posts_with_resources.values():
        feed_item = build_feed_response(item["post"], item["resources"])
        final_response.append(feed_item)
    
    # # ===================================================================
    # # ÉTAPE 4: ENRICHISSEMENT DES URLS PRÉSIGNÉES
    # # ===================================================================
    # try:
    #     enriched_feed = await enrich_feed_with_presigned_urls(final_response)
        
    #     print(f"🎬 Feed enrichi pour les préférences de l'utilisateur {user_id}: {len(enriched_feed)} éléments")        
    #     return enriched_feed
    
    # except Exception as e:
    #     print(f"❌ Erreur lors de l'enrichissement des URLs: {e}")
        
    #     # FALLBACK: Retourner les données sans URLs présignées
    #     # plutôt que de tout faire échouer
    #     print(f"⚠️ Fallback: retour des données sans URLs présignées")
        
    #     # Retourner les données brutes (sans enrichissement)
    #     # Attention: vérifier que votre modèle accepte des URLs vides/None
    #     return enriched_feed
    
    random.shuffle(final_response)
    return final_response

#===============================Preferences==========================




#===============================RECOMMANDATIONS==============================
@app.get("/recommendations/feed/v1/", response_model=List[schemas2.CagnottePostFeedResponse])
async def get_recommendations_feed(
    phone: Optional[str] = None,
    session_id: Optional[str] = None,
    total_recommendations: int = 15,
    db: AsyncSession = Depends(get_db),
    redis_client: aioredis.Redis = Depends(get_redis_client)
):
    """
    Génère des recommandations de cagnottes de manière ultra-optimisée
    avec une seule requête à la base de données.
    """
    # =================================================================
    # ÉTAPE 1: OBTENIR LE PLAN DE RECOMMANDATION (INCHANGÉ, CAR DÉJÀ TRÈS RAPIDE)
    # =================================================================
    if not phone and not session_id:
        raise HTTPException(status_code=400, detail="Un 'phone' ou un 'session_id' doit être fourni.")
    if phone and session_id:
        raise HTTPException(status_code=400, detail="Fournissez 'phone' OU 'session_id', pas les deux.")
        
    redis_key = f"profile:user:{phone}" if phone else f"profile:session:{session_id}"
    user_profile = await redis_client.hgetall(redis_key)
    
    if not user_profile:
        return []

    sorted_categories = sorted(user_profile.items(), key=lambda item: float(item[1]), reverse=True)
    recommendations_plan = []
    remaining_recommendations = total_recommendations

    # Logique de distribution dynamique (inchangée)
    # ... (le code pour remplir recommendations_plan est identique et performant)
    for i, (category_id, _) in enumerate(sorted_categories):
        if i == 0: num_to_fetch = round(total_recommendations * 0.5)
        elif i == 1: num_to_fetch = round(total_recommendations * 0.3)
        elif i == 2: num_to_fetch = round(total_recommendations * 0.15)
        else: num_to_fetch = max(1, round(remaining_recommendations / (len(sorted_categories) - i)))
        num_to_fetch = min(num_to_fetch, remaining_recommendations)
        if num_to_fetch > 0:
            recommendations_plan.append((category_id, num_to_fetch))
            remaining_recommendations -= num_to_fetch
        if remaining_recommendations <= 0: break
    if remaining_recommendations > 0 and recommendations_plan:
        top_category_id, top_count = recommendations_plan[0]
        recommendations_plan[0] = (top_category_id, top_count + remaining_recommendations)

    if not recommendations_plan:
        return []

    # =================================================================
    # ÉTAPE 2: LA REQUÊTE UNIQUE ET OPTIMISÉE (LE GRAND CHANGEMENT)
    # =================================================================
    
    # --- CTE 1: Sélection des cagnottes recommandées ---
    union_queries = []
    for category_id, num_to_fetch in recommendations_plan:
        if num_to_fetch > 0:
            sq = select(models.CagnotteModel.id).where(models.CagnotteModel.id_categorie == category_id).order_by(func.random()).limit(num_to_fetch)
            union_queries.append(sq)

    if not union_queries:
        return []

    recommended_cagnottes_cte = union_all(*union_queries).cte("recommended_cagnottes")

    # --- CTE 2: Identification du post le plus récent pour chaque cagnotte ---
    Post = models.CagnottePostModel
    post_subquery = (
        select(
            Post.id,
            Post.id_cagnotte,
            func.row_number().over(
                partition_by=Post.id_cagnotte,
                order_by=Post.created_date.desc()
            ).label("rn")
        )
        .where(Post.id_cagnotte.in_(select(recommended_cagnottes_cte.c.id)))
        .subquery("ranked_posts")
    )
    latest_posts_cte = select(post_subquery.c.id, post_subquery.c.id_cagnotte).where(post_subquery.c.rn == 1).cte("latest_posts")

    # --- Requête finale: Assemblage de tout ---
    Ressource = models.RessourceModel
    final_stmt = (
        select(Post, Ressource)
        .join(latest_posts_cte, Post.id == latest_posts_cte.c.id)
        # Utiliser `outerjoin` pour les ressources au cas où un post n'en aurait aucune
        .outerjoin(Ressource, Ressource.reference == Post.id)
        .options(
            # Charger les relations du Post en une seule fois pour éviter des requêtes N+1
            selectinload(Post.cagnotte).selectinload(models.CagnotteModel.categorie),
            selectinload(Post.cagnotte).selectinload(models.CagnotteModel.sponsors).selectinload(models.SponsorModel.organisation),
            selectinload(Post.cagnotte).selectinload(models.CagnotteModel.admin),
            selectinload(Post.author)
        )
        .order_by(latest_posts_cte.c.id_cagnotte, Ressource.order_index.asc())
    )

    result = await db.execute(final_stmt)
     
    # =================================================================
    # ÉTAPE 3: ASSEMBLAGE EN PYTHON (MAINTENANT PLUS SIMPLE ET RAPIDE)
    # =================================================================
    
    # Grouper les résultats par post
    posts_with_resources = defaultdict(lambda: {"post": None, "resources": []})
    for post, resource in result.all():
        if not posts_with_resources[post.id]["post"]:
            posts_with_resources[post.id]["post"] = post
        if resource:
            posts_with_resources[post.id]["resources"].append(resource)

    # Construire la réponse finale
    final_response = []
    for item in posts_with_resources.values():
        feed_item = build_feed_response(item["post"], item["resources"])
        final_response.append(feed_item)
    
    random.shuffle(final_response)
    return final_response

#================================RECOMMANDATIONS===============================



#=============================CAGNOTTE_MOMENT_BY_USER============================
@app.get(
    "/cagnottes/personalized/",
    response_model=List[schemas2.CagnottePostFeedResponse],
    summary="Génère des recommandations de posts personnalisées",
    description="Retourne les N meilleurs posts de la catégorie préférée de l'utilisateur."
)
async def get_personalized_recommendations(
    db: AsyncSession = Depends(get_db),
    redis_client: aioredis.Redis = Depends(get_redis_client),
    phone: Optional[str] = Query(None, description="ID de l'utilisateur authentifié"),
    session_id: Optional[str] = Header(None, description="ID de session pour les utilisateurs anonymes"),
    limit: int = Query(5, ge=1, le=20, description="Nombre de posts à recommander.")
):
    """
    Cet endpoint fournit des recommandations personnalisées basées sur le profil
    d'engagement de l'utilisateur stocké dans Redis.
    """
    # ===================================================================
    # ÉTAPE 1: IDENTIFIER L'UTILISATEUR ET CONSTRUIRE LA CLÉ REDIS
    # ===================================================================
    if not phone and not session_id:
        raise HTTPException(
            status_code=400,
            detail="Un 'phone' ou un 'session_id' doit être fourni."
        )

    redis_key = f"profile:user:{phone}" if phone else f"profile:session:{session_id}"

    # ===================================================================
    # ÉTAPE 2: RÉCUPÉRER LE PROFIL ET TROUVER LA CATÉGORIE PRÉFÉRÉE
    # ===================================================================
    try:
        user_profile_raw = await redis_client.hgetall(redis_key)
        if not user_profile_raw:
            return []

        user_profile = { k: float(v) for k, v in user_profile_raw.items() }
        
        if not user_profile:
             return []

        top_category_id = max(user_profile, key=user_profile.get)

    except redis.RedisError as e:
        # logger.error(...)
        raise HTTPException(status_code=503, detail=f"Erreur de connexion à Redis: {e}")
    except (ValueError, TypeError):
        return []

    # ===================================================================
    # ÉTAPE 3: RÉCUPÉRER LES MEILLEURS POSTS DE CETTE CATÉGORIE
    # ===================================================================
    Post = models.CagnottePostModel
    Cagnotte = models.CagnotteModel
    Ressource = models.RessourceModel

    # Définition de "meilleur post" : le plus de likes, puis le plus de vues
    stmt_posts = (
        select(Post)
        .join(Post.cagnotte)
        .options(
            selectinload(Post.cagnotte).selectinload(Cagnotte.categorie),
            selectinload(Post.author)
        )
        .where(Cagnotte.id_categorie == top_category_id)
        .order_by(Post.likes_count.desc(), Post.views_count.desc())
        .limit(limit)
    )

    result_posts = await db.execute(stmt_posts)
    top_posts = result_posts.scalars().unique().all()

    if not top_posts:
        return []

    # ===================================================================
    # ÉTAPE 4: ENRICHIR AVEC LES RESSOURCES ET FORMATER LA RÉPONSE
    # ===================================================================
    post_ids = [p.id for p in top_posts]
    
    stmt_resources = (
        select(Ressource)
        .where(Ressource.reference.in_(post_ids))
        .order_by(Ressource.order_index.asc())
    )
    result_resources = await db.execute(stmt_resources)
    resources_by_post_id = defaultdict(list)
    for res in result_resources.scalars().all():
        resources_by_post_id[res.reference].append(res)
    
    # Assemblage de la réponse finale
    final_response = []
    for post in top_posts:
        ressources = resources_by_post_id.get(post.id, [])
        feed_item = build_feed_response(post, ressources)
        final_response.append(feed_item)
        
    return final_response

#=============================CAGNOTTE_MOMENT_BY_USER============================


#=========================POPULARITER==========================
@app.get("/countries/popularity")
async def get_countries_popularity(
    redis: aioredis.Redis = Depends(get_redis_client),
    limit: Optional[int] = Query(None, ge=1, le=100, description="Nombre maximum de pays à retourner"),
    sort_by: str = Query("popularity_score", description="Critère de tri: popularity_score, unique_users, total_events"),
    order: str = Query("desc", description="Ordre: asc ou desc")
):
    """
    Récupère la popularité par pays depuis Redis
    """
    try:
        # Récupérer les données depuis Redis
        data = await redis.get("analytics_popularity_by_country")
        if not data:
            raise HTTPException(status_code=404, detail="Données de popularité par pays non trouvées")
        
        countries_data = json.loads(data)
        
        # Convertir en liste pour le tri
        countries_list = list(countries_data.values())
        
        # Tri
        reverse = order.lower() == "desc"
        try:
            countries_list.sort(key=lambda x: x.get(sort_by, 0), reverse=reverse)
        except (KeyError, TypeError):
            raise HTTPException(status_code=400, detail=f"Critère de tri '{sort_by}' invalide")
        
        # Limitation
        if limit:
            countries_list = countries_list[:limit]
        
        return {
            "status": "success",
            "data": countries_list,
            "total_countries": len(countries_data),
            "returned_countries": len(countries_list),
            "sorted_by": sort_by,
            "order": order,
            "timestamp": datetime.now().isoformat()
        }
        
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Erreur de décodage des données Redis")
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des données pays: {e}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")


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

#=========================POPULARITER==========================


#==========================TRENDING==========================
@app.get("/trending/countries")
async def get_trending_countries(
    redis: aioredis.Redis = Depends(get_redis_client),
    trend_direction: Optional[str] = Query(None, description="Filtrer par direction: up, down, stable"),
    limit: Optional[int] = Query(None, ge=1, le=100, description="Nombre maximum de pays à retourner")
):
    """
    Récupère les tendances par pays depuis Redis
    """
    try:
        data = await redis.get("analytics_trending_by_country")
        if not data:
            raise HTTPException(status_code=404, detail="Données de tendances par pays non trouvées")
        
        trending_data = json.loads(data)
        trending_list = list(trending_data.values())
        
        # Filtrage par direction de tendance
        if trend_direction:
            trending_list = [item for item in trending_list if item.get('trend_direction') == trend_direction]
        
        # Tri par trending_score
        trending_list.sort(key=lambda x: x.get('trending_score', 0), reverse=True)
        
        # Limitation
        if limit:
            trending_list = trending_list[:limit]
        
        return {
            "status": "success",
            "data": trending_list,
            "total_countries": len(trending_data),
            "returned_countries": len(trending_list),
            "filter_direction": trend_direction,
            "timestamp": datetime.now().isoformat()
        }
    
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Erreur de décodage des données Redis")
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des tendances pays: {e}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")

@app.get("/trending/categories")
async def get_trending_categories(
    redis: aioredis.Redis = Depends(get_redis_client),
    trend_direction: Optional[str] = Query(None, description="Filtrer par direction: up, down, stable"),
    limit: Optional[int] = Query(None, ge=1, le=100, description="Nombre maximum de catégories à retourner")
):
    """
    Récupère les tendances par catégorie depuis Redis
    """
    try:
        data = await redis.get("analytics_trending_by_category")
        if not data:
            raise HTTPException(status_code=404, detail="Données de tendances par catégorie non trouvées")
        
        trending_data = json.loads(data)
        trending_list = list(trending_data.values())
        
        # Filtrage par direction de tendance
        if trend_direction:
            trending_list = [item for item in trending_list if item.get('trend_direction') == trend_direction]
        
        # Tri par trending_score
        trending_list.sort(key=lambda x: x.get('trending_score', 0), reverse=True)
        
        # Limitation
        if limit:
            trending_list = trending_list[:limit]
        
        return {
            "status": "success",
            "data": trending_list,
            "total_categories": len(trending_data),
            "returned_categories": len(trending_list),
            "filter_direction": trend_direction,
            "timestamp": datetime.now().isoformat()
        }
    
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Erreur de décodage des données Redis")
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des tendances catégories: {e}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")

# trending cold start recommendations videos "up" only
@app.get("/trending/up/{country}")
async def get_cold_start_recommendations(
    country: str,
    redis: aioredis.Redis = Depends(get_redis_client),
    min_velocity_percent: Optional[float] = Query(0.0, description="Vélocité minimale requise (%)"),
    min_trending_score: Optional[float] = Query(0.0, description="Score de tendance minimal"),
    exclude_stable_trends: Optional[bool] = Query(True, description="Exclure les tendances stables"),
    max_recommendations: Optional[int] = Query(10, ge=1, le=50, description="Nombre max de recommandations")
):
    """
    Génère des recommandations cold start pour un pays donné.
    Recommande uniquement les vidéos avec une tendance croissante (velocity > 0 et direction 'up').
    """
    try:
        # Récupérer les données de tendance par pays
        data = await redis.get("analytics_trending_by_country")
        if not data:
            raise HTTPException(status_code=404, detail="Données de tendances non disponibles")
        
        trending_data = json.loads(data)
        
        if country not in trending_data:
            available_countries = list(trending_data.keys())
            raise HTTPException(
                status_code=404,
                detail=f"Pays '{country}' non trouvé. Pays disponibles: {available_countries[:10]}"
            )
        
        country_data = trending_data[country]
        trending_videos = country_data.get('trending_videos', [])
        
        if not trending_videos:
            return {
                "status": "success",
                "data": {
                    "country": country,
                    "recommended_videos": [],
                    "country_context": country_data,
                },
                "total_videos": 0,
                "filtered_videos": 0,
                "filter_criteria": {
                    "min_velocity_percent": min_velocity_percent,
                    "min_trending_score": min_trending_score,
                    "exclude_stable_trends": exclude_stable_trends
                },
                "message": f"Aucune vidéo en tendance trouvée pour le pays '{country}'",
                "timestamp": datetime.now().isoformat()
            }
        
        # Filtrer les vidéos selon les critères cold start
        filtered_videos = []
        for video in trending_videos:
            # Critère 1: Tendance croissante uniquement (vélocité positive)
            if video.get('velocity_percent', 0) <= min_velocity_percent:
                continue
            
            # Critère 2: Direction de tendance "up" uniquement
            if video.get('trend_direction') != 'up':
                continue
            
            # Critère 3: Score de tendance minimum
            if video.get('trending_score', 0) < min_trending_score:
                continue
            
            # Critère 4: Exclure les tendances stables si demandé
            if exclude_stable_trends and video.get('trend_direction') == 'stable':
                continue
            
            filtered_videos.append(video)
        
        # Trier par score de tendance décroissant et limiter
        filtered_videos.sort(key=lambda x: x.get('trending_score', 0), reverse=True)
        final_videos = filtered_videos[:max_recommendations]
        
        return {
            "status": "success",
            "data": {
                "country": country,
                "recommended_videos": final_videos,
                "country_context": {
                    "country_trending_score": country_data.get('trending_score', 0),
                    "country_velocity_percent": country_data.get('velocity_percent', 0),
                    "country_trend_direction": country_data.get('trend_direction', 'stable'),
                    "total_trending_videos": len(trending_videos),
                    "unique_users_recent": country_data.get('unique_users_recent', 0)
                }
            },
            "total_videos": len(trending_videos),
            "filtered_videos": len(final_videos),
            "filter_criteria": {
                "min_velocity_percent": min_velocity_percent,
                "min_trending_score": min_trending_score,
                "exclude_stable_trends": exclude_stable_trends,
                "max_recommendations": max_recommendations
            },
            "statistics": {
                "average_velocity": sum(v.get('velocity_percent', 0) for v in final_videos) / len(final_videos) if final_videos else 0,
                "average_trending_score": sum(v.get('trending_score', 0) for v in final_videos) / len(final_videos) if final_videos else 0
            },
            "timestamp": datetime.now().isoformat()
        }
        
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Erreur de décodage des données Redis")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erreur lors de la génération des recommandations pour {country}: {e}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")

@app.get("/trending/global")
async def get_global_trending_recommendations(
    redis: aioredis.Redis = Depends(get_redis_client),
    min_velocity_percent: Optional[float] = Query(5.0, description="Vélocité minimale requise (%)"),
    min_trending_score: Optional[float] = Query(1.0, description="Score de tendance minimal"),
    max_recommendations: Optional[int] = Query(20, ge=1, le=100, description="Nombre max de recommandations"),
    trend_direction: Optional[str] = Query("up", description="Direction de tendance: up, down, stable")
):
    """
    Récupère les vidéos les plus en tendance globalement (tous pays confondus)
    avec filtrage par direction de tendance
    """
    try:
        data = await redis.get("analytics_trending_by_country")
        if not data:
            raise HTTPException(status_code=404, detail="Données de tendances non disponibles")
        
        trending_data = json.loads(data)
        
        # Collecter toutes les vidéos en tendance de tous les pays
        all_videos = []
        video_scores = {}  # Pour déduplication
        
        for country, country_data in trending_data.items():
            trending_videos = country_data.get('trending_videos', [])
            
            for video in trending_videos:
                # Filtrer selon les critères
                if (video.get('velocity_percent', 0) >= min_velocity_percent and 
                    video.get('trending_score', 0) >= min_trending_score):
                    
                    # Filtrer par direction si spécifiée
                    if trend_direction and video.get('trend_direction') != trend_direction:
                        continue
                    
                    video_id = video['video_id']
                    # Garder le meilleur score si la vidéo apparaît dans plusieurs pays
                    if (video_id not in video_scores or 
                        video['trending_score'] > video_scores[video_id]['trending_score']):
                        video_copy = video.copy()
                        video_copy['source_country'] = country
                        video_scores[video_id] = video_copy
        
        # Trier par score de tendance et limiter
        sorted_videos = sorted(video_scores.values(), 
                             key=lambda x: x['trending_score'], reverse=True)
        final_videos = sorted_videos[:max_recommendations]
        
        return {
            "status": "success",
            "data": final_videos,
            "total_unique_videos": len(video_scores),
            "returned_videos": len(final_videos),
            "filter_criteria": {
                "min_velocity_percent": min_velocity_percent,
                "min_trending_score": min_trending_score,
                "trend_direction": trend_direction,
                "max_recommendations": max_recommendations
            },
            "statistics": {
                "countries_analyzed": len(trending_data),
                "average_velocity": sum(v.get('velocity_percent', 0) for v in final_videos) / len(final_videos) if final_videos else 0,
                "average_trending_score": sum(v.get('trending_score', 0) for v in final_videos) / len(final_videos) if final_videos else 0
            },
            "timestamp": datetime.now().isoformat()
        }
        
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Erreur de décodage des données Redis")
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des recommandations globales: {e}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")
#==========================TRENDING==========================


#=========================CATEGORIES==========================
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
@app.get("/categories/popularity/{category_id}")
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

#=========================CATEGORIES==========================


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Analytiques & Recommandations
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++