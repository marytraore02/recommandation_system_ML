import asyncio
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
from sqlalchemy.dialects.postgresql import UUID # ou autre selon votre DB
# from sqlalchemy.sql.window import Window
# from sqlalchemy.sql import window


from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Query, status, Depends, APIRouter
from contextlib import asynccontextmanager
from datetime import datetime
# from uuid import UUID
import random
from decimal import Decimal
from pydantic import BaseModel, Field


import redis.asyncio as redis
import redis.asyncio as aioredis
from db.redis_client import get_redis_client, redis_connection_pool
from db.postgres_config import get_db, AsyncSession, engine, create_db_and_tables
import asyncpg
import models
import schemas
from models import RessourceModel, CagnottePostModel, CagnotteModel, UserModel, CategorieModel
from schemas import RessourceEnrichie, Ressource, Author, Cagnotte, Categorie


# --- Configuration ---
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


#===============================Endpoints==========================

@app.post(
    "/events/collect",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Accept a batch of user events"
)
async def accept_events(
    batch: schemas.EventBatchModel,
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


# ===================================================================
# OPTIMISATION 2: Refonte de l'endpoint de recommandations pour une seule requête SQL
# ===================================================================
@app.get("/recommendations/", response_model=List[schemas.Cagnotte])
async def get_recommendations(
    user_id: Optional[str] = None,
    session_id: Optional[str] = None,
    total_recommendations: int = 10,
    db: AsyncSession = Depends(get_db),
    redis_client: aioredis.Redis = Depends(get_redis_client)
):
    """
    Génère une liste de cagnottes recommandées pour un utilisateur
    en se basant sur son profil d'intérêt stocké dans Redis.
    Cette version utilise une seule requête SQL pour une performance optimale.
    """
    if not user_id and not session_id:
        raise HTTPException(status_code=400, detail="Un 'user_id' ou un 'session_id' doit être fourni.")
    if user_id and session_id:
        raise HTTPException(status_code=400, detail="Fournissez 'user_id' OU 'session_id', pas les deux.")
        
    redis_key = f"profile:user:{user_id}" if user_id else f"profile:session:{session_id}"

    user_profile = await redis_client.hgetall(redis_key)
    if not user_profile:
        return []

    sorted_categories = sorted(
        user_profile.items(), key=lambda item: float(item[1]), reverse=True
    )

    # Création du plan de recommandation
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

    # Construction d'une seule requête UNION pour récupérer N cagnottes aléatoires par catégorie
    # C'est beaucoup plus efficace que de faire une requête par catégorie.
    union_queries = []
    for category_id, num_to_fetch in recommendations_plan:
        if num_to_fetch > 0:
            sq = (
                select(models.CagnotteModel)
                .where(models.CagnotteModel.id_categorie == category_id)
                .order_by(func.random())
                .limit(num_to_fetch)
            )
            union_queries.append(sq)

    if not union_queries:
        return []   

    # Combinaison de toutes les sous-requêtes
    full_query = union_all(*union_queries).alias("all_cagnottes")
    
    # Requête finale pour charger les objets complets avec leurs relations
    final_stmt = (
        select(models.CagnotteModel)
        .options(
            selectinload(models.CagnotteModel.categorie),
            selectinload(models.CagnotteModel.admin)
        )
        .join(full_query, models.CagnotteModel.id == full_query.c.id)
    )

    result = await db.execute(final_stmt)
    final_recommendations = result.scalars().all()
    
    # Mélanger les résultats finaux pour que les catégories ne soient pas groupées
    random.shuffle(final_recommendations)
    
    return final_recommendations



@app.get("/recommendations/two", response_model=List[schemas.CagnotteRecommandee])
async def get_recommendations(
    user_id: Optional[str] = None,
    session_id: Optional[str] = None,
    total_recommendations: int = 10,
    db: AsyncSession = Depends(get_db),
    redis_client: aioredis.Redis = Depends(get_redis_client)
):
    """
    Génère une liste de cagnottes recommandées pour un utilisateur
    en se basant sur son profil d'intérêt stocké dans Redis.
    Cette version utilise une seule requête SQL pour une performance optimale.
    """
    if not user_id and not session_id:
        raise HTTPException(status_code=400, detail="Un 'user_id' ou un 'session_id' doit être fourni.")
    if user_id and session_id:
        raise HTTPException(status_code=400, detail="Fournissez 'user_id' OU 'session_id', pas les deux.")
        
    redis_key = f"profile:user:{user_id}" if user_id else f"profile:session:{session_id}"

    user_profile = await redis_client.hgetall(redis_key)
    if not user_profile:
        return []

    sorted_categories = sorted(
        user_profile.items(), key=lambda item: float(item[1]), reverse=True
    )

    # Création du plan de recommandation
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

    # Construction d'une seule requête UNION pour récupérer N cagnottes aléatoires par catégorie
    union_queries = []
    for category_id, num_to_fetch in recommendations_plan:
        if num_to_fetch > 0:
            sq = (
                select(models.CagnotteModel.id) # On a seulement besoin de l'ID ici
                .where(
                    models.CagnotteModel.id_categorie == category_id,
                    models.CagnotteModel.deleted == False,
                    # models.CagnotteModel.statut == 'VALIDE'
                )
                .order_by(func.random())
                .limit(num_to_fetch)
            )
            union_queries.append(sq)

    if not union_queries:
        return []

    # Alias pour la sous-requête des cagnottes recommandées
    recommended_cagnottes_sq = union_all(*union_queries).alias("recommended_cagnottes")

    # 2. Définition de la CTE pour trouver la ressource média principale de CHAQUE cagnotte
    # On cherche la ressource du "main post" avec le plus petit order_index
    ranked_resources_cte = (
        select(
            models.RessourceModel,
            models.CagnottePostModel.id_cagnotte,
            # On classe les ressources pour chaque cagnotte
            func.row_number().over(
                partition_by=models.CagnottePostModel.id_cagnotte,
                order_by=models.RessourceModel.order_index.asc()
            ).label("rn") # rn = row_number
        )
        .join(models.CagnottePostModel, models.RessourceModel.reference == models.CagnottePostModel.id)
        .where(models.CagnottePostModel.is_main_post == True) # On ne veut que les ressources du post principal
        .cte("ranked_resources")
    )


    # 3. Requête finale combinant tout
    # On sélectionne les cagnottes recommandées ET on fait une jointure (LEFT JOIN)
    # avec notre CTE pour récupérer le média principal (s'il existe).
    final_stmt = (
        select(
            models.CagnotteModel,
            # ranked_resources_cte # On sélectionne aussi les colonnes de la ressource
            models.RessourceModel
        )
        # Jointure pour filtrer uniquement les cagnottes recommandées
        .join(
            recommended_cagnottes_sq,
            models.CagnotteModel.id == recommended_cagnottes_sq.c.id
        )
        # Jointure GAUCHE pour récupérer le média (il peut ne pas y en avoir)
        .join(
            ranked_resources_cte,
            (models.CagnotteModel.id == ranked_resources_cte.c.id_cagnotte) &
            (ranked_resources_cte.c.rn == 1), # On prend seulement la 1ère ressource classée
            isouter=True # isouter=True fait un LEFT JOIN
        )
        # Chargement eager des relations pour éviter les requêtes N+1
        .options(
            selectinload(models.CagnotteModel.categorie),
            selectinload(models.CagnotteModel.admin)
        )
    )

    result = await db.execute(final_stmt)
    # Le résultat est maintenant une liste de tuples : (CagnotteModel, RessourceModel | None)
    recommendations_with_media = result.all()

    # Mélanger les résultats
    random.shuffle(recommendations_with_media)

    # 4. Formater la réponse pour qu'elle corresponde au schéma Pydantic
    final_recommendations = []
    for cagnotte, ressource, in recommendations_with_media:
        cagnotte_reco = schemas.CagnotteRecommandee.from_orm(cagnotte)
        if ressource:
            cagnotte_reco.main_media = schemas.Ressource.from_orm(ressource)
        final_recommendations.append(cagnotte_reco)

    return final_recommendations


# ===================================================================
# OPTIMISATION 3: Ajouter la pagination à l'endpoint de ressources
# ===================================================================
@app.get("/ressources/", response_model=List[schemas.Ressource], tags=["Ressources"])
async def get_all_ressources(
    skip: int = Query(0, ge=0, description="Nombre d'éléments à sauter"),
    limit: int = Query(100, ge=1, le=500, description="Nombre maximum d'éléments à retourner"),
    db: AsyncSession = Depends(get_db)
):
    """
    Récupère une liste paginée de ressources.
    C'est la pratique recommandée pour éviter de surcharger le service.
    """
    query = select(models.RessourceModel).offset(skip).limit(limit)
    result = await db.execute(query)
    ressources = result.scalars().all()
    return ressources



@app.get("/ressources/{ressource_id}/enriched", response_model=RessourceEnrichie, tags=["Ressources"])
async def get_enriched_ressource(ressource_id: int, db: AsyncSession = Depends(get_db)):
    """
    Récupère une ressource et l'enrichit avec les détails du post,
    incluant les informations sur l'auteur et la cagnotte.
    """
    # 1. Récupérer la ressource
    result_res = await db.execute(select(RessourceModel).where(RessourceModel.id == ressource_id))
    ressource = result_res.scalars().first()

    if not ressource:
        raise HTTPException(status_code=404, detail="Ressource non trouvée")

    cagnotte_posts = None
    if ressource.reference:
        # 2. Construire la requête pour le post avec les jointures complètes
        # Chaînage des joinedload pour charger les relations imbriquées
        query = (
            select(CagnottePostModelModel)
            .options(
                # Charger l'auteur du post
                joinedload(CagnottePostModelModel.author), 
                joinedload(CagnottePostModelModel.cagnotte).joinedload(CagnotteModel.admin),
                joinedload(CagnottePostModelModel.cagnotte).joinedload(CagnotteModel.categorie)
            )
            .where(CagnottePostModelModel.id == ressource.reference)
        )
        
        result_post = await db.execute(query)
        post = result_post.scalars().first()
        
        if post:
            # Assurez-vous d'utiliser `from_orm` ou `model_validate`
            # pour transformer l'objet SQLAlchemy en Pydantic
            cagnotte_posts = CagnottePostModel.from_orm(post)
    
    # 3. Construire la réponse enrichie
    enriched_data = RessourceEnrichie(
        # Utilisez le dictionnaire de l'objet Ressource pour les données de base
        **ressource.__dict__,
        # Assignez le post enrichi à la propriété correcte
        cagnotte_posts=cagnotte_posts
    )

    return enriched_data


# ===================================================================
# Analytics endpoints
# ===================================================================
# Liste des pays par popularité avec tri et pagination
@app.get("/analytics/countries/popularity")
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

#Détails pour un pays spécifique
@app.get("/analytics/countries/popularity/{country}")
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

#Liste des catégories par popularité
@app.get("/analytics/categories/popularity")
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
        
        # Tri par trending_score (décroissant par défaut)
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

@app.get("/user-engagement")
async def get_user_engagement_analytics(
    redis: aioredis.Redis = Depends(get_redis_client),
    segment: Optional[str] = Query(None, description="Segment d'utilisateurs: high_engagement, medium_engagement, low_engagement")
):
    """
    Récupère les analytics d'engagement utilisateur depuis Redis
    """
    try:
        data = await redis.get("analytics_user_engagement")
        if not data:
            raise HTTPException(status_code=404, detail="Données d'engagement utilisateur non trouvées")
        
        engagement_data = json.loads(data)
        
        # Si un segment spécifique est demandé
        if segment and segment in engagement_data.get('user_segments', {}):
            return {
                "status": "success",
                "data": {
                    "segment": segment,
                    "users": engagement_data['user_segments'][segment]
                },
                "timestamp": datetime.now().isoformat()
            }
        
        return {
            "status": "success",
            "data": engagement_data,
            "timestamp": datetime.now().isoformat()
        }
        
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Erreur de décodage des données Redis")
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des analytics d'engagement: {e}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")

@app.get("/content-performance")
async def get_content_performance(
    redis: aioredis.Redis = Depends(get_redis_client),
    limit: Optional[int] = Query(None, ge=1, le=100, description="Nombre maximum de vidéos à retourner"),
    sort_by: str = Query("quality_score", description="Critère de tri: quality_score, total_views, avg_completion_rate"),
    min_views: Optional[int] = Query(None, ge=0, description="Nombre minimum de vues")
):
    """
    Récupère les analytics de performance de contenu depuis Redis
    """
    try:
        data = await redis.get("analytics_content_performance")
        if not data:
            raise HTTPException(status_code=404, detail="Données de performance de contenu non trouvées")
        
        content_data = json.loads(data)
        content_list = list(content_data.values())
        
        # Filtrage par nombre minimum de vues
        if min_views is not None:
            content_list = [item for item in content_list if item.get('total_views', 0) >= min_views]
        
        # Tri
        try:
            content_list.sort(key=lambda x: x.get(sort_by, 0), reverse=True)
        except (KeyError, TypeError):
            raise HTTPException(status_code=400, detail=f"Critère de tri '{sort_by}' invalide")
        
        # Limitation
        if limit:
            content_list = content_list[:limit]
        
        return {
            "status": "success",
            "data": content_list,
            "total_videos": len(content_data),
            "returned_videos": len(content_list),
            "sorted_by": sort_by,
            "min_views_filter": min_views,
            "timestamp": datetime.now().isoformat()
        }
        
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Erreur de décodage des données Redis")
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des performances de contenu: {e}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")

@app.get("/content-performance/{video_id}")
async def get_video_performance(
    video_id: str,
    redis: aioredis.Redis = Depends(get_redis_client)
):
    """
    Récupère les détails de performance pour une vidéo spécifique
    """
    try:
        data = await redis.get("analytics_content_performance")
        if not data:
            raise HTTPException(status_code=404, detail="Données de performance non trouvées")
        
        content_data = json.loads(data)
        video_data = content_data.get(video_id)
        if not video_data:
            raise HTTPException(status_code=404, detail=f"Vidéo '{video_id}' non trouvée")
        
        return {
            "status": "success",
            "data": video_data,
            "timestamp": datetime.now().isoformat()
        }
        
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Erreur de décodage des données Redis")
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des données pour la vidéo {video_id}: {e}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")

#Résumé de toutes les analytics disponibles
@app.get("/summary")
async def get_analytics_summary(
    redis: aioredis.Redis = Depends(get_redis_client)
):
    """
    Récupère un résumé de toutes les analytics disponibles
    """
    try:
        # Récupérer les métadonnées
        metadata = await redis.get("comprehensive_analytics_metadata")
        if metadata:
            metadata = json.loads(metadata)
        
        summary = {
            "status": "success",
            "metadata": metadata,
            "available_analytics": [],
            "timestamp": datetime.now().isoformat()
        }
        
        # Vérifier la disponibilité de chaque type d'analytics
        analytics_keys = [
            ("popularity_by_country", "Popularité par pays"),
            ("popularity_by_category", "Popularité par catégorie"),
            ("trending_by_country", "Tendances par pays"),
            ("trending_by_category", "Tendances par catégorie"),
            ("user_engagement", "Analytics d'engagement"),
            ("content_performance", "Performance de contenu")
        ]
        
        for key, description in analytics_keys:
            redis_key = f"analytics_{key}"
            exists = await redis.exists(redis_key)
            if exists:
                ttl = await redis.ttl(redis_key)
                summary["available_analytics"].append({
                    "type": key,
                    "description": description,
                    "redis_key": redis_key,
                    "available": True,
                    "ttl_seconds": ttl if ttl > 0 else None
                })
            else:
                summary["available_analytics"].append({
                    "type": key,
                    "description": description,
                    "redis_key": redis_key,
                    "available": False,
                    "ttl_seconds": None
                })
        
        return summary
        
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Erreur de décodage des métadonnées")
    except Exception as e:
        logger.error(f"Erreur lors de la récupération du résumé: {e}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")
