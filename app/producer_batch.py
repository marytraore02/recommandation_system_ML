import asyncio
import logging
from dotenv import load_dotenv
import os
from typing import List, Dict, Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, Session
from sqlalchemy.orm import selectinload
import uuid as uuid_pkg
from sqlalchemy import select, func, union_all

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Query, status, Depends
from contextlib import asynccontextmanager
from datetime import datetime
# from uuid import UUID
import random
from decimal import Decimal
from pydantic import BaseModel, Field


import redis.asyncio as redis
from db.redis_client import get_redis_client, redis_connection_pool
from db.postgres_config import get_db, AsyncSession, engine, create_db_and_tables
import asyncpg
import models
import schemas
from models import RessourceModel, CagnottePostModel, CagnotteModel, UserModel, CategorieModel
from schemas import RessourceEnrichie, CagnottePost, Ressource, Author, Cagnotte, Categorie


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
# OPTIMISATION 2: Remplacer les requêtes en boucle par une seule requête SQL
# ===================================================================
@app.get("/recommendations/", response_model=List[schemas.Cagnotte])
async def get_recommendations(
    user_id: Optional[str] = None,
    session_id: Optional[str] = None,
    total_recommendations: int = 10,
    db: AsyncSession = Depends(get_db),
    redis_client: redis.Redis = Depends(get_redis_client)
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
        recommendations_plan = [(top_cat_1.decode('utf-8'), num_cat_1), (top_cat_2.decode('utf-8'), num_cat_2)]
    elif len(sorted_categories) == 1:
        top_cat_1, _ = sorted_categories[0]
        recommendations_plan = [(top_cat_1.decode('utf-8'), total_recommendations)]
    
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
            select(CagnottePostModel)
            .options(
                # Charger l'auteur du post
                joinedload(CagnottePostModel.author), 
                joinedload(CagnottePostModel.cagnotte).joinedload(CagnotteModel.admin),
                joinedload(CagnottePostModel.cagnotte).joinedload(CagnotteModel.categorie)
            )
            .where(CagnottePostModel.id == ressource.reference)
        )
        
        result_post = await db.execute(query)
        post = result_post.scalars().first()
        
        if post:
            # Assurez-vous d'utiliser `from_orm` ou `model_validate`
            # pour transformer l'objet SQLAlchemy en Pydantic
            cagnotte_posts = CagnottePost.from_orm(post)
    
    # 3. Construire la réponse enrichie
    enriched_data = RessourceEnrichie(
        # Utilisez le dictionnaire de l'objet Ressource pour les données de base
        **ressource.__dict__,
        # Assignez le post enrichi à la propriété correcte
        cagnotte_posts=cagnotte_posts
    )

    return enriched_data




    
    

# @app.get("/recommendations/two2", response_model=List[schemas.Cagnotte])
# async def get_recommendations(
#     user_identifier: str, 
#     user_type: str = Query(..., pattern="^(authenticated|anonymous)$"),
#     limit: int = 10 # Le nombre total de recommandations souhaitées
# ):
#     """
#     Génère une liste de cagnottes recommandées pour un utilisateur en se basant sur son profil d'intérêt.
#     Le profil est construit à partir des IDs de catégories stockés dans Redis.
#     """
#     if not db_pool:
#         raise HTTPException(status_code=503, detail="Database connection is not available.")

#     # 1. Construire la clé Redis pour le profil utilisateur
#     redis_key = f"profile:{user_identifier}" if user_type == "authenticated" else f"profile:session:{user_identifier}"
    
#     # 2. Récupérer le profil et trier les catégories (par ID) par score
#     profile = await redis_pool.hgetall(redis_key)
    
#     # Si aucun profil n'existe, retourner les cagnottes les plus populaires comme fallback
#     if not profile:
#         logger.info(f"No profile for {redis_key}. Falling back to popular cagnottes.")
#         query = "SELECT * FROM cagnottes WHERE statut = 'VALIDE' ORDER BY total_contributors DESC LIMIT $1"
#         records = await db_pool.fetch(query, limit)
#         return [schemas.Cagnotte.model_validate(dict(r)) for r in records]

#     # Trier les IDs de catégorie par score (du plus élevé au plus bas)
#     sorted_category_ids = sorted(profile.items(), key=lambda item: float(item[1]), reverse=True)
    
#     # Garder uniquement les catégories avec un score d'intérêt positif
#     top_category_ids = [cat_id for cat_id, score in sorted_category_ids if float(score) > 0]

#     # Si l'utilisateur n'a aucun intérêt positif, retourner les plus populaires
#     if not top_category_ids:
#         logger.info(f"No positive scores for {redis_key}. Falling back to popular cagnottes.")
#         query = "SELECT * FROM cagnottes WHERE statut = 'VALIDE' ORDER BY total_contributors DESC LIMIT $1"
#         records = await db_pool.fetch(query, limit)
#         return [schemas.Cagnotte.model_validate(dict(r)) for r in records]

#     # 3. Distribuer les recommandations sur les top catégories
#     recommendations = []
    
#     # Définir la distribution (ex: 50% pour la 1ère, 30% pour la 2ème, 20% pour la 3ème)
#     distribution = [0.5, 0.3, 0.2]
#     # Prendre au maximum N catégories en fonction de la distribution définie
#     category_ids_to_query = top_category_ids[:len(distribution)] 

#     # Préparer les requêtes en parallèle
#     tasks = []
#     # query_template = "SELECT * FROM cagnottes WHERE id_categorie = $1 AND statut = 'VALIDE' ORDER BY total_contributors DESC LIMIT $2"
#     query_template = """
#         SELECT
#             c.id, c.name, c.description, c.pays, c.objectif,
#             c.total_solde AS "totalSolde", c.current_solde AS "currentSolde", c.statut, c.type, c.commission,
#             c.total_contributors AS "totalContributors", c.date_start AS "dateStart", c.date_end AS "dateEnd", c.ressources,
#             cat.id AS "categorie_id", cat.name AS "categorie_name",
#             u.first_name AS "admin_firstName", u.last_name AS "admin_lastName",
#             u.phone AS "admin_phone", u.email AS "admin_email", u.picture AS "admin_picture"
#         FROM cagnottes AS c
#         LEFT JOIN categories AS cat ON c.id_categorie = cat.id
#         LEFT JOIN users AS u ON c.admin = u.id
#         WHERE c.id_categorie = $1 AND c.statut = 'VALIDE'
#         ORDER BY c.total_contributors DESC LIMIT $2
#     """
    
#     for i, category_id in enumerate(category_ids_to_query):
#         # Le category_id est maintenant un UUID, ce qui est correct pour la requête
#         num_to_fetch = int(limit * distribution[i])
#         if num_to_fetch > 0:
#             tasks.append(db_pool.fetch(query_template, category_id, num_to_fetch))
#             logger.info(f"Fetching {num_to_fetch} cagnottes for category ID '{category_id}'")

#     # Exécuter toutes les requêtes en parallèle pour une performance maximale
#     results_from_db = await asyncio.gather(*tasks)

#     # Aplatir et TRANSFORMER les résultats
#     recommendations_flat = []
#     for record_list in results_from_db:
#         recommendations_flat.extend(record_list)

#     # Transformer chaque record plat en objet imbriqué
#     recommendations = []
#     for r in recommendations_flat:
#         cagnotte_data = {
#             "id": r["id"],
#             "name": r["name"],
#             "description": r["description"],
#             "pays": r["pays"],
#             "objectif": r["objectif"],
#             "totalSolde": r["totalSolde"],
#             "currentSolde": r["currentSolde"],
#             "statut": r["statut"],
#             "type": r["type"],
#             "commission": r["commission"],
#             "totalContributors": r["totalContributors"],
#             "dateStart": r["dateStart"],
#             "dateEnd": r["dateEnd"],
#             "categorie": {
#                 "id": r["categorie_id"],
#                 "name": r["categorie_name"]
#             },
#             "admin": {
#                 "firstName": r["admin_firstName"],
#                 "lastName": r["admin_lastName"],
#                 "phone": r["admin_phone"],
#                 "email": r["admin_email"],
#                 "picture": r["admin_picture"],
#                 "ressources": r["ressources"],
#             }
#         }
#         # Valider et convertir avec Pydantic
#         recommendations.append(Cagnotte.model_validate(cagnotte_data))

#     # 4. Assurer qu'on a le bon nombre de recommandations et mélanger
#     # S'il manque des résultats, on comble avec des cagnottes populaires au hasard
#     if len(recommendations) < limit:
#         needed = limit - len(recommendations)
#         # Utiliser `random()` pour éviter de toujours proposer les mêmes cagnottes de fallback
#         fallback_query = "SELECT * FROM cagnottes WHERE statut = 'VALIDE' ORDER BY random() LIMIT $1"
#         fallback_records = await db_pool.fetch(fallback_query, needed)
#         recommendations.extend([Cagnotte.model_validate(dict(r)) for r in fallback_records])
    
#     # Mélanger la liste finale pour une meilleure expérience utilisateur
#     random.shuffle(recommendations)
    
#     # S'assurer de ne jamais dépasser la limite demandée
#     return recommendations[:limit]



