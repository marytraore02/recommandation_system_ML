# ingestion_api.py
import asyncio
import logging
from typing import List, Dict, Any, Optional

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Query, status
from datetime import datetime
# from uuid import UUID
import uuid
import random
from decimal import Decimal
from pydantic import BaseModel, Field
import os
import redis.asyncio as redis
import asyncpg
from dotenv import load_dotenv






# --- Configuration ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv() 

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_TOPIC = "user-events-topic"

REDIS_HOST = "localhost"
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_HOST = "localhost"


# Modèle pour l'objet "categorie" imbriqué
class CategorieInfo(BaseModel):
    id: uuid.UUID
    name: str

# Modèle pour l'objet "admin" imbriqué
class AdminInfo(BaseModel):
    firstName: str
    lastName: str
    phone: Optional[str] = None
    email: str
    picture: Optional[str] = None

class Cagnotte(BaseModel):
    id: uuid.UUID
    name: str
    description: Optional[str] = None
    categorie: CategorieInfo  # <-- Objet imbriqué
    pays: str
    admin: AdminInfo          # <-- Objet imbriqué
    dateStart: datetime
    dateEnd: Optional[datetime] = None
    objectif: float
    totalContributors: int
    totalSolde: float
    currentSolde: float
    type: str
    statut: str
    # commission: Optional[Decimal] = Field(None)
    # Le champ "ressources" peut être ajouté ici si nécessaire
    ressources: List = []

    class Config:
        from_attributes = True
        

app = FastAPI(
    title="Event Ingestion API",
    description="An ultra-fast API to receive event batches and push them to Kafka."
)

redis_pool = redis.from_url(f"redis://{REDIS_HOST}", decode_responses=True)
db_pool = None

@app.on_event("startup")
async def startup():
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(
            user=DB_USER, password=DB_PASSWORD, database=DB_NAME, host=DB_HOST
        )
        logger.info("Database connection pool started.")
    except Exception as e:
        logger.error(f"Could not connect to the database: {e}")

@app.on_event("shutdown")
async def shutdown():
    if db_pool:
        await db_pool.close()
    await redis_pool.close()



# --- Modèles de Données (Validation avec Pydantic) ---
# Ce modèle valide chaque événement individuel dans le lot.
class EventModel(BaseModel):
    event_id: int
    event_type: str
    timestamp: str
    user_id: str
    user_type: str
    cagnotte_id: str
    video_id: str | None = None  # Optionnel, car pas présent dans tous les events
    id_categorie: str
    pays: str
    data: Dict[str, Any]

# Ce modèle valide le lot d'événements reçu.
class EventBatchModel(BaseModel):
    events: List[EventModel] = Field(..., min_length=1)

# --- Logique Kafka ---
async def get_kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    return producer





# --- Fonctions de Logique ---
async def get_top_categories_from_profile(user_key: str) -> List[str]:
    profile = await redis_pool.hgetall(user_key)
    if not profile:
        return []
    
    # Trier les catégories par score, du plus haut au plus bas
    sorted_categories = sorted(profile.items(), key=lambda item: float(item[1]), reverse=True)
    print(sorted_categories)
    
    # Retourner les catégories avec un score positif
    return [category for category, score in sorted_categories if float(score) > 0]

async def fetch_cagnottes(pool, query, *args):
    async with pool.acquire() as connection:
        return await connection.fetch(query, *args)

# # --- Endpoint de Recommandation ---
# @app.get("/recommendations", response_model=List[Cagnotte])
# async def get_recommendations(
#     user_identifier: str,
#     user_type: str = Query(..., pattern="^(authenticated|anonymous)$"),
#     limit: int = 10
# ):
#     if not db_pool:
#         raise HTTPException(status_code=503, detail="Database connection is not available.")

#     if user_type == "authenticated":
#         redis_key = f"profile:{user_identifier}"
#     else: # anonymous
#         redis_key = f"profile:session:{user_identifier}"
        
#     top_categories = await get_top_categories_from_profile(redis_key)
#     print(top_categories)

#     if not top_categories:
#         # Fallback : Si l'utilisateur n'a pas de profil ou que des scores négatifs
#         logger.info(f"No positive profile for {redis_key}. Falling back to popular cagnottes.")
#         query = "SELECT id::text, name, description FROM cagnottes WHERE statut = 'VALIDE' ORDER BY total_contributors DESC LIMIT $1"
#         cagnottes_records = await fetch_cagnottes(db_pool, query, limit)
#     else:
#         # Recommandations personnalisées basées sur la meilleure catégorie
#         best_category = top_categories[0]
#         logger.info(f"Profile found for {redis_key}. Recommending from top category: '{best_category}'")
#         query = "SELECT * FROM cagnottes WHERE categorie = $1 ORDER BY total_contributors DESC LIMIT $2"
#         cagnottes_records = await fetch_cagnottes(db_pool, query, best_category, limit)

#     return [Cagnotte(**dict(record)) for record in cagnottes_records]



@app.get("/recommendations/two", response_model=List[Cagnotte])
async def get_recommendations(
    user_identifier: str, 
    user_type: str = Query(..., pattern="^(authenticated|anonymous)$"),
    limit: int = 10 # Le nombre total de recommandations souhaitées
):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database connection is not available.")

    # 1. Construire la clé Redis pour le profil utilisateur
    redis_key = f"profile:{user_identifier}" if user_type == "authenticated" else f"profile:session:{user_identifier}"
    
    # 2. Récupérer le profil et trier les catégories par score
    profile = await redis_pool.hgetall(redis_key)
    if not profile:
        logger.info(f"No profile for {redis_key}. Falling back to popular cagnottes.")
        query = "SELECT * FROM cagnottes WHERE statut = 'EN_COUR' ORDER BY total_contributors DESC LIMIT $1"
        records = await db_pool.fetch(query, limit)
        return [Cagnotte.model_validate(dict(r)) for r in records]

    # sorted_categories = sorted(profile.items(), key=lambda item: float(item[1]), reverse=True)
    # top_categories = [cat for cat, score in sorted_categories if float(score) > 0]

    sorted_categories = sorted(profile.items(), key=lambda item: float(item[1]), reverse=True)
    top_categories = [cat_id for cat_id, score in sorted_categories if float(score) > 0]


    if not top_categories:
        logger.info(f"No positive scores for {redis_key}. Falling back to popular cagnottes.")
        query = "SELECT * FROM cagnottes WHERE statut = 'EN_COUR' ORDER BY total_contributors DESC LIMIT $1"
        records = await db_pool.fetch(query, limit)
        return [Cagnotte.model_validate(dict(r)) for r in records]

    # 3. NOUVELLE LOGIQUE : Distribuer les recommandations sur les top catégories
    recommendations = []
    
    # Définir la distribution (ex: 50% pour la 1ère, 30% pour la 2ème, 20% pour la 3ème)
    distribution = [0.5, 0.3, 0.2]
    categories_to_query = top_categories[:len(distribution)] # Prendre au max 3 catégories

    # Préparer les requêtes en parallèle
    tasks = []
    query_template = "SELECT * FROM cagnottes WHERE id_categorie = $1 AND statut = 'EN_COUR' ORDER BY total_contributors DESC LIMIT $2"
    
    for i, category_id in enumerate(categories_to_query): # category_id est un UUID
        num_to_fetch = int(limit * distribution[i])
        if num_to_fetch > 0:
            # La requête attend un UUID, et on lui en passe un. Problème résolu !
            tasks.append(db_pool.fetch(query_template, category_id, num_to_fetch))
            logger.info(f"Fetching {num_to_fetch} cagnottes for category ID '{category_id}'")

    # Exécuter toutes les requêtes en parallèle pour une performance maximale
    results_from_db = await asyncio.gather(*tasks)
    
    # Aplatir la liste de listes de résultats
    for record_list in results_from_db: 
        recommendations.extend([Cagnotte.model_validate(dict(r)) for r in record_list])

    # 4. Assurer qu'on a le bon nombre de recommandations et mélanger
    # S'il manque des résultats (catégories avec peu de cagnottes), on comble avec les plus populaires
    if len(recommendations) < limit:
        needed = limit - len(recommendations)
        fallback_query = "SELECT * FROM cagnottes WHERE statut = 'EN_COUR' ORDER BY random() LIMIT $1"
        fallback_records = await db_pool.fetch(fallback_query, needed)
        recommendations.extend([Cagnotte.model_validate(dict(r)) for r in fallback_records])
    
    # Mélanger la liste finale pour une meilleure expérience utilisateur
    random.shuffle(recommendations)
    
    return recommendations[:limit]





@app.get("/recommendations/two2", response_model=List[Cagnotte])
async def get_recommendations(
    user_identifier: str, 
    user_type: str = Query(..., pattern="^(authenticated|anonymous)$"),
    limit: int = 10 # Le nombre total de recommandations souhaitées
):
    """
    Génère une liste de cagnottes recommandées pour un utilisateur en se basant sur son profil d'intérêt.
    Le profil est construit à partir des IDs de catégories stockés dans Redis.
    """
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database connection is not available.")

    # 1. Construire la clé Redis pour le profil utilisateur
    redis_key = f"profile:{user_identifier}" if user_type == "authenticated" else f"profile:session:{user_identifier}"
    
    # 2. Récupérer le profil et trier les catégories (par ID) par score
    profile = await redis_pool.hgetall(redis_key)
    
    # Si aucun profil n'existe, retourner les cagnottes les plus populaires comme fallback
    if not profile:
        logger.info(f"No profile for {redis_key}. Falling back to popular cagnottes.")
        query = "SELECT * FROM cagnottes WHERE statut = 'EN_COUR' ORDER BY total_contributors DESC LIMIT $1"
        records = await db_pool.fetch(query, limit)
        return [Cagnotte.model_validate(dict(r)) for r in records]

    # Trier les IDs de catégorie par score (du plus élevé au plus bas)
    sorted_category_ids = sorted(profile.items(), key=lambda item: float(item[1]), reverse=True)
    
    # Garder uniquement les catégories avec un score d'intérêt positif
    top_category_ids = [cat_id for cat_id, score in sorted_category_ids if float(score) > 0]

    # Si l'utilisateur n'a aucun intérêt positif, retourner les plus populaires
    if not top_category_ids:
        logger.info(f"No positive scores for {redis_key}. Falling back to popular cagnottes.")
        query = "SELECT * FROM cagnottes WHERE statut = 'EN_COUR' ORDER BY total_contributors DESC LIMIT $1"
        records = await db_pool.fetch(query, limit)
        return [Cagnotte.model_validate(dict(r)) for r in records]

    # 3. Distribuer les recommandations sur les top catégories
    recommendations = []
    
    # Définir la distribution (ex: 50% pour la 1ère, 30% pour la 2ème, 20% pour la 3ème)
    distribution = [0.5, 0.3, 0.2]
    # Prendre au maximum N catégories en fonction de la distribution définie
    category_ids_to_query = top_category_ids[:len(distribution)] 

    # Préparer les requêtes en parallèle
    tasks = []
    # query_template = "SELECT * FROM cagnottes WHERE id_categorie = $1 AND statut = 'EN_COUR' ORDER BY total_contributors DESC LIMIT $2"
    query_template = """
        SELECT
            c.id, c.name, c.description, c.pays, c.objectif,
            c.total_solde AS "totalSolde", c.current_solde AS "currentSolde", c.statut, c.type, c.commission,
            c.total_contributors AS "totalContributors", c.date_start AS "dateStart", c.date_end AS "dateEnd", c.ressources,
            cat.id AS "categorie_id", cat.name AS "categorie_name",
            u.first_name AS "admin_firstName", u.last_name AS "admin_lastName",
            u.phone AS "admin_phone", u.email AS "admin_email", u.picture AS "admin_picture"
        FROM cagnottes AS c
        LEFT JOIN categories AS cat ON c.id_categorie = cat.id
        LEFT JOIN users AS u ON c.admin = u.id
        WHERE c.id_categorie = $1 AND c.statut = 'EN_COUR'
        ORDER BY c.total_contributors DESC LIMIT $2
    """
    
    for i, category_id in enumerate(category_ids_to_query):
        # Le category_id est maintenant un UUID, ce qui est correct pour la requête
        num_to_fetch = int(limit * distribution[i])
        if num_to_fetch > 0:
            tasks.append(db_pool.fetch(query_template, category_id, num_to_fetch))
            logger.info(f"Fetching {num_to_fetch} cagnottes for category ID '{category_id}'")

    # Exécuter toutes les requêtes en parallèle pour une performance maximale
    results_from_db = await asyncio.gather(*tasks)

    # Aplatir et TRANSFORMER les résultats
    recommendations_flat = []
    for record_list in results_from_db:
        recommendations_flat.extend(record_list)

    # Transformer chaque record plat en objet imbriqué
    recommendations = []
    for r in recommendations_flat:
        cagnotte_data = {
            "id": r["id"],
            "name": r["name"],
            "description": r["description"],
            "pays": r["pays"],
            "objectif": r["objectif"],
            "totalSolde": r["totalSolde"],
            "currentSolde": r["currentSolde"],
            "statut": r["statut"],
            "type": r["type"],
            "commission": r["commission"],
            "totalContributors": r["totalContributors"],
            "dateStart": r["dateStart"],
            "dateEnd": r["dateEnd"],
            "categorie": {
                "id": r["categorie_id"],
                "name": r["categorie_name"]
            },
            "admin": {
                "firstName": r["admin_firstName"],
                "lastName": r["admin_lastName"],
                "phone": r["admin_phone"],
                "email": r["admin_email"],
                "picture": r["admin_picture"],
                "ressources": r["ressources"],
            }
        }
        # Valider et convertir avec Pydantic
        recommendations.append(Cagnotte.model_validate(cagnotte_data))

    # 4. Assurer qu'on a le bon nombre de recommandations et mélanger
    # S'il manque des résultats, on comble avec des cagnottes populaires au hasard
    if len(recommendations) < limit:
        needed = limit - len(recommendations)
        # Utiliser `random()` pour éviter de toujours proposer les mêmes cagnottes de fallback
        fallback_query = "SELECT * FROM cagnottes WHERE statut = 'EN_COUR' ORDER BY random() LIMIT $1"
        fallback_records = await db_pool.fetch(fallback_query, needed)
        recommendations.extend([Cagnotte.model_validate(dict(r)) for r in fallback_records])
    
    # Mélanger la liste finale pour une meilleure expérience utilisateur
    random.shuffle(recommendations)
    
    # S'assurer de ne jamais dépasser la limite demandée
    return recommendations[:limit]





# --- Point de Terminaison (Endpoint) ---
@app.post(
    "/events/collect",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Accept a batch of user events"
)
async def accept_events(batch: EventBatchModel):
    """
    Accepte un lot d'événements, les envoie à Kafka et répond immédiatement.
    C'est un endpoint "fire-and-forget".
    """
    producer = None
    try:
        producer = await get_kafka_producer()
        logger.info(f"Received a batch of {len(batch.events)} events. Sending to Kafka topic '{KAFKA_TOPIC}'.")

        # Préparation des messages pour Kafka de manière asynchrone
        tasks = []
        for event in batch.events:
            # Sérialisation de l'événement en JSON
            message = event.model_dump_json().encode("utf-8")
            tasks.append(producer.send(KAFKA_TOPIC, message))
        
        # Envoi de tous les messages en parallèle
        await asyncio.gather(*tasks)

        logger.info("Batch successfully sent to Kafka.")
        return {"status": "accepted", "message": f"{len(batch.events)} events queued."}

    except Exception as e:
        logger.error(f"Error connecting or sending to Kafka: {e}")
        # Si Kafka est indisponible, on retourne une erreur 503
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not send events to the processing queue. Please try again later."
        )
    finally:
        if producer:
            await producer.stop()



