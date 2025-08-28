# ingestion_api.py
import asyncio
import logging
from typing import Any, Dict, List

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Query, status

from pydantic import BaseModel, Field
import os
import redis.asyncio as redis
import asyncpg

# --- Configuration ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_TOPIC = "user-events-topic"

REDIS_HOST = "localhost"
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_HOST = "localhost"

# --- Modèles Pydantic ---
class Cagnotte(BaseModel):
    id: str
    name: str
    description: str | None
    
    # categorie: str

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
    categorie: str
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

# --- Endpoint de Recommandation ---
@app.get("/recommendations", response_model=List[Cagnotte])
async def get_recommendations(
    user_identifier: str,
    user_type: str = Query(..., pattern="^(authenticated|anonymous)$"),
    limit: int = 5
):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database connection is not available.")

    if user_type == "authenticated":
        redis_key = f"profile:{user_identifier}"
    else: # anonymous
        redis_key = f"profile:session:{user_identifier}"
        
    top_categories = await get_top_categories_from_profile(redis_key)
    print(top_categories)

    if not top_categories:
        # Fallback : Si l'utilisateur n'a pas de profil ou que des scores négatifs
        logger.info(f"No positive profile for {redis_key}. Falling back to popular cagnottes.")
        query = "SELECT id::text, name, description FROM cagnottes WHERE statut = 'VALIDE' ORDER BY total_contributors DESC LIMIT $1"
        cagnottes_records = await fetch_cagnottes(db_pool, query, limit)
    else:
        # Recommandations personnalisées basées sur la meilleure catégorie
        best_category = top_categories[0]
        logger.info(f"Profile found for {redis_key}. Recommending from top category: '{best_category}'")
        query = "SELECT id::text, name, description FROM cagnottes WHERE categorie = $1 AND statut = 'VALIDE' ORDER BY total_contributors DESC LIMIT $2"
        cagnottes_records = await fetch_cagnottes(db_pool, query, best_category, limit)

    return [Cagnotte(**dict(record)) for record in cagnottes_records]






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



