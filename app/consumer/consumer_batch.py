# profile_builder_consumer.py
import asyncio
import json
import logging
import redis
from aiokafka import AIOKafkaConsumer

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_TOPIC = "user-events-topic"
KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_CONSUMER_GROUP = "profile-builder-group"
REDIS_HOST = "localhost"
REDIS_PORT = 6379

# Définition des poids pour chaque événement
EVENT_WEIGHTS = {
    "video_share": 5.0,
    "video_favorite": 4.0,
    "cagnotte_detail_view": 2.0,
    "video_view": 1.0,
    "video_replay": 0.5,
    "video_skip": -2.0  # Signal négatif
}

# --- Connexion à Redis ---
try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    logger.info("Successfully connected to Redis.")
except redis.exceptions.ConnectionError as e:
    logger.error(f"Could not connect to Redis: {e}")
    exit()

# --- Logique de Traitement ---
def update_user_profile(events: list):
    logger.info(f"--- Updating user profiles for a batch of {len(events)} events ---")
    
    with redis_client.pipeline() as pipe:
        for event in events:
            event_type = event.get("event_type")
            weight = EVENT_WEIGHTS.get(event_type)

            if weight is None: # Si l'événement n'est pas dans notre dictionnaire de poids
                continue

            category = event.get("categorie")
            if not category:
                continue

            # Déterminer si l'utilisateur est authentifié ou anonyme
            user_type = event.get("user_type")
            if user_type == "authenticated":
                user_id = event.get("user_id")
                if user_id:
                    redis_key = f"profile:{user_id}"
                else: continue
            elif user_type == "anonymous":
                session_id = event.get("session_id")
                if session_id:
                    redis_key = f"profile:session:{session_id}"
                else: continue
            else:
                continue

            # HINCRBYFLOAT met à jour le score pour une catégorie dans le profil de l'utilisateur
            # C'est atomique et très performant
            pipe.hincrbyfloat(redis_key, category, weight)
            logger.info(f"User '{redis_key}' interest in '{category}' updated by {weight}")

        pipe.execute()

# --- Logique du Consumer ---
async def consume_and_build_profiles():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP,
    )
    await consumer.start()
    logger.info("Profile builder consumer started...")
    try:
        while True:
            result = await consumer.getmany(timeout_ms=1000, max_records=200)
            for tp, messages in result.items():
                if messages:
                    events = [json.loads(msg.value.decode('utf-8')) for msg in messages]
                    update_user_profile(events)
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume_and_build_profiles())