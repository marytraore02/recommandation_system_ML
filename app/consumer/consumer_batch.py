# Les profils des utilisateurs actifs ne seront jamais supprimés, car leur TTL est constamment repoussé. En revanche, si un utilisateur ne se connecte pas pendant 90 jours, son profil sera automatiquement nettoyé, libérant de la mémoire.

# si une session est considérée comme inactive après 30 minutes, fixez le TTL à 30 minutes.

import asyncio
import json
import os
import logging
import redis
from aiokafka import AIOKafkaConsumer
from typing import List, Dict, Any
from dotenv import load_dotenv
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from utils.config import CalculationConfig
load_dotenv()

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_TOPIC_USER_EVENT = os.getenv("KAFKA_TOPIC_USER_EVENT")

# KAFKA_TOPIC_USER_EVENT = os.getenv('KAFKA_TOPIC', 'user-events'),
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_CONSUMER_GROUP_PROFIL = os.getenv("KAFKA_CONSUMER_GROUP_PROFIL")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_DB = os.getenv("REDIS_DB")


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
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    logger.info("Successfully connected to Redis.")
except redis.exceptions.ConnectionError as e:
    logger.error(f"Could not connect to Redis: {e}")
    exit()

# --- Logique de Traitement ---
def update_user_profile(events: List[Dict[str, Any]]):
    """Met à jour les profils utilisateurs dans Redis en se basant sur un lot d'événements."""
    logger.info(f"--- Updating user profiles for a batch of {len(events)} events ---")
    
    with redis_client.pipeline() as pipe:
        for event in events:
            event_type = event.get("event_type")
            weight = EVENT_WEIGHTS.get(event_type)

            if weight is None:
                continue

            category_id = event.get("id_categorie")
            if not category_id:
                continue

            user_type = event.get("user_type")
            # phone = event.get("phone")
            redis_key = None
            is_anonymous = False

            if user_type == "authenticated":
                if user_id := event.get("user_id"):
                    print(f"le user_id actuel: {user_id}")
                    redis_key = f"profile:user:{user_id}"

                    if phone := event.get("phone"):
                        print(f"le phone actuel: {phone}")
                        redis_key = f"profile:user:{phone}"

            elif user_type == "anonymous":
                if session_id := event.get("session_id"):
                    print(f"la session actuel: {session_id}")
                    redis_key = f"profile:session:{session_id}"
                    is_anonymous = True

            if redis_key:
                # 1. Mettre à jour le score de la catégorie
                pipe.hincrbyfloat(redis_key, category_id, weight)
                
                # 2. Définir ou réinitialiser le TTL
                if is_anonymous:
                    pipe.expire(redis_key, CalculationConfig.ANONYMOUS_PROFILE_TTL)
                else:
                    pipe.expire(redis_key, CalculationConfig.AUTHENTICATED_PROFILE_TTL)

            if redis_key:
                pipe.hincrbyfloat(redis_key, category_id, weight)
                # Note: logger dans une boucle aussi serrée peut ralentir. À n'utiliser qu'en debug.
                # logger.info(f"Profile '{redis_key}' category '{category_id}' updated by {weight}")

        pipe.execute()
    logger.info("--- Batch update complete ---")


# --- Logique du Consumer ---
async def consume_and_build_profiles():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC_USER_EVENT,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP_PROFIL,
        auto_offset_reset='earliest' # Commence au début si le groupe est nouveau
    )
    await consumer.start()
    logger.info("Profile builder consumer started...")
    try:
        while True:
            result = await consumer.getmany(timeout_ms=1000, max_records=1000) # Augmentation du batch size
            for tp, messages in result.items():
                if not messages:
                    continue

                valid_events = []
                for msg in messages:
                    try:
                        # Décodage et parsing robustes
                        event_data = json.loads(msg.value.decode('utf-8'))
                        valid_events.append(event_data)
                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        logger.warning(f"Could not decode message offset {msg.offset}: {e}")
                
                if valid_events:
                    update_user_profile(valid_events)

    except Exception as e:
        logger.error(f"An unexpected error occurred in consumer loop: {e}")
    finally:
        logger.info("Stopping consumer...")
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume_and_build_profiles())
