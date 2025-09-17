import asyncio
import json
import logging
from datetime import datetime
from uuid import uuid4
from io import BytesIO

from aiokafka import AIOKafkaConsumer
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import os


load_dotenv()

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration Kafka ---
KAFKA_TOPIC_USER_EVENT = os.getenv("KAFKA_TOPIC_USER_EVENT")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_CONSUMER_GROUP_MINIO = os.getenv("KAFKA_CONSUMER_GROUP_MINIO")

# --- Configuration MinIO ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
MINIO_SECURE = os.getenv("MINIO_SECURE") == 'True' 

# 1. Initialisation du client MinIO
# Ce client sera utilisé pour interagir avec votre instance MinIO.
try:
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )
    logger.info("Successfully connected to MinIO.")
except Exception as e:
    logger.error(f"Failed to connect to MinIO: {e}")
    exit(1)


# 2. La fonction d'archivage est adaptée pour utiliser le client MinIO
def archive_batch_to_minio(events: list):
    """
    Archive un lot d'événements dans un fichier unique sur MinIO.
    Cette fonction est synchrone et sera exécutée dans un thread séparé.
    """
    if not events:
        return

    # Convertit la liste d'événements en une seule chaîne de caractères, format JSONL
    content = "\n".join([json.dumps(e) for e in events])
    content_bytes = content.encode('utf-8')
    content_stream = BytesIO(content_bytes)
    
    # Détermine le chemin de l'objet basé sur la date du premier événement
    try:
        first_event_ts = datetime.fromisoformat(events[0]['timestamp'])
        year, month, day, hour = first_event_ts.strftime('%Y'), first_event_ts.strftime('%m'), first_event_ts.strftime('%d'), first_event_ts.strftime('%H')
    except (KeyError, ValueError):
        now = datetime.now()
        year, month, day, hour = now.strftime('%Y'), now.strftime('%m'), now.strftime('%d'), now.strftime('%H')

    # Crée un nom de fichier unique
    now_for_filename = datetime.now()
    file_name = f"events_{now_for_filename.strftime('%Y%m%d_%H%M%S')}_{uuid4()}.jsonl"
    
    # Le chemin complet dans MinIO (appelé "object name")
    object_name = f"events/year={year}/month={month}/day={day}/hour={hour}/{file_name}"
    
    logger.info(f"Archiving batch of {len(events)} events to minio://{MINIO_BUCKET_NAME}/{object_name}")

    try:
        # 3. Utilise put_object pour téléverser les données vers MinIO
        # C'est l'équivalent de `s3.pipe` mais pour le client MinIO.
        minio_client.put_object(
            bucket_name=MINIO_BUCKET_NAME,
            object_name=object_name,
            data=content_stream,
            length=len(content_bytes),
            content_type='application/jsonl'
        )
        logger.info(f"Successfully archived batch to minio://{MINIO_BUCKET_NAME}/{object_name}")
    except S3Error as e:
        logger.error(f"Failed to write to MinIO bucket '{MINIO_BUCKET_NAME}': {e}")

async def consume_and_archive():
    # Vérifie si le bucket existe et le crée si nécessaire.
    try:
        found = minio_client.bucket_exists(MINIO_BUCKET_NAME)
        if not found:
            minio_client.make_bucket(MINIO_BUCKET_NAME)
            logger.info(f"Bucket '{MINIO_BUCKET_NAME}' created on MinIO.")
        else:
            logger.info(f"Bucket '{MINIO_BUCKET_NAME}' already exists on MinIO.")
    except S3Error as e:
        logger.error(f"Error checking or creating bucket on MinIO: {e}")
        return # Arrête le script si le bucket n'est pas accessible

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC_USER_EVENT,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP_MINIO,
        auto_offset_reset='earliest'
    )
    await consumer.start()
    logger.info("MinIO Archiver consumer started...")
    try:
        while True:
            # Récupère un lot de messages depuis Kafka
            result = await consumer.getmany(timeout_ms=10_000, max_records=500)
            for tp, messages in result.items():
                if messages:
                    events = [json.loads(msg.value.decode('utf-8')) for msg in messages]
                    if events:
                        # 4. Appelle la nouvelle fonction d'archivage dans un thread
                        # pour ne pas bloquer la boucle d'événements asynchrone.
                        await asyncio.to_thread(archive_batch_to_minio, events)
    finally:
        await consumer.stop()
        logger.info("MinIO Archiver consumer stopped.")

if __name__ == "__main__":
    asyncio.run(consume_and_archive())
