# s3_archiver_consumer_v2.py
import asyncio
import json
import logging
from datetime import datetime
from uuid import uuid4

import s3fs
from aiokafka import AIOKafkaConsumer

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration Kafka ---
KAFKA_TOPIC = "user-events-topic"
KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_CONSUMER_GROUP = "s3-archiver-group" 

# --- Configuration S3 ---
S3_BUCKET_NAME = "deme-events"

# 1. On initialise s3fs en mode synchrone (plus simple et robuste avec to_thread)
s3 = s3fs.S3FileSystem()

# 2. La fonction d'archivage devient une fonction normale (non-async)
def archive_batch_to_s3(events: list):
    """
    Archive un lot d'événements dans un fichier unique sur S3.
    Cette fonction est synchrone et sera exécutée dans un thread séparé.
    """
    if not events:
        return

    content = "\n".join([json.dumps(e) for e in events])
    
    try:
        first_event_ts = datetime.fromisoformat(events[0]['timestamp'])
        year, month, day, hour = first_event_ts.strftime('%Y'), first_event_ts.strftime('%m'), first_event_ts.strftime('%d'), first_event_ts.strftime('%H')
    except (KeyError, ValueError):
        now = datetime.now()
        year, month, day, hour = now.strftime('%Y'), now.strftime('%m'), now.strftime('%d'), now.strftime('%H')

    now_for_filename = datetime.now()
    file_name = f"events_{now_for_filename.strftime('%Y%m%d_%H%M%S')}_{uuid4()}.jsonl"
    s3_path = f"{S3_BUCKET_NAME}/events/year={year}/month={month}/day={day}/hour={hour}/{file_name}"
    
    logger.info(f"Archiving batch of {len(events)} events to s3://{s3_path}")

    try:
        # L'écriture se fait maintenant de manière bloquante (mais dans un autre thread)
        s3.pipe(s3_path, content.encode('utf-8'))
        logger.info(f"Successfully archived batch to s3://{s3_path}")
    except Exception as e:
        logger.error(f"Failed to write to S3 bucket '{S3_BUCKET_NAME}': {e}")

async def consume_and_archive():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP,
        auto_offset_reset='earliest'
    )
    await consumer.start()
    logger.info("S3 Archiver consumer started...")
    try:
        while True:
            result = await consumer.getmany(timeout_ms=10_000, max_records=500)
            for tp, messages in result.items():
                if messages:
                    events = [json.loads(msg.value.decode('utf-8')) for msg in messages]
                    if events:
                        # 3. On appelle la fonction synchrone dans un thread séparé
                        # Cela empêche de bloquer la boucle d'événements principale du consumer.
                        await asyncio.to_thread(archive_batch_to_s3, events)
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume_and_archive())