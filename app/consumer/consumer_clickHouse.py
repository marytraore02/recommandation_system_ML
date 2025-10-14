import asyncio
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import os
from dataclasses import dataclass
from dotenv import load_dotenv

from aiokafka import AIOKafkaConsumer
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client

load_dotenv()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ClickHouseConfig:
    host: str = os.getenv('CLICKHOUSE_HOST'),
    port: int = os.getenv('CLICKHOUSE_PORT'),
    username: str = os.getenv('CLICKHOUSE_USER'),
    password: str = os.getenv('CLICKHOUSE_PASSWORD'),
    database: str = os.getenv('CLICKHOUSE_DATABASE'),

@dataclass
class KafkaConfig:
    bootstrap_servers: str = os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    topic: str = os.getenv("KAFKA_TOPIC_USER_EVENT"),
    group_id: str = os.getenv('KAFKA_CONSUMER_GROUP_EVENTS'),
    auto_offset_reset: str = os.getenv('KAFKA_AUTO_OFFSET_RESET')

class EventProcessor:
    def __init__(self, clickhouse_config: ClickHouseConfig, kafka_config: KafkaConfig):
        self.ch_config = clickhouse_config
        self.kafka_config = kafka_config
        self.clickhouse_client: Client = None
        self.consumer: AIOKafkaConsumer = None
        self.batch_size = 999
        self.batch_timeout = 10  # secondes
        
        # Définir l'ordre des colonnes pour l'insertion
        self.column_order = [
            'event_id', 'event_type', 'timestamp', 'user_type', 'cagnotte_id',
            'video_id', 'post_id', 'id_categorie', 'pays', 'user_id', 'phone',
            'session_id', 'video_duration', 'watch_duration', 'completion_rate',
            'device', 'video_width', 'video_height', 'file_size', 'mime_type',
            'thumbnail_url', 'skip_time', 'raw_data'
        ]
        
    async def initialize(self):
        """Initialise les connexions ClickHouse et Kafka"""
        await self._init_clickhouse()
        await self._init_kafka()
        
    async def _init_clickhouse(self):
        """Initialise la connexion ClickHouse et crée la table"""
        try:
            self.clickhouse_client = get_client(
                host=self.ch_config.host,
                port=self.ch_config.port,
                username=self.ch_config.username,
                password=self.ch_config.password
            )
            
            # Créer la base de données si elle n'existe pas
            self.clickhouse_client.command(f"CREATE DATABASE IF NOT EXISTS {self.ch_config.database}")
            
            # Utiliser la base de données
            self.clickhouse_client = get_client(
                host=self.ch_config.host,
                port=self.ch_config.port,
                username=self.ch_config.username,
                password=self.ch_config.password,
                database=self.ch_config.database
            )
            
            # Créer la table des événements
            await self._create_events_table()
            
            logger.info("Connexion ClickHouse établie et table créée")
            
        except Exception as e:
            logger.error(f"Erreur lors de l'initialisation ClickHouse: {e}")
            raise
            
    async def _create_events_table(self):
        """Crée la table des événements dans ClickHouse"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS user_events (
            event_id UInt64,
            event_type String,
            timestamp DateTime64(3, 'UTC'),
            user_type String,
            cagnotte_id String,
            video_id Nullable(String),
            post_id Nullable(String),
            id_categorie Nullable(String),
            pays String,
            user_id Nullable(String),
            phone Nullable(String),
            session_id Nullable(String),
            
            -- Champs du sous-objet data
            video_duration Nullable(UInt32),
            watch_duration Nullable(UInt32),
            completion_rate Nullable(Float32),
            device Nullable(String),
            video_width Nullable(UInt32),
            video_height Nullable(UInt32),
            file_size Nullable(UInt64),
            mime_type Nullable(String),
            thumbnail_url Nullable(String),
            skip_time Nullable(UInt32),
            
            -- Données JSON brutes pour flexibilité
            raw_data String,
            
            -- Métadonnées
            inserted_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (timestamp, event_type, user_type)
        SETTINGS index_granularity = 8192
        """
        
        self.clickhouse_client.command(create_table_sql)
        logger.info("Table user_events créée ou vérifiée")
        
    async def _init_kafka(self):
        """Initialise le consumer Kafka"""
        try:
            self.consumer = AIOKafkaConsumer(
                self.kafka_config.topic,
                bootstrap_servers=self.kafka_config.bootstrap_servers,
                group_id=self.kafka_config.group_id,
                auto_offset_reset=self.kafka_config.auto_offset_reset,
                enable_auto_commit=False,  # Commit manuel après insertion ClickHouse
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            
            await self.consumer.start()
            logger.info(f"Consumer Kafka démarré pour le topic: {self.kafka_config.topic}")
            
        except Exception as e:
            logger.error(f"Erreur lors de l'initialisation Kafka: {e}")
            raise
    
    def _safe_get(self, data: Dict[str, Any], key: str, default_value: Any = None) -> Any:
        """Récupère une valeur de manière sécurisée avec une valeur par défaut"""
        value = data.get(key, default_value)
        return value if value is not None else default_value
    
    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """Parse le timestamp de manière robuste"""
        if not timestamp_str:
            return datetime.utcnow()
        
        try:
            # Gestion de différents formats de timestamp
            if timestamp_str.endswith('Z'):
                timestamp_str = timestamp_str[:-1] + '+00:00'
            
            # Essayer le format ISO avec timezone
            if '+' in timestamp_str or timestamp_str.endswith('Z'):
                return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            else:
                # Assume UTC si pas de timezone
                dt = datetime.fromisoformat(timestamp_str)
                return dt.replace(tzinfo=None)  # ClickHouse DateTime64 sans timezone
                
        except ValueError as e:
            logger.warning(f"Erreur parsing timestamp '{timestamp_str}': {e}, utilisation de now()")
            return datetime.utcnow()
            
    def _transform_event(self, event_data: Dict[str, Any]) -> List[Any]:
        """Transforme un événement pour l'insertion dans ClickHouse"""
        try:
            data = event_data.get('data', {})
            
            # Conversion du timestamp
            timestamp = self._parse_timestamp(event_data.get('timestamp', ''))
            
            # Création de la ligne avec toutes les colonnes dans l'ordre
            row = [
                # Champs principaux
                self._safe_get(event_data, 'event_id', 0),
                self._safe_get(event_data, 'event_type', ''),
                timestamp,
                self._safe_get(event_data, 'user_type', ''),
                self._safe_get(event_data, 'cagnotte_id', ''),
                
                # Champs optionnels
                self._safe_get(event_data, 'video_id'),
                self._safe_get(event_data, 'post_id'),
                self._safe_get(event_data, 'id_categorie'),
                self._safe_get(event_data, 'pays', ''),
                self._safe_get(event_data, 'user_id'),
                self._safe_get(event_data, 'phone'),
                self._safe_get(event_data, 'session_id'),
                
                # Champs du sous-objet data
                self._safe_get(data, 'video_duration'),
                self._safe_get(data, 'watch_duration'),
                self._safe_get(data, 'completion_rate'),
                self._safe_get(data, 'device'),
                self._safe_get(data, 'video_width'),
                self._safe_get(data, 'video_height'),
                self._safe_get(data, 'file_size'),
                self._safe_get(data, 'mime_type'),
                self._safe_get(data, 'thumbnail_url'),
                self._safe_get(data, 'skip_time'),
                
                # Données JSON brutes
                json.dumps(data) if data else '{}'
            ]
            
            return row
            
        except Exception as e:
            logger.error(f"Erreur lors de la transformation de l'événement: {e}")
            logger.error(f"Données d'événement: {event_data}")
            raise
        
    async def _insert_batch(self, events: List[Dict[str, Any]]):
        """Insère un lot d'événements dans ClickHouse"""
        if not events:
            return
            
        try:
            # Transformation des événements en lignes
            rows = []
            for event in events:
                try:
                    row = self._transform_event(event)
                    rows.append(row)
                except Exception as e:
                    logger.error(f"Erreur transformation événement: {e}")
                    logger.error(f"Événement problématique: {event}")
                    continue  # Skip cet événement et continue avec les autres
            
            if not rows:
                logger.warning("Aucune ligne valide à insérer après transformation")
                return
            
            # Insertion avec colonnes explicites (sans inserted_at qui a une valeur par défaut)
            insert_columns = [col for col in self.column_order]
            
            self.clickhouse_client.insert(
                'user_events',
                rows,
                column_names=insert_columns
            )
            
            logger.info(f"Inséré {len(rows)} événements dans ClickHouse")
            
        except Exception as e:
            logger.error(f"Erreur lors de l'insertion batch: {e}")
            logger.error(f"Nombre d'événements: {len(events)}")
            logger.error(f"Nombre de lignes transformées: {len(rows) if 'rows' in locals() else 0}")
            
            # Log du premier événement pour debug
            if events:
                logger.error(f"Premier événement: {events[0]}")
            
            raise
            
    async def process_events(self):
        """Traite les événements depuis Kafka"""
        batch = []
        last_commit_time = asyncio.get_event_loop().time()
        
        try:
            async for message in self.consumer:
                try:
                    event_data = message.value
                    
                    # Validation basique des données
                    if not isinstance(event_data, dict):
                        logger.warning(f"Événement ignoré - format invalide: {type(event_data)}")
                        continue
                    
                    # Vérifier les champs obligatoires
                    required_fields = ['event_id', 'event_type', 'timestamp']
                    if not all(field in event_data for field in required_fields):
                        logger.warning(f"Événement ignoré - champs manquants: {event_data}")
                        continue
                    
                    batch.append(event_data)
                    
                    current_time = asyncio.get_event_loop().time()
                    
                    # Traitement du batch si conditions remplies
                    if (len(batch) >= self.batch_size or 
                        current_time - last_commit_time >= self.batch_timeout):
                        
                        if batch:
                            await self._insert_batch(batch)
                            await self.consumer.commit()
                            
                            logger.info(f"Traité un batch de {len(batch)} événements")
                            batch.clear()
                            last_commit_time = current_time
                            
                except json.JSONDecodeError as e:
                    logger.error(f"Erreur JSON: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Erreur lors du traitement du message: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Erreur dans la boucle de traitement: {e}")
            raise
        finally:
            # Traitement du dernier batch
            if batch:
                try:
                    await self._insert_batch(batch)
                    await self.consumer.commit()
                except Exception as e:
                    logger.error(f"Erreur lors du traitement du dernier batch: {e}")
                
    async def cleanup(self):
        """Nettoie les ressources"""
        if self.consumer:
            await self.consumer.stop()
            logger.info("Consumer Kafka arrêté")
            
        if self.clickhouse_client:
            self.clickhouse_client.close()
            logger.info("Connexion ClickHouse fermée")

async def main():
    """Fonction principale"""
    # Configuration depuis les variables d'environnement
    clickhouse_config = ClickHouseConfig(
        host=os.getenv('CLICKHOUSE_HOST'),
        port=(os.getenv('CLICKHOUSE_PORT')),
        username=os.getenv('CLICKHOUSE_USER'),
        password=os.getenv('CLICKHOUSE_PASSWORD'),
        database=os.getenv('CLICKHOUSE_DATABASE')
    )
    
    kafka_config = KafkaConfig(
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
        topic=os.getenv("KAFKA_TOPIC_USER_EVENT"),
        group_id=os.getenv('KAFKA_CONSUMER_GROUP_EVENTS'),
        auto_offset_reset=os.getenv('KAFKA_AUTO_OFFSET_RESET')
    )
    
    processor = EventProcessor(clickhouse_config, kafka_config)
    
    try:
        logger.info("Démarrage du consumer Kafka vers ClickHouse...")
        await processor.initialize()
        await processor.process_events()
        
    except KeyboardInterrupt:
        logger.info("Arrêt demandé par l'utilisateur")
    except Exception as e:
        logger.error(f"Erreur fatale: {e}")
    finally:
        await processor.cleanup()

if __name__ == "__main__":
    asyncio.run(main())