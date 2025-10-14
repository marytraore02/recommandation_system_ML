import asyncio
import json
import logging
from typing import List, Dict, Any, Optional, Callable
from datetime import datetime
from enum import Enum
import os
from dataclasses import dataclass
from dotenv import load_dotenv
from collections import defaultdict

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

class TopicType(Enum):
    """Types de topics gérés"""
    USER_EVENTS = "user_events"
    CAGNOTTE_CREATION = "cagnotte_creation"

@dataclass
class ClickHouseConfig:
    host: str = os.getenv('CLICKHOUSE_HOST')
    port: int = int(os.getenv('CLICKHOUSE_PORT', 8123))
    username: str = os.getenv('CLICKHOUSE_USER')
    password: str = os.getenv('CLICKHOUSE_PASSWORD')
    database: str = os.getenv('CLICKHOUSE_DATABASE')

@dataclass
class TopicConfig:
    """Configuration pour un topic spécifique"""
    name: str
    topic_type: TopicType
    batch_size: int = 1000
    batch_timeout: int = 10

class AnalyticsConsumer:
    """Consumer analytique multi-topics performant"""
    
    def __init__(self, clickhouse_config: ClickHouseConfig):
        self.ch_config = clickhouse_config
        self.clickhouse_client: Client = None
        self.consumers: Dict[str, AIOKafkaConsumer] = {}
        self.topic_configs: List[TopicConfig] = []
        
        # Batches par topic
        self.batches: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.last_commit_times: Dict[str, float] = {}
        
        # Handlers de transformation par type de topic
        self.transform_handlers: Dict[TopicType, Callable] = {
            TopicType.USER_EVENTS: self._transform_user_event,
            TopicType.CAGNOTTE_CREATION: self._transform_cagnotte_creation
        }
        
        # Ordre des colonnes pour chaque table
        self.column_orders = {
            TopicType.USER_EVENTS: [
                'event_id', 'event_type', 'timestamp', 'user_type', 'cagnotte_id',
                'video_id', 'post_id', 'id_categorie', 'pays', 'user_id', 'phone',
                'session_id', 'video_duration', 'watch_duration', 'completion_rate',
                'device', 'video_width', 'video_height', 'file_size', 'mime_type',
                'thumbnail_url', 'skip_time', 'raw_data'
            ],
            TopicType.CAGNOTTE_CREATION: [
                'cagnotte_id', 'name', 'description', 'date_start', 'date_end',
                'objectif', 'statut', 'id_categorie', 'admin_id', 'type',
                'created_date', 'pays', 'total_contributors', 'is_certified',
                'commission', 'total_contributed', 'total_solde', 'current_solde'
            ]
        }
        
    def add_topic(self, topic_name: str, topic_type: TopicType,
                  batch_size: int = 1000, batch_timeout: int = 10):
        """Ajoute un topic à surveiller"""
        config = TopicConfig(
            name=topic_name,
            topic_type=topic_type,
            batch_size=batch_size,
            batch_timeout=batch_timeout
        )
        self.topic_configs.append(config)
        logger.info(f"Topic ajouté: {topic_name} (type: {topic_type.value})")
        
    async def initialize(self):
        """Initialise les connexions ClickHouse et Kafka"""
        await self._init_clickhouse()
        await self._init_all_consumers()
        
    async def _init_clickhouse(self):
        """Initialise la connexion ClickHouse et crée les tables"""
        try:
            self.clickhouse_client = get_client(
                host=self.ch_config.host,
                port=self.ch_config.port,
                username=self.ch_config.username,
                password=self.ch_config.password
            )
            
            # Créer la base de données
            self.clickhouse_client.command(
                f"CREATE DATABASE IF NOT EXISTS {self.ch_config.database}"
            )
            
            # Se connecter à la base de données
            self.clickhouse_client = get_client(
                host=self.ch_config.host,
                port=self.ch_config.port,
                username=self.ch_config.username,
                password=self.ch_config.password,
                database=self.ch_config.database
            )
            
            # Créer toutes les tables
            await self._create_all_tables()
            
            logger.info("Connexion ClickHouse établie et tables créées")
            
        except Exception as e:
            logger.error(f"Erreur lors de l'initialisation ClickHouse: {e}")
            raise
            
    async def _create_all_tables(self):
        """Crée toutes les tables analytiques"""
        await self._create_user_events_table()
        await self._create_cagnotte_analytics_table()
        
    async def _create_user_events_table(self):
        """Crée la table des événements utilisateurs"""
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
            
            -- Données JSON brutes
            raw_data String,
            
            -- Métadonnées
            inserted_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (timestamp, event_type, user_type)
        SETTINGS index_granularity = 8192
        """
        self.clickhouse_client.command(create_table_sql)
        logger.info("Table user_events créée")
        
    async def _create_cagnotte_analytics_table(self):
        """Crée la table analytique pour les cagnottes"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS cagnotte_analytics (
            cagnotte_id String,
            name String,
            description String,
            date_start DateTime64(3, 'UTC'),
            date_end DateTime64(3, 'UTC'),
            objectif UInt64,
            statut String,
            id_categorie String,
            admin_id String,
            type String,
            created_date DateTime64(3, 'UTC'),
            pays String,
            total_contributors UInt32,
            is_certified Bool,
            commission Decimal(5, 4),
            total_contributed UInt64,
            total_solde UInt64,
            current_solde UInt64,
            
            -- Métriques calculées
            days_to_end Int32,
            completion_percentage Float32,
            avg_contribution_per_user Float32,
            is_expired Bool,
            
            -- Métadonnées
            inserted_at DateTime DEFAULT now(),
            updated_at DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updated_at)
        PARTITION BY toYYYYMM(created_date)
        ORDER BY (created_date, pays, statut, cagnotte_id)
        SETTINGS index_granularity = 8192
        """
        self.clickhouse_client.command(create_table_sql)
        
        # Créer une vue matérialisée pour les statistiques agrégées
        create_mv_sql = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS cagnotte_stats_daily
        ENGINE = SummingMergeTree()
        PARTITION BY toYYYYMM(date)
        ORDER BY (date, pays, statut)
        AS SELECT
            toDate(created_date) as date,
            pays,
            statut,
            count() as total_cagnottes,
            sum(objectif) as total_objectifs,
            sum(total_contributed) as total_contributions,
            sum(total_contributors) as total_unique_contributors,
            avg(completion_percentage) as avg_completion
        FROM cagnotte_analytics
        GROUP BY date, pays, statut
        """
        
        try:
            self.clickhouse_client.command(create_mv_sql)
            logger.info("Table cagnotte_analytics et vue matérialisée créées")
        except Exception as e:
            if "already exists" not in str(e).lower():
                raise
            logger.info("Vue matérialisée déjà existante")
        
    async def _init_all_consumers(self):
        """Initialise tous les consumers Kafka"""
        bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
        auto_offset_reset = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')
        
        for config in self.topic_configs:
            try:
                group_id = f"{os.getenv('KAFKA_CONSUMER_GROUP_CLICKHOUSE')}_{config.topic_type.value}"
                
                consumer = AIOKafkaConsumer(
                    config.name,
                    bootstrap_servers=bootstrap_servers,
                    group_id=group_id,
                    auto_offset_reset=auto_offset_reset,
                    enable_auto_commit=False,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
                )
                
                await consumer.start()
                self.consumers[config.name] = consumer
                self.last_commit_times[config.name] = asyncio.get_event_loop().time()
                
                logger.info(f"Consumer démarré pour topic: {config.name}")
                
            except Exception as e:
                logger.error(f"Erreur initialisation consumer {config.name}: {e}")
                raise
    
    def _safe_get(self, data: Dict[str, Any], key: str, default: Any = None) -> Any:
        """Récupère une valeur de manière sécurisée"""
        value = data.get(key, default)
        return value if value is not None else default
    
    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """Parse un timestamp de manière robuste"""
        if not timestamp_str:
            return datetime.utcnow()
        
        try:
            if timestamp_str.endswith('Z'):
                timestamp_str = timestamp_str[:-1] + '+00:00'
            
            if '+' in timestamp_str:
                dt = datetime.fromisoformat(timestamp_str)
                return dt.replace(tzinfo=None)
            else:
                return datetime.fromisoformat(timestamp_str)
                
        except (ValueError, AttributeError) as e:
            logger.warning(f"Erreur parsing timestamp '{timestamp_str}': {e}")
            return datetime.utcnow()
            
    def _transform_user_event(self, event_data: Dict[str, Any]) -> List[Any]:
        """Transforme un événement utilisateur"""
        data = event_data.get('data', {})
        timestamp = self._parse_timestamp(event_data.get('timestamp', ''))
        
        return [
            self._safe_get(event_data, 'event_id', 0),
            self._safe_get(event_data, 'event_type', ''),
            timestamp,
            self._safe_get(event_data, 'user_type', ''),
            self._safe_get(event_data, 'cagnotte_id', ''),
            self._safe_get(event_data, 'video_id'),
            self._safe_get(event_data, 'post_id'),
            self._safe_get(event_data, 'id_categorie'),
            self._safe_get(event_data, 'pays', ''),
            self._safe_get(event_data, 'user_id'),
            self._safe_get(event_data, 'phone'),
            self._safe_get(event_data, 'session_id'),
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
            json.dumps(data) if data else '{}'
        ]
        
    def _transform_cagnotte_creation(self, cagnotte_data: Dict[str, Any]) -> List[Any]:
        """Transforme les données de création de cagnotte"""
        date_start = self._parse_timestamp(cagnotte_data.get('date_start', ''))
        date_end = self._parse_timestamp(cagnotte_data.get('date_end', ''))
        created_date = self._parse_timestamp(cagnotte_data.get('created_date', ''))
        
        # Calculs de métriques
        objectif = self._safe_get(cagnotte_data, 'objectif', 0)
        total_contributed = self._safe_get(cagnotte_data, 'total_contributed', 0)
        total_contributors = self._safe_get(cagnotte_data, 'total_contributors', 0)
        
        completion_percentage = (total_contributed / objectif * 100) if objectif > 0 else 0
        avg_contribution = (total_contributed / total_contributors) if total_contributors > 0 else 0
        
        days_to_end = (date_end - datetime.utcnow()).days
        is_expired = days_to_end < 0
        
        # Commission en Decimal
        commission_str = str(self._safe_get(cagnotte_data, 'commission', '0.00'))
        
        return [
            self._safe_get(cagnotte_data, 'id', ''),
            self._safe_get(cagnotte_data, 'name', ''),
            self._safe_get(cagnotte_data, 'description', ''),
            date_start,
            date_end,
            objectif,
            self._safe_get(cagnotte_data, 'statut', ''),
            self._safe_get(cagnotte_data, 'id_categorie', ''),
            self._safe_get(cagnotte_data, 'admin', ''),
            self._safe_get(cagnotte_data, 'type', ''),
            created_date,
            self._safe_get(cagnotte_data, 'pays', ''),
            total_contributors,
            self._safe_get(cagnotte_data, 'is_certified', False),
            float(commission_str),
            total_contributed,
            self._safe_get(cagnotte_data, 'total_solde', 0),
            self._safe_get(cagnotte_data, 'current_solde', 0),
            days_to_end,
            completion_percentage,
            avg_contribution,
            is_expired
        ]
        
    async def _insert_batch(self, topic_type: TopicType, events: List[Dict[str, Any]]):
        """Insère un lot d'événements dans la table appropriée"""
        if not events:
            return
            
        table_names = {
            TopicType.USER_EVENTS: 'user_events',
            TopicType.CAGNOTTE_CREATION: 'cagnotte_analytics'
        }
        
        try:
            rows = []
            transform_func = self.transform_handlers[topic_type]
            
            for event in events:
                try:
                    row = transform_func(event)
                    rows.append(row)
                except Exception as e:
                    logger.error(f"Erreur transformation {topic_type.value}: {e}")
                    logger.debug(f"Event: {event}")
                    continue
            
            if not rows:
                logger.warning(f"Aucune ligne valide pour {topic_type.value}")
                return
            
            table_name = table_names[topic_type]
            columns = self.column_orders[topic_type]
            
            self.clickhouse_client.insert(
                table_name,
                rows,
                column_names=columns
            )
            
            logger.info(f"✓ Inséré {len(rows)} événements dans {table_name}")
            
        except Exception as e:
            logger.error(f"Erreur insertion batch {topic_type.value}: {e}")
            if events:
                logger.debug(f"Premier event: {events[0]}")
            raise
            
    async def _process_topic(self, topic_config: TopicConfig):
        """Traite les messages d'un topic spécifique"""
        consumer = self.consumers[topic_config.name]
        topic_name = topic_config.name
        
        try:
            async for message in consumer:
                try:
                    event_data = message.value
                    
                    if not isinstance(event_data, dict):
                        logger.warning(f"Format invalide sur {topic_name}")
                        continue
                    
                    self.batches[topic_name].append(event_data)
                    
                    current_time = asyncio.get_event_loop().time()
                    batch = self.batches[topic_name]
                    last_commit = self.last_commit_times[topic_name]
                    
                    # Traitement si batch plein ou timeout
                    should_process = (
                        len(batch) >= topic_config.batch_size or
                        current_time - last_commit >= topic_config.batch_timeout
                    )
                    
                    if should_process and batch:
                        await self._insert_batch(topic_config.topic_type, batch)
                        await consumer.commit()
                        
                        logger.info(
                            f"📊 {topic_name}: traité {len(batch)} événements"
                        )
                        
                        batch.clear()
                        self.last_commit_times[topic_name] = current_time
                        
                except Exception as e:
                    logger.error(f"Erreur traitement message {topic_name}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Erreur boucle {topic_name}: {e}")
            raise
            
    async def start_processing(self):
        """Démarre le traitement de tous les topics en parallèle"""
        tasks = [
            self._process_topic(config)
            for config in self.topic_configs
        ]
        
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Erreur traitement global: {e}")
            raise
        finally:
            # Traiter les derniers batches
            for topic_name, batch in self.batches.items():
                if batch:
                    topic_config = next(
                        (c for c in self.topic_configs if c.name == topic_name),
                        None
                    )
                    if topic_config:
                        try:
                            await self._insert_batch(
                                topic_config.topic_type,
                                batch
                            )
                            consumer = self.consumers[topic_name]
                            await consumer.commit()
                        except Exception as e:
                            logger.error(f"Erreur dernier batch {topic_name}: {e}")
                
    async def cleanup(self):
        """Nettoie toutes les ressources"""
        for topic_name, consumer in self.consumers.items():
            try:
                await consumer.stop()
                logger.info(f"Consumer {topic_name} arrêté")
            except Exception as e:
                logger.error(f"Erreur arrêt consumer {topic_name}: {e}")
            
        if self.clickhouse_client:
            self.clickhouse_client.close()
            logger.info("Connexion ClickHouse fermée")

async def main():
    """Fonction principale"""
    clickhouse_config = ClickHouseConfig(
        host=os.getenv('CLICKHOUSE_HOST'),
        port=int(os.getenv('CLICKHOUSE_PORT', 8123)),
        username=os.getenv('CLICKHOUSE_USER'),
        password=os.getenv('CLICKHOUSE_PASSWORD'),
        database=os.getenv('CLICKHOUSE_DATABASE')
    )
    
    consumer = AnalyticsConsumer(clickhouse_config)
    
    # Ajouter les topics à surveiller
    consumer.add_topic(
        topic_name=os.getenv("KAFKA_TOPIC_USER_EVENT"),
        topic_type=TopicType.USER_EVENTS,
        batch_size=1000,
        batch_timeout=10
    )
    
    consumer.add_topic(
        topic_name="create-cagnotte",
        topic_type=TopicType.CAGNOTTE_CREATION,
        batch_size=500,
        batch_timeout=5
    )
    
    try:
        logger.info("🚀 Démarrage du consumer analytique multi-topics...")
        await consumer.initialize()
        await consumer.start_processing()
        
    except KeyboardInterrupt:
        logger.info("⏹️  Arrêt demandé par l'utilisateur")
    except Exception as e:
        logger.error(f"❌ Erreur fatale: {e}")
        raise
    finally:
        await consumer.cleanup()

if __name__ == "__main__":
    asyncio.run(main())