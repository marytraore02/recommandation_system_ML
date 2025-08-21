import psycopg2
import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable, KafkaTimeoutError
import os
import time
import logging
from typing import Dict, Any, Optional
import traceback

# Configuration du logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Configuration de la base de données ---
DB_CONFIG = {
    'host': os.environ.get("DB_HOST", "localhost"),
    'database': os.environ.get("DB_NAME", "postgres"),
    'user': os.environ.get("DB_USER", "postgres"),
    'password': os.environ.get("DB_PASSWORD", "postgres"),
    'port': os.environ.get("DB_PORT", "5432")
}

def get_db_connection():
    """Établir une connexion à la base de données"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("✅ Connexion à la base de données établie")
        return conn
    except Exception as e:
        logger.error(f"❌ Erreur de connexion à la base de données: {e}")
        return None

def insert_cagnotte_analytics(conn, cagnotte_data: Dict[str, Any]):
    """Insérer les données analytiques de cagnotte"""
    try:
        with conn.cursor() as cur:
            # Adapter cette requête selon votre schéma de base
            cur.execute("""
                INSERT INTO cagnotte_analytics 
                (cagnotte_id, name, total_solde, current_solde, type, completion_rate, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (cagnotte_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    total_solde = EXCLUDED.total_solde,
                    current_solde = EXCLUDED.current_solde,
                    type = EXCLUDED.type,
                    completion_rate = EXCLUDED.completion_rate,
                    updated_at = NOW();
            """, (
                cagnotte_data.get('id'),
                cagnotte_data.get('name'),
                cagnotte_data.get('total_solde', 0),
                cagnotte_data.get('current_solde', 0),
                cagnotte_data.get('type'),
                cagnotte_data.get('completion_rate', 0)
            ))
        conn.commit()
        logger.info(f"✅ Données analytiques sauvegardées pour cagnotte {cagnotte_data.get('id')}")
    except Exception as e:
        logger.error(f"❌ Erreur lors de la sauvegarde: {e}")
        conn.rollback()

class CagnotteAnalyticsConsumer:
    def __init__(self, bootstrap_servers: str = 'localhost:29092'):
        self.bootstrap_servers = bootstrap_servers
        self.consumer = None
        self.db_connection = None
        self.message_count = 0

    def create_consumer(self, topics: list, group_id: str) -> bool:
        """Créer un consumer Kafka optimisé pour recevoir des données externes"""
        try:
            # consumer_config = {
            #     'bootstrap_servers': self.bootstrap_servers,
            #     'group_id': group_id,
            #     'auto_offset_reset': 'earliest',
            #     'enable_auto_commit': False,
            #     'max_poll_records': 100,
            #     'session_timeout_ms': 30000,
            #     'heartbeat_interval_ms': 10000,
            #     'max_poll_interval_ms': 300000,
            #     'value_deserializer': self._safe_json_deserializer,
            #     'key_deserializer': lambda x: x.decode('utf-8') if x else None,
            #     'consumer_timeout_ms': 1000,
            #     # Ajout de paramètres pour gérer les connexions
            #     'request_timeout_ms': 30000,
            #     'retry_backoff_ms': 100,
            #     'reconnect_backoff_ms': 50,
            #     'reconnect_backoff_max_ms': 1000
            # }
            self.consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=group_id,
                auto_offset_reset='earliest',  # 'earliest' pour lire depuis le début
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
                key_deserializer=lambda x: x.decode('utf-8') if x else None
            )
            
            # self.consumer = KafkaConsumer(*topics, **consumer_config)
            
            # Vérifier la connexion en testant les métadonnées du cluster
            try:
                # Méthode correcte pour vérifier la connexion
                cluster_metadata = self.consumer.list_consumer_groups()
                logger.info(f"✅ Consumer créé pour topics: {topics}")
                logger.info(f"📡 Groupe de consommation: {group_id}")
                logger.info(f"🔗 Serveurs Kafka: {self.bootstrap_servers}")
                logger.info(f"🌐 Connexion au cluster Kafka établie")
            except Exception as e:
                # Si la vérification échoue, on continue quand même
                logger.warning(f"⚠️ Impossible de vérifier les groupes de consommateurs: {e}")
                logger.info(f"✅ Consumer créé pour topics: {topics}")
                logger.info(f"📡 Groupe de consommation: {group_id}")
                logger.info(f"🔗 Serveurs Kafka: {self.bootstrap_servers}")
            
            return True
            
        except NoBrokersAvailable as e:
            logger.error(f"❌ Aucun broker Kafka disponible: {e}")
            logger.error(f"🔧 Vérifiez que Kafka est démarré sur {self.bootstrap_servers}")
            return False
        except KafkaTimeoutError as e:
            logger.error(f"❌ Timeout de connexion Kafka: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Erreur lors de la création du consumer: {e}")
            logger.error(f"📋 Traceback: {traceback.format_exc()}")
            return False

    def _safe_json_deserializer(self, data):
        """Désérializer JSON sécurisé"""
        if data is None:
            return None
        try:
            return json.loads(data.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning(f"⚠️ Impossible de décoder le message: {e}")
            return None

    def connect_to_database(self) -> bool:
        """Établir la connexion à la base de données"""
        self.db_connection = get_db_connection()
        return self.db_connection is not None

    def test_kafka_connection(self) -> bool:
        """Tester la connexion Kafka avant de démarrer"""
        try:
            # Créer un consumer temporaire pour tester
            test_consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                consumer_timeout_ms=5000,
                request_timeout_ms=10000
            )
            
            # Tenter d'obtenir les métadonnées
            topics = test_consumer.topics()
            test_consumer.close()
            
            logger.info(f"✅ Test connexion Kafka réussi - Topics disponibles: {len(topics)}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Test connexion Kafka échoué: {e}")
            return False

    def process_cagnotte_message(self, message_value: Dict[str, Any]) -> bool:
        """Traiter les messages du topic create-cagnotte"""
        try:
            if not message_value:
                logger.warning("⚠️ Message vide reçu")
                return False

            # Extraire et valider les données
            cagnotte_id = message_value.get('id')
            if not cagnotte_id:
                logger.warning("⚠️ Message sans ID de cagnotte")
                return False

            # Données extraites
            cagnotte_data = {
                'id': cagnotte_id,
                'name': message_value.get('name', 'Unknown'),
                'total_solde': float(message_value.get('totalSolde', 0)),
                'current_solde': float(message_value.get('currentSolde', 0)),
                'type': message_value.get('type', 'Unknown'),
                'completion_rate': 0
            }
            logger.info(f"📥 Cagnotte reçue: {cagnotte_data}")


            # # Calcul du taux de completion
            # if cagnotte_data['total_solde'] > 0:
            #     cagnotte_data['completion_rate'] = (
            #         cagnotte_data['current_solde'] / cagnotte_data['total_solde'] * 100
            #     )

            # logger.info(f"📊 Cagnotte reçue: {cagnotte_data['name']} ({cagnotte_data['completion_rate']:.1f}%)")

            # # Analyser les données
            # self.analyze_cagnotte_data(cagnotte_data)

            # # Sauvegarder en base si connexion disponible
            # if self.db_connection:
            #     insert_cagnotte_analytics(self.db_connection, cagnotte_data)

            return True

        except Exception as e:
            logger.error(f"❌ Erreur lors du traitement du message: {e}")
            logger.error(f"📋 Message reçu: {message_value}")
            return False

    def analyze_cagnotte_data(self, cagnotte_data: Dict[str, Any]):
        """Analyser les données de la cagnotte"""
        completion_rate = cagnotte_data.get('completion_rate', 0)
        name = cagnotte_data.get('name', 'Unknown')
        
        # Analyse basée sur le taux de completion
        if completion_rate >= 100:
            logger.info(f"🎉 OBJECTIF ATTEINT! '{name}' - {completion_rate:.1f}%")
        elif completion_rate >= 90:
            logger.info(f"🔥 PRESQUE FINI! '{name}' - {completion_rate:.1f}%")
        elif completion_rate >= 75:
            logger.info(f"📈 EN BONNE VOIE '{name}' - {completion_rate:.1f}%")
        elif completion_rate >= 50:
            logger.info(f"📊 MI-PARCOURS '{name}' - {completion_rate:.1f}%")
        elif completion_rate >= 25:
            logger.info(f"🚀 BON DÉMARRAGE '{name}' - {completion_rate:.1f}%")
        else:
            logger.info(f"🌱 NOUVELLE CAGNOTTE '{name}' - {completion_rate:.1f}%")

        # Alertes pour des cas spéciaux
        total_solde = cagnotte_data.get('total_solde', 0)
        if total_solde > 50000:  # Gros montant
            logger.info(f"💰 GROS PROJET détecté: {total_solde}€")

    def start_consuming(self):
        """Démarrer l'écoute des messages"""
        if not self.consumer:
            logger.error("❌ Consumer non initialisé")
            return

        logger.info("🎧 Démarrage de l'écoute des messages Kafka...")
        logger.info(f"📡 Connexion à {self.bootstrap_servers}")
        
        try:
            # Attendre les messages
            for message in self.consumer:
                try:
                    self.message_count += 1
                    topic = message.topic
                    partition = message.partition
                    offset = message.offset
                    key = message.key
                    value = message.value

                    logger.info(f"📨 Message #{self.message_count} reçu")
                    logger.info(f"   Topic: {topic}")
                    logger.info(f"   Partition: {partition}, Offset: {offset}")
                    logger.info(f"   Key: {key}")

                    # Traitement selon le topic
                    success = False
                    if topic == "create-cagnotte":
                        success = self.process_cagnotte_message(value)
                    else:
                        logger.warning(f"⚠️ Topic non géré: {topic}")
                        success = True  # Ne pas bloquer sur un topic inconnu

                    # Commit manuel seulement si traitement réussi
                    if success:
                        self.consumer.commit()
                        logger.debug(f"✅ Message commité: {topic}:{partition}:{offset}")
                    else:
                        logger.warning(f"⚠️ Message non commité (échec traitement)")

                except Exception as e:
                    logger.error(f"❌ Erreur lors du traitement du message: {e}")
                    logger.error(f"📋 Traceback: {traceback.format_exc()}")
                    # Continue pour ne pas bloquer le consumer
                    continue

        except KeyboardInterrupt:
            logger.info("🛑 Arrêt demandé par l'utilisateur")
        except Exception as e:
            logger.error(f"❌ Erreur fatale du consumer: {e}")
            logger.error(f"📋 Traceback: {traceback.format_exc()}")
        finally:
            self.close_consumer()

    def close_consumer(self):
        """Fermer le consumer et les connexions"""
        logger.info("🔄 Fermeture des connexions...")
        
        if self.consumer:
            self.consumer.close()
            logger.info("✅ Consumer Kafka fermé")
        
        if self.db_connection:
            self.db_connection.close()
            logger.info("✅ Connexion base de données fermée")
        
        logger.info(f"📊 Total messages traités: {self.message_count}")

def main():
    """Point d'entrée principal"""
    # Configuration
    KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"  # Port externe Docker
    TOPICS = ["create-cagnotte"]  # Topics à écouter
    GROUP_ID = "python-analytics-consumer"  # ID unique pour ce consumer

    logger.info("🚀 Démarrage du Consumer Analytics...")
    logger.info(f"📡 Serveurs Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"📋 Topics: {TOPICS}")
    logger.info(f"👥 Groupe: {GROUP_ID}")

    # Créer et démarrer le consumer
    analytics_consumer = CagnotteAnalyticsConsumer(KAFKA_BOOTSTRAP_SERVERS)

    # Test de connexion Kafka
    if not analytics_consumer.test_kafka_connection():
        logger.error("❌ Impossible de se connecter à Kafka - Vérifiez que Kafka est démarré")
        return

    # # Connexion à la base (optionnelle)
    # if analytics_consumer.connect_to_database():
    #     logger.info("✅ Base de données connectée")
    # else:
    #     logger.warning("⚠️ Pas de connexion BDD - mode logs uniquement")

    # Démarrer l'écoute
    if analytics_consumer.create_consumer(TOPICS, GROUP_ID):
        logger.info("🎧 Consumer prêt - En attente de messages...")
        analytics_consumer.start_consuming()
    else:
        logger.error("❌ Impossible de créer le consumer")

if __name__ == "__main__":
    # Attendre que Kafka soit prêt
    logger.info("⏳ Attente de Kafka (5 secondes)...")
    time.sleep(5)
    main()