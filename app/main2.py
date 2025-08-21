# app/main.py
from fastapi import FastAPI, HTTPException, BackgroundTasks
from contextlib import asynccontextmanager
from typing import List, Dict, Any
import asyncio
import threading
import time
import logging
import json
import os
import traceback

# Imports pour Kafka et PostgreSQL
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable, KafkaTimeoutError

# Imports de votre application existante
import crud
import models
from recommender import engine

# Configuration du logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Configuration globale ---
KAFKA_CONFIG = {
    'bootstrap_servers': os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
    # 'topics': ["create-cagnotte", "update-cagnotte"],  # Ajoutez vos topics ici
    'topics' : [
            "create-cagnotte",
            "update-cagnotte",
            "create-contribution",
            "create-user"
        ],
    'group_id': "fastapi-analytics-consumer"
}

DB_CONFIG = {
    'host': os.environ.get("DB_HOST", "localhost"),
    'database': os.environ.get("DB_NAME", "postgres"),
    'user': os.environ.get("DB_USER", "postgres"),
    'password': os.environ.get("DB_PASSWORD", "postgres"),
    'port': os.environ.get("DB_PORT", "5432")
}

# Variable globale pour contrôler le consumer
consumer_instance = None
consumer_thread = None
running = False

class CagnotteAnalyticsConsumer:
    def __init__(self):
        self.consumer = None
        self.db_connection = None
        self.message_count = 0
        self.running = False

    def get_db_connection(self):
        """Établir une connexion à la base de données"""
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            logger.info("✅ Connexion à la base de données établie")
            return conn
        except Exception as e:
            logger.error(f"❌ Erreur de connexion à la base de données: {e}")
            return None

    def create_consumer(self) -> bool:
        """Créer un consumer Kafka"""
        try:
            self.consumer = KafkaConsumer(
                *KAFKA_CONFIG['topics'],
                bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
                group_id=KAFKA_CONFIG['group_id'],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
                key_deserializer=lambda x: x.decode('utf-8') if x else None,
                consumer_timeout_ms=1000  # Timeout pour permettre l'arrêt propre
            )
            
            logger.info(f"✅ Consumer créé pour topics: {KAFKA_CONFIG['topics']}")
            logger.info(f"📡 Groupe de consommation: {KAFKA_CONFIG['group_id']}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la création du consumer: {e}")
            return False

    def insert_cagnotte_analytics(self, cagnotte_data: Dict[str, Any]):
        """Insérer les données analytiques de cagnotte, création de la table si nécessaire"""
        if not self.db_connection:
            self.db_connection = self.get_db_connection()
            if not self.db_connection:
                return False

        try:
            with self.db_connection.cursor() as cur:
                # Vérifier/créer la table si elle n'existe pas
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS cagnotte_analytics (
                        cagnotte_id VARCHAR PRIMARY KEY,
                        name VARCHAR,
                        total_solde FLOAT,
                        current_solde FLOAT,
                        type VARCHAR,
                        completion_rate FLOAT,
                        created_at TIMESTAMP,
                        updated_at TIMESTAMP
                    );
                """)
                # Insérer ou mettre à jour la cagnotte
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
            self.db_connection.commit()
            logger.info(f"✅ Données sauvegardées pour cagnotte {cagnotte_data.get('id')}")
            return True
        except Exception as e:
            logger.error(f"❌ Erreur lors de la sauvegarde: {e}")
            self.db_connection.rollback()
            self.db_connection = None
            return False

    def process_cagnotte_message(self, topic: str, message_value: Dict[str, Any]) -> bool:
        """Traiter les messages selon le topic"""
        try:
            if not message_value:
                logger.warning("⚠️ Message vide reçu")
                return False

            # Extraire l'ID de la cagnotte
            cagnotte_id = message_value.get('id')
            if not cagnotte_id:
                logger.warning("⚠️ Message sans ID de cagnotte")
                return False

            # Traitement selon le topic
            if topic == "create-cagnotte":
                return self.handle_create_cagnotte(message_value)
            elif topic == "update-cagnotte":
                return self.handle_update_cagnotte(message_value)
            else:
                logger.warning(f"⚠️ Topic non géré: {topic}")
                return True

        except Exception as e:
            logger.error(f"❌ Erreur lors du traitement du message: {e}")
            return False

    def handle_create_cagnotte(self, message_value: Dict[str, Any]) -> bool:
        """Traiter la création d'une cagnotte"""
        try:
            cagnotte_data = {
                'id': message_value.get('id'),
                'name': message_value.get('name', 'Unknown'),
                'total_solde': float(message_value.get('totalSolde', 0)),
                'current_solde': float(message_value.get('currentSolde', 0)),
                'type': message_value.get('type', 'Unknown'),
                'completion_rate': 0
            }
            logger.info(f"📥 Message reçue du topic: {message_value}")
            logger.info(f"📥 Cagnotte analytique: {cagnotte_data}")

            # # Calcul du taux de completion
            # if cagnotte_data['total_solde'] > 0:
            #     cagnotte_data['completion_rate'] = (
            #         cagnotte_data['current_solde'] / cagnotte_data['total_solde'] * 100
            #     )

            # logger.info(f"📊 Nouvelle cagnotte: {cagnotte_data['name']} ({cagnotte_data['completion_rate']:.1f}%)")

            # # Sauvegarder en base
            # return self.insert_cagnotte_analytics(cagnotte_data)

        except Exception as e:
            logger.error(f"❌ Erreur dans handle_create_cagnotte: {e}")
            return False

    def handle_update_cagnotte(self, message_value: Dict[str, Any]) -> bool:
        """Traiter la mise à jour d'une cagnotte"""
        try:
            # Logique similaire mais pour les mises à jour
            logger.info(f"📝 Mise à jour cagnotte: {message_value.get('id')}")
            return self.handle_create_cagnotte(message_value)  # Même logique pour l'instant
        except Exception as e:
            logger.error(f"❌ Erreur dans handle_update_cagnotte: {e}")
            return False

    def start_consuming(self):
        """Démarrer l'écoute des messages dans un thread séparé"""
        if not self.consumer:
            logger.error("❌ Consumer non initialisé")
            return

        logger.info("🎧 Démarrage de l'écoute Kafka en arrière-plan...")
        self.running = True
        
        # Établir la connexion DB
        self.db_connection = self.get_db_connection()

        try:
            while self.running:
                try:
                    # Poll avec timeout pour permettre l'arrêt
                    message_batch = self.consumer.poll(timeout_ms=1000)
                    
                    if not message_batch:
                        continue

                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            if not self.running:
                                break
                                
                            self.message_count += 1
                            topic = message.topic
                            
                            logger.info(f"📨 Message #{self.message_count} reçu du topic {topic}")
                            
                            # Traitement du message
                            success = self.process_cagnotte_message(topic, message.value)
                            
                            if success:
                                logger.debug(f"✅ Message traité avec succès")
                            else:
                                logger.warning(f"⚠️ Échec du traitement du message")

                except Exception as e:
                    logger.error(f"❌ Erreur dans la boucle de consommation: {e}")
                    time.sleep(1)  # Pause avant de continuer

        except Exception as e:
            logger.error(f"❌ Erreur fatale du consumer: {e}")
        finally:
            self.close_consumer()

    def stop_consuming(self):
        """Arrêter l'écoute"""
        logger.info("🛑 Arrêt du consumer demandé...")
        self.running = False

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

def start_kafka_consumer():
    """Fonction pour démarrer le consumer dans un thread"""
    global consumer_instance, running
    
    consumer_instance = CagnotteAnalyticsConsumer()
    
    # Attendre que Kafka soit prêt
    logger.info("⏳ Attente de Kafka...")
    time.sleep(5)
    
    if consumer_instance.create_consumer():
        running = True
        consumer_instance.start_consuming()
    else:
        logger.error("❌ Impossible de créer le consumer")

def stop_kafka_consumer():
    """Fonction pour arrêter le consumer"""
    global consumer_instance, consumer_thread, running
    
    if consumer_instance:
        consumer_instance.stop_consuming()
        running = False
    
    if consumer_thread and consumer_thread.is_alive():
        consumer_thread.join(timeout=10)
        logger.info("✅ Thread consumer arrêté")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gestionnaire du cycle de vie de l'application"""
    global consumer_thread
    
    # Démarrage
    logger.info("🚀 Démarrage de l'application FastAPI...")
    
    # Démarrer le consumer Kafka dans un thread séparé
    consumer_thread = threading.Thread(target=start_kafka_consumer, daemon=True)
    consumer_thread.start()
    logger.info("🎧 Consumer Kafka démarré en arrière-plan")
    
    yield
    
    # Arrêt
    logger.info("🛑 Arrêt de l'application...")
    stop_kafka_consumer()

# Création de l'application FastAPI avec le gestionnaire de cycle de vie
app = FastAPI(
    title="API de Recommandation pour les Cagnottes",
    description="Une API pour recommander des cagnottes en fonction du profil utilisateur ou de la popularité.",
    lifespan=lifespan
)

@app.get("/")
def read_root():
    return {"message": "Bienvenue sur l'API de recommandation de cagnottes !"}

@app.get("/health")
def health_check():
    """Point de contrôle de santé incluant le statut du consumer"""
    global consumer_instance, running
    
    kafka_status = "running" if running and consumer_instance else "stopped"
    message_count = consumer_instance.message_count if consumer_instance else 0
    
    return {
        "status": "healthy",
        "kafka_consumer": kafka_status,
        "messages_processed": message_count,
        "topics": KAFKA_CONFIG['topics']
    }

@app.get(
    "/recommendations/{user_id}",
    response_model=List[models.RecommandationResult],
    summary="Obtenir des recommandations pour un utilisateur"
)
def get_recommendations(user_id: str, limit: int = 5):
    """
    Retourne une liste de cagnottes recommandées pour un utilisateur donné.
         
    - **user_id**: L'ID de l'utilisateur.
    - **limit**: Le nombre de recommandations à retourner.
    """
         
    # Étape 1: Récupérer le catalogue de cagnottes
    cagnottes = crud.get_cagnottes_from_db()
    if not cagnottes:
        raise HTTPException(status_code=503, detail="Service de base de données indisponible.")

    # Étape 2: Récupérer le profil de l'utilisateur
    user_profile = crud.get_user_profile_from_db(user_id)
         
    # Étape 3: Exécuter la logique de recommandation
    if user_profile:
        # Recommandation par préférences si un profil existe
        recommended_cagnottes = engine.recommander_videos_par_preferences(user_profile, cagnottes, limit)
    else:
        # Stratégie de repli par popularité si aucun profil n'existe
        recommended_cagnottes = engine.recommander_videos_par_popularite(cagnottes, limit)
             
    # Étape 4: Formater la réponse en utilisant le modèle Pydantic
    results = []
    for cagnotte in recommended_cagnottes:
        results.append(models.RecommandationResult(
            id=cagnotte['id'],
            titre=cagnotte['titre'],
            description=cagnotte['description'],
            categorie=cagnotte['categorie'],
            score_reco=cagnotte.get('score_calculé', 0)
        ))
             
    return results

@app.get("/consumer/stats")
def get_consumer_stats():
    """Statistiques du consumer Kafka"""
    global consumer_instance
    
    if not consumer_instance:
        return {"error": "Consumer non initialisé"}
    
    return {
        "running": running,
        "messages_processed": consumer_instance.message_count,
        "topics": KAFKA_CONFIG['topics'],
        "group_id": KAFKA_CONFIG['group_id'],
        "bootstrap_servers": KAFKA_CONFIG['bootstrap_servers']
    }

# Point d'entrée pour le développement
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )