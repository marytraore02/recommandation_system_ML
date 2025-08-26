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
import app.app.models as models
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
    'topics': ["create-cagnotte", "update-cagnotte", "create-user"],  # Ajout du topic create-user
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
                # auto_offset_reset='latest',
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

    def create_table_from_data(self, table_name: str, data: Dict[str, Any]):
        """Créer une table dynamiquement basée sur la structure des données"""
        if not self.db_connection:
            self.db_connection = self.get_db_connection()
            if not self.db_connection:
                return False

        try:
            with self.db_connection.cursor() as cur:
                # Vérifier si la table existe
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    );
                """, (table_name,))
                
                table_exists = cur.fetchone()[0]
                
                if not table_exists:
                    # Créer la table complète
                    columns = []
                    for key, value in data.items():
                        # Renommer 'id' en 'entity_id' pour éviter le conflit
                        column_name = "entity_id" if key == "id" else key
                        # column_name = key
                        
                        if isinstance(value, int):
                            col_type = "INTEGER"
                        elif isinstance(value, str):
                            col_type = "varchar(200)"
                        elif isinstance(value, float):
                            col_type = "DECIMAL(15,2)"
                        elif isinstance(value, bool):
                            col_type = "boolean"
                        elif isinstance(value, dict) or isinstance(value, list):
                            col_type = "JSONB"
                        else:
                            col_type = "TEXT"
                        
                        columns.append(f'"{column_name}" {col_type}')
                    
                    # Ajouter les colonnes de timestamp
                    columns.extend([
                        "created_at TIMESTAMP DEFAULT NOW()",
                        "updated_at TIMESTAMP DEFAULT NOW()"
                    ])
                    
                    create_sql = f"""
                        CREATE TABLE "{table_name}" (
                            id SERIAL PRIMARY KEY,
                            {', '.join(columns)}
                        );
                    """
                    
                    cur.execute(create_sql)
                    logger.info(f"✅ Table '{table_name}' créée avec {len(columns)} colonnes")
                
                else:
                    # La table existe, vérifier et ajouter les colonnes manquantes
                    logger.debug(f"📋 Table '{table_name}' existe déjà - vérification des colonnes")
                    self._ensure_columns_exist(cur, table_name, data)
                
            self.db_connection.commit()
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la création/mise à jour de table {table_name}: {e}")
            logger.error(f"📋 Traceback: {traceback.format_exc()}")
            self.db_connection.rollback()
            return False

    def _ensure_columns_exist(self, cursor, table_name: str, data: Dict[str, Any]):
        """S'assurer que toutes les colonnes nécessaires existent dans la table"""
        # Récupérer les colonnes existantes
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' AND table_name = %s;
        """, (table_name,))
        
        existing_columns = {row[0] for row in cursor.fetchall()}
        logger.debug(f"🔍 Colonnes existantes dans '{table_name}': {existing_columns}")
        
        # Vérifier chaque colonne nécessaire
        columns_added = 0
        for key, value in data.items():
            # Renommer 'id' en 'entity_id' pour éviter le conflit
            column_name = "entity_id" if key == "id" else key
            
            if column_name not in existing_columns:
                # Déterminer le type de colonne
                if isinstance(value, int):
                    col_type = "INTEGER"
                elif isinstance(value, float):
                    col_type = "DECIMAL(15,2)"
                elif isinstance(value, bool):
                    col_type = "BOOLEAN"
                elif isinstance(value, dict) or isinstance(value, list):
                    col_type = "JSONB"
                else:
                    col_type = "TEXT"
                
                # Ajouter la colonne manquante
                alter_sql = f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {col_type};'
                cursor.execute(alter_sql)
                columns_added += 1
                logger.info(f"➕ Colonne '{column_name}' ({col_type}) ajoutée à la table '{table_name}'")
        
        # S'assurer que updated_at existe
        if 'updated_at' not in existing_columns:
            cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "updated_at" TIMESTAMP DEFAULT NOW();')
            columns_added += 1
            logger.info(f"➕ Colonne 'updated_at' ajoutée à la table '{table_name}'")
        
        if columns_added > 0:
            logger.info(f"✅ {columns_added} colonnes ajoutées à la table '{table_name}'")
        else:
            logger.debug(f"✅ Toutes les colonnes nécessaires existent dans '{table_name}'")

    def insert_dynamic_data(self, table_name: str, data: Dict[str, Any]):
        """Insérer des données dans une table créée dynamiquement"""
        if not self.db_connection:
            self.db_connection = self.get_db_connection()
            if not self.db_connection:
                return False

        try:
            # Créer la table si elle n'existe pas
            if not self.create_table_from_data(table_name, data):
                return False

            with self.db_connection.cursor() as cur:
                # Préparer les données pour l'insertion
                columns = []
                values = []
                placeholders = []
                
                for key, value in data.items():
                    # Renommer 'id' en 'entity_id' pour éviter le conflit
                    # column_name = "entity_id" if key == "id" else key
                    columns.append(f'"{key}"')
                    
                    if isinstance(value, (dict, list)):
                        values.append(json.dumps(value))
                    else:
                        values.append(value)
                    placeholders.append("%s")
                
                # Ajouter updated_at
                columns.append("updated_at")
                placeholders.append("NOW()")
                
                # Construction de la requête d'insertion simple
                insert_sql = f"""
                    INSERT INTO "{table_name}" ({', '.join(columns)}) 
                    VALUES ({', '.join(placeholders)});
                """
                
                logger.debug(f"🔧 SQL généré: {insert_sql}")
                logger.debug(f"🔧 Valeurs: {values}")
                
                cur.execute(insert_sql, values)
                
            self.db_connection.commit()
            logger.info(f"✅ Données insérées dans la table '{table_name}' (entity_id: {data.get('id', 'N/A')})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'insertion dans {table_name}: {e}")
            # logger.error(f"📋 Données reçues: {data}")
            # logger.error(f"📋 Colonnes préparées: {columns if 'columns' in locals() else 'Non définies'}")
            # logger.error(f"📋 Valeurs préparées: {values if 'values' in locals() else 'Non définies'}")
            logger.error(f"📋 SQL: {insert_sql if 'insert_sql' in locals() else 'Non généré'}")
            logger.error(f"📋 Traceback complet: {traceback.format_exc()}")
            
            # Rollback et reconnexion
            self.db_connection.rollback()
            self.db_connection = None
            return False
    
    def insert_cagnotte_analytics(self, cagnotte_data: Dict[str, Any]):
        """Insérer les données analytiques de cagnotte (méthode héritée)"""
        if not self.db_connection:
            self.db_connection = self.get_db_connection()
            if not self.db_connection:
                return False

        try:
            with self.db_connection.cursor() as cur:
                # Adaptez cette requête selon votre schéma de base
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS cagnotte_analytics (
                        cagnotte_id VARCHAR(255) PRIMARY KEY,
                        name VARCHAR(255),
                        total_solde DECIMAL(10,2),
                        current_solde DECIMAL(10,2),
                        type VARCHAR(100),
                        completion_rate DECIMAL(5,2),
                        created_at TIMESTAMP DEFAULT NOW(),
                        updated_at TIMESTAMP DEFAULT NOW()
                    );
                """)
                
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
            # Reconnexion en cas d'erreur
            self.db_connection = None
            return False

    def process_cagnotte_message(self, topic: str, message_value: Dict[str, Any]) -> bool:
        """Traiter les messages selon le topic"""
        try:
            if not message_value:
                logger.warning("⚠️ Message vide reçu")
                return False

            # Traitement selon le topic
            if topic == "create-cagnotte":
                return self.handle_create_cagnotte(message_value)
            elif topic == "update-cagnotte":
                return self.handle_update_cagnotte(message_value)
            elif topic == "create-user":
                return self.handle_create_user(message_value)
            else:
                logger.warning(f"⚠️ Topic non géré: {topic}")
                return True

        except Exception as e:
            logger.error(f"❌ Erreur lors du traitement du message: {e}")
            return False

    def handle_create_cagnotte(self, message_value: Dict[str, Any]) -> bool:
        """Traiter la création d'une cagnotte"""
        try:
            logger.info(f"📥 Message reçue du topic: {message_value}")
            
            # 1. Enregistrement dans la table analytique (logique métier)
            cagnotte_data = {
                'id': message_value.get('id'),
                'name': message_value.get('name', 'Unknown'),
                'total_solde': float(message_value.get('totalSolde', 0)),
                'current_solde': float(message_value.get('currentSolde', 0)),
                'type': message_value.get('type', 'Unknown'),
                'completion_rate': 0
            }

            # Calcul du taux de completion
            if cagnotte_data['total_solde'] > 0:
                cagnotte_data['completion_rate'] = (
                    cagnotte_data['current_solde'] / cagnotte_data['total_solde'] * 100
                )

            logger.info(f"📥 Cagnotte analytique: {cagnotte_data}")
            logger.info(f"📊 Nouvelle cagnotte: {cagnotte_data['name']} ({cagnotte_data['completion_rate']:.1f}%)")

            # Sauvegarder dans la table analytique
            analytics_success = self.insert_cagnotte_analytics(cagnotte_data)
            
            # 2. Enregistrement brut des données originales
            raw_success = self.insert_dynamic_data("raw_create_cagnotte", message_value)
            
            return analytics_success and raw_success

        except Exception as e:
            logger.error(f"❌ Erreur dans handle_create_cagnotte: {e}")
            logger.error(f"📋 Traceback: {traceback.format_exc()}")
            return False

    def handle_update_cagnotte(self, message_value: Dict[str, Any]) -> bool:
        """Traiter la mise à jour d'une cagnotte"""
        try:
            logger.info(f"📝 Mise à jour cagnotte: {message_value.get('id')}")
            
            # Enregistrer les données brutes de mise à jour
            raw_success = self.insert_dynamic_data("raw_update_cagnotte", message_value)
            
            # Traiter aussi comme une création pour les analytics (même logique)
            analytics_success = self.handle_create_cagnotte(message_value)
            
            return raw_success and analytics_success
            
        except Exception as e:
            logger.error(f"❌ Erreur dans handle_update_cagnotte: {e}")
            return False

    def handle_create_user(self, message_value: Dict[str, Any]) -> bool:
        """Traiter la création d'un utilisateur"""
        try:
            user_id = message_value.get('id') or message_value.get('user_id')
            # logger.info(f"👤 Nouvel utilisateur: {user_id}")
            logger.info(f"📥 Données utilisateur reçues: {message_value}")
            
            # Enregistrer toutes les données utilisateur telles qu'elles arrivent
            # Utiliser 'users' comme nom de table si c'est ce que vous préférez
            success = self.insert_dynamic_data("users", message_value)
            
            if success:
                logger.info(f"✅ Utilisateur {user_id} enregistré avec succès dans la table 'users'")
            else:
                logger.error(f"❌ Échec de l'enregistrement de l'utilisateur {user_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Erreur dans handle_create_user: {e}")
            logger.error(f"📋 Traceback: {traceback.format_exc()}")
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

@app.get("/admin/tables")
def list_database_tables():
    """Lister toutes les tables de la base de données pour debug"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name, column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                ORDER BY table_name, ordinal_position;
            """)
            
            results = cur.fetchall()
            
            tables = {}
            for table_name, column_name, data_type in results:
                if table_name not in tables:
                    tables[table_name] = []
                tables[table_name].append({
                    "column": column_name,
                    "type": data_type
                })
        
        conn.close()
        return {"tables": tables}
        
    except Exception as e:
        return {"error": f"Erreur lors de la récupération des tables: {e}"}

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

@app.post("/admin/test-user")
def test_user_insertion():
    """Tester l'insertion d'un utilisateur manuellement"""
    test_data = {
        "id": "test-123-456",
        "firstName": "Test",
        "lastName": "User",
        "email": "test@example.com",
        "phone": "+1234567890",
        "role": "USER",
        "confirmed": True,
        "statut": "ACTIVE",
        "pointFidelite": 100
    }
    
    global consumer_instance
    if consumer_instance:
        success = consumer_instance.handle_create_user(test_data)
        return {"success": success, "test_data": test_data}
    else:
        return {"error": "Consumer non disponible"}

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