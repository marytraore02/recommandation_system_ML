from fastapi import FastAPI, Depends, HTTPException, Query, BackgroundTasks

from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional
import asyncio
import threading
import time
import logging
import json
import os
import traceback
from datetime import datetime
import asyncpg

# Imports pour Kafka et PostgreSQL 
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable, KafkaTimeoutError

# Imports de votre application existante
import crud
import models
import database
from recommender import engine
import redis.asyncio as redis
from pydantic import BaseModel, Field, EmailStr



# Schémas de tables prédéfinis
TABLE_SCHEMAS = {
    "users": {
        "id": "TEXT",
        "firstName": "TEXT",
        "lastName": "TEXT", 
        "email": "TEXT",
        "phone": "TEXT",
        "statut": "TEXT",
        "confirmed": "BOOLEAN",
        "role": "TEXT",
        "deleted": "BOOLEAN",
        "createdDate": "TIMESTAMP",
        "lastModifiedDate": "TIMESTAMP",
        # "picture": "TEXT",
        # "myCodeParrain": "TEXT",
        # "codeParrain": "TEXT",
        # "pointFidelite": "INTEGER"
    },
    "cagnottes": {
        "id": "TEXT",
        "name": "Aide pour l'école du Quartier",
        "categorie": {
          "id": "e3835430-d46a-4984-905f-dd8bf5c87cc1",
          "name": "Education"
        },
        "description": "Cette cagnotte vise à collecter des fonds pour construire une nouvelle école dans notre village.",
        "pays": "Cote d'ivoire",
        "admin": {
          "firstName": "ulrich",
          "lastName": "Odaoh",
          "phone": "+33612345678",
          "email": "admin@deme.com",
          "picture": " "
        },
        "dateStart": "2025-08-21T13:09:19.224399",
        "dateEnd": "2025-09-29T23:59:59",
        "objectif": 50000,
        "totalContributeurs": 1,
        "totalContribuer": 500000,
        "type": "PUBLIC",
        "statut": "VALIDE",
        "ressources": []
      },
}

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

redis_client = redis.from_url("redis://localhost:6379/0", decode_responses=True)

# Dépendance FastAPI pour injecter le client Redis dans les routes
async def get_redis():
    """Fournit une instance du client Redis."""
    return redis_client

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
                auto_offset_reset='latest',
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

    def create_table_from_schema(self, table_name: str):
        """Créer une table basée sur un schéma prédéfini"""
        if not self.db_connection:
            self.db_connection = self.get_db_connection()
            if not self.db_connection:
                return False

        # Vérifier si on a un schéma pour cette table
        if table_name not in TABLE_SCHEMAS:
            logger.warning(f"⚠️ Pas de schéma prédéfini pour la table '{table_name}' - création dynamique")
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
                    # Créer la table avec le schéma prédéfini
                    schema = TABLE_SCHEMAS[table_name]
                    columns = []
                    
                    for column_name, column_type in schema.items():
                        columns.append(f'"{column_name}" {column_type}')
                    
                    # # Ajouter les colonnes de timestamp système
                    # columns.extend([
                    #     "created_at TIMESTAMP DEFAULT NOW()",
                    #     "updated_at TIMESTAMP DEFAULT NOW()"
                    # ])
                    
                    create_sql = f"""
                        CREATE TABLE "{table_name}" (
                            {', '.join(columns)}
                        );
                    """
                    
                    cur.execute(create_sql)
                    logger.info(f"✅ Table '{table_name}' créée avec schéma prédéfini ({len(columns)} colonnes)")
                
                else:
                    # La table existe, s'assurer que toutes les colonnes du schéma sont présentes
                    logger.debug(f"📋 Table '{table_name}' existe - vérification du schéma")
                    self._ensure_schema_columns(cur, table_name)
                
            self.db_connection.commit()
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la création de table {table_name}: {e}")
            logger.error(f"📋 Traceback: {traceback.format_exc()}")
            self.db_connection.rollback()
            return False

    def _ensure_schema_columns(self, cursor, table_name: str):
        """S'assurer que toutes les colonnes du schéma existent"""
        if table_name not in TABLE_SCHEMAS:
            return
        
        # Récupérer les colonnes existantes
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' AND table_name = %s;
        """, (table_name,))
        
        existing_columns = {row[0] for row in cursor.fetchall()}
        logger.debug(f"🔍 Colonnes existantes dans '{table_name}': {existing_columns}")
        
        # Vérifier chaque colonne du schéma
        schema = TABLE_SCHEMAS[table_name]
        columns_added = 0
        
        for column_name, column_type in schema.items():
            if column_name not in existing_columns:
                # Ajouter la colonne manquante
                alter_sql = f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {column_type};'
                cursor.execute(alter_sql)
                columns_added += 1
                logger.info(f"➕ Colonne '{column_name}' ({column_type}) ajoutée à la table '{table_name}'")
        
        # # S'assurer que updated_at existe
        # if 'updated_at' not in existing_columns:
        #     cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "updated_at" TIMESTAMP DEFAULT NOW();')
        #     columns_added += 1
        #     logger.info(f"➕ Colonne 'updated_at' ajoutée à la table '{table_name}'")
        
        # if columns_added > 0:
        #     logger.info(f"✅ {columns_added} colonnes ajoutées à la table '{table_name}'")
        # else:
        #     logger.debug(f"✅ Toutes les colonnes du schéma existent dans '{table_name}'")

    def create_table_from_data(self, table_name: str, data: Dict[str, Any]):
        """Créer une table - d'abord essayer le schéma prédéfini, sinon dynamique"""
        # D'abord essayer avec le schéma prédéfini
        if self.create_table_from_schema(table_name):
            return True
        
        # Fallback vers la création dynamique
        logger.info(f"📋 Création dynamique de la table '{table_name}'")
        # return True
        return self._create_table_dynamically(table_name, data)

    def _create_table_dynamically(self, table_name: str, data: Dict[str, Any]):
        """Créer une table dynamiquement basée sur la structure des données (fallback)"""
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
                    # Créer les colonnes basées sur les données
                    columns = []
                    for key, value in data.items():
                        # Renommer 'id' en 'entity_id' pour éviter le conflit
                        # column_name = "entity_id" if key == "id" else key
                        column_name = key
                        
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
                        
                        columns.append(f'"{column_name}" {col_type}')
                    
                    # # Ajouter les colonnes de timestamp
                    # columns.extend([
                    #     "created_at TIMESTAMP DEFAULT NOW()",
                    #     "updated_at TIMESTAMP DEFAULT NOW()"
                    # ])
                    
                    create_sql = f"""
                        CREATE TABLE "{table_name}" (
                            id SERIAL PRIMARY KEY,
                            {', '.join(columns)}
                        );
                    """
                    
                    cur.execute(create_sql)
                    logger.info(f"✅ Table '{table_name}' créée dynamiquement avec {len(columns)} colonnes")
                else:
                    # La table existe, vérifier et ajouter les colonnes manquantes
                    logger.debug(f"📋 Table '{table_name}' existe déjà - vérification des colonnes")
                    self._ensure_columns_exist(cur, table_name, data)
                
            self.db_connection.commit()
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la création dynamique de table {table_name}: {e}")
            logger.error(f"📋 Traceback: {traceback.format_exc()}")
            self.db_connection.rollback()
            return False

    def insert_dynamic_data(self, table_name: str, data: Dict[str, Any]):
        """Insérer des données dans une table (utilise le schéma prédéfini si disponible)"""
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
                    column_name = "entity_id" if key == "id" else key
                    columns.append(f'"{column_name}"')
                    
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
            logger.error(f"📋 Données reçues: {data}")
            logger.error(f"📋 Traceback complet: {traceback.format_exc()}")
            
            # Rollback et reconnexion
            self.db_connection.rollback()
            self.db_connection = None
            return False

    def insert_user_data(self, user_data: Dict[str, Any]):
        """Insérer les données utilisateur avec mapping des champs"""
        if not self.db_connection:
            self.db_connection = self.get_db_connection()
            if not self.db_connection:
                return False

        try:
            # Créer/vérifier la table avec le schéma prédéfini
            if not self.create_table_from_schema("users"):
                logger.error("❌ Impossible de créer la table users")
                return False

            with self.db_connection.cursor() as cur:
                # Mapping des champs reçus vers le schéma de la base
                mapped_data = self._map_user_fields(user_data)
                
                # Préparer l'insertion
                columns = []
                values = []
                placeholders = []
                
                for key, value in mapped_data.items():
                    columns.append(f'"{key}"')
                    values.append(value)
                    placeholders.append("%s")
                
                # # Ajouter updated_at
                # columns.append("updated_at")
                # placeholders.append("NOW()")
                
                # Construction de la requête d'insertion
                insert_sql = f"""
                    INSERT INTO "users" ({', '.join(columns)}) 
                    VALUES ({', '.join(placeholders)});
                """
                
                logger.debug(f"🔧 SQL utilisateur: {insert_sql}")
                logger.debug(f"🔧 Valeurs: {values}")
                
                cur.execute(insert_sql, values)
                
            self.db_connection.commit()
            logger.info(f"✅ Utilisateur inséré dans la table 'users' (entity_id: {user_data.get('id', 'N/A')})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'insertion utilisateur: {e}")
            logger.error(f"📋 Données reçues: {user_data}")
            logger.error(f"📋 Traceback: {traceback.format_exc()}")
            self.db_connection.rollback()
            self.db_connection = None
            return False

    def _map_user_fields(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Mapper les champs reçus vers le schéma de la base de données"""
        mapped = {}
        # Mapping des champs
        field_mapping = {
            'id': 'id',
            'firstName': 'firstName',
            'lastName': 'lastName',
            'email': 'email',
            'phone': 'phone',
            'statut': 'statut',
            'confirmed': 'confirmed',
            'role': 'role',
            'deleted': 'deleted',
            'createdDate': 'createdDate',
            'lastModifiedDate': 'lastModifiedDate',
            # 'picture': 'picture',
            # 'myCodeParrain': 'myCodeParrain',
            # 'codeParrain': 'codeParrain',
            # 'pointFidelite': 'pointFidelite',
        }

        def convert_to_datetime(date_list):
            """Convertit une liste de date [année, mois, jour, ...] en un objet datetime."""
            if not date_list:
                return None
            # La 7ème valeur est en nanosecondes. On la convertit en microsecondes.
            return datetime(
                date_list[0], 
                date_list[1], 
                date_list[2], 
                date_list[3], 
                date_list[4], 
                date_list[5], 
                date_list[6] // 1000
            )
        
        # Appliquer le mapping
        for source_field, target_field in field_mapping.items():
            if source_field in user_data:
                value = user_data[source_field]
                
                # Traiter spécifiquement les champs de date
                if source_field in ['createdDate', 'lastModifiedDate']:
                    value = convert_to_datetime(value)
                
                mapped[target_field] = value        
        # # Appliquer le mapping
        # for source_field, target_field in field_mapping.items():
        #     if source_field in user_data:
        #         mapped[target_field] = user_data[source_field]
        
        # # Valeurs par défaut pour les champs manquants
        # if 'created_date' not in mapped:
        #     mapped['created_date'] = 'NOW()'
        # if 'last_modified_date' not in mapped:
        #     mapped['last_modified_date'] = 'NOW()'
        # if 'deleted' not in mapped:
        #     mapped['deleted'] = False
        
        logger.debug(f"🔄 Mapping utilisateur: {user_data} -> {mapped}")
        return mapped

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
            raw_success = self.insert_dynamic_data("cagnottes", message_value)
            
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

    def _ensure_columns_exist(self, cursor, table_name: str, data: Dict[str, Any]):
        """S'assurer que toutes les colonnes nécessaires existent dans la table (fallback dynamique)"""
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

    def handle_create_user(self, message_value: Dict[str, Any]) -> bool:
        """Traiter la création d'un utilisateur avec le schéma prédéfini"""
        try:
            user_id = message_value.get('id') or message_value.get('user_id')
            logger.info(f"👤 Nouvel utilisateur: {user_id}")
            logger.info(f"📥 Données utilisateur reçues: {message_value}")
            
            # Utiliser la méthode spécialisée pour les utilisateurs
            success = self.insert_user_data(message_value)
            
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

@app.get(
    "/rankings", 
    response_model=List[models.VideoDetails],
    summary="Récupérer le top des vidéos classées"
)
async def get_top_videos(
    limit: int = Query(50, ge=1, le=100, description="Nombre de vidéos à récupérer"),
    redis: redis.Redis = Depends(get_redis)
):
    """
    Cet endpoint récupère les ID des vidéos les mieux classées depuis le sorted set `video_rankings`,
    puis charge les détails de chaque vidéo depuis les hashes correspondants en utilisant un pipeline Redis pour des performances optimales.
    """
    try:
        # Étape 1: Récupérer les ID des vidéos classées (du score le plus haut au plus bas)
        # On utilise ZRANGE avec desc=True, ce qui est l'équivalent de ZREVRANGE.
        # On récupère les `limit` premiers éléments (indices de 0 à limit-1).
        video_ids = await redis.zrange("video_rankings", 0, limit - 1, desc=True)

        if not video_ids:
            return [] # Retourne une liste vide si le classement n'existe pas

        # Étape 2: Utiliser un pipeline pour récupérer tous les détails en une seule fois
        pipe = redis.pipeline()
        for video_id in video_ids:
            pipe.hgetall(f"video:{video_id}")
        
        # Exécuter le pipeline : une seule communication réseau avec Redis
        video_data_list = await pipe.execute()

        # Étape 3: Formater la réponse
        # Pydantic va automatiquement convertir les types (ex: str -> int)
        results = []
        for data in video_data_list:
            if data:  # S'assurer que le hash n'était pas vide ou supprimé
                results.append(models.VideoDetails(**data))

        return results

    except Exception as e:
        # Loggez l'erreur de manière appropriée dans une application réelle
        print(f"❌ Erreur lors de la récupération depuis Redis: {e}")
        raise HTTPException(
            status_code=500, 
            detail="Impossible de récupérer le classement depuis Redis."
        )
    

@app.get(
    "/rankings/enriched", 
    response_model=List[models.VideoEnrichedDetails],
    summary="Récupérer le top des vidéos avec les détails de la cagnotte"
)
async def get_top_videos_enriched(
    limit: int = Query(50, ge=1, le=100, description="Nombre de vidéos à récupérer"),
    redis: redis.Redis = Depends(get_redis),
    # db: asyncpg.Connection = Depends(get_db_conn)
):
    """
    Cet endpoint effectue les étapes suivantes :
    1. Récupère les IDs des vidéos les mieux classées depuis Redis (Sorted Set).
    2. Récupère les détails de chaque vidéo depuis Redis (Hashes).
    3. Extrait les IDs uniques des cagnottes.
    4. Interroge PostgreSQL pour obtenir les détails de ces cagnottes.
    5. Fusionne les deux sources de données pour fournir une réponse complète.
    """
    try:
        # --- ÉTAPE 1 & 2 : Récupérer les données de base depuis Redis ---
        video_ids = await redis.zrange("video_rankings", 0, limit - 1, desc=True)
        if not video_ids:
            return []

        pipe = redis.pipeline()
        for video_id in video_ids:
            pipe.hgetall(f"video:{video_id}")
        videos_from_redis_raw = await pipe.execute()

        # Valider les données Redis avec Pydantic
        videos_from_redis = [models.VideoDetails(**data) for data in videos_from_redis_raw if data]

        # --- ÉTAPE 3 : Extraire les IDs de cagnottes uniques ---
        cagnotte_ids = {video.cagnotte_id for video in videos_from_redis}
        # Convertir en liste d'entiers (ou le type approprié pour votre BDD)
        cagnotte_ids_int = [int(cid) for cid in cagnotte_ids]
        print(f"🔍 Cagnotte IDs extraits: {cagnotte_ids_int}")

        if not cagnotte_ids_int:
            raise HTTPException(status_code=404, detail="Aucune cagnotte associée aux vidéos trouvées.")

        # # --- ÉTAPE 4 : Interroger PostgreSQL pour les détails des cagnottes ---
        # # Utilisation de la syntaxe `WHERE id = ANY($1)` pour une requête efficace
        # query = """
        #     SELECT id, name, description, creator_name 
        #     FROM cagnottes 
        #     WHERE id = ANY($1)
        # """
        # cagnottes_records = await db.fetch(query, cagnotte_ids_int)

        # # Créer un dictionnaire pour un accès rapide : {cagnotte_id: CagnotteDetails}
        # cagnottes_map: Dict[int, CagnotteDetails] = {
        #     record['id']: CagnotteDetails(**record) for record in cagnottes_records
        # }

        # # --- ÉTAPE 5 : Fusionner les données ---
        # final_results = []
        # for video in videos_from_redis:
        #     cagnotte_id_int = int(video.cagnotte_id)
        #     cagnotte_details = cagnottes_map.get(cagnotte_id_int)
            
        #     if cagnotte_details:
        #         # Créer l'objet de réponse final en combinant les deux sources
        #         enriched_video = VideoEnrichedDetails(
        #             **video.model_dump(),  # Copie les champs de la vidéo
        #             cagnotte=cagnotte_details # Ajoute l'objet cagnotte
        #         )
        #         final_results.append(enriched_video)

        # return final_results
        return []  # Placeholder tant que la BDD n'est pas intégrée



    except asyncpg.PostgresError as e:
        print(f"❌ Erreur PostgreSQL: {e}")
        raise HTTPException(status_code=500, detail="Erreur lors de la communication avec la base de données.")
    except Exception as e:
        print(f"❌ Erreur inattendue: {e}")
        raise HTTPException(status_code=500, detail="Une erreur interne est survenue.")
    

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