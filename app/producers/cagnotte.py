#!/usr/bin/env python3
"""
Script de test qui envoie un message à Kafka ET l'enregistre dans PostgreSQL
"""

import json
import time
from kafka import KafkaProducer
import uuid
import random
import logging
from faker import Faker
import psycopg2
from datetime import datetime, timezone, timedelta # <-- CORRECTION : Ajout de timedelta

# --- Configuration ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker('fr_FR')

CATEGORIES = ["Education", "Santé", "Sport", "Environnement", "Religion"]

# --- NOUVEAU: Configuration de la connexion PostgreSQL ---
# !!! REMPLACEZ PAR VOS PROPRES INFORMATIONS !!!
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432"
}

def create_table_if_not_exists():
    """Crée la table 'cagnottes' dans PostgreSQL si elle n'existe pas."""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Requête SQL pour créer la table avec les types de données appropriés
        sql_create_table = """
            CREATE TABLE IF NOT EXISTS cagnottes (
                id TEXT PRIMARY KEY,
                name TEXT,
                description TEXT,
                date_start TIMESTAMP,
                date_end TIMESTAMP,
                objectif INTEGER,
                statut TEXT,
                id_categorie TEXT,
                admin TEXT,
                type TEXT,
                created_date TIMESTAMP,
                last_modified_date TIMESTAMP,
                deleted BOOLEAN,
                total_solde INTEGER,
                current_solde INTEGER,
                pays TEXT,
                total_contributors INTEGER,
                total_contributed INTEGER
            );
        """
        
        cur.execute(sql_create_table)
        conn.commit()
        cur.close()
        logger.info("✅ Table 'cagnottes' vérifiée/créée avec succès.")
        return True
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"❌ Erreur lors de la création de la table: {error}")
        if conn is not None:
            conn.rollback()
        return False
    finally:
        if conn is not None:
            conn.close()

# --- Fonction pour sauvegarder les données dans la BDD ---
def save_to_postgres(data):
    """Se connecte à Postgres et insère un dictionnaire de données."""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        sql = """
            INSERT INTO cagnottes (
                id, name, description, date_start, date_end, objectif, statut,
                id_categorie, admin, type, created_date, last_modified_date,
                deleted, total_solde, current_solde, pays, total_contributors,
                total_contributed
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cur.execute(sql, (
            data['id'], data['name'], data['description'], data['date_start'],
            data['date_end'], data['objectif'], data['statut'], data['id_categorie'],
            data['admin'], data['type'], data['created_date'], data['last_modified_date'],
            data['deleted'], data['total_solde'], data['current_solde'], data['pays'],
            data['total_contributors'], data['total_contributed']
        ))
        
        conn.commit()
        cur.close()
        logger.info(f"💾 Données enregistrées dans PostgreSQL avec succès (ID: {data['id']})")
        return True
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"❌ Erreur lors de l'enregistrement dans PostgreSQL: {error}")
        if conn is not None:
            conn.rollback() # Annuler la transaction en cas d'erreur
        return False
    finally:
        if conn is not None:
            conn.close()


def send_test_message():
    """Crée un message, l'enregistre dans Postgres, puis l'envoie à Kafka."""
    
    now = datetime.now(timezone.utc)
    
    # --- Création des données de test (clés en snake_case pour correspondre à la BDD) ---
    random_category = random.choice(CATEGORIES)
    test_data = {
        "id": str(uuid.uuid4()),
        "name": f"Projet pour la catégorie {random_category}",
        "description": "Cette cagnotte vise à collecter des fonds.",
        "date_start": now.isoformat(),
        "date_end": (now + timedelta(days=random.randint(30, 90))).isoformat(), # <-- CORRECTION
        "objectif": random.randint(10000, 100000),
        "statut": "VALIDE",
        "id_categorie": str(uuid.uuid4()),
        "admin": str(uuid.uuid4()),
        "type": "PUBLIC",
        "created_date": now.isoformat(),
        "last_modified_date": now.isoformat(),
        "deleted": False,
        "total_solde": 0,
        "current_solde": 0,
        "pays": "Mali",
        "total_contributors": random.randint(1, 200),
        "total_contributed": random.randint(100000, 1000000)
    }

    # --- Étape 1: Sauvegarder dans PostgreSQL ---
    if not save_to_postgres(test_data):
        logger.warning("L'envoi à Kafka continue malgré l'échec de la BDD...")

    # --- Étape 2: Envoyer le message à Kafka ---
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:29092',
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8')
        )
        future = producer.send('create-cagnotte', value=test_data, key=test_data['id'])
        future.get(timeout=10)
        logger.info(f"✅ Message envoyé à Kafka avec succès!")
        producer.close()
        return True
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'envoi à Kafka: {e}")
        return False

def send_multiple_tests(count=3):
    """Envoyer plusieurs messages de test"""
    logger.info(f"🚀 Envoi de {count} messages de test...")
    success = 0
    for i in range(count):
        logger.info(f"📤 Envoi message {i+1}/{count}")
        if send_test_message():
            success += 1
        time.sleep(1)
    logger.info(f"📊 Résultat: {success}/{count} messages traités")

if __name__ == "__main__":
    print("🧪 Test Rapide Producer (Kafka + PostgreSQL)")
    print("=" * 40)

    if not create_table_if_not_exists():
        exit()

    choice = input("1. Un message\n2. Plusieurs messages\nChoix (1-2): ").strip()
    if choice == "2":
        try:
            count = int(input("Combien de messages? (défaut: 3): ") or "3")
            send_multiple_tests(count)
        except ValueError:
            send_multiple_tests(3)
    else:
        send_test_message()
    print("✅ Test terminé!")
