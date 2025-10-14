import json
import time
from kafka import KafkaProducer
import uuid
import random
import logging
from faker import Faker
import psycopg2
import os
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv 

load_dotenv()

# --- Configuration ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker('fr_FR')

# --- NOUVEAU: MAPPING entre les IDs et les noms de catégories ---
CATEGORY_MAPPING = {
    "7c732b16-2984-4136-881b-7e49cf450459": "Education",
    "7931366a-b995-4cfb-8072-6f0e076e5564": "Santé",
    "71487702-6ded-4b72-b23c-dbad5166228b": "Fêtes",
    "2a930a84-fb5c-4e16-a6da-954b6eb15c90": "Solidarité"
}

ADMIN = ["7056bb7f-6b04-470b-9811-aceaac75bcfe", "44456737-15dc-45b3-a8b3-b201eb3b12f7", "319c6627-0e13-4613-9c56-ad455618fad1", "185b74d2-9b28-4674-9420-55138027a8cf"]
STATUS = ["EN_COURS"]
TYPE = ["PUBLIC", "PRIVE"]
PAYS = ["Mali"]
IS_CERTIFIED = [True, True, True, False] # CORRECTION: Plus de poids pour True
COMMISSION = ["0.04", "0.05", "0.06"]

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
                total_contributed INTEGER,
                is_certified BOOLEAN,
                commission REAL,
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
                total_contributed, is_certified, commission
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cur.execute(sql, (
            data['id'], data['name'], data['description'], data['date_start'],
            data['date_end'], data['objectif'], data['statut'], data['id_categorie'],
            data['admin'], data['type'], data['created_date'], data['last_modified_date'],
            data['deleted'], data['total_solde'], data['current_solde'], data['pays'],
            data['total_contributors'], data['total_contributed'], data['is_certified'], data['commission']
            # json.dumps(data.get('ressources', []))
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
    
    # --- Création des données de test ---
    # CORRECTION: On choisit une paire ID/nom de catégorie pour garantir la concordance
    random_id_category, random_category_name = random.choice(list(CATEGORY_MAPPING.items()))
    
    status = random.choice(STATUS)
    types = random.choice(TYPE)
    pays = random.choice(PAYS)
    is_certified = random.choice(IS_CERTIFIED)
    commission = random.choice(COMMISSION)
    admin = random.choice(ADMIN)

    # Création de ressources vidéo de test
    video_resources = [
        {
            "id": str(uuid.uuid4()),
            "type": "video",
            "url": f"http://example.com/videos/video-{uuid.uuid4()}.mp4",
            "title": f"Vidéo de présentation {fake.word()}",
            "duration": random.randint(60, 300)
        } for _ in range(random.randint(1, 3))
    ]
    
    test_data = {
        "id": str(uuid.uuid4()),
        "name": f"Projet pour la catégorie {random_category_name}",
        "description": "Cette cagnotte vise à collecter des fonds.",
        "date_start": now.isoformat(),
        "date_end": (now + timedelta(days=random.randint(30, 90))).isoformat(),
        "objectif": random.randint(10000, 100000),
        "statut": f"{status}",
        "id_categorie": random_id_category,
        "admin": admin,
        "type": f"{types}",
        "created_date": now.isoformat(),
        "last_modified_date": now.isoformat(),
        "deleted": False,
        "total_solde": 0,
        "current_solde": 0,
        "pays": f"{pays}",
        "total_contributors": random.randint(1, 200),
        "is_certified": is_certified,
        "commission": commission,
        "total_contributed": random.randint(100000, 1000000)
        # "ressources": video_resources
    }

    # # --- Étape 1: Sauvegarder dans PostgreSQL ---
    # if not save_to_postgres(test_data):
    #     logger.warning("L'envoi à Kafka continue malgré l'échec de la BDD...")

    # --- Étape 2: Envoyer le message à Kafka ---
    try:
        producer = KafkaProducer(
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8')
        )
        future = producer.send(os.getenv('KAFKA_TOPIC_CREATE_CAGNOTTE'), value=test_data, key=test_data['id'])
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

    # if not create_table_if_not_exists():
    #     exit()

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
