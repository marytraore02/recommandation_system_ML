import json
import time
from kafka import KafkaProducer
import random
import logging
from faker import Faker
import psycopg2
from datetime import datetime, timezone

# --- Configuration ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker('fr_FR')

# Les IDs et noms de catégories doivent correspondre pour une cohérence des données
CATEGORIES = {
    "Éducation": "7c732b16-2984-4136-881b-7e49cf450459",
    "Santé": "7931366a-b995-4cfb-8072-6f0e076e5564",
    "Fêtes": "71487702-6ded-4b72-b23c-dbad5166228b",
    "Solidarité": "2a930a84-fb5c-4e16-a6da-954b6eb15c90"
}

# --- Configuration de la connexion PostgreSQL ---
# !!! REMPLACEZ PAR VOS PROPRES INFORMATIONS !!!
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432"
}

def create_table_if_not_exists():
    """
    Crée la table 'categories' dans PostgreSQL si elle n'existe pas.
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Requête SQL pour créer la table avec les types de données appropriés
        sql_create_table = """
            CREATE TABLE IF NOT EXISTS categories (
                id TEXT PRIMARY KEY,
                name TEXT,
                description TEXT,
                picture TEXT,
                created_date TIMESTAMP,
                last_modified_date TIMESTAMP,
                deleted BOOLEAN
            );
        """
        
        cur.execute(sql_create_table)
        conn.commit()
        cur.close()
        logger.info("✅ Table 'categories' vérifiée/créée avec succès.")
        return True
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"❌ Erreur lors de la création de la table: {error}")
        if conn is not None:
            conn.rollback()
        return False
    finally:
        if conn is not None:
            conn.close()

def save_category_to_postgres(data):
    """
    Se connecte à Postgres et insère un dictionnaire de données de catégorie.
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        sql = """
            INSERT INTO categories (
                id, name, description, picture, created_date, last_modified_date, deleted
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                picture = EXCLUDED.picture,
                last_modified_date = EXCLUDED.last_modified_date,
                deleted = EXCLUDED.deleted;
        """
        
        cur.execute(sql, (
            data['id'], data['name'], data['description'], data['picture'], 
            data['created_date'], data['last_modified_date'], data['deleted']
        ))
        
        conn.commit()
        cur.close()
        logger.info(f"💾 Données de catégorie enregistrées dans PostgreSQL avec succès (ID: {data['id']})")
        return True
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"❌ Erreur lors de l'enregistrement de la catégorie dans PostgreSQL: {error}")
        if conn is not None:
            conn.rollback()
        return False
    finally:
        if conn is not None:
            conn.close()

def send_category_message(name, category_id):
    """
    Crée un message de catégorie, l'enregistre dans Postgres, puis l'envoie à Kafka.
    """
    now = datetime.now(timezone.utc)
    
    test_data = {
        "id": category_id,
        "name": name,
        "description": fake.paragraph(nb_sentences=2),
        "picture": f"https://example.com/images/{category_id}.jpg",
        "created_date": now.isoformat(),
        "last_modified_date": now.isoformat(),
        "deleted": False
    }

    # Étape 1: Sauvegarder dans PostgreSQL
    if not save_category_to_postgres(test_data):
        logger.warning("L'envoi à Kafka continue malgré l'échec de la BDD...")

    # Étape 2: Envoyer le message à Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:29092',
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8')
        )
        future = producer.send('create-category', value=test_data, key=test_data['id'])
        future.get(timeout=10)
        logger.info(f"✅ Message de catégorie envoyé à Kafka avec succès!")
        producer.close()
        return True
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'envoi de la catégorie à Kafka: {e}")
        return False

def send_all_categories():
    """
    Envoyer un message pour chaque catégorie prédéfinie.
    """
    logger.info("🚀 Envoi de tous les messages de catégorie...")
    success = 0
    for name, category_id in CATEGORIES.items():
        logger.info(f"📤 Envoi de la catégorie '{name}'...")
        if send_category_message(name, category_id):
            success += 1
        time.sleep(1)
    logger.info(f"📊 Résultat: {success}/{len(CATEGORIES)} messages de catégorie traités")

if __name__ == "__main__":
    print("🧪 Test Rapide Producer (Kafka + PostgreSQL) pour les catégories")
    print("=" * 60)

    if not create_table_if_not_exists():
        exit()

    send_all_categories()
    print("✅ Test terminé!")
