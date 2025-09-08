import json
import time
from kafka import KafkaProducer
import uuid
import random
import logging
from faker import Faker
import psycopg2
from datetime import datetime, timezone, timedelta

# --- Configuration ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker('fr_FR')

# Mappage des IDs aux rôles, selon les instructions
USER_ID_ROLE_MAPPING = {
    "185b74d2-9b28-4674-9420-55138027a8cf": "USER",
    "319c6627-0e13-4613-9c56-ad455618fad1": "ADMIN",
    "44456737-15dc-45b3-a8b3-b201eb3b12f7": "INFLUENCEUR",
    "7056bb7f-6b04-470b-9811-aceaac75bcfe": "ORGANISATEUR"
}
# ROLE = ["USER", "USER", "USER", "ADMIN"]

# Listes de valeurs pour les champs à varier
CONFIRMED = [True, True, True, False]
COMPTE_LEVELS = ["Bronze", "Silver", "Gold", "Platinum"]

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
    Crée la table 'users' dans PostgreSQL si elle n'existe pas, avec les nouveaux champs.
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        sql_create_table = """
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                email TEXT,
                phone TEXT,
                created_date TIMESTAMP,
                last_modified_date TIMESTAMP,
                deleted BOOLEAN,
                role TEXT,
                picture TEXT,
                statut TEXT,
                confirmed BOOLEAN,
                my_code_parrain TEXT,
                code_parrain TEXT,
                point_fidelite INTEGER,
                current_solde REAL,
                total_solde REAL,
                total_contributed INTEGER,
                total_contributed_amount REAL,
                last_contribution_date TIMESTAMP,
                last_contribution_amount REAL,
                compte_level TEXT,
                user_profil TEXT
            );
        """
        
        cur.execute(sql_create_table)
        conn.commit()
        cur.close()
        logger.info("✅ Table 'users' vérifiée/créée avec succès.")
        return True
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"❌ Erreur lors de la création de la table: {error}")
        if conn is not None:
            conn.rollback()
        return False
    finally:
        if conn is not None:
            conn.close()

def save_user_to_postgres(data):
    """
    Se connecte à Postgres et insère un dictionnaire de données utilisateur.
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        sql = """
            INSERT INTO users (
                id, first_name, last_name, email, phone, created_date, 
                last_modified_date, deleted, role, picture, statut, confirmed, 
                my_code_parrain, code_parrain, point_fidelite, current_solde, 
                total_solde, total_contributed, total_contributed_amount, 
                last_contribution_date, last_contribution_amount, compte_level, user_profil
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                last_modified_date = EXCLUDED.last_modified_date,
                current_solde = EXCLUDED.current_solde,
                total_solde = EXCLUDED.total_solde,
                total_contributed = EXCLUDED.total_contributed,
                total_contributed_amount = EXCLUDED.total_contributed_amount,
                last_contribution_date = EXCLUDED.last_contribution_date,
                last_contribution_amount = EXCLUDED.last_contribution_amount,
                compte_level = EXCLUDED.compte_level,
                user_profil = EXCLUDED.user_profil;
        """
        
        cur.execute(sql, (
            data['id'], data['firstName'], data['lastName'], data['email'], data['phone'],
            data['createdDate'], data['lastModifiedDate'], data['deleted'], data['role'],
            data['picture'], data['statut'], data['confirmed'], data['myCodeParrain'],
            data['codeParrain'], data['pointFidelite'], data['current_solde'],
            data['total_solde'], data['total_contributed'], data['total_contributed_amount'],
            data['last_contribution_date'], data['last_contribution_amount'],
            data['compte_level'], data['user_profil']
        ))
        
        conn.commit()
        cur.close()
        logger.info(f"💾 Données utilisateur enregistrées dans PostgreSQL (ID: {data['id']})")
        return True
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"❌ Erreur lors de l'enregistrement de l'utilisateur: {error}")
        if conn is not None:
            conn.rollback()
        return False
    finally:
        if conn is not None:
            conn.close()

def send_test_message():
    """Envoyer un message de test utilisateur."""
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:29092',
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8')
        )
        
        now = datetime.now(timezone.utc)
        
        # Sélectionne un ID et son rôle correspondant de manière aléatoire
        user_id, role = random.choice(list(USER_ID_ROLE_MAPPING.items()))
        
        # role = random.choice(ROLE)
        # Données de test avec les nouveaux champs
        test_data = {
            "id": user_id,
            # "id": str(uuid.uuid4()),
            "firstName": fake.first_name(),
            "lastName": fake.last_name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "createdDate": now.isoformat(),
            "lastModifiedDate": now.isoformat(),
            "deleted": False,
            "role": role,
            "picture": "media/default.jpg",
            "statut": "ACTIVE",
            "confirmed": random.choice(CONFIRMED),
            "myCodeParrain": fake.bothify(text='??#####??#'),
            "codeParrain": fake.bothify(text='??#####??#'),
            "pointFidelite": random.randint(0, 1000),
            "current_solde": round(random.uniform(0, 500), 2),
            "total_solde": round(random.uniform(500, 5000), 2),
            "total_contributed": random.randint(0, 50),
            "total_contributed_amount": round(random.uniform(10, 10000), 2),
            "last_contribution_date": (now - timedelta(days=random.randint(1, 365))).isoformat(),
            "last_contribution_amount": round(random.uniform(5, 500), 2),
            "compte_level": random.choice(COMPTE_LEVELS),
            "user_profil": fake.text(max_nb_chars=100)
        }
        
        # Sauvegarder dans PostgreSQL
        if not save_user_to_postgres(test_data):
            logger.warning("L'envoi à Kafka continue malgré l'échec de la BDD...")

        # Envoyer le message
        future = producer.send('create-user', value=test_data, key=test_data['id'])
        result = future.get(timeout=10)
        
        logger.info(f"✅ Message utilisateur envoyé avec succès!")
        logger.info(f"   ID: {test_data['id']}")
        logger.info(f"   Topic: {result.topic}")
        logger.info(f"   Partition: {result.partition}")
        logger.info(f"   Offset: {result.offset}")
        
        producer.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur: {e}")
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
    
    logger.info(f"📊 Résultat: {success}/{count} messages envoyés")

if __name__ == "__main__":
    print("🧪 Test Rapide Producer Kafka pour les utilisateurs")
    print("=" * 60)
    
    if not create_table_if_not_exists():
        exit()
    
    # Options
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
