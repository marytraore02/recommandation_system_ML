import json
import random
import time
from confluent_kafka import Producer
from faker import Faker

# --- Initialisation de Faker et des données de test ---
fake = Faker('fr_FR')

CATEGORIES = ["Éducation", "Santé", "Construction", "Environnement", "Intérêt public"]
PROVENANCE_CAPITAL = ["banque", "épargne", "don familial", "revenu personnel"]
BUDGETS = [
    {"min": 100, "max": 500},
    {"min": 1000, "max": 5000},
    {"min": 10000, "max": 50000},
    {"min": 100000, "max": 500000}
]

def generate_user_data(user_id):
    budget = random.choice(BUDGETS)
    return {
        "id": user_id,
        "nom": fake.last_name(),
        "prenom": fake.first_name(),
        "telephone": fake.phone_number(),
        "profession": fake.job(),
        "provenance_capital": random.choice(PROVENANCE_CAPITAL),
        "budget": {"min": budget["min"], "max": budget["max"]},
        "pourquoi": fake.sentence(nb_words=4),
        "categorie_dons": random.sample(CATEGORIES, k=random.randint(1, 3))
    }

def generate_cagnotte_data(cagnotte_id):
    categorie_choisie = random.choice(CATEGORIES)
    return {
        "id": cagnotte_id,
        "titre": fake.sentence(nb_words=5),
        "description": fake.paragraph(nb_sentences=3),
        "categorie": categorie_choisie,
        "media_url": [f"https://www.example.com/media/{cagnotte_id}.mp4"],
        "nbreLike": random.randint(5, 500),
        "nbreVue": random.randint(100, 10000),
        "nbreCommentaire": random.randint(0, 100),
        "budget_necessaire": random.randint(1000, 100000)
    }

# Callback pour gérer les erreurs de delivery
def delivery_report(err, msg):
    if err is not None:
        print(f'Producteur: Échec de livraison: {err}')
    else:
        print(f'Producteur: Message livré au topic {msg.topic()} [partition {msg.partition()}]')

# --- Le Producteur Kafka ---
def run_producer():
    # Configuration avec gestion d'erreur améliorée
    conf = {
        'bootstrap.servers': 'localhost:29092',  # Port externe pour connexion depuis la machine locale
        'client.id': 'horizon_producer',
        'acks': 'all',  # Attendre la confirmation de tous les brokers
        'retries': 5,
        'retry.backoff.ms': 300,
        'request.timeout.ms': 10000,
        'delivery.timeout.ms': 30000
    }
    
    producer = Producer(conf)
    
    print("Producteur: Démarrage de l'envoi de messages vers Kafka.")
    print("Producteur: Connexion à localhost:29092")
    
    try:
        for i in range(1, 11):
            # Génération et envoi des données utilisateur
            user_data = generate_user_data(f"user_{i}")
            producer.produce(
                'user_onboarding', 
                key=str(i), 
                value=json.dumps(user_data, ensure_ascii=False).encode('utf-8'),
                callback=delivery_report
            )
            print(f"Producteur: Message pour l'utilisateur {user_data['id']} en cours d'envoi...")
            
            # Génération et envoi des données cagnotte
            cagnotte_data = generate_cagnotte_data(f"cagnotte_{i}")
            producer.produce(
                'cagnotte_created', 
                key=str(i), 
                value=json.dumps(cagnotte_data, ensure_ascii=False).encode('utf-8'),
                callback=delivery_report
            )
            print(f"Producteur: Message pour la cagnotte {cagnotte_data['id']} en cours d'envoi...")
            
            # Attendre un peu entre les envois et traiter les callbacks
            producer.poll(0)
            time.sleep(2)
            
        # S'assurer que tous les messages sont envoyés
        print("Producteur: Finalisation de l'envoi...")
        producer.flush()
        print("Producteur: Tous les messages ont été envoyés avec succès!")
        
    except Exception as e:
        print(f"Producteur: Erreur lors de l'envoi: {e}")
    finally:
        producer.flush()

# Point de lancement
if __name__ == '__main__':
    # Petit délai pour s'assurer que Kafka est prêt
    print("Attente de 5 secondes pour que Kafka soit prêt...")
    time.sleep(5)
    run_producer()