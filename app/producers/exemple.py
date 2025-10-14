import json
import time
import uuid
from kafka import KafkaProducer
from datetime import datetime

# --- Configuration ---
# Remplacez par l'adresse de votre broker Kafka
KAFKA_BROKER = 'localhost:29092'
KAFKA_TOPIC = 'user_events'

# --- Configuration de la sécurité (SASL_SSL) ---
# Adaptez ces paramètres à votre configuration Kafka
# security_protocol='SASL_SSL' signifie que la connexion est chiffrée (SSL)
# et authentifiée (SASL).
# SECURITY_CONFIG = {
#     'security_protocol': 'SASL_SSL',
#     'sasl_mechanism': 'SCRAM-SHA-256',
#     'sasl_plain_username': 'votre_utilisateur',
#     'sasl_plain_password': 'votre_mot_de_passe',
#     # Chemin vers le certificat de l'autorité de certification (CA) pour valider le certificat du broker
#     'ssl_cafile': '/path/to/your/ca.crt',
# }

# --- Initialisation du Producer Kafka ---
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        # Sérialiseur pour convertir les dictionnaires Python en JSON (bytes)
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # Appliquer la configuration de sécurité
        # **SECURITY_CONFIG
    )
    print("✅ Producer Kafka connecté avec succès.")
except Exception as e:
    print(f"❌ Erreur de connexion au producer Kafka : {e}")
    exit()


# --- Simulation d'envoi de données ---
print("🚀 Démarrage de l'envoi des événements...")
for i in range(10):
    event_data = {
        'event_id': str(uuid.uuid4()),
        'user_id': f'user_{i % 3}', # 3 utilisateurs différents
        'event_type': 'page_view',
        'url': f'/page/{i}',
        'timestamp': datetime.utcnow().isoformat()
    }

    try:
        # Envoi de l'événement au topic Kafka
        producer.send(KAFKA_TOPIC, value=event_data)
        print(f"📦 Événement envoyé : {event_data['event_id']}")
    except Exception as e:
        print(f"❌ Erreur lors de l'envoi : {e}")
    
    time.sleep(1) # Attendre 1 seconde entre chaque message

# S'assurer que tous les messages sont bien envoyés avant de quitter
producer.flush()
producer.close()
print("✅ Tous les événements ont été envoyés.")