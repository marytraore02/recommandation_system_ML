#!/usr/bin/env python3
"""
Consumer Kafka qui lit les messages du topic 'create-user'
"""

import json
import logging
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TOPIC_NAME = 'create-cagnotte'
KAFKA_BROKERS = ['localhost:29092']

def create_kafka_consumer():
    """
    Crée et retourne un consommateur Kafka.
    Gère les erreurs de connexion.
    """
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_BROKERS,
            auto_offset_reset='earliest',  # Commence à lire depuis le début du topic si l'offset n'est pas sauvegardé
            enable_auto_commit=True,       # Valide automatiquement les offsets
            group_id='mon_groupe_de_consommateur', # Permet le "load balancing" entre plusieurs instances
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logger.info(f"✅ Connecté au topic '{TOPIC_NAME}' sur les brokers : {KAFKA_BROKERS}")
        return consumer
    except NoBrokersAvailable as e:
        logger.error(f"❌ Impossible de se connecter aux brokers Kafka. Veuillez vérifier que le serveur est en cours d'exécution. Erreur: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Erreur inattendue lors de la connexion au consommateur: {e}")
        return None

def main():
    """
    Fonction principale pour démarrer le consommateur et traiter les messages.
    """
    consumer = create_kafka_consumer()
    if not consumer:
        return

    logger.info("Démarrage de l'écoute des messages...")
    try:
        # La boucle infinie permet au consommateur de rester actif et d'attendre de nouveaux messages
        for message in consumer:
            logger.info(f"--- Nouveau message reçu ---")
            logger.info(f"Partition: {message.partition}, Offset: {message.offset}")
            
            # Le message.value contient les données de la cagnotte désérialisées en dictionnaire
            cagnotte_data = message.value
            
            # Traitement des données (ici, on les affiche)
            logger.info(f"Cagnotte: {cagnotte_data}")
            # logger.info(f"Nom de la cagnotte: {cagnotte_data.get('name', 'N/A')}")
            # logger.info(f"Catégorie: {cagnotte_data.get('categorie', {}).get('name', 'N/A')}")
            
            # Log de l'ensemble du message pour la vérification
            logger.debug(f"Message complet: {cagnotte_data}")
            
            # Ici, vous inséreriez votre logique métier, par exemple :
            # - Enregistrer les données dans une autre base de données
            # - Envoyer une notification par e-mail ou SMS
            # - Déclencher un autre service ou une API
            
            logger.info(f"--- Fin du traitement du message ---")

    except KeyboardInterrupt:
        logger.info("Arrêt du consommateur par l'utilisateur.")
    finally:
        consumer.close()
        logger.info("Consommateur fermé.")

if __name__ == "__main__":
    main()