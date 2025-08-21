#!/usr/bin/env python3
"""
Script de test rapide pour vérifier le consumer Kafka
Usage: python quick_test.py
"""

import json
import time
from kafka import KafkaProducer
import uuid
import random
import logging

# Configuration simple
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def send_test_message():
    """Envoyer un message de test simple"""
    try:
        # Configuration producer
        producer = KafkaProducer(
            bootstrap_servers='localhost:29092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8')
        )
        
        # Données de test
        test_data = {
            "id": str(uuid.uuid4()),
            "name": f"Test Cagnotte {random.randint(1, 1000)}",
            "description": "Cagnotte de test pour vérifier le consumer",
            "type": "Test",
            "category": "Test",
            "totalSolde": 10000,
            "currentSolde": random.randint(1000, 9000),
            "createdAt": "2024-01-01T10:00:00Z",
            "updatedAt": "2024-01-01T10:00:00Z",
            "status": "active"
        }
        
        # Envoyer le message
        future = producer.send('create-cagnotte', value=test_data, key=test_data['id'])
        result = future.get(timeout=10)
        
        logger.info(f"✅ Message envoyé avec succès!")
        logger.info(f"   ID: {test_data['id']}")
        logger.info(f"   Nom: {test_data['name']}")
        logger.info(f"   Budget: {test_data['currentSolde']}/{test_data['totalSolde']}€")
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
        time.sleep(1)  # Pause entre les messages
    
    logger.info(f"📊 Résultat: {success}/{count} messages envoyés")

if __name__ == "__main__":
    print("🧪 Test Rapide Producer Kafka")
    print("=" * 40)
    
    # Test de connexion
    logger.info("🔍 Test de connexion Kafka...")
    time.sleep(2)  # Attendre Kafka
    
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