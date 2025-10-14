import sys
from confluent_kafka import Consumer, KafkaError, KafkaException

# ==========================================================
# CONFIGURATION STATIQUE (MODIFIEZ CES VALEURS) 🛠️
# ==========================================================

# 1. Adresses des brokers Kafka
BOOTSTRAP_SERVERS = 'localhost:29092' 

# 2. Nom du topic à écouter
TOPIC_NAME = 'create-user'

# 3. ID du groupe de consommateurs
# Important : changez-le si vous voulez ignorer l'historique d'offset sauvegardé.
GROUP_ID = 'permanent-console-listener-001'

# 4. Où commencer la lecture : 'earliest' (début) ou 'latest' (nouveaux messages)
# 'latest' est souvent préférable pour une écoute en temps réel.
AUTO_OFFSET_RESET = 'latest' 

# ==========================================================

def consume_permanently():
    """
    Crée un consommateur Kafka et entre dans une boucle d'attente infinie.
    """
    
    # Configuration du Consommateur
    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': AUTO_OFFSET_RESET,
        'enable.auto.commit': False  # Ne pas valider les offsets pour les tests
    }

    try:
        consumer = Consumer(conf)
    except Exception as e:
        print(f"❌ Erreur de configuration Kafka : {e}", file=sys.stderr)
        sys.exit(1)

    # Abonnement au Topic
    try:
        consumer.subscribe([TOPIC_NAME])
        print("=" * 60)
        print(f"✅ Consommateur PERMANENT démarré. Écoute du topic '{TOPIC_NAME}'...")
        print(f"   Broker: {BOOTSTRAP_SERVERS} | Group ID: {GROUP_ID}")
        print("   -> ATTENTE DE MESSAGES...")
        print("=" * 60)
    except Exception as e:
        print(f"❌ Erreur lors de l'abonnement au topic : {e}", file=sys.stderr)
        consumer.close()
        sys.exit(1)

    # Boucle de Consommation Permanente
    try:
        while True:
            # poll(timeout) : Attend un message. Si aucun n'arrive dans le timeout, retourne None.
            msg = consumer.poll(timeout=1.0) 

            # Si le timeout est atteint (aucune réception), on continue la boucle.
            if msg is None:
                continue
            
            # Gestion des erreurs
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Condition normale : fin de partition si on lit l'historique
                    continue 
                # Erreur fatale
                raise KafkaException(msg.error())
            
            # Message Reçu (Code d'affichage agnostique)
            key = msg.key().decode('utf-8', errors='ignore') if msg.key() else 'None'
            value = msg.value().decode('utf-8', errors='ignore') if msg.value() else 'None'
            
            print("\n" + "#" * 5)
            print(f"📡 NOUVEAU MESSAGE | Topic: {msg.topic()} | Part: {msg.partition()} | Offset: {msg.offset()}")
            print(f"🔑 Clé:   {key}")
            print(f"📦 Valeur: {value}")
                
    except KeyboardInterrupt:
        print("\n\n👋 Arrêt demandé par l'utilisateur.")
    except Exception as e:
        print(f"\n❌ Erreur fatale de consommation : {e}", file=sys.stderr)
        
    finally:
        # Nettoyage
        consumer.close()
        print("Consommateur Kafka fermé.")

if __name__ == "__main__":
    consume_permanently()