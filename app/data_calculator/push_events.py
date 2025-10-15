import json
import time
import requests
from clickhouse_driver import Client
import os
from dotenv import load_dotenv

load_dotenv()

# === CONFIGURATION ===
FILE_PATH = "user_events2.json"  # Chemin vers ton fichier
ENDPOINT = "http://127.0.0.1:8002/events/collect"
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_DB = "events_db"
CLICKHOUSE_TABLE = "user_events"
BATCH_SIZE = 1000
SLEEP_SECONDS = 3

def get_clickhouse_count():
    """Retourne le nombre total d'enregistrements dans la table ClickHouse."""
    try:
        client = Client(
            host=os.getenv("CLICKHOUSE_HOST"),
            port=int(os.getenv("CLICKHOUSE_PORT2")),
            database=os.getenv("CLICKHOUSE_DATABASE"),
            user=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD")
            )
        result = client.execute(f"SELECT count() FROM {CLICKHOUSE_TABLE}")
        return result[0][0]
    except Exception as e:
        print(f"⚠️ Erreur lors de la connexion à ClickHouse : {e}")
        return None

def send_batch(events_batch, batch_index):
    """Envoie un batch d'événements à l'endpoint FastAPI."""
    payload = {"events": events_batch}
    try:
        response = requests.post(ENDPOINT, json=payload, timeout=30)
        print(f"✅ Batch {batch_index} envoyé ({len(events_batch)} événements) - "
              f"Code HTTP: {response.status_code}")
        if response.status_code != 200:
            print("⚠️ Réponse du serveur:", response.text)
    except Exception as e:
        print(f"❌ Erreur lors de l'envoi du batch {batch_index}: {e}")

def main():
    # Chargement du fichier JSON
    with open(FILE_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    events = data["events"]
    total_events = len(events)
    print(f"📦 Total d'événements à envoyer: {total_events}\n")

    for start in range(0, total_events, BATCH_SIZE):
        end = min(start + BATCH_SIZE, total_events)
        batch = events[start:end]
        batch_index = start // BATCH_SIZE + 1

        print(f"🚀 Envoi du batch {batch_index} : {start
        } → {end - 1}")
        send_batch(batch, batch_index)

        # Vérification du total dans ClickHouse après envoi
        count = get_clickhouse_count()
        if count is not None:
            print(f"📊 Total actuel dans ClickHouse: {count} enregistrements")
        else:
            print("⚠️ Impossible de récupérer le nombre total dans ClickHouse")

        # Pause entre les batchs
        if end < total_events:
            print(f"⏳ Pause de {SLEEP_SECONDS} secondes avant le prochain batch...\n")
            time.sleep(SLEEP_SECONDS)

    print("\n🎯 Tous les événements ont été envoyés et comptés avec succès !")

if __name__ == "__main__":
    main()
