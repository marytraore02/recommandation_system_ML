import json
import csv
import random
from datetime import datetime, timedelta, timezone
import uuid
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import os
from dotenv import load_dotenv


load_dotenv()

# --- Configuration de la connexion PostgreSQL ---
# !!! REMPLACEZ PAR VOS PROPRES INFORMATIONS !!!
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT")
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class EventGenerator:
    def __init__(self):
        self.users = []
        self.cagnottes = []
        self.categories = []
        self.posts = []
        self.ressources = []
        self.event_types = [
            "video_view", "video_share",
            "video_favorite", "video_skip",
            "cagnotte_detail_view", "video_replay"
        ]
        self.event_counter = 0

        # Charger les données depuis la base de données au démarrage
        self.load_data_from_db()

    def load_data_from_db(self):
        """
        Récupère les données des nouvelles tables depuis la base de données PostgreSQL.
        """
        conn = None
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor(cursor_factory=RealDictCursor)

            logger.info("🔍 Connexion à la base de données et chargement des données...")

            # 1. Charger les utilisateurs
            cur.execute("SELECT id, phone FROM users;")
            self.users = cur.fetchall()
            logger.info(f"✅ {len(self.users)} utilisateurs chargés.")
            if not self.users:
                logger.warning("Aucun utilisateur trouvé. La génération d'événements sera limitée aux utilisateurs anonymes.")

            # 2. Charger les catégories
            cur.execute("SELECT id FROM categories;")
            self.categories = cur.fetchall()
            logger.info(f"✅ {len(self.categories)} catégories chargées.")
            if not self.categories:
                logger.error("Aucune catégorie trouvée. La génération d'événements échouera.")
                return False

            # 3. Charger les cagnottes
            cur.execute("SELECT id, id_categorie, pays FROM cagnottes;")
            self.cagnottes = cur.fetchall()
            logger.info(f"✅ {len(self.cagnottes)} cagnottes chargées.")
            if not self.cagnottes:
                logger.error("Aucune cagnotte trouvée.")
                return False

            # 4. Charger les posts de cagnottes
            cur.execute("""
                SELECT cp.id, cp.id_cagnotte
                FROM cagnotte_posts cp
            """)
            self.posts = cur.fetchall()
            logger.info(f"✅ {len(self.posts)} posts de cagnottes chargés.")

            # 5. Charger les ressources vidéo avec leurs métadonnées (sans author_id)
            cur.execute("""
                SELECT r.id, r.file, r.type, r.reference, r.thumbnail_url, 
                    r.duration, r.width, r.height, r.file_size, 
                    r.order_index, r.alt_text, r.mime_type,
                    cp.id_cagnotte, cp.id_author,
                    c.id_categorie, c.pays
                FROM ressources r
                JOIN cagnotte_posts cp ON r.reference = cp.id
                JOIN cagnottes c ON cp.id_cagnotte = c.id
                WHERE r.type = 'POST_VIDEO'
            """)
            self.ressources = cur.fetchall()
            logger.info(f"✅ {len(self.ressources)} ressources vidéo chargées.")
            
            if not self.ressources:
                logger.error("Aucune ressource vidéo n'a été trouvée. La génération d'événements vidéo sera impossible.")
                return False

        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(f"❌ Erreur lors du chargement des données depuis la base : {error}")
            return False
        finally:
            if conn:
                conn.close()
        return True

    def generate_timestamp(self, start_date, end_date):
        """Génère un timestamp aléatoire dans l'intervalle spécifié"""
        delta = end_date - start_date
        total_seconds = int(delta.total_seconds())
        random_seconds = random.randint(0, total_seconds)
        return start_date + timedelta(seconds=random_seconds)

    def get_random_video(self):
        """Sélectionne une ressource vidéo aléatoire avec ses informations de cagnotte"""
        if not self.ressources:
            return None
        
        return random.choice(self.ressources)

    def get_random_cagnotte(self):
        """Sélectionne une cagnotte aléatoire"""
        if not self.cagnottes:
            return None
        
        return random.choice(self.cagnottes)

    def get_user_for_event(self):
        """
        Sélectionne un utilisateur (authentifié ou anonyme) pour l'événement.
        """
        # 25% de chance d'avoir un utilisateur anonyme
        if random.random() < 0.25:
            return {
                "type": "anonymous",
                "session_id": str(uuid.uuid4())
            }
        else:
            if not self.users:
                return {
                    "type": "anonymous",
                    "session_id": str(uuid.uuid4())
                }
            
            user = random.choice(self.users)
            return {
                "id": str(user["id"]),
                "phone": user["phone"],
                "type": "authenticated"
            }

    def generate_video_view_event(self, user_info, timestamp):
        """Génère un événement de vue de vidéo basé sur les nouvelles tables"""
        video_resource = self.get_random_video()
        if not video_resource:
            return None
        
        # Durée de visionnage aléatoire (max = durée de la vidéo)
        video_duration = video_resource["duration"] or 60  # Valeur par défaut si NULL
        watch_duration = random.randint(5, video_duration)
        
        event = {
            "event_id": self.event_counter,
            "event_type": "video_view",
            "timestamp": timestamp.isoformat(),
            "user_type": user_info["type"],
            "cagnotte_id": str(video_resource["id_cagnotte"]),
            "video_id": str(video_resource["id"]),
            "post_id": str(video_resource["reference"]),  # ID du post
            "id_categorie": str(video_resource["id_categorie"]),
            "pays": video_resource["pays"],
            "data": {
                "video_duration": video_duration,
                "watch_duration": watch_duration,
                "completion_rate": round(watch_duration / video_duration, 2),
                "device": random.choice(["mobile", "tablet", "desktop"]),
                "video_width": video_resource["width"],
                "video_height": video_resource["height"],
                "file_size": video_resource["file_size"],
                "mime_type": video_resource["mime_type"],
                "thumbnail_url": video_resource["thumbnail_url"]
            }
        }
        
        # Ajouter user_id et phone pour les utilisateurs authentifiés
        # Pour les anonymes, ajouter session_id seulement
        if user_info["type"] == "anonymous":
            event["session_id"] = user_info["session_id"]
        else:
            event["user_id"] = user_info["id"]
            event["phone"] = user_info["phone"]
        
        return event

    def generate_interaction_event(self, user_info, event_type, timestamp):
        """Génère un événement d'interaction (share, favorite, skip) basé sur les nouvelles tables"""
        video_resource = self.get_random_video()
        if not video_resource:
            return None

        event = {
            "event_id": self.event_counter,
            "event_type": event_type,
            "timestamp": timestamp.isoformat(),
            "user_type": user_info["type"],
            "cagnotte_id": str(video_resource["id_cagnotte"]),
            "video_id": str(video_resource["id"]),
            "post_id": str(video_resource["reference"]),
            "id_categorie": str(video_resource["id_categorie"]),
            "pays": video_resource["pays"],
            "data": {}
        }
        
        # Ajouter user_id et phone pour les utilisateurs authentifiés
        # Pour les anonymes, ajouter session_id seulement
        if user_info["type"] == "anonymous":
            event["session_id"] = user_info["session_id"]
        else:
            event["user_id"] = user_info["id"]
            event["phone"] = user_info["phone"]
        
        if event_type == "video_share":
            event["data"]["platform"] = random.choice(["whatsapp", "facebook", "twitter", "telegram"])
        elif event_type == "video_skip":
            event["data"]["skip_time"] = random.randint(1, 3)
        
        return event

    def generate_cagnotte_detail_event(self, user_info, timestamp):
        """Génère un événement de vue détaillée d'une cagnotte basé sur les nouvelles tables"""
        cagnotte = self.get_random_cagnotte()
        if not cagnotte:
            return None
        
        event = {
            "event_id": self.event_counter,
            "event_type": "cagnotte_detail_view",
            "timestamp": timestamp.isoformat(),
            "user_type": user_info["type"],
            "cagnotte_id": str(cagnotte["id"]),
            "id_categorie": str(cagnotte["id_categorie"]),
            "pays": cagnotte["pays"],
            "data": {
                "view_duration": random.randint(10, 300),
                "clicked_from": random.choice(["video", "search", "recommendation", "trending"])
            }
        }
        
        # Ajouter user_id et phone pour les utilisateurs authentifiés
        # Pour les anonymes, ajouter session_id seulement
        if user_info["type"] == "anonymous":
            event["session_id"] = user_info["session_id"]
        else:
            event["user_id"] = user_info["id"]
            event["phone"] = user_info["phone"]
        
        return event

    def generate_events(self, num_events, start_date=None, end_date=None):
        """Génère un ensemble d'événements simulés dans un intervalle de temps"""
        events = []
        
        if start_date is None:
            start_date = datetime.now(timezone.utc) - timedelta(days=7)
        if end_date is None:
            end_date = datetime.now(timezone.utc)
        
        if start_date >= end_date:
            raise ValueError("La date de début doit être antérieure à la date de fin")
        
        logger.info(f"📅 Génération de {num_events} événements entre {start_date.isoformat()} et {end_date.isoformat()}")
        
        for i in range(num_events):
            self.event_counter = i
            user_info = self.get_user_for_event()
            timestamp = self.generate_timestamp(start_date, end_date)
            
            event_weights = {
                "video_view": 0.51, "video_share": 0.13, "video_favorite": 0.10,
                "video_skip": 0.19, "cagnotte_detail_view": 0.09, "video_replay": 0.04
            }
            
            event_type = random.choices(list(event_weights.keys()), weights=list(event_weights.values()))[0]
            
            if event_type == "video_view":
                event = self.generate_video_view_event(user_info, timestamp)
            elif event_type == "cagnotte_detail_view":
                event = self.generate_cagnotte_detail_event(user_info, timestamp)
            else:
                event = self.generate_interaction_event(user_info, event_type, timestamp)
            
            if event:
                events.append(event)
        
        events.sort(key=lambda x: x["timestamp"])
        
        for idx, event in enumerate(events):
            event["event_id"] = idx
        
        return events

    def flatten_event_for_export(self, event):
        """Aplatit la structure d'un événement pour l'export CSV/Excel"""
        flat_event = {
            "event_id": event["event_id"],
            "event_type": event["event_type"],
            "timestamp": event["timestamp"],
            "user_type": event["user_type"],
            "cagnotte_id": event.get("cagnotte_id", ""),
            "video_id": event.get("video_id", ""),
            "post_id": event.get("post_id", ""),
            "id_categorie": event.get("id_categorie", ""),
        }
        
        # Ajouter user_id et phone pour les utilisateurs authentifiés
        # Ajouter session_id pour les anonymes
        if "user_id" in event:
            flat_event["user_id"] = event["user_id"]
        if "phone" in event:
            flat_event["phone"] = event["phone"]
        if "session_id" in event:
            flat_event["session_id"] = event["session_id"]
        
        if "data" in event:
            for key, value in event["data"].items():
                flat_event[f"data_{key}"] = value
        
        return flat_event

    def save_to_json(self, events, filename="user_events.json"):
        """Sauvegarde en format JSON"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump({
                "metadata": {
                    "total_events": len(events),
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "users_count": len(self.users),
                    "cagnottes_count": len(self.cagnottes),
                    "posts_count": len(self.posts),
                    "video_resources_count": len(self.ressources),
                    "anonymous_events": len([e for e in events if e.get("user_type") == "anonymous"]),
                    "authenticated_events": len([e for e in events if e.get("user_type") == "authenticated"])
                },
                "users": [{"id": str(u["id"]), "phone": u["phone"]} for u in self.users],
                "cagnottes": [{"id": str(c["id"]), "id_categorie": str(c["id_categorie"])} for c in self.cagnottes],
                "categories": [{"id": str(c["id"])} for c in self.categories],
                "video_resources_summary": {
                    "total_videos": len(self.ressources),
                    "avg_duration": sum([r["duration"] or 0 for r in self.ressources]) / len(self.ressources) if self.ressources else 0
                },
                "events": events
            }, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"✅ {len(events)} événements sauvegardés dans {filename} (JSON)")

    def save_to_csv(self, events, filename="user_events.csv"):
        """Sauvegarde en format CSV"""
        if not events:
            logger.warning("⚠️  Aucun événement à sauvegarder")
            return
        
        flat_events = [self.flatten_event_for_export(event) for event in events]
        
        all_fields = set()
        for event in flat_events:
            all_fields.update(event.keys())
        
        fieldnames = sorted(list(all_fields))
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(flat_events)
        
        logger.info(f"✅ {len(events)} événements sauvegardés dans {filename} (CSV)")

    def export_events(self, events, output_format="json", filename=None, pg_params=None, table_name="user_events"):
        """Exporte les événements dans le format spécifié"""
        if not events:
            logger.warning("⚠️  Aucun événement à exporter")
            return
        
        if output_format.lower() == "json":
            filename = filename or "user_events2.json"
            self.save_to_json(events, filename)
            
        elif output_format.lower() == "csv":
            filename = filename or "user_events2.csv"
            self.save_to_csv(events, filename)
            
        elif output_format.lower() in ["postgresql", "postgres", "pg"]:
            if not pg_params:
                logger.error("❌ Paramètres de connexion PostgreSQL requis")
                return
            self.save_to_postgresql(events, pg_params, table_name)
            
        else:
            logger.error(f"❌ Format '{output_format}' non supporté. Formats disponibles: json, csv, postgresql")

def parse_date(date_string):
    """Parse une chaîne de date dans différents formats"""
    formats = [
        "%Y-%m-%d", "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y", "%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S"
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_string, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    
    raise ValueError(f"Format de date non reconnu: {date_string}")

def main():
    print("🚀 Générateur d'événements utilisateurs - Version adaptée aux nouvelles tables")
    print("=" * 90)
    
    generator = EventGenerator()
    
    if not generator.users or not generator.cagnottes or not generator.categories or not generator.ressources:
        print("\n❌ Impossible de générer des événements : certaines tables sont vides ou mal configurées.")
        print("Vérifiez que vous avez des données dans les tables : users, cagnottes, categories, cagnotte_posts, ressources")
        return
        
    cutoff_time = datetime.now() - timedelta(hours=48)

    config = {
        "num_events": 10000,
        "start_date": cutoff_time.strftime("%Y-%m-%d %H:%M:%S"),
        "end_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "output_format": "json",
        "filename": None,
        "pg_params": DB_CONFIG
    }
    
    try:
        start_date = parse_date(config["start_date"])
        end_date = parse_date(config["end_date"])
        
        events = generator.generate_events(
            num_events=config["num_events"],
            start_date=start_date,
            end_date=end_date
        )
        
        generator.export_events(
            events=events,
            output_format=config["output_format"],
            filename=config["filename"],
            pg_params=config["pg_params"]
        )
        
        # Statistiques détaillées
        event_types_count = {}
        users_activity = {}
        categories_count = {}
        user_types_count = {}
        
        for event in events:
            event_type = event["event_type"]
            id_categorie = event.get("id_categorie", "Inconnue")
            user_type = event["user_type"]
            
            # Compter par identifiant d'utilisateur (user_id ou session_id)
            user_identifier = event.get("user_id") or event.get("session_id")
            
            event_types_count[event_type] = event_types_count.get(event_type, 0) + 1
            users_activity[user_identifier] = users_activity.get(user_identifier, 0) + 1
            categories_count[id_categorie] = categories_count.get(id_categorie, 0) + 1
            user_types_count[user_type] = user_types_count.get(user_type, 0) + 1
        
        print(f"\n📊 Statistiques ({len(events)} événements générés):")
        print("=" * 50)
        print("🎯 Par type d'événement:")
        for event_type, count in sorted(event_types_count.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / len(events)) * 100
            print(f"  {event_type:20} : {count:4} événements ({percentage:5.1f}%)")
        
        print("\n👤 Par type d'utilisateur:")
        for user_type, count in sorted(user_types_count.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / len(events)) * 100
            print(f"  {user_type:20} : {count:4} événements ({percentage:5.1f}%)")
        
        print(f"\n📂 Catégories impliquées: {len(categories_count)}")
        
        print(f"\n📅 Période: {start_date.isoformat()} → {end_date.isoformat()}")
        print(f"⏱️  Durée: {(end_date - start_date).days} jours")
        print(f"👥 Utilisateurs en base: {len(generator.users)}")
        print(f"👻 Sessions anonymes générées: {len(set([e.get('session_id') for e in events if e.get('user_type') == 'anonymous' and e.get('session_id')]))}")
        print(f"🏦 Cagnottes: {len(generator.cagnottes)}")
        print(f"📝 Posts: {len(generator.posts)}")
        print(f"🎥 Ressources vidéo: {len(generator.ressources)}")
        
        if events:
            print(f"\n🎯 Exemple d'événement d'utilisateur authentifié:")
            print("=" * 55)
            auth_event = next((e for e in events if e.get("user_type") == "authenticated"), events[0])
            print(json.dumps(auth_event, indent=2, ensure_ascii=False, default=str))
            
            anon_events = [e for e in events if e.get("user_type") == "anonymous"]
            if anon_events:
                print(f"\n👻 Exemple d'événement d'utilisateur anonyme:")
                print("=" * 50)
                print(json.dumps(anon_events[0], indent=2, ensure_ascii=False, default=str))
            
    except Exception as e:
        logger.error(f"❌ Erreur: {e}")

if __name__ == "__main__":
    main()