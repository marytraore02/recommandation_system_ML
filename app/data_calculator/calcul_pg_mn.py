import json
import redis
import psycopg2
import pandas as pd
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import math
import logging
from minio import Minio
from dotenv import load_dotenv
import os
from minio.error import S3Error

load_dotenv()

MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnhancedPopularityAnalyzer:
    def __init__(self, redis_config=None, pg_config=None, minio_config=None):
        """
        Analyseur de popularité étendu avec analytics par pays/catégorie
        
        Args:
            redis_config: Configuration Redis pour le cache
            pg_config: Configuration PostgreSQL pour la base de données
        """
        self.redis_client = None
        self.pg_connection = None
        self.minio_client = None # Ajout de l'attribut minio_client
        self.minio_bucket_name = None # Pour stocker le nom du bucket
        
        # Configuration des poids pour le calcul de popularité
        self.event_weights = {
            "video_view": 1.0,
            "video_share": 3.0,
            "video_favorite": 2.5,
            "video_replay": 2.0,
            "video_skip": -0.5,
            "cagnotte_detail_view": 1.5
        }
        
        # Facteur de décroissance temporelle
        self.time_decay_hours = 24
        
        # Configuration des connexions
        if redis_config:
            try:
                self.redis_client = redis.Redis(**redis_config)
                self.redis_client.ping()
                logger.info("✅ Connexion Redis établie")
            except Exception as e:
                logger.error(f"❌ Erreur connexion Redis: {e}")
        
        if pg_config:
            try:
                self.pg_connection = psycopg2.connect(**pg_config)
                logger.info("✅ Connexion PostgreSQL établie")
            except Exception as e:
                logger.error(f"❌ Erreur connexion PostgreSQL: {e}")

        if minio_config:
            try:
                # Copier la config pour ne pas modifier l'original
                client_config = minio_config.copy()
                self.minio_bucket_name = client_config.pop('bucket_name')
                
                # S'assurer que le bucket_name existe avant de continuer
                if not self.minio_bucket_name:
                    raise ValueError("Le 'bucket_name' est manquant dans la configuration MinIO.")

                self.minio_client = Minio(**client_config)
                
                found = self.minio_client.bucket_exists(self.minio_bucket_name)
                if found:
                    logger.info(f"✅ Connexion MinIO établie et bucket '{self.minio_bucket_name}' trouvé.")
                else:
                    logger.error(f"❌ Bucket MinIO '{self.minio_bucket_name}' non trouvé.")
                    self.minio_client = None
            except (S3Error, ValueError, KeyError) as e: # Capturer les erreurs spécifiques
                logger.error(f"❌ Erreur de configuration ou de connexion MinIO: {e}")
                self.minio_client = None # S'assurer que le client est invalidé


    def load_events_from_json(self, json_file):
        """Charge les événements depuis un fichier JSON"""
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                events = data.get('events', [])
                logger.info(f"📁 {len(events)} événements chargés depuis le fichier JSON {json_file}")
                return events
        except Exception as e:
            logger.error(f"❌ Erreur lors du chargement du JSON: {e}")
            return []
    
    def load_events_from_minio(self, hours_back=24):
        """Charge les événements des X dernières heures depuis MinIO."""
        if not self.minio_client:
            logger.error("❌ Pas de connexion MinIO disponible.")
            return []

        logger.info(f"⬇️ Chargement des événements des {hours_back} dernières heures depuis MinIO...")
        
        # 1. Déterminer les préfixes horaires à scanner
        # Le format est : events/year=YYYY/month=MM/day=DD/hour=HH/
        now = datetime.now(timezone.utc)
        prefixes_to_scan = set()
        for i in range(hours_back + 1): # +1 pour inclure l'heure actuelle
            target_time = now - timedelta(hours=i)
            prefix = (
                f"events/year={target_time.year}/"
                f"month={target_time.month:02d}/"
                f"day={target_time.day:02d}/"
                f"hour={target_time.hour:02d}/"
            )
            prefixes_to_scan.add(prefix)

        all_events = []
        # 2. Lister et télécharger les objets pour chaque préfixe
        for prefix in prefixes_to_scan:
            try:
                objects = self.minio_client.list_objects(
                    self.minio_bucket_name, prefix=prefix, recursive=True
                )
                for obj in objects:
                    if obj.object_name.endswith('.jsonl'):
                        logger.info(f"  -> Lecture du fichier : {obj.object_name}")
                        response = self.minio_client.get_object(self.minio_bucket_name, obj.object_name)
                        content_bytes = response.read()
                        
                        # 3. Parser le contenu JSONL (un JSON par ligne)
                        content_str = content_bytes.decode('utf-8')
                        for line in content_str.strip().split('\n'):
                            if line:
                                try:
                                    all_events.append(json.loads(line))
                                except json.JSONDecodeError:
                                    logger.warning(f"⚠️ Ligne JSON invalide ignorée dans {obj.object_name}")
                        response.close()
                        response.release_conn()
            except Exception as e:
                logger.error(f"❌ Erreur lors de la lecture du préfixe '{prefix}' depuis MinIO: {e}")

        logger.info(f"📁 {len(all_events)} événements chargés depuis MinIO.")
        return all_events
    
    def parse_timestamp(self, timestamp_str):
        """Parse un timestamp de manière robuste"""
        if not timestamp_str:
            return None
        
        timestamp_formats = [
            "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S+00:00", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d"
        ]
        
        clean_timestamp = str(timestamp_str).strip()
        
        try:
            if clean_timestamp.endswith('Z'):
                clean_timestamp = clean_timestamp[:-1] + '+00:00'
            return datetime.fromisoformat(clean_timestamp)
        except:
            pass
        
        for fmt in timestamp_formats:
            try:
                return datetime.strptime(clean_timestamp, fmt)
            except ValueError:
                continue
        
        try:
            import pandas as pd
            return pd.to_datetime(clean_timestamp).to_pydatetime()
        except:
            pass
        
        logger.warning(f"⚠️ Impossible de parser le timestamp: {timestamp_str}")
        return None

    def filter_events_by_timerange(self, events, hours_back=24):
        """Filtre les événements dans une plage temporelle"""
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours_back)
        
        filtered_events = []
        skipped_count = 0
        
        for event in events:
            event_time = self.parse_timestamp(event.get('timestamp'))
            if event_time:
                if event_time >= cutoff_time:
                    filtered_events.append(event)
            else:
                skipped_count += 1
        
        if skipped_count > 0:
            logger.warning(f"⚠️ {skipped_count} événements ignorés à cause de timestamps invalides")
        
        logger.info(f"📅 {len(filtered_events)} événements dans les dernières {hours_back}h")
        return filtered_events

    def calculate_time_decay_factor(self, event_timestamp):
        """Calcule le facteur de décroissance temporelle"""
        event_time = self.parse_timestamp(event_timestamp)
        if not event_time:
            return 0.01
        
        try:
            hours_ago = (datetime.now() - event_time).total_seconds() / 3600
            decay_factor = math.exp(-hours_ago / self.time_decay_hours)
            return max(decay_factor, 0.01)
        except:
            return 0.01

    def calculate_popularity_by_geography(self, events):
        """Calcule la popularité par pays"""
        country_scores = defaultdict(lambda: {
            'total_score': 0.0,
            'video_count': 0,
            'unique_users': set(),
            'total_events': 0,
            'event_breakdown': defaultdict(int),
            'avg_completion_rate': [],
            'top_videos': defaultdict(float)
        })
        
        for event in events:
            country = event.get('pays', 'Unknown')
            if not country:
                country = 'Unknown'
                
            video_id = event.get('video_id')
            event_type = event['event_type']
            user_id = event.get('user_id') or event.get('session_id', 'anonymous')
            timestamp = event['timestamp']
            
            weight = self.event_weights.get(event_type, 0)
            time_factor = self.calculate_time_decay_factor(timestamp)
            weighted_score = weight * time_factor
            
            country_data = country_scores[country]
            country_data['total_score'] += weighted_score
            country_data['unique_users'].add(user_id)
            country_data['total_events'] += 1
            country_data['event_breakdown'][event_type] += 1
            
            if video_id:
                country_data['top_videos'][video_id] += weighted_score
            
            if event_type == 'video_view' and 'data' in event:
                completion_rate = event['data'].get('completion_rate', 0)
                if completion_rate > 0:
                    country_data['avg_completion_rate'].append(completion_rate)
        
        # Finaliser les calculs
        final_scores = {}
        for country, data in country_scores.items():
            unique_user_count = len(data['unique_users'])
            avg_completion = sum(data['avg_completion_rate']) / len(data['avg_completion_rate']) if data['avg_completion_rate'] else 0
            top_videos = sorted(data['top_videos'].items(), key=lambda x: x[1], reverse=True)[:10]
            
            final_scores[country] = {
                'country': country,
                'popularity_score': round(data['total_score'], 2),
                'unique_users': unique_user_count,
                'total_events': data['total_events'],
                'video_count': len(data['top_videos']),
                'event_breakdown': dict(data['event_breakdown']),
                'avg_completion_rate': round(avg_completion, 2),
                'top_videos': [{'video_id': vid, 'score': round(score, 2)} for vid, score in top_videos],
                'last_updated': datetime.now().isoformat()
            }
        
        return final_scores

    def calculate_popularity_by_category(self, events):
        """Calcule la popularité par catégorie"""
        category_scores = defaultdict(lambda: {
            'total_score': 0.0,
            'video_count': 0,
            'unique_users': set(),
            'total_events': 0,
            'event_breakdown': defaultdict(int),
            'avg_completion_rate': [],
            'top_videos': defaultdict(float),
            'countries': set()
        })
        
        for event in events:
            category = event.get('id_categorie', 'Unknown')
            if not category:
                category = 'Unknown'
                
            video_id = event.get('video_id')
            event_type = event['event_type']
            user_id = event.get('user_id') or event.get('session_id', 'anonymous')
            timestamp = event['timestamp']
            country = event.get('pays', 'Unknown')
            
            weight = self.event_weights.get(event_type, 0)
            time_factor = self.calculate_time_decay_factor(timestamp)
            weighted_score = weight * time_factor
            
            category_data = category_scores[category]
            category_data['total_score'] += weighted_score
            category_data['unique_users'].add(user_id)
            category_data['total_events'] += 1
            category_data['event_breakdown'][event_type] += 1
            category_data['countries'].add(country)
            
            if video_id:
                category_data['top_videos'][video_id] += weighted_score
            
            if event_type == 'video_view' and 'data' in event:
                completion_rate = event['data'].get('completion_rate', 0)
                if completion_rate > 0:
                    category_data['avg_completion_rate'].append(completion_rate)
        
        # Finaliser les calculs
        final_scores = {}
        for category, data in category_scores.items():
            unique_user_count = len(data['unique_users'])
            avg_completion = sum(data['avg_completion_rate']) / len(data['avg_completion_rate']) if data['avg_completion_rate'] else 0
            top_videos = sorted(data['top_videos'].items(), key=lambda x: x[1], reverse=True)[:5]
            
            final_scores[category] = {
                'category': category,
                'popularity_score': round(data['total_score'], 2),
                'unique_users': unique_user_count,
                'total_events': data['total_events'],
                'video_count': len(data['top_videos']),
                'countries_count': len(data['countries']),
                'countries': list(data['countries']),
                'event_breakdown': dict(data['event_breakdown']),
                'avg_completion_rate': round(avg_completion, 2),
                'top_videos': [{'video_id': vid, 'score': round(score, 2)} for vid, score in top_videos],
                'last_updated': datetime.now().isoformat()
            }
        
        return final_scores

    def analyze_comprehensive(self, events_source=None, source_type="json", hours_back=24):
        """Analyse comprehensive avec toutes les métriques"""
        logger.info("🚀 Début de l'analyse comprehensive")
        
        # Charger les événements
        all_events = []
        if source_type == "json":
            # Si la source est JSON, events_source ne doit pas être vide
            if not events_source:
                logger.error("❌ Pour source_type='json', le paramètre 'events_source' (chemin du fichier) est requis.")
                return
            all_events = self.load_events_from_json(events_source)
        elif source_type == "minio":
            all_events = self.load_events_from_minio(hours_back=hours_back) 
        else:
            logger.error(f"❌ Type de source non supporté: {source_type}")
            return

        recent_events = self.filter_events_by_timerange(all_events, hours_back=hours_back)
        
        if not recent_events:
            logger.warning("⚠️ Aucun événement récent à analyser après filtrage.")
            return
            
        results = {}
        
        # 1. Analyse par pays
        logger.info("🌍 Calcul de la popularité par pays...")
        results['popularity_by_country'] = self.calculate_popularity_by_geography(recent_events)
        
        # 2. Analyse par catégorie
        logger.info("📂 Calcul de la popularité par catégorie...")
        results['popularity_by_category'] = self.calculate_popularity_by_category(recent_events)
        
        # # 3. Analytics d'engagement utilisateur
        # logger.info("👥 Calcul des analytics d'engagement...")
        # results['user_engagement'] = self.calculate_user_engagement_analytics(recent_events)
        
        # # 4. Analytics de performance de contenu
        # logger.info("🎥 Calcul des analytics de contenu...")
        # results['content_performance'] = self.calculate_content_performance_analytics(recent_events)
        
        # 5. Tendances par pays et catégorie
        logger.info("📈 Calcul des tendances par géographie et catégorie...")
        results['trending_by_country'] = self.calculate_trending_by_geography(all_events)
        results['trending_by_category'] = self.calculate_trending_by_category(all_events)
        
        # Afficher les résultats
        self.display_comprehensive_results(results)
        
        return results

    def calculate_trending_by_geography(self, events, recent_hours=1, comparison_hours=2):
        """Calcule les tendances par pays"""
        # now = self.parse_timestamp("2025-08-29T13:00:00")
        now = datetime.now(timezone.utc) 
        recent_cutoff = now - timedelta(hours=recent_hours)
        comparison_start = now - timedelta(hours=recent_hours + comparison_hours)
        comparison_end = recent_cutoff
        
        recent_events = []
        comparison_events = []
        
        for event in events:
            event_time = self.parse_timestamp(event.get('timestamp'))
            if not event_time:
                continue
            
            if event_time >= recent_cutoff:
                recent_events.append(event)
            elif comparison_start <= event_time < comparison_end:
                comparison_events.append(event)
        
        recent_scores = self.calculate_popularity_by_geography(recent_events)
        comparison_scores = self.calculate_popularity_by_geography(comparison_events)
        
        trending_scores = {}
        all_countries = set(list(recent_scores.keys()) + list(comparison_scores.keys()))
        
        for country in all_countries:
            recent_score = recent_scores.get(country, {}).get('popularity_score', 0)
            comparison_score = comparison_scores.get(country, {}).get('popularity_score', 0)
            
            if comparison_score == 0:
                velocity = 100.0 if recent_score > 0 else 0.0
            else:
                velocity = ((recent_score - comparison_score) / comparison_score) * 100
            
            trending_score = recent_score + (velocity * 0.1)
            
            trending_scores[country] = {
                'country': country,
                'trending_score': round(trending_score, 2),
                'current_score': recent_score,
                'previous_score': comparison_score,
                'velocity_percent': round(velocity, 1),
                'trend_direction': 'up' if velocity > 10 else 'down' if velocity < -10 else 'stable'
            }
        
        return trending_scores

    def calculate_trending_by_category(self, events, recent_hours=1, comparison_hours=2):
        """Calcule les tendances par catégorie"""
        # now = self.parse_timestamp("2025-08-29T13:00:00")
        now = datetime.now(timezone.utc) 
        recent_cutoff = now - timedelta(hours=recent_hours)
        comparison_start = now - timedelta(hours=recent_hours + comparison_hours)
        comparison_end = recent_cutoff
        
        recent_events = []
        comparison_events = []
        
        for event in events:
            event_time = self.parse_timestamp(event.get('timestamp'))
            if not event_time:
                continue
            
            if event_time >= recent_cutoff:
                recent_events.append(event)
            elif comparison_start <= event_time < comparison_end:
                comparison_events.append(event)
        
        recent_scores = self.calculate_popularity_by_category(recent_events)
        comparison_scores = self.calculate_popularity_by_category(comparison_events)
        
        trending_scores = {}
        all_categories = set(list(recent_scores.keys()) + list(comparison_scores.keys()))
        
        for category in all_categories:
            recent_score = recent_scores.get(category, {}).get('popularity_score', 0)
            comparison_score = comparison_scores.get(category, {}).get('popularity_score', 0)
            
            if comparison_score == 0:
                velocity = 100.0 if recent_score > 0 else 0.0
            else:
                velocity = ((recent_score - comparison_score) / comparison_score) * 100
            
            trending_score = recent_score + (velocity * 0.1)
            
            trending_scores[category] = {
                'category': category,
                'trending_score': round(trending_score, 2),
                'current_score': recent_score,
                'previous_score': comparison_score,
                'velocity_percent': round(velocity, 1),
                'trend_direction': 'up' if velocity > 10 else 'down' if velocity < -10 else 'stable'
            }
        
        return trending_scores

    def display_comprehensive_results(self, results, show_top=5):
        """Affiche tous les résultats de l'analyse comprehensive"""
        print("\n" + "="*100)
        print("📊 ANALYSE COMPREHENSIVE DE POPULARITÉ ET TENDANCES")
        print("="*100)
        
        # Popularité par pays
        if 'popularity_by_country' in results:
            print(f"\n🌍 TOP {show_top} PAYS PAR POPULARITÉ:")
            print("-" * 80)
            country_data = sorted(results['popularity_by_country'].values(), 
                                key=lambda x: x['popularity_score'], reverse=True)
            for i, country in enumerate(country_data[:show_top], 1):
                print(f"{i:2d}. {country['country']:20} | "
                      f"Score: {country['popularity_score']:8.1f} | "
                      f"Utilisateurs: {country['unique_users']:4d} | "
                      f"Vidéos: {country['video_count']:3d} | "
                      f"Complétion: {country['avg_completion_rate']:.0%}")
        
        # Popularité par catégorie
        if 'popularity_by_category' in results:
            print(f"\n📂 TOP {show_top} CATÉGORIES PAR POPULARITÉ:")
            print("-" * 80)
            category_data = sorted(results['popularity_by_category'].values(), 
                                 key=lambda x: x['popularity_score'], reverse=True)
            for i, category in enumerate(category_data[:show_top], 1):
                category_name = category['category'][:20] if len(category['category']) > 20 else category['category']
                print(f"{i:2d}. {category_name:20} | "
                      f"Score: {category['popularity_score']:8.1f} | "
                      f"Utilisateurs: {category['unique_users']:4d} | "
                      f"Pays: {category['countries_count']:2d} | "
                      f"Complétion: {category['avg_completion_rate']:.0%}")
        
        # Tendances par pays
        if 'trending_by_country' in results:
            print(f"\n🔥 TOP {show_top} PAYS TENDANCES:")
            print("-" * 80)
            trending_countries = sorted(results['trending_by_country'].values(), 
                                      key=lambda x: x['trending_score'], reverse=True)
            for i, country in enumerate(trending_countries[:show_top], 1):
                trend_icon = "📈" if country['trend_direction'] == 'up' else "📉" if country['trend_direction'] == 'down' else "➡️"
                print(f"{i:2d}. {trend_icon} {country['country']:20} | "
                      f"Score: {country['trending_score']:8.1f} | "
                      f"Vélocité: {country['velocity_percent']:+6.1f}% | "
                      f"Direction: {country['trend_direction']}")
        
        # Tendances par catégorie
        if 'trending_by_category' in results:
            print(f"\n📊 TOP {show_top} CATÉGORIES TENDANCES:")
            print("-" * 80)
            trending_categories = sorted(results['trending_by_category'].values(), 
                                       key=lambda x: x['trending_score'], reverse=True)
            for i, category in enumerate(trending_categories[:show_top], 1):
                trend_icon = "📈" if category['trend_direction'] == 'up' else "📉" if category['trend_direction'] == 'down' else "➡️"
                category_name = category['category'][:20] if len(category['category']) > 20 else category['category']
                print(f"{i:2d}. {trend_icon} {category_name:20} | "
                      f"Score: {category['trending_score']:8.1f} | "
                      f"Vélocité: {category['velocity_percent']:+6.1f}% | "
                      f"Direction: {category['trend_direction']}")
        
        # Top vidéos par performance
        if 'content_performance' in results:
            print(f"\n🎥 TOP {show_top} VIDÉOS PAR QUALITÉ:")
            print("-" * 80)
            top_videos = sorted(results['content_performance'].values(), 
                              key=lambda x: x['quality_score'], reverse=True)
            for i, video in enumerate(top_videos[:show_top], 1):
                print(f"{i:2d}. Video: {video['video_id'][:12]}... | "
                      f"Qualité: {video['quality_score']:6.1f} | "
                      f"Vues: {video['total_views']:4d} | "
                      f"Complétion: {video['avg_completion_rate']:.0%} | "
                      f"Partage: {video['share_rate']:.1%}")
        
        # Segmentation des utilisateurs
        if 'user_engagement' in results and 'user_segments' in results['user_engagement']:
            segments = results['user_engagement']['user_segments']
            print(f"\n👥 SEGMENTATION UTILISATEURS:")
            print("-" * 80)
            print(f"🔥 Engagement élevé: {len(segments.get('high_engagement', []))} utilisateurs")
            print(f"🔶 Engagement moyen: {len(segments.get('medium_engagement', []))} utilisateurs") 
            print(f"🔸 Engagement faible: {len(segments.get('low_engagement', []))} utilisateurs")
            
            if segments.get('high_engagement'):
                print(f"\n🏆 TOP 3 UTILISATEURS ENGAGÉS:")
                for i, user in enumerate(segments['high_engagement'][:3], 1):
                    user_display = user['user_id'][:12] + "..." if len(user['user_id']) > 12 else user['user_id']
                    print(f"   {i}. {user_display} | "
                          f"Score: {user['engagement_score']:6.1f} | "
                          f"Vidéos: {user['videos_watched']:3d} | "
                          f"Événements: {user['total_events']:3d}")
        
        # Performance par device
        if 'user_engagement' in results and 'device_performance' in results['user_engagement']:
            devices = results['user_engagement']['device_performance']
            print(f"\n📱 PERFORMANCE PAR APPAREIL:")
            print("-" * 80)
            for device, data in sorted(devices.items(), key=lambda x: x[1]['total_events'], reverse=True):
                print(f"   {device:12} | "
                      f"Utilisateurs: {data['unique_users']:4d} | "
                      f"Événements: {data['total_events']:5d} | "
                      f"Complétion: {data['avg_completion_rate']:.0%} | "
                      f"Evt/User: {data['events_per_user']:4.1f}")
        
        # Patterns temporels
        if 'user_engagement' in results and 'time_patterns' in results['user_engagement']:
            time_patterns = results['user_engagement']['time_patterns']
            print(f"\n⏰ ACTIVITÉ PAR JOUR DE LA SEMAINE:")
            print("-" * 80)
            for day, hours in time_patterns.items():
                total_events = sum(hours.values())
                peak_hour = max(hours.items(), key=lambda x: x[1])[0] if hours else 0
                print(f"   {day:10} | "
                      f"Total événements: {total_events:5d} | "
                      f"Heure de pointe: {peak_hour:2d}h")

    def store_comprehensive_results(self, results, store_redis=True, store_postgresql=False):
        """Stocke tous les résultats de l'analyse comprehensive"""
        if store_redis and self.redis_client:
            try:
                # Stocker chaque type d'analyse séparément
                for key, data in results.items():
                    redis_key = f"analytics_{key}"
                    self.redis_client.setex(redis_key, 3600, json.dumps(data, ensure_ascii=False, default=str))
                
                # Stocker les métadonnées
                metadata = {
                    'last_analysis': datetime.now().isoformat(),
                    'analysis_types': list(results.keys()),
                    'expires_at': (datetime.now() + timedelta(seconds=3600)).isoformat()
                }
                self.redis_client.setex("comprehensive_analytics_metadata", 3600, json.dumps(metadata))
                
                logger.info("✅ Résultats comprehensive stockés dans Redis")
            except Exception as e:
                logger.error(f"❌ Erreur stockage Redis comprehensive: {e}")
        
        if store_postgresql and self.pg_connection:
            try:
                cur = self.pg_connection.cursor()
                
                # Créer une table pour les analytics comprehensive
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS comprehensive_analytics (
                        id SERIAL PRIMARY KEY,
                        analysis_type VARCHAR(50) NOT NULL,
                        analysis_key VARCHAR(100) NOT NULL,
                        data JSONB NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(analysis_type, analysis_key)
                    );
                """)
                
                # Vider les anciennes données
                cur.execute("DELETE FROM comprehensive_analytics WHERE created_at < NOW() - INTERVAL '24 hours'")
                
                # Insérer les nouvelles données
                for analysis_type, analysis_data in results.items():
                    if isinstance(analysis_data, dict):
                        for key, value in analysis_data.items():
                            cur.execute("""
                                INSERT INTO comprehensive_analytics (analysis_type, analysis_key, data)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (analysis_type, analysis_key) 
                                DO UPDATE SET data = EXCLUDED.data, created_at = CURRENT_TIMESTAMP
                            """, (analysis_type, str(key), json.dumps(value, default=str)))
                
                self.pg_connection.commit()
                logger.info("✅ Résultats comprehensive stockés dans PostgreSQL")
                
            except Exception as e:
                logger.error(f"❌ Erreur stockage PostgreSQL comprehensive: {e}")
                if self.pg_connection:
                    self.pg_connection.rollback()


def main():
    """Fonction principale avec exemple d'utilisation"""
    
    # Configuration Redis (optionnel)
    redis_config = {
        'host': os.getenv("REDIS_HOST"),
        'port': os.getenv("REDIS_PORT"),
        'db': os.getenv("REDIS_DB"),
        'decode_responses': True
    }
    
    # Configuration PostgreSQL (optionnel)
    pg_config = {
        'host': 'localhost',
        'database': 'postgres',
        'user': 'postgres',
        'password': 'postgres',
        'port': 5432
    }

    minio_config = {
        'endpoint': os.getenv("MINIO_ENDPOINT"),
        'access_key': os.getenv("MINIO_ACCESS_KEY"),
        'secret_key': os.getenv("MINIO_SECRET_KEY"),
        'secure': os.getenv("MINIO_SECURE") == 'True', 
        'bucket_name': os.getenv("MINIO_BUCKET_NAME")
    }
    
    try:
        # Créer l'analyseur étendu
        analyzer = EnhancedPopularityAnalyzer(
            redis_config=redis_config,  
            pg_config=pg_config,    
            minio_config=minio_config 
        )
        
        # Lancer l'analyse comprehensive
        results = analyzer.analyze_comprehensive(
            events_source="user_events2.json",
            source_type="json",
            hours_back=24
        )

        # results = analyzer.analyze_comprehensive(
        #     source_type="minio",
        #     hours_back=24
        # )
        
        if results:
            analyzer.store_comprehensive_results(
                results,  
                store_redis=True,
                store_postgresql=False
            )
            logger.info("✅ Analyse comprehensive terminée avec succès!")
        else:
            logger.info("ℹ️ Analyse terminée, mais aucun résultat n'a été généré (probablement pas de données récentes).")

    except Exception as e:
        logger.error(f"❌ Erreur lors de l'analyse comprehensive: {e}")

if __name__ == "__main__":
    main()