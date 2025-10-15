import os
import json
import logging
from datetime import datetime, timedelta, timezone
from arq import create_pool, cron
from arq.connections import RedisSettings
import redis
from clickhouse_driver import Client
from dotenv import load_dotenv

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from utils import config
# from worker.feed.feed_popular_trending_new import schedule_feed_generation, generate_and_cache_feed_for_country

load_dotenv()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EnhancedPopularityAnalyzer:
    """Classe d'analyse inchangée mais adaptée pour ARQ"""
    
    def __init__(self, redis_client=None, clickhouse_config=None):
        self.redis_client = redis_client
        self.clickhouse_client = None
        
        # Configuration des poids pour le calcul de popularité
        self.event_weights = {
            "video_view": 1.0,
            "video_share": 3.0,
            "video_favorite": 2.5,
            "video_replay": 2.0,
            "video_skip": -0.5,
            "cagnotte_detail_view": 1.5
        }
        
        # Facteur de décroissance temporelle (en jours maintenant)
        self.time_decay_days = 1  # 24 heures = 1 jour
        
        # Configuration ClickHouse
        if clickhouse_config:
            try:
                self.clickhouse_client = Client(
                    host=clickhouse_config.get('host'),
                    port=clickhouse_config.get('port'),
                    user=clickhouse_config.get('user'),
                    password=clickhouse_config.get('password'),
                    database=clickhouse_config.get('database')
                )
                self.clickhouse_client.execute('SELECT 1')
                logger.info("✅ Connexion ClickHouse établie")
            except Exception as e:
                logger.error(f"❌ Erreur connexion ClickHouse: {e}")
                self.clickhouse_client = None

    def load_events_from_clickhouse(self, days_back=1, table_name='user_events'):
        """Charge les événements des X derniers jours depuis ClickHouse"""
        if not self.clickhouse_client:
            logger.error("❌ Pas de connexion ClickHouse disponible.")
            return []

        logger.info(f"⬇️ Chargement des événements des {days_back} derniers jours depuis ClickHouse...")
        
        try:
            cutoff_time = datetime.now(timezone.utc) - timedelta(days=days_back)
            
            query = f"""
            SELECT
                event_type,
                video_id,
                user_id,
                session_id,
                timestamp,
                pays,
                id_categorie,
                raw_data
            FROM {table_name}
            WHERE timestamp >= %(cutoff_time)s
            ORDER BY timestamp DESC
            """
            
            rows = self.clickhouse_client.execute(
                query,
                {'cutoff_time': cutoff_time}
            )
            
            events = []
            for row in rows:
                event = {
                    'event_type': row[0],
                    'video_id': row[1],
                    'user_id': row[2],
                    'session_id': row[3],
                    'timestamp': row[4].isoformat() if hasattr(row[4], 'isoformat') else str(row[4]),
                    'pays': row[5],
                    'id_categorie': row[6],
                    'raw_data': json.loads(row[7]) if row[7] and isinstance(row[7], str) else row[7] or {}
                }
                events.append(event)
            
            logger.info(f"📁 {len(events)} événements chargés depuis ClickHouse.")
            return events
            
        except Exception as e:
            logger.error(f"❌ Erreur lors du chargement depuis ClickHouse: {e}")
            return []

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
        
        logger.warning(f"⚠️ Impossible de parser le timestamp: {timestamp_str}")
        return None

    def filter_events_by_timerange(self, events, days_back=1):
        """Filtre les événements dans une plage temporelle (en jours)"""
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=days_back)
        
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
        
        logger.info(f"📅 {len(filtered_events)} événements dans les derniers {days_back} jour(s)")
        return filtered_events

    def calculate_time_decay_factor(self, event_timestamp):
        """Calcule le facteur de décroissance temporelle (en jours)"""
        event_time = self.parse_timestamp(event_timestamp)
        if not event_time:
            return 0.01
        
        try:
            import math
            days_ago = (datetime.now() - event_time).total_seconds() / 86400  # 86400 sec = 1 jour
            decay_factor = math.exp(-days_ago / self.time_decay_days)
            return max(decay_factor, 0.01)
        except:
            return 0.01

    def calculate_popularity_by_geography(self, events):
        """Calcule la popularité par pays"""
        from collections import defaultdict
        
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
        from collections import defaultdict
        
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

    def calculate_trending_by_geography(self, events, recent_days=1, comparison_days=2):
        """Calcule les tendances par pays (cycle journalier: 24h récent vs 48h comparaison)"""
        now = datetime.now(timezone.utc)
        recent_cutoff = now - timedelta(days=recent_days)
        comparison_start = now - timedelta(days=recent_days + comparison_days)
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
            recent_data = recent_scores.get(country, {})
            comparison_data = comparison_scores.get(country, {})
            
            recent_score = recent_data.get('popularity_score', 0)
            comparison_score = comparison_data.get('popularity_score', 0)
            
            recent_videos = {video['video_id']: video['score'] for video in recent_data.get('top_videos', [])}
            comparison_videos = {video['video_id']: video['score'] for video in comparison_data.get('top_videos', [])}
            
            video_trends = {}
            all_video_ids = set(list(recent_videos.keys()) + list(comparison_videos.keys()))
            
            for video_id in all_video_ids:
                recent_video_score = recent_videos.get(video_id, 0)
                comparison_video_score = comparison_videos.get(video_id, 0)
                
                if comparison_video_score == 0:
                    video_velocity = 100.0 if recent_video_score > 0 else 0.0
                else:
                    video_velocity = ((recent_video_score - comparison_video_score) / comparison_video_score) * 100
                
                video_trending_score = recent_video_score + (video_velocity * 0.1)
                
                video_trends[video_id] = {
                    'video_id': video_id,
                    'trending_score': round(video_trending_score, 2),
                    'current_score': recent_video_score,
                    'previous_score': comparison_video_score,
                    'velocity_percent': round(video_velocity, 1),
                    'trend_direction': 'up' if video_velocity > 10 else 'down' if video_velocity < -10 else 'stable'
                }
            
            top_trending_videos = sorted(video_trends.values(), 
                                    key=lambda x: x['trending_score'], reverse=True)[:10]
            
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
                'trend_direction': 'up' if velocity > 10 else 'down' if velocity < -10 else 'stable',
                'trending_videos': top_trending_videos,
                'total_trending_videos': len(video_trends),
                'unique_users_recent': recent_data.get('unique_users', 0),
                'unique_users_previous': comparison_data.get('unique_users', 0),
                'total_events_recent': recent_data.get('total_events', 0),
                'total_events_previous': comparison_data.get('total_events', 0),
                'last_updated': datetime.now().isoformat()
            }
        
        return trending_scores

    def calculate_trending_by_category(self, events, recent_days=1, comparison_days=2):
        """Calcule les tendances par catégorie (cycle journalier: 24h récent vs 48h comparaison)"""
        now = datetime.now(timezone.utc)
        recent_cutoff = now - timedelta(days=recent_days)
        comparison_start = now - timedelta(days=recent_days + comparison_days)
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
            recent_data = recent_scores.get(category, {})
            comparison_data = comparison_scores.get(category, {})
            
            recent_score = recent_data.get('popularity_score', 0)
            comparison_score = comparison_data.get('popularity_score', 0)
            
            recent_videos = {video['video_id']: video['score'] for video in recent_data.get('top_videos', [])}
            comparison_videos = {video['video_id']: video['score'] for video in comparison_data.get('top_videos', [])}
            
            video_trends = {}
            all_video_ids = set(list(recent_videos.keys()) + list(comparison_videos.keys()))
            
            for video_id in all_video_ids:
                recent_video_score = recent_videos.get(video_id, 0)
                comparison_video_score = comparison_videos.get(video_id, 0)
                
                if comparison_video_score == 0:
                    video_velocity = 100.0 if recent_video_score > 0 else 0.0
                else:
                    video_velocity = ((recent_video_score - comparison_video_score) / comparison_video_score) * 100
                
                video_trending_score = recent_video_score + (video_velocity * 0.1)
                
                video_trends[video_id] = {
                    'video_id': video_id,
                    'trending_score': round(video_trending_score, 2),
                    'current_score': recent_video_score,
                    'previous_score': comparison_video_score,
                    'velocity_percent': round(video_velocity, 1),
                    'trend_direction': 'up' if video_velocity > 10 else 'down' if video_velocity < -10 else 'stable'
                }
            
            top_trending_videos = sorted(video_trends.values(), 
                                    key=lambda x: x['trending_score'], reverse=True)[:5]
            
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
                'trend_direction': 'up' if velocity > 10 else 'down' if velocity < -10 else 'stable',
                'trending_videos': top_trending_videos,
                'total_trending_videos': len(video_trends),
                'unique_users_recent': recent_data.get('unique_users', 0),
                'unique_users_previous': comparison_data.get('unique_users', 0),
                'total_events_recent': recent_data.get('total_events', 0),
                'total_events_previous': comparison_data.get('total_events', 0),
                'countries_recent': recent_data.get('countries', []),
                'countries_previous': comparison_data.get('countries', []),
                'last_updated': datetime.now().isoformat()
            }
        
        return trending_scores

    def store_results_in_redis(self, results):
        """Stocke les résultats dans Redis"""
        if not self.redis_client:
            logger.warning("⚠️ Pas de client Redis disponible")
            return
        
        try:
            for key, data in results.items():
                redis_key = f"analytics_{key}"
                # Expiration après 25 heures pour avoir une marge
                self.redis_client.setex(
                    redis_key,
                    config.CalculationConfig.DUREE_ANALYTICS, # 25 heures en secondes
                    json.dumps(data, ensure_ascii=False, default=str)
                )
            
            metadata = {
                'last_analysis': datetime.now().isoformat(),
                'analysis_types': list(results.keys()),
                'expires_at': (datetime.now() + timedelta(hours=25)).isoformat()
            }
            self.redis_client.setex(
                "comprehensive_analytics_metadata",
                config.CalculationConfig.DUREE_ANALYTICS,
                json.dumps(metadata)
            )
            
            logger.info("✅ Résultats stockés dans Redis")
        except Exception as e:
            logger.error(f"❌ Erreur stockage Redis: {e}")


# ============================================================================
# TÂCHE ARQ POUR L'ANALYSE PÉRIODIQUE
# ============================================================================

async def run_analytics_task(ctx):
    """
    Tâche ARQ qui s'exécute toutes les 24h pour calculer la popularité et les tendances.
    
    Cycle journalier:
    - Popularité: événements des dernières 24h
    - Tendances: 24h récentes vs 48h de comparaison
    """
    logger.info("🚀 Démarrage de l'analyse analytics planifiée")
    
    try:
        # Configuration ClickHouse
        clickhouse_config = {
            'host': os.getenv("CLICKHOUSE_HOST"),
            'port': int(os.getenv("CLICKHOUSE_PORT2")),
            'user': os.getenv("CLICKHOUSE_USER"),
            'password': os.getenv("CLICKHOUSE_PASSWORD"),
            'database': os.getenv("CLICKHOUSE_DATABASE")
        }
        
        # Créer un client Redis synchrone pour l'analyseur
        redis_sync_client = redis.Redis(
            host=os.getenv("REDIS_HOST"),
            port=int(os.getenv("REDIS_PORT")),
            db=int(os.getenv("REDIS_DB")),
            decode_responses=True
        )
        
        # Créer l'analyseur
        analyzer = EnhancedPopularityAnalyzer(
            redis_client=redis_sync_client,
            clickhouse_config=clickhouse_config
        )
        
        # Charger les événements des 3 derniers jours (pour avoir assez de données pour les tendances)
        all_events = analyzer.load_events_from_clickhouse(
            days_back=3,
            table_name=config.CalculationConfig.TABLE_NAME
        )
        
        if not all_events:
            logger.warning("⚠️ Aucun événement trouvé dans ClickHouse")
            return {"status": "no_data", "timestamp": datetime.now().isoformat()}
        
        # Filtrer les événements récents (dernier jour pour la popularité)
        recent_events = analyzer.filter_events_by_timerange(all_events, days_back=1)
        
        if not recent_events:
            logger.warning("⚠️ Aucun événement récent (24h)")
            return {"status": "no_recent_data", "timestamp": datetime.now().isoformat()}
        
        results = {}
        
        # 1. Calcul de la popularité (dernières 24h)
        logger.info("🌍 Calcul de la popularité par pays (24h)...")
        results['popularity_by_country'] = analyzer.calculate_popularity_by_geography(recent_events)
        
        logger.info("📂 Calcul de la popularité par catégorie (24h)...")
        results['popularity_by_category'] = analyzer.calculate_popularity_by_category(recent_events)
        
        # 2. Calcul des tendances (24h récent vs 48h comparaison)
        logger.info("📈 Calcul des tendances par pays (24h vs 48h)...")
        results['trending_by_country'] = analyzer.calculate_trending_by_geography(
            all_events,
            recent_days=1,
            comparison_days=2
        )
        
        logger.info("📊 Calcul des tendances par catégorie (24h vs 48h)...")
        results['trending_by_category'] = analyzer.calculate_trending_by_category(
            all_events,
            recent_days=1,
            comparison_days=2
        )
        
        # 3. Stockage dans Redis
        analyzer.store_results_in_redis(results)
        
        # Fermer les connexions
        redis_sync_client.close()
        
        logger.info("✅ Analyse analytics terminée avec succès")
        
        return {
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "events_analyzed": len(recent_events),
            "total_events_loaded": len(all_events),
            "countries": len(results.get('popularity_by_country', {})),
            "categories": len(results.get('popularity_by_category', {}))
        }
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'analyse analytics: {e}")
        import traceback
        traceback.print_exc()
        return {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


# ============================================================================
# CONFIGURATION DU WORKER ARQ
# ============================================================================

class WorkerSettings:
    """Configuration du worker ARQ"""
    
    # Configuration Redis pour ARQ
    redis_settings = RedisSettings(
        host=os.getenv("REDIS_HOST"),
        port=int(os.getenv("REDIS_PORT")),
        database=int(os.getenv("REDIS_DB")),
    )
    
     # Nom de queue unique pour ce worker
    queue_name = 'arq:queue:analytics'

    # Liste des fonctions que le worker peut exécuter
    functions = [run_analytics_task]
    
    # Planification avec cron: tous les jours à 2h du matin
    cron_jobs = [
        cron(run_analytics_task, hour=2, minute=0, run_at_startup=True)
    ]

    # cron_jobs = [
    #     cron(
    #         run_analytics_task,
    #         minute={i for i in range(0, 60, 2)},
    #         run_at_startup=True
    #     )
    # ]
    
    # Nom du worker (pour les logs)
    job_timeout = 3600  # 1 heure max pour l'exécution
    max_jobs = 5  # Maximum de jobs simultanés
    
    # Configuration du logging
    log_results = True


# Pour exécution directe (test)
if __name__ == "__main__":
    import asyncio
    
    async def test_task():
        """Test direct de la tâche"""
        result = await run_analytics_task(None)
        print(f"\n✅ Résultat du test: {result}")
    
    asyncio.run(test_task())