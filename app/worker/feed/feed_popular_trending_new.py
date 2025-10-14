# Ce code est un worker ARQ (système de tâches asynchrones) qui génère automatiquement des feeds personnalisés par pays et les met en cache dans Redis.

#Cron automatique : Toutes les 2 minutes, le scheduler planifie 3 jobs (un par pays : Mali, Cameroun, Sénégal)
#Enrichit les données en récupérant les posts et ressources associées depuis la base PostgreSQL
#Assemble un feed de 100 vidéos maximum avec leur raison d'apparition (populaire/tendance/nouveauté)

#Mise en cache : Stocke le résultat final dans Redis avec la clé feed:combined:{country} pendant 2 minutes


import asyncio
import json
from uuid import UUID
from datetime import datetime
from enum import Enum

def custom_json_serializer(obj):
    if isinstance(obj, UUID):
        return str(obj)
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, Enum):
        return obj.value
    elif hasattr(obj, "__dict__"):
        return obj.__dict__
    return str(obj)

from datetime import datetime
from collections import defaultdict
from typing import List, Dict, Any
from contextlib import asynccontextmanager

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from pydentics import models
from pydentics import schemas2
from pydentics import schemas3

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from sqlalchemy import case

from arq import cron
from arq.connections import RedisSettings, ArqRedis

from db.postgres_config import engine
from db.config_redis import settings
from utils.auxiliaires_fonctions import (
    _get_popular_ids_from_redis,
    _get_trending_data_from_redis,
    _get_new_arrivals_ids_from_db,
    build_feed_response
)
from utils.config import FeedConfig, EndpointConfig


# ===================================================================
# FACTORY DE SESSION DB OPTIMISÉE
# ===================================================================
db_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False
)

@asynccontextmanager
async def get_worker_db_session():
    """Gestionnaire de contexte optimisé pour les sessions DB dans le worker."""
    session = db_session_factory()
    try:
        yield session
        await session.commit()
    except Exception as e:
        await session.rollback()
        raise
    finally:
        await session.close()


# ===================================================================
# FONCTIONS UTILITAIRES OPTIMISÉES
# ===================================================================
async def fetch_data_with_timeout(coro, timeout: float, error_msg: str):
    """Exécute une coroutine avec timeout pour éviter les blocages."""
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        print(f"⚠️ Timeout: {error_msg}")
        raise
    except Exception as e:
        print(f"❌ Erreur: {error_msg} - {e}")
        raise


# ===================================================================
# ENRICHISSEMENT UNIFIÉ VIA LA BASE DE DONNÉES
# ===================================================================
async def fetch_enriched_posts_optimized(
    db: AsyncSession,
    video_ids: List[int]
) -> tuple[List[models.CagnottePostModel], Dict[int, List[models.RessourceModel]]]:
    """
    OPTIMISATION MAJEURE: Une seule requête pour les posts, cagnottes, auteurs, ressources
    ET MAINTENANT: Chargement avide des sponsors et de leurs organisations.
    """
    if not video_ids:
        return [], {}
    
    # Map pour préserver l'ordre du mélange
    order_map = {video_id: idx for idx, video_id in enumerate(video_ids)}
    order_logic = case(order_map, value=models.RessourceModel.id)
    
    # Requête unique pour récupérer tous les posts nécessaires
    stmt = (
        select(models.CagnottePostModel, models.RessourceModel)
        .join(models.RessourceModel, models.CagnottePostModel.id == models.RessourceModel.reference)
        .options(
            # 1. Chargement de la CAGNOTTE
            selectinload(models.CagnottePostModel.cagnotte)
                # 1.1. Chargement de la CATÉGORIE de la cagnotte
                .selectinload(models.CagnotteModel.categorie),
            
            # 2. Chargement de l'ADMIN de la cagnotte
            selectinload(models.CagnottePostModel.cagnotte)
                .selectinload(models.CagnotteModel.admin),

            # 3. NOUVEAU : Chargement des SPONSORS de la cagnotte
            selectinload(models.CagnottePostModel.cagnotte)
                .selectinload(models.CagnotteModel.sponsors)
                # 3.1. Chargement de l'ORGANISATION associée à chaque sponsor
                .selectinload(models.SponsorModel.organisation),

            # 4. Chargement de l'AUTEUR du post
            selectinload(models.CagnottePostModel.author)
        )
        .where(models.RessourceModel.id.in_(video_ids))
        .order_by(order_logic, models.RessourceModel.order_index.asc())
    )
    
    result = await db.execute(stmt)
    
    posts_dict = {}
    resources_by_post_id = defaultdict(list)
    
    for post, resource in result.unique().all():
        if post.id not in posts_dict:
            posts_dict[post.id] = post
        resources_by_post_id[post.id].append(resource)
    
    ordered_posts = [posts_dict[pid] for pid in posts_dict if pid in posts_dict]
    
    return ordered_posts, resources_by_post_id


# ===================================================================
# MÉLANGE INTELLIGENT ET DÉDOUBLONNAGE
# ===================================================================
def blend_ids_optimized(
    popular_ids: List[int],
    trending_ids: List[int],
    new_arrivals_ids: List[int],
    limit: int,
    popular_ratio: int,
    trending_ratio: int
) -> tuple[List[int], Dict[int, str]]:
    """
    Mélange optimisé des IDs avec algorithme plus efficace.
    """
    final_ids = []
    reasons_map = {}
    seen = set()
    
    # Étape 1: Ajouter les new arrivals en priorité
    for vid in new_arrivals_ids:
        if vid not in seen and len(final_ids) < limit:
            final_ids.append(vid)
            reasons_map[vid] = schemas3.CombinedFeedReason.NEW_ARRIVAL
            seen.add(vid)
    
    popular_idx, trending_idx = 0, 0
    
    while len(final_ids) < limit:
        added = False
        
        for _ in range(popular_ratio):
            if popular_idx < len(popular_ids):
                vid = popular_ids[popular_idx]
                popular_idx += 1
                if vid not in seen:
                    final_ids.append(vid)
                    reasons_map[vid] = schemas3.CombinedFeedReason.POPULAR
                    seen.add(vid)
                    added = True
                    if len(final_ids) >= limit:
                        break
        
        if len(final_ids) >= limit:
            break
        
        for _ in range(trending_ratio):
            if trending_idx < len(trending_ids):
                vid = trending_ids[trending_idx]
                trending_idx += 1
                if vid not in seen:
                    final_ids.append(vid)
                    reasons_map[vid] = schemas3.CombinedFeedReason.TRENDING
                    seen.add(vid)
                    added = True
                    if len(final_ids) >= limit:
                        break
        
        if not added:
            break
    
    return final_ids[:limit], reasons_map


# ===================================================================
# TÂCHE PRINCIPALE OPTIMISÉE
# ===================================================================
async def generate_and_cache_feed_for_country(ctx: Dict[str, Any], country: str):
    """
    Version optimisée de la génération de feed avec gestion d'erreurs robuste.
    ⚠️ ATTENTION: Cette fonction est appelée par ARQ via enqueue_job
    """
    print(f"🚀 [WORKER] Génération du feed pour: {country} - START")
    
    arq_redis: ArqRedis = ctx['redis']
    
    try:
        # 🔍 DIAGNOSTIC: Vérifier la connexion Redis
        try:
            await arq_redis.ping()
            print(f"✅ [WORKER] Redis ping OK pour {country}")
        except Exception as e:
            print(f"❌ [WORKER] Redis ping FAILED pour {country}: {e}")
            return
        
        # ÉTAPE 1: Récupération parallèle avec timeouts
        async with get_worker_db_session() as db:
            print(f"📊 [WORKER] Récupération des données pour {country}...")
            
            popular_task = fetch_data_with_timeout(
                _get_popular_ids_from_redis(arq_redis, country),
                FeedConfig.REDIS_QUERY_TIMEOUT,
                f"Récupération popular_ids pour {country}"
            )
            trending_task = fetch_data_with_timeout(
                _get_trending_data_from_redis(arq_redis, country),
                FeedConfig.REDIS_QUERY_TIMEOUT,
                f"Récupération trending_data pour {country}"
            )
            new_arrivals_task = fetch_data_with_timeout(
                _get_new_arrivals_ids_from_db(db, FeedConfig.NEW_ARRIVALS_LIMIT),
                FeedConfig.DB_QUERY_TIMEOUT,
                f"Récupération new_arrivals pour {country}"
            )
            
            popular_ids, trending_videos_raw, new_arrivals_ids = await asyncio.gather(
                popular_task, trending_task, new_arrivals_task,
                return_exceptions=False
            )

            print(f'✅ [WORKER] Etape 1 réussie pour {country}')
            print(f'   - Popular IDs: {len(popular_ids)}')
            print(f'   - Trending videos: {len(trending_videos_raw)}')
            print(f'   - New arrivals: {len(new_arrivals_ids)}')
        
        # ÉTAPE 2: Traitement des tendances
        trending_videos_up = [v for v in trending_videos_raw if v.get('trend_direction') == 'up']
        trending_ids = [int(v['video_id']) for v in trending_videos_up]
        trending_data_map = {int(v['video_id']): v for v in trending_videos_up}
        
        # ÉTAPE 3: Mélange intelligent optimisé
        final_ids, reasons_map = blend_ids_optimized(
            popular_ids=popular_ids,
            trending_ids=trending_ids,
            new_arrivals_ids=new_arrivals_ids,
            limit=FeedConfig.FEED_LIMIT,
            popular_ratio=FeedConfig.POPULAR_RATIO,
            trending_ratio=FeedConfig.TRENDING_RATIO
        )
        
        if not final_ids:
            print(f"⚠️ [WORKER] Aucune vidéo à traiter pour {country}")
            return
        
        print(f'📝 [WORKER] {len(final_ids)} IDs finaux pour {country}')
        
        # ÉTAPE 4: Enrichissement optimisé (une seule requête)
        async with get_worker_db_session() as db:
            posts, resources_by_post = await fetch_data_with_timeout(
                fetch_enriched_posts_optimized(db, final_ids),
                FeedConfig.DB_QUERY_TIMEOUT,
                f"Enrichissement posts pour {country}"
            )

            # NOUVEAU: Récupérer les IDs de cagnottes
        # cagnotte_ids = list(set([post.cagnotte.id for post in posts]))

        # # Récupérer les sponsors pour toutes les cagnottes
        # sponsors_map = await fetch_data_with_timeout(
        #     fetch_sponsors_for_cagnottes(db, cagnotte_ids),
        #     FeedConfig.DB_QUERY_TIMEOUT,
        #     f"Récupération sponsors pour {country}"
        # )
            
        print(f'✅ [WORKER] Etape 4 réussie: {len(posts)} posts et sponsors enrichis pour {country}')
        
        # ÉTAPE 5: Assemblage de la réponse
        feed_items = []
        for post in posts:
            resources = resources_by_post.get(post.id, [])
            trigger_resource_id = next((r.id for r in resources if r.id in reasons_map), None)
            
            if trigger_resource_id is None:
                continue
            
            base_item = build_feed_response(
                post,
                resources,
                # sponsors_map.get(post.cagnotte.id, []) # Passe la liste des sponsors
            )
            item_data = base_item.model_dump()
            reason = reasons_map[trigger_resource_id]
            item_data['reason'] = reason
            
            if reason == schemas3.CombinedFeedReason.TRENDING:
                trending_info = trending_data_map.get(trigger_resource_id, {})
                item_data.update({
                    'trendingScore': trending_info.get('trending_score'),
                    'velocityPercent': trending_info.get('velocity_percent'),
                    'trendDirection': trending_info.get('trend_direction'),
                })
            
            feed_items.append(item_data)
        
        print(f'✅ [WORKER] Etape 5 réussie: {len(feed_items)} items assemblés pour {country}')

        # ÉTAPE 6: Mise en cache avec gestion d'erreur
        if feed_items:
            cache_key = FeedConfig.CACHE_KEY_TEMPLATE.format(country=country)
            
            # 🔍 DIAGNOSTIC: Logger avant le set
            print(f"💾 [WORKER] Tentative de mise en cache pour {country}")
            print(f"   - Clé Redis: {cache_key}")
            print(f"   - Nombre d'items: {len(feed_items)}")
            print(f"   - TTL: {FeedConfig.CACHE_TTL}s")
            
            try:
                await arq_redis.set(
                    cache_key,
                    json.dumps(feed_items, default=custom_json_serializer),
                    ex=FeedConfig.CACHE_TTL
                )
                
                # 🔍 DIAGNOSTIC: Vérifier que la clé existe
                exists = await arq_redis.exists(cache_key)
                if exists:
                    print(f"✅ [WORKER] Feed pour {country}: {len(feed_items)} éléments mis en cache avec succès")
                    
                    # 🔍 Vérifier le TTL
                    ttl = await arq_redis.ttl(cache_key)
                    print(f"   - TTL actuel: {ttl}s")
                else:
                    print(f"❌ [WORKER] ERREUR: La clé {cache_key} n'existe pas après set()!")
                    
            except Exception as e:
                print(f"❌ [WORKER] Erreur lors du set Redis pour {country}: {type(e).__name__}: {e}")
                raise
        else:
            print(f"⚠️ [WORKER] Feed vide généré pour {country}")
        
        print(f"🎉 [WORKER] Génération du feed pour {country} - TERMINÉE")
    
    except asyncio.TimeoutError:
        print(f"⏱️ [WORKER] Timeout lors de la génération du feed pour {country}")
    except Exception as e:
        print(f"❌ [WORKER] Erreur critique pour {country}: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()


# ===================================================================
# PLANIFICATION DES TÂCHES
# ===================================================================
async def schedule_feed_generation(ctx: Dict[str, Any]):
    """
    Planifie la génération pour tous les pays avec gestion d'erreurs.
    """
    print(f"📅 [SCHEDULER] Début de planification des feeds")
    arq_redis: ArqRedis = ctx['redis']
    
    # 🔍 DIAGNOSTIC: Vérifier l'état de la queue
    try:
        queue_length = await arq_redis.llen('arq:queue')
        print(f"📊 [SCHEDULER] Taille actuelle de la queue: {queue_length}")
    except Exception as e:
        print(f"⚠️ [SCHEDULER] Impossible de lire la queue: {e}")
    
    tasks = []
    for country in FeedConfig.COUNTRIES:
        try:
            job = await arq_redis.enqueue_job(
                'generate_and_cache_feed_for_country',
                country
            )
            tasks.append(country)
            print(f"✅ [SCHEDULER] Job planifié pour {country} - Job ID: {job.job_id if job else 'N/A'}")
        except Exception as e:
            print(f"❌ [SCHEDULER] Échec de planification pour {country}: {e}")
    
    # 🔍 DIAGNOSTIC: Vérifier la queue après enqueue
    try:
        queue_length = await arq_redis.llen('arq:queue')
        print(f"📊 [SCHEDULER] Taille de la queue après enqueue: {queue_length}")
    except Exception as e:
        print(f"⚠️ [SCHEDULER] Impossible de lire la queue: {e}")
    
    print(f"🔄 [SCHEDULER] {len(tasks)} tâches planifiées: {', '.join(tasks)}")


# ===================================================================
# CONFIGURATION DU WORKER ARQ
# ===================================================================
async def on_startup(ctx: Dict[str, Any]):
    """Initialisation optimisée du worker."""
    print(f"🔧 [STARTUP] Initialisation du worker...")
    # print(f"🔧 [STARTUP] REDIS_HOST={settings.REDIS_HOST}")
    # print(f"🔧 [STARTUP] REDIS_PORT={settings.REDIS_PORT}")
    # print(f"🔧 [STARTUP] REDIS_DB={settings.REDIS_DB}")
    # print(f"🔧 [STARTUP] REDIS_PASSWORD={'***' if settings.REDIS_PASSWORD else 'None'}")
    
    redis_settings = RedisSettings(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        password=settings.REDIS_PASSWORD,
        database=settings.REDIS_DB
    )
    
    from arq import create_pool
    ctx['redis'] = await create_pool(redis_settings)
    
    # 🔍 DIAGNOSTIC: Tester la connexion
    try:
        await ctx['redis'].ping()
        print(f"✅ [STARTUP] Connexion Redis réussie")
    except Exception as e:
        print(f"❌ [STARTUP] Échec de connexion Redis: {e}")
    
    print(f"🚀 [STARTUP] Worker démarré - Redis: {settings.REDIS_HOST}:{settings.REDIS_PORT}")
    print(f"📊 [STARTUP] Config: {len(FeedConfig.COUNTRIES)} pays, cache TTL={FeedConfig.CACHE_TTL}s")


async def on_shutdown(ctx: Dict[str, Any]):
    """Arrêt propre du worker."""
    await ctx['redis'].close()
    print("🔌 [SHUTDOWN] Worker arrêté proprement")


class WorkerSettings:
    """Configuration ARQ optimisée."""
    functions = [generate_and_cache_feed_for_country, schedule_feed_generation]
    on_startup = on_startup
    on_shutdown = on_shutdown
    
    # 🔍 DIAGNOSTIC: Utiliser les mêmes paramètres Redis
    redis_settings = RedisSettings(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        password=settings.REDIS_PASSWORD,
        database=settings.REDIS_DB
    )
    
    # Job settings pour la performance
    max_jobs = 10
    job_timeout = 300
    keep_result = 600
    
    # Cron toutes les 2 minutes
    cron_jobs = [
        cron(
            schedule_feed_generation,
            minute={i for i in range(0, 60, FeedConfig.CRON_INTERVAL_MINUTES)},
            run_at_startup=True
        )
    ]