# Fichier: generate_feed_now.py
import asyncio
from feed_popular_trending_new import generate_and_cache_feed_for_country, FeedConfig
from arq.connections import create_pool, RedisSettings
import os

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from db.config_redis import settings


async def generate_all_feeds():
    """Génère tous les feeds immédiatement."""
    redis_settings = RedisSettings(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        password=settings.REDIS_PASSWORD,
        database=settings.REDIS_DB
    )
    
    redis_pool = await create_pool(redis_settings)
    ctx = {'redis': redis_pool}
    
    print("🚀 Génération manuelle de tous les feeds...")
    
    for country in FeedConfig.COUNTRIES:
        print(f"\n📍 Génération pour {country}...")
        try:
            await generate_and_cache_feed_for_country(ctx, country)
        except Exception as e:
            print(f"❌ Erreur pour {country}: {e}")
    
    await redis_pool.aclose()
    print("\n✅ Génération terminée!")

if __name__ == "__main__":
    asyncio.run(generate_all_feeds())