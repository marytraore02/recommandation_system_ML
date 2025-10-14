"""
Script pour déclencher manuellement l'analyse analytics sans attendre le cron.
Utile pour tester ou forcer une exécution immédiate.
"""

import asyncio
import os
from arq import create_pool
from arq.connections import RedisSettings
from dotenv import load_dotenv

load_dotenv()


async def trigger_analytics():
    """Déclenche manuellement la tâche d'analyse"""
    
    # Configuration Redis
    redis_settings = RedisSettings(
        host=os.getenv("REDIS_HOST"),
        port=int(os.getenv("REDIS_PORT")),
        database=int(os.getenv("REDIS_DB")),
    )
    
    # Créer une connexion au pool Redis ARQ
    redis = await create_pool(redis_settings)
    
    try:
        print("🚀 Déclenchement manuel de l'analyse analytics...")
        
        # Enqueue la tâche pour exécution immédiate
        job = await redis.enqueue_job('run_analytics_task')
        
        print(f"✅ Tâche enregistrée avec l'ID: {job.job_id}")
        print("⏳ En attente de l'exécution...")
        
        # Attendre le résultat (max 1 heure)
        result = await job.result(timeout=3600)
        
        print("\n" + "="*80)
        print("📊 RÉSULTAT DE L'ANALYSE:")
        print("="*80)
        print(f"Status: {result.get('status')}")
        print(f"Timestamp: {result.get('timestamp')}")
        
        if result.get('status') == 'success':
            print(f"Événements analysés (24h): {result.get('events_analyzed')}")
            print(f"Total événements chargés: {result.get('total_events_loaded')}")
            print(f"Pays analysés: {result.get('countries')}")
            print(f"Catégories analysées: {result.get('categories')}")
            print("\n✅ Analyse terminée avec succès!")
        elif result.get('status') == 'error':
            print(f"\n❌ Erreur: {result.get('error')}")
        else:
            print(f"\n⚠️ Statut: {result.get('status')}")
        
        print("="*80)
        
    except Exception as e:
        print(f"❌ Erreur lors du déclenchement: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Fermer la connexion
        await redis.close()


if __name__ == "__main__":
    asyncio.run(trigger_analytics())