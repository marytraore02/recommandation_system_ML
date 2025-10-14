import redis.asyncio as aioredis
from .config_redis import settings

redis_url = f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}"
if settings.REDIS_PASSWORD:
    redis_url = f"redis://:{settings.REDIS_PASSWORD}@{settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}"


# Création du pool de connexions asynchrone
# decode_responses=True permet de recevoir des chaînes de caractères (str) au lieu de bytes.
redis_connection_pool = aioredis.from_url(
    redis_url,
    max_connections=20, # Nombre max de connexions dans le pool
    decode_responses=True
)
    
# Fonction simple pour obtenir une connexion (sera utilisée avec l'injection de dépendances)
async def get_redis_client() -> aioredis.Redis:
    """
    Fournit un client Redis à partir du pool de connexions partagé.
    """
    return redis_connection_pool