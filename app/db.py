import os
import asyncpg
from fastapi import Request, HTTPException

# Configuration de la base de données
POSTGRES_URL = f"postgresql://{os.environ.get('DB_USER', 'postgres')}:{os.environ.get('DB_PASSWORD', 'postgres')}@{os.environ.get('DB_HOST', 'localhost')}:{os.environ.get('DB_PORT', '5432')}/{os.environ.get('DB_NAME', 'postgres')}"

# Le pool de connexions sera stocké dans l'état de l'application FastAPI
# pour éviter une variable globale.

async def get_db_conn(request: Request) -> asyncpg.Connection:
    """
    Dépendance FastAPI pour fournir une connexion BDD depuis le pool.
    Le pool est géré par le cycle de vie de l'application (lifespan).
    """
    if not hasattr(request.app.state, 'db_pool') or request.app.state.db_pool is None:
        raise HTTPException(
            status_code=503, # Service Unavailable
            detail="Le pool de connexions à la base de données n'est pas disponible."
        )
    async with request.app.state.db_pool.acquire() as connection:
        yield connection