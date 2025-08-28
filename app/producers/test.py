# recommendation_api.py
import asyncio
import logging
import os
from typing import List
import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Query, status
from pydantic import BaseModel

# --- Configuration ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REDIS_HOST = "localhost"
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_HOST = "localhost"

# --- Modèles Pydantic ---
class Cagnotte(BaseModel):
    id: str
    name: str
    description: str | None
    categorie: str

# --- Initialisation de l'App et des Connexions ---
app = FastAPI(title="Recommendation API")
redis_pool = redis.from_url(f"redis://{REDIS_HOST}", decode_responses=True)
db_pool = None

@app.on_event("startup")
async def startup():
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(
            user=DB_USER, password=DB_PASSWORD, database=DB_NAME, host=DB_HOST
        )
        logger.info("Database connection pool started.")
    except Exception as e:
        logger.error(f"Could not connect to the database: {e}")

@app.on_event("shutdown")
async def shutdown():
    if db_pool:
        await db_pool.close()
    await redis_pool.close()

# --- Fonctions de Logique ---
async def get_top_categories_from_profile(user_key: str) -> List[str]:
    profile = await redis_pool.hgetall(user_key)
    if not profile:
        return []
    
    # Trier les catégories par score, du plus haut au plus bas
    sorted_categories = sorted(profile.items(), key=lambda item: float(item[1]), reverse=True)
    
    # Retourner les catégories avec un score positif
    return [category for category, score in sorted_categories if float(score) > 0]

async def fetch_cagnottes(pool, query, *args):
    async with pool.acquire() as connection:
        return await connection.fetch(query, *args)

# --- Endpoint de Recommandation ---
@app.get("/recommendations", response_model=List[Cagnotte])
async def get_recommendations(
    user_identifier: str, 
    user_type: str = Query(..., pattern="^(authenticated|anonymous)$"),
    limit: int = 5
):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database connection is not available.")

    if user_type == "authenticated":
        redis_key = f"profile:{user_identifier}"
    else: # anonymous
        redis_key = f"profile:session:{user_identifier}"
        
    top_categories = await get_top_categories_from_profile(redis_key)
    
    if not top_categories:
        # Fallback : Si l'utilisateur n'a pas de profil ou que des scores négatifs
        logger.info(f"No positive profile for {redis_key}. Falling back to popular cagnottes.")
        query = "SELECT id::text, name, description, categorie FROM cagnottes WHERE statut = 'VALIDE' ORDER BY total_contributors DESC LIMIT $1"
        cagnottes_records = await fetch_cagnottes(db_pool, query, limit)
    else:
        # Recommandations personnalisées basées sur la meilleure catégorie
        best_category = top_categories[0]
        logger.info(f"Profile found for {redis_key}. Recommending from top category: '{best_category}'")
        query = "SELECT id::text, name, description, categorie FROM cagnottes WHERE categorie = $1 AND statut = 'VALIDE' ORDER BY total_contributors DESC LIMIT $2"
        cagnottes_records = await fetch_cagnottes(db_pool, query, best_category, limit)

    return [Cagnotte(**dict(record)) for record in cagnottes_records]