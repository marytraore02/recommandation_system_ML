from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
import os
from dotenv import load_dotenv

load_dotenv()

# --- OPTIMISATION ICI ---
# pool_size : Le nombre de connexions à garder ouvertes en permanence dans le pool.
# max_overflow : Le nombre de connexions supplémentaires qui peuvent être ouvertes 
#                temporairement si le pool est plein.

# Attention : La somme (pool_size + max_overflow) multipliée par le nombre d'instances de votre application ne doit pas dépasser la limite de max_connections de votre serveur PostgreSQL (généralement 100 par défaut).

engine = create_async_engine(
    os.getenv("DATABASE_URL_PRODUCTION"),
    # os.getenv("DATABASE_URL_LOCAL"),
    echo=False,  # Mettre à False en production pour éviter de logger toutes les requêtes SQL
    pool_size=10,
    max_overflow=20
)


SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    class_=AsyncSession
)

Base = declarative_base()


# Fonction pour créer les tables dans la base de données
async def create_db_and_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

# Dépendance FastAPI pour obtenir une session de base de données
async def get_db():
    async with SessionLocal() as session:
        yield session

