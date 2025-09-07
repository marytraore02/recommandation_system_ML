# # import psycopg2
# # import uuid
# # import os


# # def get_db_connection2():
# #     try:
# #         conn = psycopg2.connect(
# #             host=os.environ.get("DB_HOST", "localhost"), # "db" est le nom du service dans docker-compose
# #             database=os.environ.get("DB_NAME", "postgres"),
# #             user=os.environ.get("DB_USER", "postgres"),
# #             password=os.environ.get("DB_PASSWORD", "postgres"),
# #             port=os.environ.get("DB_PORT", "5432")
# #         )
# #         return conn
# #     except Exception as e:
# #         print(f"Erreur de connexion à la base de données: {e}")
# #         return None






# import os
# from sqlalchemy import create_engine, Column, Integer, String, Enum, UUID, BigInteger
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm import sessionmaker
# import enum
# import uuid

# # Configuration de la connexion à la base de données
# # Assurez-vous d'avoir les variables d'environnement configurées

# # Configuration du pool de connexions
# engine = create_engine(
#     DATABASE_URL, 
#     pool_size=10,        # Nombre de connexions maintenues dans le pool
#     max_overflow=20,     # Nombre de connexions supplémentaires qui peuvent être créées
#     pool_recycle=3600    # Recycler les connexions après une heure pour éviter les déconnexions inattendues
# )
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# Base = declarative_base()

# # Définition de l'énumération pour le champ 'type'
# class TypeRessource(enum.Enum):
#     JUSTIFICATIF = "JUSTIFICATIF"
#     CAGNOTTE = "CAGNOTTE"
#     TEMOIGNAGE = "TEMOIGNAGE"
#     POST_IMAGE = "POST_IMAGE"
#     POST_VIDEO = "POST_VIDEO"
#     POST_THUMBNAIL = "POST_THUMBNAIL"
#     POST_PREVIEW = "POST_PREVIEW"
#     POST_DOCUMENT = "POST_DOCUMENT"

# # Définition du modèle SQLAlchemy
# class RessourceModel(Base):
#     __tablename__ = "ressources"

#     id = Column(Integer, primary_key=True, index=True)
#     file = Column(String, nullable=False)
#     type = Column(Enum(TypeRessource), default=TypeRessource.POST_IMAGE)
#     reference = Column(UUID)
#     thumbnail_url = Column(String)
#     duration = Column(Integer)
#     width = Column(Integer)
#     height = Column(Integer)
#     file_size = Column(BigInteger)
#     order_index = Column(Integer, default=0)
#     alt_text = Column(String)
#     mime_type = Column(String)

# def init_db():
#     Base.metadata.create_all(bind=engine)












# database.py
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

# --- OPTIMISATION ICI ---
# pool_size : Le nombre de connexions à garder ouvertes en permanence dans le pool.
# max_overflow : Le nombre de connexions supplémentaires qui peuvent être ouvertes 
#                temporairement si le pool est plein.

# Attention : La somme (pool_size + max_overflow) multipliée par le nombre d'instances de votre application ne doit pas dépasser la limite de max_connections de votre serveur PostgreSQL (généralement 100 par défaut).

engine = create_async_engine(
    DATABASE_URL,
    echo=False,  # Mettre à False en production pour éviter de logger toutes les requêtes SQL
    pool_size=10, 
    max_overflow=20
)
# -------------------------

SessionLocal = sessionmaker(
    autocommit=False, 
    autoflush=False, 
    bind=engine, 
    class_=AsyncSession
)

Base = declarative_base()

async def get_db():
    async with SessionLocal() as session:
        yield session