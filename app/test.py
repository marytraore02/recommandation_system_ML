from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import joinedload
from typing import List
import uuid as uuid_pkg


import models
import schemas
# from database import engine, get_db

from sqlalchemy.orm import joinedload
from sqlalchemy import select
from fastapi import APIRouter, Depends, HTTPException

# Assurez-vous d'importer vos modèles et schémas corrects
from models import RessourceModel, CagnottePostModel, CagnotteModel, UserModel, CategorieModel
from schemas import RessourceEnrichie, CagnottePost, Ressource, Author, Cagnotte, Categorie
from database import get_db, AsyncSession, engine

app = FastAPI(
    title="API de Ressources",
    description="Une API pour gérer des ressources multimédias et leurs données associées.",
    version="1.1.0"
)

@app.on_event("startup")
async def startup():
    # Crée les tables dans la base de données au démarrage (pour le développement)
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.create_all)


# ### A. Récupérer toutes les ressources (sans limite) ###
@app.get("/ressources/", response_model=List[schemas.Ressource], tags=["Ressources"])
async def get_all_ressources(db: AsyncSession = Depends(get_db)):
    """
    Récupère LA TOTALITÉ des ressources de la table.
    ATTENTION : Non recommandé en production pour les tables volumineuses,
    car cela peut consommer beaucoup de mémoire et être lent.
    La pagination est généralement une meilleure pratique.
    """
    result = await db.execute(select(models.RessourceModel))
    ressources = result.scalars().all()
    return ressources


# @app.get("/ressources/{ressource_id}/enriched", response_model=schemas.RessourceEnrichie, tags=["Ressources"])
# async def get_enriched_ressource(ressource_id: int, db: AsyncSession = Depends(get_db)):
#     """
#     Récupère une ressource et l'enrichit avec les détails du post,
#     incluant les informations sur l'auteur et la cagnotte.
#     """
#     # 1. Récupérer la ressource
#     result_res = await db.execute(select(models.RessourceModel).where(models.RessourceModel.id == ressource_id))
#     ressource = result_res.scalars().first()

#     if not ressource:
#         raise HTTPException(status_code=404, detail="Ressource non trouvée")

#     cagnotte_posts = None
#     if ressource.reference:
#         # 2. Construire la requête pour le post avec les jointures (chargement anticipé)
#         query = (
#             select(models.CagnottePostModel)
#             .options(
#                 joinedload(models.CagnottePostModel.author), 
#                 joinedload(models.CagnottePostModel.cagnotte)
#             )
#             .where(models.CagnottePostModel.id == ressource.reference)
#         )
#         # Le .options(joinedload(...)) est le cœur de l'optimisation.
#         # Il dit à SQLAlchemy : "Quand tu récupères le post, fais aussi une jointure
#         # pour récupérer l'auteur et la cagnotte dans la MÊME requête SQL."

#         result_post = await db.execute(query)
#         post = result_post.scalars().first()
        
#         if post:
#             # Pydantic s'occupe de la conversion du modèle SQLAlchemy complexe en schéma JSON
#             cagnotte_posts = schemas.CagnottePost.from_orm(post)
    
#     # 3. Construire la réponse enrichie
#     enriched_data = schemas.RessourceEnrichie(
#         **ressource.__dict__,
#         cagnotte_posts=cagnotte_posts
#     )

#     return enriched_data








@app.get("/ressources/{ressource_id}/enriched", response_model=RessourceEnrichie, tags=["Ressources"])
async def get_enriched_ressource(ressource_id: int, db: AsyncSession = Depends(get_db)):
    """
    Récupère une ressource et l'enrichit avec les détails du post,
    incluant les informations sur l'auteur et la cagnotte.
    """
    # 1. Récupérer la ressource
    result_res = await db.execute(select(RessourceModel).where(RessourceModel.id == ressource_id))
    ressource = result_res.scalars().first()

    if not ressource:
        raise HTTPException(status_code=404, detail="Ressource non trouvée")

    cagnotte_posts = None
    if ressource.reference:
        # 2. Construire la requête pour le post avec les jointures complètes
        # Chaînage des joinedload pour charger les relations imbriquées
        query = (
            select(CagnottePostModel)
            .options(
                joinedload(CagnottePostModel.author), 
                # Charger la cagnotte, puis l'administrateur et la catégorie de la cagnotte
                joinedload(CagnottePostModel.cagnotte)
                .joinedload(CagnotteModel.admin)
            )
            .where(CagnottePostModel.id == ressource.reference)
        )
        
        result_post = await db.execute(query)
        post = result_post.scalars().first()
        
        if post:
            # Assurez-vous d'utiliser `from_orm` ou `model_validate`
            # pour transformer l'objet SQLAlchemy en Pydantic
            cagnotte_posts = CagnottePost.from_orm(post)
    
    # 3. Construire la réponse enrichie
    enriched_data = RessourceEnrichie(
        # Utilisez le dictionnaire de l'objet Ressource pour les données de base
        **ressource.__dict__,
        # Assignez le post enrichi à la propriété correcte
        cagnotte_posts=cagnotte_posts
    )

    return enriched_data