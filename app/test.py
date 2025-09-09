from fastapi import FastAPI, Depends, HTTPException,status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, Session
from typing import List, Optional
import uuid as uuid_pkg
import redis
import redis.asyncio as redis

from db.redis_client import get_redis_client, redis_connection_pool

import models
import schemas
# from database import engine, get_db

from sqlalchemy import select

# Assurez-vous d'importer vos modèles et schémas corrects
from models import RessourceModel, CagnottePostModel, CagnotteModel, UserModel, CategorieModel
from schemas import RessourceEnrichie, CagnottePost, Ressource, Author, Cagnotte, Categorie
from db.postgres_config import get_db, AsyncSession, engine

app = FastAPI(
    title="API de Ressources",
    description="Une API pour gérer des ressources multimédias et leurs données associées.",
    version="1.1.0"
)

redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


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
                # Charger l'auteur du post
                joinedload(CagnottePostModel.author), 
                joinedload(CagnottePostModel.cagnotte).joinedload(CagnotteModel.admin),
                joinedload(CagnottePostModel.cagnotte).joinedload(CagnotteModel.categorie)
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




@app.get("/recommendations/", response_model=List[schemas.Cagnotte])
def get_recommendations(
    user_id: Optional[str] = None, 
    session_id: Optional[str] = None,
    db: Session = Depends(get_db) # Injection de dépendance pour la session BDD
):
    # --- 0. Valider l'identifiant de l'utilisateur ---
    if not user_id and not session_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="user_id ou session_id doit être fourni."
        )

    redis_key = f"profile:user:{user_id}" if user_id else f"profile:session:{session_id}"

    # --- 1. Obtenir le profil de l'utilisateur depuis Redis ---
    user_profile = redis_client.hgetall(redis_key)
    
    if not user_profile:
        # Cas "Cold Start": l'utilisateur est nouveau, pas de profil.
        # On pourrait retourner les cagnottes les plus populaires.
        # Pour l'instant, on retourne une liste vide.
        return []

    # --- 2. Identifier les N catégories préférées ---
    # Convertir les scores en float et trier par score décroissant
    sorted_categories = sorted(
        user_profile.items(), 
        key=lambda item: float(item[1]), 
        reverse=True
    )
    
    # Prendre les IDs des 3 meilleures catégories
    top_category_ids = [cat_id for cat_id, score in sorted_categories[:3]]

    if not top_category_ids:
        return []

    # --- 3. (À FAIRE) Récupérer les cagnottes déjà vues par l'utilisateur ---
    # Idéalement, vous avez un autre set Redis par utilisateur listant les cagnotte_id vues
    # ex: seen_cagnottes_key = f"seen:cagnottes:{redis_key}"
    # seen_cagnottes_ids = redis_client.smembers(seen_cagnottes_key)
    seen_cagnottes_ids = set() # Pour l'exemple, on suppose qu'il n'en a vu aucune

    # --- 4. Récupérer les cagnottes à recommander depuis la BDD principale ---
    # CECI EST UN EXEMPLE AVEC SQLAlchemy. Adaptez-le à votre ORM ou BDD.
    
    # La magie de l'ORM : `joinedload` pré-charge les données imbriquées (categorie, admin)
    # en une seule requête SQL optimisée (via des JOINs) pour éviter le problème N+1.
    
    query = (
        db.query(models.Cagnotte)
        .options( 
            joinedload(models.Cagnotte.categorie), 
            joinedload(models.Cagnotte.admin)
        )
        .filter(models.Cagnotte.categorie_id.in_(top_category_ids))
        .filter(models.Cagnotte.id.notin_(seen_cagnottes_ids))
        .filter(models.Cagnotte.statut == "VALIDE")
        .order_by(models.Cagnotte.created_date.desc())
        .limit(10)
    )
    
    recommended_cagnottes = query.all()

    # --- Pour la démo, nous utilisons des données factices ---
    # Remplacez cette partie par votre vraie requête à la base de données
    from .fake_data import get_fake_cagnottes # Fichier factice que vous créez
    recommended_cagnottes = get_fake_cagnottes(top_category_ids)


    # --- 5. Retourner la réponse ---
    # Pydantic va automatiquement convertir vos objets de BDD en JSON bien structuré.
    return recommended_cagnottes

