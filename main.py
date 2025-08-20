# app/main.py
from fastapi import FastAPI, HTTPException
from typing import List
from . import crud, models
from .recommender import engine
import time

app = FastAPI(
    title="API de Recommandation pour les Cagnottes",
    description="Une API pour recommander des cagnottes en fonction du profil utilisateur ou de la popularité."
)

@app.get("/")
def read_root():
    return {"message": "Bienvenue sur l'API de recommandation de cagnottes !"}

@app.get(
    "/recommendations/{user_id}",
    response_model=List[models.RecommandationResult],
    summary="Obtenir des recommandations pour un utilisateur"
)
def get_recommendations(user_id: str, limit: int = 5):
    """
    Retourne une liste de cagnottes recommandées pour un utilisateur donné.
    
    - **user_id**: L'ID de l'utilisateur.
    - **limit**: Le nombre de recommandations à retourner.
    """
    
    # Étape 1: Récupérer le catalogue de cagnottes
    cagnottes = crud.get_cagnottes_from_db()
    if not cagnottes:
        raise HTTPException(status_code=503, detail="Service de base de données indisponible.")

    # Étape 2: Récupérer le profil de l'utilisateur
    user_profile = crud.get_user_profile_from_db(user_id)
    
    # Étape 3: Exécuter la logique de recommandation
    if user_profile:
        # Recommandation par préférences si un profil existe
        recommended_cagnottes = engine.recommander_videos_par_preferences(user_profile, cagnottes, limit)
    else:
        # Stratégie de repli par popularité si aucun profil n'existe
        recommended_cagnottes = engine.recommander_videos_par_popularite(cagnottes, limit)
        
    # Étape 4: Formater la réponse en utilisant le modèle Pydantic
    results = []
    for cagnotte in recommended_cagnottes:
        results.append(models.RecommandationResult(
            id=cagnotte['id'],
            titre=cagnotte['titre'],
            description=cagnotte['description'],
            categorie=cagnotte['categorie'],
            score_reco=cagnotte.get('score_calculé', 0)
        ))
        
    return results