# Pydantic est la colonne vertébrale de FastAPI. Il valide les données entrantes et sortantes, ce qui rend votre API très robuste.

# app/models.py
from pydantic import BaseModel
from typing import List, Dict, Any, Optional

class Cagnotte(BaseModel):
    id: str
    titre: str
    description: str
    categorie: str
    media_url: List[str]
    nbreLike: int
    nbreVue: int
    nbreCommentaire: int
    budget_necessaire: int
    
    class Config:
        from_attributes = True

class UserProfile(BaseModel):
    user_id: str
    nom: str
    prenom: str
    telephone: str
    profession: str
    provenance_capital: str
    budget_min: int
    budget_max: int
    pourquoi: str
    categorie_dons: Optional[List[str]] = []

class RecommandationResult(BaseModel):
    id: str
    titre: str
    description: str
    categorie: str
    score_reco: float