# Pydantic est la colonne vertébrale de FastAPI. Il valide les données entrantes et sortantes, ce qui rend votre API très robuste.

from typing import List, Dict, Any, Optional
import uuid
from datetime import datetime
from pydantic import BaseModel, Field, EmailStr
from enum import Enum


class StatutCagnotte(str, Enum):
    # Ajoutez ici les statuts possibles de votre cagnotte
    # Exemple :
    EN_COURS = "EN_COURS"
    TERMINEE = "TERMINEE"
    ANNULEE = "ANNULEE"

class TypeCagnotte(str, Enum):
    PUBLIC = "PUBLIC"
    PRIVE = "PRIVE"

class CagnotteDetails(BaseModel):
    id: Optional[uuid.UUID] = Field(default_factory=uuid.uuid4)
    name: str = Field(..., max_length=255)
    description: Optional[str] = Field(None)
    pays: str = Field("Mali", max_length=100)
    date_start: Optional[datetime] = Field(None)
    date_end: Optional[datetime] = Field(None)
    objectif: Optional[int] = Field(None, ge=0)
    total_solde: int = Field(0, ge=0)
    current_solde: int = Field(0, ge=0)
    statut: Optional[StatutCagnotte] = Field(None)
    type: TypeCagnotte = Field(TypeCagnotte.PUBLIC)
    
    # Relations avec d'autres modèles (pas de validation automatique des types)
    # Pydantic ne valide pas les instances d'autres classes par défaut. 
    # Vous devrez les importer si elles sont nécessaires.
    id_categorie: Optional[uuid.UUID] = Field(None)
    admin: Optional[uuid.UUID] = Field(None) # Représenté par son UUID
    
    created_date: Optional[datetime] = Field(None)
    last_modified_date: Optional[datetime] = Field(None)
    deleted: bool = Field(False)
    

class VideoDetails(BaseModel):
    video_id: Optional[str] = None
    cagnotte_id: Optional[str] = None
    score: Optional[float] = None
    views: Optional[int] = None
    shares: Optional[int] = None
    favorites: Optional[int] = None
    skips: Optional[int] = None
    trending_score: Optional[float] = None
    # current_score: Optional[float] = None
    # previous_score: Optional[float] = None
    velocity_percent: Optional[float] = None
    # recent_events: Optional[int] = None
    # comparison_events: Optional[int] = None
    trend_direction: Optional[str] = None
    cagnotte_details: Optional[CagnotteDetails] = None
    
class VideoList(BaseModel):
    popular: List[VideoDetails]
    trending: List[VideoDetails]




# Ce modèle Pydantic permet de valider et de structurer les données de sortie.
# Il doit correspondre à la structure que vous sauvegardez dans les Hashes Redis.
# class VideoDetails(BaseModel):
#     video_id: str
#     cagnotte_id: str
#     score: float
#     views: int
#     shares: int
#     favorites: int
#     skips: int
#     cagnotte_details: Optional[CagnotteDetails] = None


class AdminDetails(BaseModel):
    firstName: str
    lastName: str
    phone: Optional[str] = None
    email: str
    picture: Optional[str] = None

# Modèle pour les détails de la catégorie
class CategorieDetails(BaseModel):
    id: uuid.UUID
    name: str

# # Modèle principal pour la cagnotte, reflétant la structure de sortie souhaitée
# class CagnotteDetails(BaseModel):
#     id: uuid.UUID
#     name: str
#     description: Optional[str] = None
#     pays: str
#     objectif: int
#     totalContribuer: int # Notez le camelCase pour correspondre à votre JSON
#     statut: str
#     type: str
#     categorie: CategorieDetails
#     admin: AdminDetails

# Modèle de réponse final et enrichi pour votre endpoint
class VideoEnrichedDetails(BaseModel):
    video_id: str
    cagnotte_id: str
    # title: str
    # cagnotte_name: str
    # category: str
    score: float
    views: int
    shares: int
    favorites: int
    skips: int
    # replays: int
    # completion_rate: float
    # engagement_rate: float
    # recency_factor: float
    # rank: int
    # last_updated: Optional[str] = None
    cagnotte: CagnotteDetails # L'objet cagnotte complet et imbriqué



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

    class Config:
        from_attributes = True





# Définition des énumérations
# En Python, les énumérations sont des classes qui héritent de `enum.Enum` ou `str, enum.Enum`
# pour avoir des valeurs de type `str`.
class StatutCagnotte(str, Enum):
    # Ajoutez ici les statuts possibles de votre cagnotte
    # Exemple :
    EN_COURS = "EN_COURS"
    TERMINEE = "TERMINEE"
    ANNULEE = "ANNULEE"

class TypeCagnotte(str, Enum):
    PUBLIC = "PUBLIC"
    PRIVE = "PRIVE"

# Définition de la classe `CagnotteModel` qui hérite de `BaseModel`
class CagnotteModel(BaseModel):
    id: Optional[uuid.UUID] = Field(default_factory=uuid.uuid4)
    name: str = Field(..., max_length=255)
    description: Optional[str] = Field(None)
    pays: str = Field("Mali", max_length=100)
    date_start: Optional[datetime] = Field(None)
    date_end: Optional[datetime] = Field(None)
    objectif: Optional[int] = Field(None, ge=0)
    total_solde: int = Field(0, ge=0)
    current_solde: int = Field(0, ge=0)
    statut: Optional[StatutCagnotte] = Field(None)
    type: TypeCagnotte = Field(TypeCagnotte.PUBLIC)
    
    # Relations avec d'autres modèles (pas de validation automatique des types)
    # Pydantic ne valide pas les instances d'autres classes par défaut. 
    # Vous devrez les importer si elles sont nécessaires.
    id_categorie: Optional[uuid.UUID] = Field(None)
    admin: Optional[uuid.UUID] = Field(None) # Représenté par son UUID
    
    created_date: Optional[datetime] = Field(None)
    last_modified_date: Optional[datetime] = Field(None)
    deleted: bool = Field(False)


# Définition de l'énumération pour le statut de l'utilisateur
class StatutUser(str, Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    BLOQUE = "BLOQUE"

# Définition de la classe `UserModel` qui hérite de `BaseModel`
class UserModel(BaseModel):
    id: uuid.UUID = Field(...)
    firstname: str = Field(...)
    lastname: str = Field(...)
    email: EmailStr = Field(...) # Utilisation du type `EmailStr` pour la validation
    phone: str = Field(...)
    picture: Optional[str] = Field(None)
    statut: StatutUser = Field(StatutUser.ACTIVE)
    confirmed: bool = Field(...)
    role: str = Field(...)
    deleted: bool = Field(False)
    my_code_parrain: Optional[str] = Field(None, max_length=10)
    code_parrain: Optional[str] = Field(None, max_length=10)
    point_fidelite: int = Field(0, ge=0)
    created_date: datetime = Field(...)
    last_modified_date: datetime = Field(...)


class RecommandationResult(BaseModel):
    id: str
    titre: str
    description: str
    categorie: str
    score_reco: float