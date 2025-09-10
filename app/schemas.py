#Pydantic est utilisé pour définir la forme des données que nous attendons en entrée et en sortie de notre API. C'est une excellente pratique pour la validation et la documentation automatique.

from pydantic import BaseModel, Field
from uuid import UUID
import uuid
from datetime import datetime
from models import TypeRessource
import enum
from typing import List, Dict, Any, Optional



class TypeRessource(str, enum.Enum):
    JUSTIFICATIF = "JUSTIFICATIF"
    CAGNOTTE = "CAGNOTTE"
    TEMOIGNAGE = "TEMOIGNAGE"
    POST_IMAGE = "POST_IMAGE"
    POST_VIDEO = "POST_VIDEO"
    POST_THUMBNAIL = "POST_THUMBNAIL"
    POST_PREVIEW = "POST_PREVIEW"
    POST_DOCUMENT = "POST_DOCUMENT"

class StatutCagnotte(str, enum.Enum):
    EN_COURS = "EN_COURS"
    VALIDE = "VALIDE"
    SUSPENDU = "SUSPENDU"

class TypeCagnotte(str, enum.Enum):
    PUBLIC = "PUBLIC"
    PRIVE = "PRIVE"

class TypePost(str, enum.Enum):
    STANDARD = "iNITIAL"
    MEDIA_ONLY = "MEDIA_ONLY"

class Ressource(BaseModel):
    id: int
    file: str
    type: Optional[TypeRessource] = None
    reference: Optional[UUID] = None
    thumbnail_url: Optional[str] = None
    duration: Optional[int] = None
    width: Optional[int] = None
    height: Optional[int] = None
    file_size: Optional[int] = None
    order_index: Optional[int] = 0
    alt_text: Optional[str] = None
    mime_type: Optional[str] = None

    class Config:
        from_attributes = True 

class Author(BaseModel):
    id: UUID
    firstname: str
    lastname: str
    picture: Optional[str] = None
    role: Optional[str] = None

    class Config:
        from_attributes = True

class Categorie(BaseModel):
    id: UUID
    name: str = Field(..., max_length=255)
    description: Optional[str] = None
    picture: Optional[str] = None
    created_date: datetime
    last_modified_date: datetime
    deleted: bool
    
    class Config:
        from_attributes = True

class Cagnotte(BaseModel):
    id: UUID
    name: str = Field(..., max_length=255)
    description: Optional[str] = None
    pays: str = Field(..., max_length=100)
    date_start: Optional[datetime] = None
    date_end: Optional[datetime] = None
    objectif: Optional[int] = Field(None, ge=0)
    total_solde: int = Field(0, ge=0)
    current_solde: int = Field(0, ge=0)
    statut: Optional[StatutCagnotte] = StatutCagnotte.EN_COURS
    type: Optional[TypeCagnotte] = TypeCagnotte.PUBLIC
    
    categorie: Categorie
    admin: Author  # Utiliser le schéma Author pour la relation
    
    created_date: datetime
    last_modified_date: datetime
    deleted: bool   

    class Config:
        from_attributes = True


class CagnottePost(BaseModel):
    id: UUID
    type: Optional[str] = None
    title: Optional[str] = None
    content: Optional[str] = None
    order_index: Optional[int]
    is_main_post: bool
    likes_count: int
    comments_count: int
    views_count: int
    shares_count: int
    is_pinned: bool
    deleted: bool
    created_date: datetime
    author: Author
    cagnotte: Cagnotte
    ressource: Optional[Ressource] = None

    class Config:
        from_attributes = True

class PostWithRessources(BaseModel):
    id: UUID
    type: Optional[str] = None
    title: Optional[str] = None
    content: Optional[str] = None
    order_index: Optional[int]
    is_main_post: bool
    likes_count: int
    comments_count: int
    views_count: int
    shares_count: int
    is_pinned: bool
    deleted: bool
    created_date: datetime
    author: Author
    # cagnotte: Cagnotte
    # ressource: Optional[Ressource] = None
    ressources: List[Ressource] = []

    class Config:
        from_attributes = True 

class CagnotteWithMeta(Cagnotte): # Hérite de votre schéma Cagnotte de base
    post: Optional[PostWithRessources] = None


class RessourceEnrichie(Ressource):
    """
    Ce schéma hérite de toutes les propriétés de Ressource
    et y ajoute les détails du post associé.
    """
    cagnotte_posts: Optional[CagnottePost] = None


# Le schéma final pour la recommandation
# Il contient la cagnotte et potentiellement son média principal
class CagnotteRecommandee(BaseModel):
    id: UUID
    name: str = Field(..., max_length=255)
    description: Optional[str] = None
    pays: str = Field(..., max_length=100)
    date_start: Optional[datetime] = None
    date_end: Optional[datetime] = None
    objectif: Optional[int] = Field(None, ge=0)
    total_solde: int = Field(0, ge=0)
    current_solde: int = Field(0, ge=0)
    statut: Optional[StatutCagnotte] = StatutCagnotte.EN_COURS
    type: Optional[TypeCagnotte] = TypeCagnotte.PUBLIC
    categorie: Categorie
    admin: Author
    created_date: datetime
    last_modified_date: datetime
    deleted: bool   

    # Le nouveau champ pour le média !
    main_media: Optional[Ressource] = None

    class Config:
        from_attributes = True


class EventModel(BaseModel):
    event_id: int
    event_type: str
    timestamp: str
    user_type: str
    cagnotte_id:  Optional[str] = None
    video_id:  Optional[str] = None # Optionnel, car pas présent dans tous les events
    post_id:  Optional[str] = None  # Optionnel, car pas présent dans tous les events
    id_categorie: str
    pays: str
    data: Dict[str, Any]
    user_id:  Optional[str] = None
    session_id:  Optional[str] = None

# Ce modèle valide le lot d'événements reçu.
class EventBatchModel(BaseModel):
    events: List[EventModel] = Field(..., min_length=1)