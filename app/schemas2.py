# Dans schemas.py
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
from uuid import UUID
from enum import Enum
import enum
from typing import List, Dict, Any, Optional


# Vous devrez définir ces Enums pour correspondre à votre base de données
class TypeRessource(str, Enum):
    POST_VIDEO = "POST_VIDEO"
    POST_THUMBNAIL = "POST_THUMBNAIL"
    POST_IMAGE = "POST_IMAGE"

class TypePost(str, Enum):
    INITIAL = "INITIAL"
    MEDAIA_ONLY = "MEDIA_ONLY"
    # ... autres types

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

class Categorie(BaseModel):
    id: UUID
    name: str = Field(..., max_length=255)
    # description: Optional[str] = None
    # picture: Optional[str] = None
    # created_date: datetime
    # last_modified_date: datetime
    # deleted: bool
    
    class Config:
        from_attributes = True

class CagnotteSimpleResponse(BaseModel):
    id: UUID
    name: str
    categorie: Categorie

    class Config:
        from_attributes=True

class Author(BaseModel):
    id: UUID
    firstname: str
    lastname: str
    picture: Optional[str] = None
    role: Optional[str] = None # Supposant que ce champ existe sur votre modèle User

    class Config:
        from_attributes=True

class RessourceResponse(BaseModel):
    id: int # En Java c'était Long, en Python ça peut être int
    file: str
    reference: UUID
    type: TypeRessource

    class Config:
        from_attributes=True


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
    # ressource: Optional[Ressource] = None

    class Config:
        from_attributes = True 

        
class CagnottePostFeedResponse(BaseModel):
    id: UUID
    cagnotte: CagnotteSimpleResponse
    author: Author
    type: TypePost
    title: Optional[str] = None
    content: Optional[str] = None
    likes_count: int = Field(0, alias='likesCount')
    comments_count: int = Field(0, alias='commentsCount')
    views_count: int = Field(0, alias='viewsCount')
    shares_count: int = Field(0, alias='sharesCount')
    created_date: datetime = Field(..., alias='createdDate')
    is_pinned: bool = Field(False, alias='isPinned')
    medias: List[RessourceResponse] = []
    main_media_url: Optional[str] = Field(None, alias='mainMediaUrl')
    thumbnail_url: Optional[str] = Field(None, alias='thumbnailUrl')
    preview_gif_url: Optional[str] = Field(None, alias='previewGifUrl')
    has_video: bool = Field(False, alias='hasVideo')
    has_multiple_medias: bool = Field(False, alias='hasMultipleMedias')
    total_medias: int = Field(0, alias='totalMedias')
    aspect_ratio: Optional[str] = Field(None, alias='aspectRatio')
    video_duration: Optional[int] = Field(None, alias='videoDuration')
    estimated_data_usage: int = Field(0, alias='estimatedDataUsage') # Long en Java
    mobile_video_url: Optional[str] = Field(None, alias='mobileVideoUrl')
    standard_video_url: Optional[str] = Field(None, alias='standardVideoUrl')
    hd_video_url: Optional[str] = Field(None, alias='hdVideoUrl')

    class Config:
        from_attributes=True
        allow_population_by_field_name = True # Pour permettre l'utilisation des alias


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