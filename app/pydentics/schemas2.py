# Dans schemas.py
from pydantic import BaseModel, Field, EmailStr, ConfigDict
from typing import List, Optional
from datetime import datetime
from uuid import UUID
from enum import Enum
import enum
from decimal import Decimal
from typing import List, Dict, Any, Optional
from .enums import TypeOrganisation, TypePost, TypeRessource, StatutCagnotte, TypeCagnotte, StatutUser, TypeCompte


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

class RessourceResponse(BaseModel):
    id: int
    file: str
    reference: UUID
    type: TypeRessource

    class Config:
        from_attributes=True

class Categorie(BaseModel):
    id: UUID
    name: str = Field(..., max_length=255)
    # description: Optional[str] = None
    # picture: Optional[str] = None
    # created_date: datetime
    # last_modified_date: datetime
    # deleted: bool

    # Permet à Pydantic de lire les données depuis un objet ORM (SQLAlchemy)
    # model_config = ConfigDict(from_attributes=True)
    
    class Config:
        from_attributes=True

class CagnotteSimpleResponse(BaseModel):
    id: UUID
    name: str
    description: Optional[str] = None
    categorie: Categorie
    sponsors: List['Sponsor'] = Field(default_factory=list, description="Liste des sponsors associés à la cagnotte.")

    class Config:
        from_attributes=True


class Author(BaseModel):
    id: UUID
    firstname: str
    lastname: str
    email: str
    phone: str
    picture: Optional[str] = None
    role: TypeCompte = TypeCompte.USER

    class Config:
        from_attributes=True


class Cagnotte(BaseModel):
    id: Optional[UUID] = None
    name: str = Field(..., max_length=255)
    description: Optional[str] = None
    pays: str = Field(default="Mali", max_length=100)

    date_start: Optional[datetime] = None
    date_end: Optional[datetime] = None

    objectif: Optional[int] = None
    total_solde: int = 0
    current_solde: int = 0
    total_contributors: int = 0

    is_certified: bool = False
    mode_retrait: str = Field(..., max_length=50)
    renew_count: int = 0

    statut: Optional[StatutCagnotte] = StatutCagnotte.EN_COURS
    type: Optional[TypeCagnotte] = TypeCagnotte.PUBLIC
    commission: Decimal = Decimal("0.000")

    categorie: Categorie
    admin: Author

    created_date: Optional[datetime] = None
    last_modified_date: Optional[datetime] = None
    deleted: bool = False

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
    last_modified_date: datetime
    author: Author
    cagnotte: Cagnotte
    ressource: Optional[Ressource] = None

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
    phone: Optional[str] = None

# Ce modèle valide le lot d'événements reçu.
class EventBatchModel(BaseModel):
    events: List[EventModel] = Field(..., min_length=1)


class OrganisationBase(BaseModel):
    # Les champs importants pour l'affichage du sponsor
    id: UUID
    name: str
    acronym: Optional[str] = None
    email: str
    phone: str
    logo: Optional[str] = None
    # Inclure le type car il peut être pertinent
    type_organisation: TypeOrganisation
    # description: Optional[str] = None
    # nif: str
    # rccm: str
    # rccm_document: Optional[str] = None
    # nif_document: Optional[str] = None
    # deleted: bool
    # created_date: datetime
    # last_modified_date: datetime
    class Config:
        from_attributes = True


class Sponsor(BaseModel):
    id: UUID
    montant: int = Field(default=0)
    created_date: datetime
    organisation: OrganisationBase
    # id_cagnotte: UUID

    class Config:
        from_attributes = True


class CagnottePostFeedResponse(BaseModel):
    id: UUID
    cagnotte: CagnotteSimpleResponse
    author: Author
    sponsors: List[Sponsor] = Field(default_factory=list, description="Liste des sponsors associés à la cagnotte.")
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
        from_attributes = True

