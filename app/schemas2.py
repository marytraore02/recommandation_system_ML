# Dans schemas.py
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
from uuid import UUID
from enum import Enum

# Vous devrez définir ces Enums pour correspondre à votre base de données
class TypeRessource(str, Enum):
    POST_VIDEO = "POST_VIDEO"
    POST_THUMBNAIL = "POST_THUMBNAIL"
    POST_IMAGE = "POST_IMAGE"

class TypePost(str, Enum):
    INITIAL = "INITIAL"
    MEDAIA_ONLY = "MEDIA_ONLY"
    # ... autres types

class CagnotteSimpleResponse(BaseModel):
    id: UUID
    name: str

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