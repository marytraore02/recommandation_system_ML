# Pydantic est la colonne vertébrale de FastAPI. Il valide les données entrantes et sortantes, ce qui rend votre API très robuste.

# from pyparsing import Enum
from sqlalchemy import (Column, String, Integer, BigInteger, Boolean, Text, 
                        DateTime, Enum as SQLAlchemyEnum, ForeignKey)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
import uuid
from database import Base
import enum
from datetime import datetime
from sqlalchemy.ext.declarative import declarative_base

# # Ce modèle valide chaque événement individuel dans le lot.
# class EventModel(BaseModel):
#     event_id: int
#     event_type: str
#     timestamp: str
#     user_id: str
#     user_type: str
#     cagnotte_id: str
#     video_id: str | None = None  # Optionnel, car pas présent dans tous les events
#     categorie: str
#     pays: str
    
# class EventBatchModel(BaseModel):
#     events: List[EventModel] = Field(..., min_length=1)

# class StatutCagnotte(str, Enum):
#     # Ajoutez ici les statuts possibles de votre cagnotte
#     # Exemple :
#     EN_COURS = "EN_COURS"
#     TERMINEE = "TERMINEE"
#     ANNULEE = "ANNULEE"

# class TypeCagnotte(str, Enum):
#     PUBLIC = "PUBLIC"
#     PRIVE = "PRIVE"

# class CagnotteDetails(BaseModel):
#     id: Optional[uuid.UUID] = Field(default_factory=uuid.uuid4)
#     name: str = Field(..., max_length=255)
#     description: Optional[str] = Field(None)
#     pays: str = Field("Mali", max_length=100)
#     date_start: Optional[datetime] = Field(None)
#     date_end: Optional[datetime] = Field(None)
#     objectif: Optional[int] = Field(None, ge=0)
#     total_solde: int = Field(0, ge=0)
#     current_solde: int = Field(0, ge=0)
#     statut: Optional[StatutCagnotte] = Field(None)
#     type: TypeCagnotte = Field(TypeCagnotte.PUBLIC)
    
#     # Relations avec d'autres modèles (pas de validation automatique des types)
#     # Pydantic ne valide pas les instances d'autres classes par défaut. 
#     # Vous devrez les importer si elles sont nécessaires.
#     id_categorie: Optional[uuid.UUID] = Field(None)
#     admin: Optional[uuid.UUID] = Field(None) # Représenté par son UUID
    
#     created_date: Optional[datetime] = Field(None)
#     last_modified_date: Optional[datetime] = Field(None)
#     deleted: bool = Field(False)
    

# class VideoDetails(BaseModel):
#     video_id: Optional[str] = None
#     cagnotte_id: Optional[str] = None
#     score: Optional[float] = None
#     views: Optional[int] = None
#     shares: Optional[int] = None
#     favorites: Optional[int] = None
#     skips: Optional[int] = None
#     trending_score: Optional[float] = None
#     # current_score: Optional[float] = None
#     # previous_score: Optional[float] = None
#     velocity_percent: Optional[float] = None
#     # recent_events: Optional[int] = None
#     # comparison_events: Optional[int] = None
#     trend_direction: Optional[str] = None
#     cagnotte_details: Optional[CagnotteDetails] = None
    
# class VideoList(BaseModel):
#     popular: List[VideoDetails]
#     trending: List[VideoDetails]



# L'énumération doit correspondre à celle de la base de données
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
    # ANNULEE = "ANNULEE"

class TypeCagnotte(str, enum.Enum):
    PUBLIC = "PUBLIC"
    PRIVE = "PRIVE"

class RessourceModel(Base):
    __tablename__ = "ressources"

    id = Column(BigInteger, primary_key=True, index=True)
    file = Column(String, nullable=False)
    type = Column(SQLAlchemyEnum(TypeRessource))
    # reference = Column(UUID(as_uuid=True), default=uuid.uuid4)
    reference = Column(UUID(as_uuid=True), index=True, nullable=True) # index=True pour des recherches plus rapides
    thumbnail_url = Column(String, nullable=True)
    duration = Column(Integer, nullable=True)
    width = Column(Integer, nullable=True)
    height = Column(Integer, nullable=True)
    file_size = Column(BigInteger, nullable=True)
    order_index = Column(Integer, default=0)
    alt_text = Column(String, nullable=True)
    mime_type = Column(String, nullable=True)


class UserModel(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    firstname = Column("firstname", String, nullable=False)
    lastname = Column("lastname", String, nullable=False)
    email = Column(String, unique=True, nullable=False)
    picture = Column(String, nullable=True)
    # Note : J'ai inclus les champs les plus pertinents pour une réponse d'API.
    # Vous pouvez ajouter les autres au besoin.

class CategorieModel(Base):
    __tablename__ = "categories"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    picture = Column(String(255), nullable=True)
    created_date = Column(DateTime, default=datetime.utcnow)
    last_modified_date = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    deleted = Column(Boolean, nullable=False, default=False)


class CagnotteModel(Base):
    __tablename__ = "cagnottes"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    pays = Column(String(100), default="Mali")
    date_start = Column(DateTime, nullable=True)
    date_end = Column(DateTime, nullable=True)
    objectif = Column(BigInteger, nullable=True)
    total_solde = Column(BigInteger, default=0)
    current_solde = Column(BigInteger, default=0)
    statut = Column(SQLAlchemyEnum(StatutCagnotte))
    type = Column(SQLAlchemyEnum(TypeCagnotte))
    
    id_categorie = Column(UUID(as_uuid=True), ForeignKey("categories.id"), nullable=False)
    # admin = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    admin_id = Column("admin", UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)

    
    created_date = Column(DateTime, default=datetime.utcnow)
    last_modified_date = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    deleted = Column(Boolean, nullable=False, default=False)
    
    # Relations
    categorie = relationship("CategorieModel")
    admin = relationship("UserModel")
    # category = relationship("CategoryModel") # Assuming a CategoryModel exists



class CagnottePostModel(Base):
    __tablename__ = "cagnotte_posts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # AJOUT DES CLÉS ÉTRANGÈRES
    id_cagnotte = Column(UUID(as_uuid=True), ForeignKey("cagnottes.id"), nullable=False)
    id_author = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    
    # AJOUT DES RELATIONS
    cagnotte = relationship("CagnotteModel")
    author = relationship("UserModel")

    type = Column(String(50), nullable=False, default="UPDATE")
    title = Column(String(255), nullable=True)
    content = Column(Text, nullable=True)
    order_index = Column(Integer, default=0)
    is_main_post = Column(Boolean, default=False)
    likes_count = Column(Integer, default=0)
    comments_count = Column(Integer, default=0)
    views_count = Column(Integer, default=0)
    shares_count = Column(Integer, default=0)
    is_pinned = Column(Boolean, default=False)
    created_date = Column(DateTime, default=datetime.utcnow)
    last_modified_date = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    deleted = Column(Boolean, nullable=False, default=False)
    


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


# class AdminDetails(BaseModel):
#     firstName: str
#     lastName: str
#     phone: Optional[str] = None
#     email: str
#     picture: Optional[str] = None

# # Modèle pour les détails de la catégorie
# class CategorieDetails(BaseModel):
#     id: uuid.UUID
#     name: str

# # # Modèle principal pour la cagnotte, reflétant la structure de sortie souhaitée
# # class CagnotteDetails(BaseModel):
# #     id: uuid.UUID
# #     name: str
# #     description: Optional[str] = None
# #     pays: str
# #     objectif: int
# #     totalContribuer: int # Notez le camelCase pour correspondre à votre JSON
# #     statut: str
# #     type: str
# #     categorie: CategorieDetails
# #     admin: AdminDetails

# # Modèle de réponse final et enrichi pour votre endpoint
# class VideoEnrichedDetails(BaseModel):
#     video_id: str
#     cagnotte_id: str
#     # title: str
#     # cagnotte_name: str
#     # category: str
#     score: float
#     views: int
#     shares: int
#     favorites: int
#     skips: int
#     # replays: int
#     # completion_rate: float
#     # engagement_rate: float
#     # recency_factor: float
#     # rank: int
#     # last_updated: Optional[str] = None
#     cagnotte: CagnotteDetails # L'objet cagnotte complet et imbriqué



# class UserProfile(BaseModel):     
#     user_id: str     
#     nom: str     
#     prenom: str     
#     telephone: str     
#     profession: str     
#     provenance_capital: str     
#     budget_min: int     
#     budget_max: int     
#     pourquoi: str     
#     categorie_dons: Optional[List[str]] = []

#     class Config:
#         from_attributes = True





# # Définition des énumérations
# # En Python, les énumérations sont des classes qui héritent de `enum.Enum` ou `str, enum.Enum`
# # pour avoir des valeurs de type `str`.
# class StatutCagnotte(str, Enum):
#     # Ajoutez ici les statuts possibles de votre cagnotte
#     # Exemple :
#     EN_COURS = "EN_COURS"
#     TERMINEE = "TERMINEE"
#     ANNULEE = "ANNULEE"

# class TypeCagnotte(str, Enum):
#     PUBLIC = "PUBLIC"
#     PRIVE = "PRIVE"

# # Définition de la classe `CagnotteModel` qui hérite de `BaseModel`
# class CagnotteModel(BaseModel):
#     id: Optional[uuid.UUID] = Field(default_factory=uuid.uuid4)
#     name: str = Field(..., max_length=255)
#     description: Optional[str] = Field(None)
#     pays: str = Field("Mali", max_length=100)
#     date_start: Optional[datetime] = Field(None)
#     date_end: Optional[datetime] = Field(None)
#     objectif: Optional[int] = Field(None, ge=0)
#     total_solde: int = Field(0, ge=0)
#     current_solde: int = Field(0, ge=0)
#     statut: Optional[StatutCagnotte] = Field(None)
#     type: TypeCagnotte = Field(TypeCagnotte.PUBLIC)
    
#     # Relations avec d'autres modèles (pas de validation automatique des types)
#     # Pydantic ne valide pas les instances d'autres classes par défaut. 
#     # Vous devrez les importer si elles sont nécessaires.
#     id_categorie: Optional[uuid.UUID] = Field(None)
#     admin: Optional[uuid.UUID] = Field(None) # Représenté par son UUID
    
#     created_date: Optional[datetime] = Field(None)
#     last_modified_date: Optional[datetime] = Field(None)
#     deleted: bool = Field(False)


# # Définition de l'énumération pour le statut de l'utilisateur
# class StatutUser(str, Enum):
#     ACTIVE = "ACTIVE"
#     INACTIVE = "INACTIVE"
#     BLOQUE = "BLOQUE"

# # Définition de la classe `UserModel` qui hérite de `BaseModel`
# class UserModel(BaseModel):
#     id: uuid.UUID = Field(...)
#     firstname: str = Field(...)
#     lastname: str = Field(...)
#     email: EmailStr = Field(...) # Utilisation du type `EmailStr` pour la validation
#     phone: str = Field(...)
#     picture: Optional[str] = Field(None)
#     statut: StatutUser = Field(StatutUser.ACTIVE)
#     confirmed: bool = Field(...)
#     role: str = Field(...)
#     deleted: bool = Field(False)
#     my_code_parrain: Optional[str] = Field(None, max_length=10)
#     code_parrain: Optional[str] = Field(None, max_length=10)
#     point_fidelite: int = Field(0, ge=0)
#     created_date: datetime = Field(...)
#     last_modified_date: datetime = Field(...)


# class RecommandationResult(BaseModel):
#     id: str
#     titre: str
#     description: str
#     categorie: str
#     score_reco: float