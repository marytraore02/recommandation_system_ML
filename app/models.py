# Pydantic est la colonne vertébrale de FastAPI. Il valide les données entrantes et sortantes, ce qui rend votre API très robuste.

# from pyparsing import Enum
from sqlalchemy import (Column, String, Integer, BigInteger, Boolean, Text, 
                        DateTime, Enum as SQLAlchemyEnum, ForeignKey)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
import uuid
from db.postgres_config import Base
import enum
from datetime import datetime
from sqlalchemy.ext.declarative import declarative_base


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
    admin_id = Column("admin", UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)

    created_date = Column(DateTime, default=datetime.utcnow)
    last_modified_date = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    deleted = Column(Boolean, nullable=False, default=False)
    
    # Relations
    categorie = relationship("CategorieModel")
    admin = relationship("UserModel")



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