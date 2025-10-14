# Pydantic est la colonne vertébrale de FastAPI. Il valide les données entrantes et sortantes, ce qui rend votre API très robuste.

# from pyparsing import Enum
from sqlalchemy import (Column, String, Integer, BigInteger, Boolean, Text, Table,
                        DateTime, Enum as SQLAlchemyEnum, ForeignKey, Numeric)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
import uuid
from db.postgres_config import Base
import enum
from datetime import datetime
from sqlalchemy.ext.declarative import declarative_base
from decimal import Decimal
from .enums import TypeOrganisation, TypeRessource, StatutCagnotte, TypeCagnotte, StatutUser, TypeCompte


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
    created_date = Column(DateTime, default=datetime.utcnow)
    last_modified_date = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# Définition de la table associative (Join Table)
# C'est l'équivalent de votre @JoinTable
preference_categories_table = Table('preference_categories', Base.metadata,
    Column('preference_id', UUID(as_uuid=True), ForeignKey('preferences.id'), primary_key=True),
    Column('categorie_id', UUID(as_uuid=True), ForeignKey('categories.id'), primary_key=True)
)

class UserModel(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    firstname = Column(String, nullable=False)
    lastname = Column(String, nullable=False)
    email = Column(String, unique=True, nullable=False)
    phone = Column(String, nullable=False)
    picture = Column(String, nullable=True)

    statut = Column(SQLAlchemyEnum(StatutUser), default=StatutUser.ACTIVE, nullable=False)
    confirmed = Column(Boolean, default=False)

    role = Column(SQLAlchemyEnum(TypeCompte), default=TypeCompte.USER, nullable=False)
    deleted = Column(Boolean, default=False)

    my_code_parrain = Column(String(10), unique=True, nullable=True)
    code_parrain = Column(String(10), nullable=True)

    point_fidelite = Column(Integer, default=0, nullable=False)
    current_solde = Column(Integer, default=0, nullable=False)
    total_solde = Column(Integer, default=0, nullable=False)
    total_contributed = Column(Integer, default=0, nullable=False)
    total_contributed_amount = Column(Integer, default=0, nullable=False)

    last_contribution_date = Column(DateTime, nullable=True)
    last_contribution_amount = Column(Integer, default=0, nullable=False)

    compte_level = Column(Integer, default=1, nullable=False)
    user_profile = Column(String, default="", nullable=False)

    created_date = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_modified_date = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # Relation One-to-Many vers PreferenceModel
    preferences = relationship("PreferenceModel", back_populates="user")


class PreferenceModel(Base):
    __tablename__ = "preferences"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    profession = Column(String(255))
    origin = Column(String(255))
    budget = Column(Integer)
    motive = Column(Text)
    createdDate = Column("created_date", DateTime, default=datetime.utcnow)
    deleted = Column(Boolean, default=False, nullable=False)

    # Clé étrangère vers UserModel
    user_id = Column(UUID(as_uuid=True), ForeignKey('users.id'), nullable=False)
    # admin_id = Column("admin", UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)

    # Relation Many-to-One vers UserModel
    user = relationship("UserModel", back_populates="preferences")

    # Relation Many-to-Many vers CategorieModel
    categories = relationship(
        "CategorieModel",
        secondary=preference_categories_table,
        back_populates="preferences"
    )


class CategorieModel(Base):
    __tablename__ = "categories"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    picture = Column(String(255), nullable=True)
    created_date = Column(DateTime, default=datetime.utcnow)
    last_modified_date = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    deleted = Column(Boolean, nullable=False, default=False)
    
    preferences = relationship(
        "PreferenceModel",
        secondary=preference_categories_table,
        back_populates="categories"
    )


class CagnotteModel(Base):
    __tablename__ = "cagnottes"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    pays = Column(String(100), default="Mali", nullable=False)

    date_start = Column(DateTime, nullable=True)
    date_end = Column(DateTime, nullable=True)

    objectif = Column(BigInteger, nullable=True)
    total_solde = Column(BigInteger, default=0)
    current_solde = Column(BigInteger, default=0)

    total_contributors = Column(Integer, default=0)
    is_certified = Column(Boolean, default=False)
    mode_retrait = Column(String(50))
    renew_count = Column(Integer, default=0)

    commission = Column(Numeric(8, 3), default=Decimal("0.000"), nullable=False)

    statut = Column(SQLAlchemyEnum(StatutCagnotte), nullable=True)
    type = Column(SQLAlchemyEnum(TypeCagnotte), default=TypeCagnotte.PUBLIC, nullable=False)

    id_categorie = Column(UUID(as_uuid=True), ForeignKey("categories.id"), nullable=False)
    admin_id = Column("admin", UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)

    created_date = Column(DateTime, default=datetime.utcnow)
    last_modified_date = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    deleted = Column(Boolean, nullable=False, default=False)

    # Relations
    categorie = relationship("CategorieModel")
    admin = relationship("UserModel")
    sponsors = relationship(
        "SponsorModel",
        back_populates="cagnotte",
        lazy="selectin" # Optionnel mais aide à la réutilisation
    )
    # justificatifs = relationship("JustificatifModel", back_populates="cagnotte", lazy="select")
    # contributeurs = relationship("ContributeurModel", back_populates="cagnotte", lazy="select")


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


class OrganisationModel(Base):
    __tablename__ = "organisations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, unique=True, nullable=False)
    acronym = Column(String)
    email = Column(String, unique=True, nullable=False)
    phone = Column(String, unique=True, nullable=False)
    logo = Column(String)
    annual_budget = Column(Numeric, nullable=False)
    type_organisation = Column(SQLAlchemyEnum(TypeOrganisation), nullable=False)
    description = Column(Text)
    nif = Column(String, nullable=False)
    rccm = Column(String, nullable=False)
    rccm_document = Column(String)
    nif_document = Column(String)
    deleted = Column(Boolean, nullable=False, default=False)
    created_date = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_modified_date = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    # Relation Many-to-one (inverse) pour les sponsors
    sponsors = relationship("SponsorModel", back_populates="organisation")


# --- Modèle SponsorModel (Reflète SponsorModel en Java) ---
class SponsorModel(Base):
    __tablename__ = "sponsors"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    montant = Column(Integer, default=0)
    created_date = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Clés étrangères (Foreign Keys)
    id_cagnotte = Column(UUID(as_uuid=True), ForeignKey("cagnottes.id"))
    id_organisation = Column(UUID(as_uuid=True), ForeignKey("organisations.id"))

    # Relations (Reflètent les ManyToOne en Java)
    cagnotte = relationship("CagnotteModel")
    organisation = relationship("OrganisationModel", back_populates="sponsors")

# *Mise à jour de CagnotteModel :*
# Vous devez ajouter la relation inverse `sponsors` dans votre CagnotteModel pour que la relation bidirectionnelle fonctionne correctement, si ce n'est pas déjà fait.

# Dans CagnotteModel:
# sponsors = relationship("SponsorModel", back_populates="cagnotte")
