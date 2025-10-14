from enum import Enum
import enum


class TypeRessource(str, enum.Enum):
    JUSTIFICATIF = "JUSTIFICATIF"
    CAGNOTTE = "CAGNOTTE"
    TEMOIGNAGE = "TEMOIGNAGE"
    POST_IMAGE = "POST_IMAGE"
    POST_VIDEO = "POST_VIDEO"
    POST_THUMBNAIL = "POST_THUMBNAIL"
    DOCUMENTCERTIFICATION = "DOCUMENTCERTIFICATION"
    POST_PREVIEW = "POST_PREVIEW"
    POST_DOCUMENT = "POST_DOCUMENT"

class StatutCagnotte(str, enum.Enum):
    EN_COURS = "EN_COURS"
    VALIDE = "VALIDE"
    SUSPENDU = "SUSPENDU"
    # ANNULEE = "ANNULEE"

class TypeCagnotte(str, enum.Enum):
    PUBLIC = "PUBLIC"
    PRIVE = "PRIVE"

class StatutUser(str, enum.Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    SUSPENDED = "SUSPENDED"
    DELETED = "DELETED"

class TypeCompte(str, enum.Enum):
    USER = "USER"
    ORGANISATION = "ORGANISATION"
    INFLUENCEUR = "INFLUENCEUR"
    AMBASSADEUR = "AMBASSADEUR"
    ADMIN = "ADMIN"

class TypePost(str, Enum):
    INITIAL = "INITIAL"
    MEDAIA_ONLY = "MEDIA_ONLY"
    UPDATE = "UPDATE"

class TypeOrganisation(str, Enum):
    ENTREPRISE = "ENTREPRISE"
    ORGANISATION = "ORGANISATION"
    # ASSOCIATION = "ASSOCIATION"
    ONG = "ONG"