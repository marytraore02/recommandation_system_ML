# dans votre fichier de schémas (par ex: schemas3.py)
import enum
from typing import Optional, List
from pydantic import Field

from pydentics import schemas2

class CombinedFeedReason(str, enum.Enum):
    """Raison pour laquelle un post est inclus dans le feed combiné."""
    TRENDING = "TRENDING"
    POPULAR = "POPULAR"
    NEW_ARRIVAL = "NEW_ARRIVAL"

class CombinedFeedItem(schemas2.CagnottePostFeedResponse):
    """
    Modèle de réponse pour un élément du feed combiné,
    enrichi avec des métadonnées de recommandation.
    """
    # Métadonnées pour le client
    reason: CombinedFeedReason = Field(..., description="Pourquoi cet élément est recommandé.")

    # Champs spécifiques aux tendances (optionnels)
    trending_score: Optional[float] = Field(None, alias='trendingScore', description="Score de tendance si la raison est 'TRENDING'.")
    velocity_percent: Optional[float] = Field(None, alias='velocityPercent', description="Vélocité de la tendance en pourcentage.")
    trend_direction: Optional[str] = Field(None, alias='trendDirection', description="Direction de la tendance (up, down, stable).")

    class Config:
        from_attributes = True
        populate_by_name = True # Permet d'utiliser les alias comme 'trendingScore'