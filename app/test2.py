from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import redis
import json
import logging
from datetime import datetime
from enum import Enum
import os
from dotenv import load_dotenv

load_dotenv()

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration Redis
redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=int(os.getenv("REDIS_DB", 0)),
    decode_responses=True
)

app = FastAPI(
    title="Système de Recommandation Cold Start",
    description="API de recommandation de vidéos basée sur les tendances croissantes par pays",
    version="1.0.0"
)

class TrendDirection(str, Enum):
    UP = "up"
    DOWN = "down"
    STABLE = "stable"

class VideoRecommendation(BaseModel):
    video_id: str = Field(..., description="ID de la vidéo")
    trending_score: float = Field(..., description="Score de tendance de la vidéo")
    velocity_percent: float = Field(..., description="Vélocité de croissance en %")
    trend_direction: TrendDirection = Field(..., description="Direction de la tendance")
    current_score: float = Field(..., description="Score actuel")
    previous_score: float = Field(..., description="Score précédent")
    rank_in_country: int = Field(..., description="Rang dans le pays")

class CountryTrend(BaseModel):
    country: str = Field(..., description="Nom du pays")
    country_trending_score: float = Field(..., description="Score de tendance global du pays")
    country_velocity_percent: float = Field(..., description="Vélocité du pays en %")
    country_trend_direction: TrendDirection = Field(..., description="Direction de tendance du pays")
    total_trending_videos: int = Field(..., description="Nombre total de vidéos en tendance")
    unique_users_recent: int = Field(..., description="Utilisateurs uniques récents")

class ColdStartRecommendation(BaseModel):
    country: str = Field(..., description="Pays de la recommandation")
    recommendation_type: str = Field(default="cold_start", description="Type de recommandation")
    recommended_videos: List[VideoRecommendation] = Field(..., description="Liste des vidéos recommandées")
    country_context: CountryTrend = Field(..., description="Contexte du pays")
    total_recommendations: int = Field(..., description="Nombre total de recommandations")
    generated_at: str = Field(..., description="Timestamp de génération")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Métadonnées additionnelles")

class RecommendationFilters(BaseModel):
    min_velocity_percent: float = Field(default=0.0, description="Vélocité minimale requise (%)")
    min_trending_score: float = Field(default=0.0, description="Score de tendance minimal")
    exclude_stable_trends: bool = Field(default=True, description="Exclure les tendances stables")
    max_recommendations: int = Field(default=10, ge=1, le=50, description="Nombre max de recommandations")

def get_redis_client():
    """Dependency pour obtenir le client Redis"""
    try:
        redis_client.ping()
        return redis_client
    except Exception as e:
        logger.error(f"Erreur de connexion Redis: {e}")
        raise HTTPException(status_code=503, detail="Service Redis indisponible")

def get_trending_data_from_redis(redis_client: redis.Redis) -> Optional[Dict]:
    """Récupère les données de tendances depuis Redis"""
    try:
        # Essayer de récupérer les données de tendances par pays
        trending_data = redis_client.get("analytics_trending_by_country")
        if not trending_data:
            logger.warning("Aucune donnée de tendance trouvée dans Redis")
            return None
        
        return json.loads(trending_data)
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des données Redis: {e}")
        return None

def filter_trending_videos(
    videos: List[Dict], 
    filters: RecommendationFilters
) -> List[Dict]:
    """Filtre les vidéos selon les critères de recommandation cold start"""
    filtered_videos = []
    
    for video in videos:
        # Critère 1: Tendance croissante uniquement (vélocité positive)
        if video.get('velocity_percent', 0) <= filters.min_velocity_percent:
            continue
        
        # Critère 2: Direction de tendance "up" uniquement
        if video.get('trend_direction') != 'up':
            continue
        
        # Critère 3: Score de tendance minimum
        if video.get('trending_score', 0) < filters.min_trending_score:
            continue
        
        # Critère 4: Exclure les tendances stables si demandé
        if filters.exclude_stable_trends and video.get('trend_direction') == 'stable':
            continue
        
        filtered_videos.append(video)
    
    # Trier par score de tendance décroissant
    filtered_videos.sort(key=lambda x: x.get('trending_score', 0), reverse=True)
    
    # Limiter le nombre de recommandations
    return filtered_videos[:filters.max_recommendations]

@app.get("/health")
async def health_check():
    """Endpoint de vérification de santé"""
    try:
        redis_client.ping()
        return {"status": "healthy", "redis": "connected", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        return {"status": "unhealthy", "redis": "disconnected", "error": str(e)}

@app.get("/countries/available", response_model=List[str])
async def get_available_countries(redis_client: redis.Redis = Depends(get_redis_client)):
    """Récupère la liste des pays disponibles pour les recommandations"""
    try:
        trending_data = get_trending_data_from_redis(redis_client)
        if not trending_data:
            raise HTTPException(status_code=404, detail="Aucune donnée de tendance disponible")
        
        countries = list(trending_data.keys())
        countries.sort()
        
        return countries
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des pays: {e}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")

@app.get("/countries/{country}/trends")
async def get_country_trends(
    country: str,
    redis_client: redis.Redis = Depends(get_redis_client)
):
    """Récupère les données de tendance brutes pour un pays"""
    try:
        trending_data = get_trending_data_from_redis(redis_client)
        if not trending_data:
            raise HTTPException(status_code=404, detail="Aucune donnée de tendance disponible")
        
        if country not in trending_data:
            available_countries = list(trending_data.keys())
            raise HTTPException(
                status_code=404, 
                detail=f"Pays '{country}' non trouvé. Pays disponibles: {available_countries}"
            )
        
        return trending_data[country]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des tendances pour {country}: {e}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")

@app.post("/recommendations/cold-start/{country}", response_model=ColdStartRecommendation)
async def get_cold_start_recommendations(
    country: str,
    filters: RecommendationFilters = RecommendationFilters(),
    redis_client: redis.Redis = Depends(get_redis_client)
):
    """
    Génère des recommandations cold start pour un pays donné.
    Recommande uniquement les vidéos avec une tendance croissante (velocity > 0 et direction 'up').
    """
    try:
        # Récupérer les données de tendance
        trending_data = get_trending_data_from_redis(redis_client)
        if not trending_data:
            raise HTTPException(status_code=404, detail="Aucune donnée de tendance disponible")
        
        if country not in trending_data:
            available_countries = list(trending_data.keys())
            raise HTTPException(
                status_code=404,
                detail=f"Pays '{country}' non trouvé. Pays disponibles: {available_countries}"
            )
        
        country_data = trending_data[country]
        trending_videos = country_data.get('trending_videos', [])
        
        if not trending_videos:
            raise HTTPException(
                status_code=404,
                detail=f"Aucune vidéo en tendance trouvée pour le pays '{country}'"
            )
        
        # Filtrer les vidéos selon les critères cold start
        filtered_videos = filter_trending_videos(trending_videos, filters)
        
        if not filtered_videos:
            return ColdStartRecommendation(
                country=country,
                recommended_videos=[],
                country_context=CountryTrend(
                    country=country,
                    country_trending_score=country_data.get('trending_score', 0),
                    country_velocity_percent=country_data.get('velocity_percent', 0),
                    country_trend_direction=country_data.get('trend_direction', 'stable'),
                    total_trending_videos=len(trending_videos),
                    unique_users_recent=country_data.get('unique_users_recent', 0)
                ),
                total_recommendations=0,
                generated_at=datetime.now().isoformat(),
                metadata={
                    "filters_applied": filters.dict(),
                    "original_videos_count": len(trending_videos),
                    "filtered_videos_count": 0,
                    "filter_reason": "Aucune vidéo ne satisfait les critères de tendance croissante"
                }
            )
        
        # Construire les recommandations
        recommended_videos = []
        for idx, video in enumerate(filtered_videos, 1):
            recommended_videos.append(VideoRecommendation(
                video_id=video['video_id'],
                trending_score=video['trending_score'],
                velocity_percent=video['velocity_percent'],
                trend_direction=video['trend_direction'],
                current_score=video['current_score'],
                previous_score=video['previous_score'],
                rank_in_country=idx
            ))
        
        # Construire le contexte du pays
        country_context = CountryTrend(
            country=country,
            country_trending_score=country_data.get('trending_score', 0),
            country_velocity_percent=country_data.get('velocity_percent', 0),
            country_trend_direction=country_data.get('trend_direction', 'stable'),
            total_trending_videos=len(trending_videos),
            unique_users_recent=country_data.get('unique_users_recent', 0)
        )
        
        return ColdStartRecommendation(
            country=country,
            recommended_videos=recommended_videos,
            country_context=country_context,
            total_recommendations=len(recommended_videos),
            generated_at=datetime.now().isoformat(),
            metadata={
                "filters_applied": filters.dict(),
                "original_videos_count": len(trending_videos),
                "filtered_videos_count": len(filtered_videos),
                "average_velocity": sum(v['velocity_percent'] for v in filtered_videos) / len(filtered_videos) if filtered_videos else 0,
                "average_trending_score": sum(v['trending_score'] for v in filtered_videos) / len(filtered_videos) if filtered_videos else 0
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erreur lors de la génération des recommandations pour {country}: {e}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")

@app.get("/recommendations/cold-start/{country}", response_model=ColdStartRecommendation)
async def get_cold_start_recommendations_get(
    country: str,
    min_velocity_percent: float = Query(default=0.0, description="Vélocité minimale requise (%)"),
    min_trending_score: float = Query(default=0.0, description="Score de tendance minimal"),
    exclude_stable_trends: bool = Query(default=True, description="Exclure les tendances stables"),
    max_recommendations: int = Query(default=10, ge=1, le=50, description="Nombre max de recommandations"),
    redis_client: redis.Redis = Depends(get_redis_client)
):
    """Version GET de l'endpoint de recommandations cold start"""
    filters = RecommendationFilters(
        min_velocity_percent=min_velocity_percent,
        min_trending_score=min_trending_score,
        exclude_stable_trends=exclude_stable_trends,
        max_recommendations=max_recommendations
    )
    
    return await get_cold_start_recommendations(country, filters, redis_client)

@app.get("/recommendations/global-trending", response_model=List[VideoRecommendation])
async def get_global_trending_recommendations(
    min_velocity_percent: float = Query(default=5.0, description="Vélocité minimale requise (%)"),
    min_trending_score: float = Query(default=1.0, description="Score de tendance minimal"),
    max_recommendations: int = Query(default=20, ge=1, le=100, description="Nombre max de recommandations"),
    redis_client: redis.Redis = Depends(get_redis_client)
):
    """
    Récupère les vidéos les plus en tendance globalement (tous pays confondus)
    avec uniquement les tendances croissantes
    """
    try:
        trending_data = get_trending_data_from_redis(redis_client)
        if not trending_data:
            raise HTTPException(status_code=404, detail="Aucune donnée de tendance disponible")
        
        # Collecter toutes les vidéos en tendance croissante de tous les pays
        all_videos = []
        video_scores = {}  # Pour déduplication
        
        for country, country_data in trending_data.items():
            trending_videos = country_data.get('trending_videos', [])
            
            for video in trending_videos:
                # Filtrer uniquement les tendances croissantes
                if (video.get('velocity_percent', 0) >= min_velocity_percent and 
                    video.get('trend_direction') == 'up' and
                    video.get('trending_score', 0) >= min_trending_score):
                    
                    video_id = video['video_id']
                    # Garder le meilleur score si la vidéo apparaît dans plusieurs pays
                    if (video_id not in video_scores or 
                        video['trending_score'] > video_scores[video_id]['trending_score']):
                        video_copy = video.copy()
                        video_copy['source_country'] = country
                        video_scores[video_id] = video_copy
        
        # Trier par score de tendance et limiter
        sorted_videos = sorted(video_scores.values(), 
                             key=lambda x: x['trending_score'], reverse=True)
        top_videos = sorted_videos[:max_recommendations]
        
        # Convertir en format de réponse
        recommendations = []
        for idx, video in enumerate(top_videos, 1):
            recommendations.append(VideoRecommendation(
                video_id=video['video_id'],
                trending_score=video['trending_score'],
                velocity_percent=video['velocity_percent'],
                trend_direction=video['trend_direction'],
                current_score=video['current_score'],
                previous_score=video['previous_score'],
                rank_in_country=idx
            ))
        
        return recommendations
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des recommandations globales: {e}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")

@app.get("/analytics/trending-summary")
async def get_trending_summary(redis_client: redis.Redis = Depends(get_redis_client)):
    """Récupère un résumé des tendances globales"""
    try:
        trending_data = get_trending_data_from_redis(redis_client)
        if not trending_data:
            raise HTTPException(status_code=404, detail="Aucune donnée de tendance disponible")
        
        summary = {
            "total_countries": len(trending_data),
            "countries_with_growing_trends": 0,
            "total_trending_videos": 0,
            "total_growing_videos": 0,
            "top_growing_countries": [],
            "average_country_velocity": 0,
            "last_updated": None
        }
        
        country_velocities = []
        country_trends = []
        
        for country, data in trending_data.items():
            trending_videos = data.get('trending_videos', [])
            summary["total_trending_videos"] += len(trending_videos)
            
            # Compter les vidéos en croissance
            growing_videos = [v for v in trending_videos if v.get('velocity_percent', 0) > 0 and v.get('trend_direction') == 'up']
            summary["total_growing_videos"] += len(growing_videos)
            
            # Analyser les tendances par pays
            country_velocity = data.get('velocity_percent', 0)
            country_velocities.append(country_velocity)
            
            if country_velocity > 0:
                summary["countries_with_growing_trends"] += 1
            
            country_trends.append({
                "country": country,
                "velocity_percent": country_velocity,
                "trending_score": data.get('trending_score', 0),
                "growing_videos_count": len(growing_videos)
            })
            
            # Capturer la dernière mise à jour
            if not summary["last_updated"]:
                summary["last_updated"] = data.get('last_updated')
        
        # Calculer la vélocité moyenne
        if country_velocities:
            summary["average_country_velocity"] = sum(country_velocities) / len(country_velocities)
        
        # Top pays en croissance
        growing_countries = [c for c in country_trends if c['velocity_percent'] > 0]
        summary["top_growing_countries"] = sorted(growing_countries, 
                                                key=lambda x: x['velocity_percent'], 
                                                reverse=True)[:5]
        
        return summary
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erreur lors de la génération du résumé: {e}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)