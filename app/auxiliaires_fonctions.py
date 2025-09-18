import json
from typing import List, Dict, Any, Optional, Set
import redis.asyncio as aioredis

from pydentics import models
from pydentics import schemas2
from pydentics import schemas3
from pydentics.models import RessourceModel, CagnottePostModel, CagnotteModel, UserModel, CategorieModel
from pydentics.schemas2 import CagnottePostFeedResponse, RessourceResponse, Author, CagnotteSimpleResponse, TypeRessource, Ressource
from pydentics.schemas3 import CombinedFeedItem, CombinedFeedReason

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import case, cast, TEXT
from sqlalchemy import select, func, union_all, literal_column, join


# ===================================================================
# FONCTIONS AUXILIAIRES POUR LA LISIBILITÉ
# ===================================================================

def build_feed_response(
    post: models.CagnottePostModel, 
    ressources: List[models.RessourceModel],
    likes_count: Optional[int] = 0,
    comments_count: Optional[int] = 0
) -> schemas2.CagnottePostFeedResponse:
    
    # Filtrer les ressources par type
    video_resources = [r for r in ressources if r.type == schemas2.TypeRessource.POST_VIDEO and r.alt_text and r.alt_text.startswith("READY_")]
    thumbnails = [r for r in ressources if r.type == schemas2.TypeRessource.POST_THUMBNAIL]
    images = [r for r in ressources if r.type == schemas2.TypeRessource.POST_IMAGE]

    # Calculer les métadonnées des médias
    has_video = bool(video_resources)
    total_medias = len(ressources)
    has_multiple_medias = total_medias > 1
    
    # Extraire les URLs des formats vidéo
    def get_video_url(alt_text_prefix):
        for r in video_resources:
            if r.alt_text == alt_text_prefix:
                return r.file
        return None

    mobile_video_url = get_video_url("READY_MOBILE")
    standard_video_url = get_video_url("READY_STANDARD")
    hd_video_url = get_video_url("READY_HD")

    # Déterminer l'URL principale
    main_media_url = None
    if has_video:
        main_media_url = standard_video_url or mobile_video_url or hd_video_url
    elif images:
        main_media_url = images[0].file

    # Déterminer le thumbnail
    thumbnail_url = thumbnails[0].file if thumbnails else (images[0].file if images else None)

    # Durée de la vidéo
    video_duration = None
    if has_video:
        for r in video_resources:
            if r.duration is not None:
                video_duration = r.duration
                break

    # Création de l'objet de réponse final
    feed_item = schemas2.CagnottePostFeedResponse(
        id=post.id,
        cagnotte=schemas2.CagnotteSimpleResponse.from_orm(post.cagnotte),
        author=schemas2.Author.from_orm(post.author),
        type=post.type,
        title=post.title,
        content=post.content,
        likesCount=post.likes_count,
        commentsCount=post.comments_count,
        viewsCount=post.views_count,
        sharesCount=post.shares_count,
        createdDate=post.created_date,
        isPinned=post.is_pinned,
        medias=[schemas2.RessourceResponse.from_orm(r) for r in ressources],
        mainMediaUrl=main_media_url,
        thumbnailUrl=thumbnail_url,
        previewGifUrl=None, # TODO
        hasVideo=has_video,
        hasMultipleMedias=has_multiple_medias,
        totalMedias=total_medias,
        aspectRatio="16:9", # TODO
        videoDuration=video_duration,
        estimatedDataUsage=0, # TODO
        mobileVideoUrl=mobile_video_url,
        standardVideoUrl=standard_video_url,
        hdVideoUrl=hd_video_url,
    )
    return feed_item


async def _get_popular_ids_from_redis(redis: aioredis.Redis, country: str) -> List[int]:
    """Récupère les IDs des vidéos populaires pour un pays depuis Redis."""
    data = await redis.get("analytics_popularity_by_country")
    if not data:
        return []
    countries_data = json.loads(data)
    country_data = countries_data.get(country, {})
    top_videos = country_data.get("top_videos", [])
    return [int(item["video_id"]) for item in top_videos]

async def _get_trending_data_from_redis(redis: aioredis.Redis, country: str) -> List[Dict[str, Any]]:
    """Récupère les données brutes des vidéos tendance pour un pays depuis Redis."""
    data = await redis.get("analytics_trending_by_country")
    if not data:
        return []
    trending_data = json.loads(data)
    country_data = trending_data.get(country, {})
    return country_data.get('trending_videos', [])

async def _get_new_arrivals_ids_from_db(db: AsyncSession, limit: int) -> List[int]:
    """Récupère les IDs des derniers posts médias de la base de données."""
    Post = models.CagnottePostModel
    Ressource = models.RessourceModel
    
    stmt = (
        select(Ressource.id)
        .join(Post, Ressource.reference == Post.id)
        # On caste explicitement la colonne ENUM en TEXT pour la comparaison
        .where(cast(Ressource.type, TEXT).in_([
            models.TypeRessource.POST_VIDEO.value, 
            models.TypeRessource.POST_IMAGE.value
        ]))
        .order_by(Post.created_date.desc())
        .limit(limit)
    )
    result = await db.execute(stmt)
    return result.scalars().all()

# ===================================================================
# ENDPOINT PRINCIPAL COMBINÉ
# ===================================================================

