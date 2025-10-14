import json
from typing import List, Dict, Any, Optional, Set, Annotated
import redis.asyncio as aioredis

from pathlib import Path
from pydentics import models
from pydentics import schemas2
from pydentics import schemas3
from pydentics.models import RessourceModel, CagnottePostModel, CagnotteModel, UserModel, CategorieModel
from pydentics.schemas2 import CagnottePostFeedResponse, RessourceResponse, Author, CagnotteSimpleResponse, TypeRessource, Ressource
from pydentics.schemas3 import CombinedFeedItem, CombinedFeedReason
import httpx
import asyncio
import os
import random
from dotenv import load_dotenv
from sqlalchemy.orm import selectinload
from collections import defaultdict

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import case, cast, TEXT
from sqlalchemy import select, func, union_all, literal_column, join
from uuid import UUID


load_dotenv()

SPRING_BOOT_API_URL = os.getenv("SPRING_BOOT_API_URL")
PLACEHOLDER_VALUES = {"TRANSCODING_PENDING", None, ""}

# ===================================================================
# FONCTIONS AUXILIAIRES POUR LA LISIBILITÉ
# ===================================================================

def build_feed_response(
    post: models.CagnottePostModel,
    ressources: List[models.RessourceModel],
    # sponsors_list: List[models.SponsorModel] = None,
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

    # Préparer l'objet CagnotteSimpleResponse
    cagnotte_response = schemas2.CagnotteSimpleResponse.from_orm(post.cagnotte)

    # Création de l'objet de réponse final
    feed_item = schemas2.CagnottePostFeedResponse(
        id=post.id,
        cagnotte=cagnotte_response,
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

async def _get_popular_ids_from_redis(redis: aioredis.Redis, country: str) -> list[int]:
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

# NOUVELLE FONCTION UTILITAIRE 1 : Obtenir une seule URL
async def _get_presigned_url(file_path: str, client: httpx.AsyncClient) -> str:
    """
    Prend un chemin de fichier (ex: 'videos/xyz/thumb.jpg' ou
    'videos/.../transcoded/standard_uuid.mp4') et retourne une URL présignée.
    Retourne le chemin original en cas d'erreur ou si c'est une valeur placeholder
    ou si c'est déjà une URL absolue.
    """
    if not file_path or file_path in PLACEHOLDER_VALUES:
        return file_path

    # Si c'est déjà une URL complète, ne rien faire
    if isinstance(file_path, str) and (file_path.startswith("http://") or file_path.startswith("https://")):
        return file_path

    try:
        path_obj = Path(file_path)
        parts = [p for p in path_obj.parts if p]

        # Logique de construction du chemin pour l'API Spring Boot
        if not parts:
            return file_path # Ne peut pas traiter

        # Déterminer le 'path' (qui sera le nom du dossier MinIO : videos, cagnottes, logos)
        root = parts[0].lower()
        
        # Cas 1: C'est un logo d'organisation
        if root == "logos":
            path = "logos"
            # Le nom du fichier est la dernière partie (ex: logo.png)
            filename = parts[-1] 
        # Cas 2: C'est une ressource vidéo/média
        elif root == "videos":
            path = "videos"
            filename = parts[-1] 
        # Cas 3: Autres (cagnotte, etc.)
        else:
            path = root if root in {"videos", "cagnottes", "logos"} else "cagnottes"
            filename = parts[-1]

        target_url = f"{SPRING_BOOT_API_URL}/{path}/{filename}"

        response = await client.get(target_url, timeout=10.0)
        response.raise_for_status()
        # le backend renvoie le presigned url en texte
        return response.text

    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        print(f"WARN: Impossible de générer l'URL pour '{file_path}'. Erreur: {e}")
        return file_path

# NOUVELLE FONCTION UTILITAIRE 2 : Orchestrateur d'enrichissement
async def enrich_feed_with_presigned_urls(feed_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Prend une liste d'éléments de feed (dictionnaires), trouve tous les chemins de fichiers,
    les convertit en URLs présignées en parallèle, et met à jour la liste.
    """
    paths_to_convert = set()

    for item in feed_items:
        # réactiver conversion de la photo de l'auteur
        if item.get("author") and item["author"].get("picture"):
            pic = item["author"]["picture"]
            if isinstance(pic, str) and not (pic.startswith("http://") or pic.startswith("https://")):
                paths_to_convert.add(pic)

        for media in item.get("medias", []):
            f = media.get("file")
            # n'ajoute que si ce n'est pas déjà une URL complète
            if f and isinstance(f, str) and not (f.startswith("http://") or f.startswith("https://")):
                paths_to_convert.add(f)

        # ajouter les autres champs principaux uniquement s'ils ne sont pas déjà des URLs
        for key in ["main_media_url", "thumbnail_url", "mobile_video_url", "standard_video_url", "hd_video_url"]:
            v = item.get(key)
            # print(f"DEBUG: Checking {key} with value '{v}'")
            if v and isinstance(v, str) and not (v.startswith("http://") or v.startswith("https://")):
                paths_to_convert.add(v)

        cagnotte_data = item.get("cagnotte")
        if cagnotte_data and cagnotte_data.get("sponsors"):
            for sponsor in cagnotte_data["sponsors"]:
                organisation = sponsor.get("organisation")
                if organisation and organisation.get("logo"):
                    logo_path = organisation["logo"]
                    # Assurez-vous que le chemin n'est pas déjà une URL complète
                    if isinstance(logo_path, str) and not (logo_path.startswith("http://") or logo_path.startswith("https://")):
                        paths_to_convert.add(logo_path)


    valid_paths = {p for p in paths_to_convert if p not in PLACEHOLDER_VALUES}
    # print("DEBUG: Valid paths to convert =", valid_paths)

    if not valid_paths:
        return feed_items

    # Exécution des requêtes en parallèle
    async with httpx.AsyncClient() as client:
        tasks = [_get_presigned_url(path, client) for path in valid_paths]
        results = await asyncio.gather(*tasks)
        url_map = dict(zip(valid_paths, results))
    # print("DEBUG: URL map =", url_map)

    for item in feed_items:
        if item.get("author") and item["author"].get("picture") in url_map:
            item["author"]["picture"] = url_map[item["author"]["picture"]]

        for media in item.get("medias", []):
            if media.get("file") in url_map:
                # print(f"DEBUG: remplacing media file: '{media['file']}' -> '{url_map[media['file']]}'")
                media["file"] = url_map[media["file"]]
        
        for key in ["main_media_url", "thumbnail_url", "mobile_video_url", "standard_video_url", "hd_video_url"]:
            orig = item.get(key)
            if orig and orig in url_map:
                # debug utile pour vérifier les mappings en local
                print(f"DEBUG: remplacing {key}: '{orig}' -> '{url_map[orig]}'")
                item[key] = url_map[orig]
                
        cagnotte_data = item.get("cagnotte")
        if cagnotte_data and cagnotte_data.get("sponsors"):
            for sponsor in cagnotte_data["sponsors"]:
                organisation = sponsor.get("organisation")
                if organisation and organisation.get("logo"):
                    orig_logo_path = organisation["logo"]
                    if orig_logo_path in url_map:
                        organisation["logo"] = url_map[orig_logo_path]

    return feed_items


# ===================================================================
# FONCTIONS UTILITAIRES
# ===================================================================
async def check_cache_freshness(redis: aioredis.Redis, cache_key: str) -> Optional[int]:
    """
    Vérifie la fraîcheur du cache et retourne le TTL restant.
    Retourne None si le cache n'existe pas.
    """
    try:
        ttl = await redis.ttl(cache_key)
        return ttl if ttl > 0 else None
    except Exception as e:
        print(f"⚠️ Erreur lors de la vérification du TTL: {e}")
        return None

def select_feed_items(
    feed_items: List[dict],
    limit: int, 
    randomize: bool = True,
    seed: Optional[int] = None
) -> List[dict]:
    """
    Sélectionne et éventuellement mélange les éléments du feed.
    
    Args:
        feed_items: Liste complète des éléments
        limit: Nombre d'éléments à retourner
        randomize: Si True, mélange les éléments
        seed: Seed pour la randomisation (pour tests reproductibles)
    """
    if not feed_items:
        return []
    
    # Créer une copie pour ne pas modifier l'original
    items = feed_items.copy()
    
    if randomize:
        if seed is not None:
            random.seed(seed)
        random.shuffle(items)
    
    return items[:limit]