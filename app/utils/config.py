import sys, os, logging, asyncio
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.fetch_functions_database import fetch_countries_from_db

logger = logging.getLogger(__name__)

# ===================================================================
# CONFIGURATION CENTRALISÉE
# ===================================================================
TOP_DEFAULT_COUNTRIES = ["Mali", "Cameroun", "Senegal"]

class FeedConfig:
    """Configuration centralisée pour la génération de feed."""
    CACHE_KEY_TEMPLATE = "feed:combined:{country}"
    # CACHE_TTL = 360  # 2 minutes
    CACHE_TTL = 3600  #  1 heure
    FEED_LIMIT = 100
    NEW_ARRIVALS_LIMIT = 10
    POPULAR_RATIO = 3
    TRENDING_RATIO = 2

    # Ne pas exécuter la DB à l'import — on garde la valeur par défaut jusqu'au chargement explicite.
    COUNTRIES = TOP_DEFAULT_COUNTRIES
    CRON_INTERVAL_MINUTES = 1440  # Toutes les 24 heures

    # Timeouts pour éviter les blocages
    DB_QUERY_TIMEOUT = 10
    REDIS_QUERY_TIMEOUT = 5


# Loader utilitaire (à appeler au démarrage de l'app/worker)
def load_countries_from_db_sync(timeout_seconds: int = 10):
    """
    Tente de récupérer les pays depuis la DB.
    Si fetch_countries_from_db est async, exécute la coroutine via asyncio.run().
    Retourne une liste de pays (dédupliqués, triés) ou TOP_DEFAULT_COUNTRIES en cas d'erreur.
    """
    try:
        result = fetch_countries_from_db()
        # si fetch_countries_from_db retourne une coroutine, exécuter proprement
        if asyncio.iscoroutine(result):
            try:
                rows = asyncio.run(result)
            except RuntimeError:
                # boucle déjà en cours (e.g. contexte async) -> ne pas bloquer : fallback
                logger.warning("Boucle événementielle déjà en cours — utilisation des pays par défaut")
                return TOP_DEFAULT_COUNTRIES
        else:
            rows = result

        if not rows:
            return TOP_DEFAULT_COUNTRIES

        countries = set()
        for r in rows:
            if isinstance(r, dict):
                val = r.get("pays") or r.get("country") or next(iter(r.values()), None)
            elif isinstance(r, (list, tuple)):
                val = r[0] if r else None
            else:
                val = r
            if val:
                countries.add(str(val).strip())
        if not countries:
            return TOP_DEFAULT_COUNTRIES
        return sorted(countries)
    except Exception as exc:
        logger.warning("Impossible de charger les pays depuis la DB, utilisation des valeurs par défaut: %s", exc)
        return TOP_DEFAULT_COUNTRIES


# ===================================================================
# CONFIGURATION
# ===================================================================
class EndpointConfig:
    """Configuration pour l'endpoint - cohérente avec le worker."""
    DEFAULT_LIMIT = 20
    MIN_LIMIT = 5
    MAX_LIMIT = 100
    CACHE_KEY_TEMPLATE = FeedConfig.CACHE_KEY_TEMPLATE
    ALLOWED_COUNTRIES = FeedConfig.COUNTRIES
    
    # Paramètres de fallback
    ENABLE_FALLBACK = True
    FALLBACK_RETRY_DELAY = 120  # secondes avant de réessayer


# ===================================================================
# CONFIGURATION CALCUL DES DONNÉES
# ===================================================================
class CalculationConfig:
    HOURS_BACK: int = 24 # Période d'analyse des événements
    TABLE_NAME: str = "user_events" # Nom de la table des événements dans ClickHouse
    
    ANONYMOUS_PROFILE_TTL = 30 * 60      # 30 minutes
    AUTHENTICATED_PROFILE_TTL = 90 * 24 * 3600 # 90 jours
    DUREE_ANALYTICS = 90000  # Durée max d'une tâche d'analytics en secondes

