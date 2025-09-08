from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Charge la configuration de l'application à partir des variables d'environnement.
    """
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_PASSWORD: str | None = None
    REDIS_DB: int = 0

    # Spécifie que les variables doivent être lues depuis un fichier .env
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

# Instance unique des paramètres, à importer dans les autres modules
settings = Settings()