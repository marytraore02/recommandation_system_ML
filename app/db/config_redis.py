from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv
import os

load_dotenv()

class Settings(BaseSettings):
    """
    Charge la configuration de l'application à partir des variables d'environnement.
    """
    REDIS_HOST: str = os.getenv('REDIS_HOST')
    REDIS_PORT: int = os.getenv('REDIS_PORT')
    REDIS_PASSWORD: str | None = None
    REDIS_DB: int = os.getenv('REDIS_DB')

    # Spécifie que les variables doivent être lues depuis un fichier .env
    # model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra='ignore'
    )

# Instance unique des paramètres, à importer dans les autres modules
settings = Settings()