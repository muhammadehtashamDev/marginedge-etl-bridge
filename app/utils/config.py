from pydantic_settings import BaseSettings, SettingsConfigDict
import os

class Settings(BaseSettings):
    MARGIN_EDGE_API_KEY: str
    BASE_URL: str = "https://api.marginedge.com/public"

    # PostgreSQL connection settings
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = "toast_db"
    DB_USER: str = "postgres"
    DB_PASSWORD: str = ""

    # API basic-auth credentials for FastAPI admin access
    ADMIN_USERNAME: str = "admin"
    ADMIN_PASSWORD: str

    # Backup configuration
    BACKUP_DIR: str = "backup"
    BACKUP_RETENTION_DAYS: int = 60

    # This tells pydantic to look for a .env file
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

settings = Settings()