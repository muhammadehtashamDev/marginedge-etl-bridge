from pydantic_settings import BaseSettings, SettingsConfigDict
import os

class Settings(BaseSettings):
    MARGIN_EDGE_API_KEY: str
    BASE_URL: str = "https://api.marginedge.com/public"

    # This tells pydantic to look for a .env file
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

settings = Settings()