"""Configuration for Spotify Analytics platform."""
from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache

class SpotifySettings(BaseSettings):
    client_id: str = Field(..., alias="SPOTIFY_CLIENT_ID")
    client_secret: str = Field(..., alias="SPOTIFY_CLIENT_SECRET")
    redirect_uri: str = Field("http://localhost:8888/callback", alias="SPOTIFY_REDIRECT_URI")
    model_config = {"env_file": ".env", "extra": "ignore"}

class SnowflakeSettings(BaseSettings):
    account: str = Field(..., alias="SNOWFLAKE_ACCOUNT")
    user: str = Field(..., alias="SNOWFLAKE_USER")
    password: str = Field(..., alias="SNOWFLAKE_PASSWORD")
    warehouse: str = Field("SPOTIFY_WH", alias="SNOWFLAKE_WAREHOUSE")
    database: str = Field("SPOTIFY_DB", alias="SNOWFLAKE_DATABASE")
    role: str = Field("SPOTIFY_ROLE", alias="SNOWFLAKE_ROLE")
    model_config = {"env_file": ".env", "extra": "ignore"}

class Settings(BaseSettings):
    spotify: SpotifySettings = SpotifySettings()
    snowflake: SnowflakeSettings = SnowflakeSettings()
    model_config = {"env_file": ".env", "extra": "ignore"}

@lru_cache()
def get_settings() -> Settings:
    return Settings()
