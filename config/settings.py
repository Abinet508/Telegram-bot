"""Configuration settings for the Telegram Bot application."""
import os
from pathlib import Path
from decouple import Config, RepositoryEnv
from pydantic import BaseModel, Field


class TelegramConfig(BaseModel):
    """Telegram API configuration."""
    api_id: int = Field(..., description="Telegram API ID")
    api_hash: str = Field(..., description="Telegram API Hash")


class AppConfig(BaseModel):
    """Application configuration."""
    project_root: Path = Field(default_factory=lambda: Path(__file__).parent.parent)
    sessions_dir: Path = Field(default_factory=lambda: Path(__file__).parent.parent / "sessions")
    logs_dir: Path = Field(default_factory=lambda: Path(__file__).parent.parent / "logs")
    data_dir: Path = Field(default_factory=lambda: Path(__file__).parent.parent / "data")
    secret_key: str = Field(default="your-secret-key-change-this")
    
    def create_directories(self):
        """Create directories if they don't exist."""
        for directory in [self.sessions_dir, self.logs_dir, self.data_dir]:
            directory.mkdir(exist_ok=True)


def load_config():
    """Load configuration from environment file."""
    env_file = Path(__file__).parent.parent / ".env"
    
    if env_file.exists():
        config = Config(RepositoryEnv(str(env_file)))
    else:
        config = Config(os.environ)
    
    # Use working API credentials for QR login
    telegram_config = TelegramConfig(
        api_id=config("API_ID", default=4849078, cast=int),
        api_hash=config("API_HASH", default="bd5f7c2c5ca67f09ed0d536826c05b7b")
    )
    
    app_config = AppConfig(
        secret_key=config("SECRET_KEY", default="dev-key-change-in-production")
    )
    app_config.create_directories()
    
    return telegram_config, app_config