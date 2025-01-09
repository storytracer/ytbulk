import os
from typing import Dict, Optional
from pathlib import Path
from dotenv import load_dotenv
from .resolutions import YTBulkResolution

# Load .env files
load_dotenv()

class YTBulkConfig:
    """Configuration handler for YTBulk."""
    
    def __init__(self):
        """Load configuration from environment variables."""
        # Download settings
        self.chunk_size = int(os.getenv("YTBULK_CHUNK_SIZE", 1024 * 1024))
        self.max_retries = int(os.getenv("YTBULK_MAX_RETRIES", 3))
        self.max_concurrent = int(os.getenv("YTBULK_MAX_CONCURRENT", 5))
        self.error_threshold = int(os.getenv("YTBULK_ERROR_THRESHOLD", 10))
        
        # API settings
        self.api_endpoint = os.getenv(
            "YTBULK_API_ENDPOINT", 
            "https://yt-api.p.rapidapi.com/dl"
        )
        self.api_key = os.getenv("YTBULK_API_KEY")
        
        # Download preferences
        self.default_resolution = YTBulkResolution(
            os.getenv("YTBULK_DEFAULT_RESOLUTION", "1080p")
        )

    @property
    def is_valid(self) -> bool:
        """Check if configuration is valid."""
        return all([
            self.chunk_size > 0,
            self.max_retries > 0,
            self.max_concurrent > 0,
            self.error_threshold > 0,
            self.api_key is not None,
            isinstance(self.default_resolution, YTBulkResolution)
        ])

    def validate(self) -> None:
        """Validate configuration and raise exceptions for invalid values."""
        if not self.api_key:
            raise ValueError("YTBULK_API_KEY must be set")
        if self.chunk_size <= 0:
            raise ValueError("YTBULK_CHUNK_SIZE must be positive")
        if self.max_retries <= 0:
            raise ValueError("YTBULK_MAX_RETRIES must be positive")
        if self.max_concurrent <= 0:
            raise ValueError("YTBULK_MAX_CONCURRENT must be positive")
        if self.error_threshold <= 0:
            raise ValueError("YTBULK_ERROR_THRESHOLD must be positive")

    def as_dict(self) -> Dict:
        """Return configuration as dictionary."""
        return {
            "chunk_size": self.chunk_size,
            "max_retries": self.max_retries,
            "max_concurrent": self.max_concurrent,
            "error_threshold": self.error_threshold,
            "api_endpoint": self.api_endpoint,
            "api_key": self.api_key,
            "default_resolution": self.default_resolution.value
        }