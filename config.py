import os
from typing import Dict, Optional
from pathlib import Path
from dotenv import load_dotenv
from resolutions import YTBulkResolution

# Load .env files
load_dotenv()

class YTBulkConfig:
    """Configuration handler for YTBulk."""
    
    def __init__(self):
        """Load configuration from environment variables."""
        # Download settings
        self.chunk_size = int(os.getenv("YTBULK_CHUNK_SIZE", 1024 * 1024))
        self.max_retries = int(os.getenv("YTBULK_MAX_RETRIES", 3))
        self.error_threshold = int(os.getenv("YTBULK_ERROR_THRESHOLD", 10))
        
        # Proxy settings
        self.proxy_max_failures = int(os.getenv("YTBULK_PROXY_MAX_FAILURES", 3))
        self.proxy_cooldown_minutes = int(os.getenv("YTBULK_PROXY_COOLDOWN", 30))
        self.proxy_test_video = os.getenv("YTBULK_PROXY_TEST_VIDEO", "PrRa-crR6tI")

        
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
            self.error_threshold > 0,
            self.proxy_max_failures > 0,
            self.proxy_cooldown_minutes > 0,
            isinstance(self.default_resolution, YTBulkResolution)
        ])

    def validate(self) -> None:
        """Validate configuration and raise exceptions for invalid values."""
        if self.chunk_size <= 0:
            raise ValueError("YTBULK_CHUNK_SIZE must be positive")
        if self.max_retries <= 0:
            raise ValueError("YTBULK_MAX_RETRIES must be positive")
        if self.error_threshold <= 0:
            raise ValueError("YTBULK_ERROR_THRESHOLD must be positive")
        if self.proxy_max_failures <= 0:
            raise ValueError("YTBULK_PROXY_MAX_FAILURES must be positive")
        if self.proxy_cooldown_minutes <= 0:
            raise ValueError("YTBULK_PROXY_COOLDOWN must be positive")

    def as_dict(self) -> Dict:
        """Return configuration as dictionary."""
        return {
            "chunk_size": self.chunk_size,
            "max_retries": self.max_retries,
            "error_threshold": self.error_threshold,
            "proxy_max_failures": self.proxy_max_failures,
            "proxy_cooldown_minutes": self.proxy_cooldown_minutes,
            "default_resolution": self.default_resolution.value
        }