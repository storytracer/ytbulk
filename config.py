# config.py
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
        self.max_retries = int(os.getenv("YTBULK_MAX_RETRIES", 3))
        self.max_concurrent = int(os.getenv("YTBULK_MAX_CONCURRENT", 5))
        self.error_threshold = int(os.getenv("YTBULK_ERROR_THRESHOLD", 10))
        self.test_video = os.getenv("YTBULK_TEST_VIDEO")  # Required
        
        # Proxy settings
        self.proxy_list_url = os.getenv("YTBULK_PROXY_LIST_URL")  # Required
        self.proxy_min_speed = float(os.getenv("YTBULK_PROXY_MIN_SPEED", "1.0"))  # MB/s
        
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
            self.proxy_min_speed > 0,
            isinstance(self.default_resolution, YTBulkResolution)
        ])

    def validate(self) -> None:
        """Validate configuration and raise exceptions for invalid values."""
        if self.max_retries <= 0:
            raise ValueError("YTBULK_MAX_RETRIES must be positive")
        if self.error_threshold <= 0:
            raise ValueError("YTBULK_ERROR_THRESHOLD must be positive")
        if self.proxy_min_speed <= 0:
            raise ValueError("YTBULK_PROXY_MIN_SPEED must be positive")
        if not self.proxy_list_url:
            raise ValueError("YTBULK_PROXY_LIST_URL is required")
        if not self.test_video:
            raise ValueError("YTBULK_TEST_VIDEO is required")