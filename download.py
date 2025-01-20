# download.py
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List
from threading import Lock

from config import YTBulkConfig
from storage import YTBulkStorage
from proxies import YTBulkProxyManager
from resolutions import YTBulkResolution

@dataclass
class VideoMetadata:
    """Metadata for a single video."""
    video_id: str
    channel_id: str
    title: str

class YTBulkDownloader:
    """Handles the downloading of videos in bulk."""
    
    def __init__(
        self,
        config: YTBulkConfig,
        proxy_manager: YTBulkProxyManager,
        storage_manager: YTBulkStorage,
    ):
        self.config = config
        self.storage = storage_manager
        self.proxy_manager = proxy_manager
        self.download_lock = Lock()

    def process_video(
        self,
        video_id: str,
        download_video: bool = True,
        download_audio: bool = True,
    ) -> bool:
        """Process a single video using yt-dlp."""
        try:
            if download_video and download_audio:
                format_str = f'bestvideo[height<={self.config.default_resolution.height}][ext=mp4]+bestaudio[ext=m4a]'
            elif download_video:
                format_str = f'bestvideo[height<={self.config.default_resolution.height}][ext=mp4]'
            else:
                format_str = 'bestaudio[ext=m4a]'

            ydl_opts = {
                'format': format_str,
                'outtmpl': str(self.storage.downloads_dir / '%(channel_id)s/%(id)s/%(id)s.%(ext)s'),
                'keepvideo': False,
                'retries': self.config.max_retries,
                'quiet': True,
                'no_warnings': True,
                'extract_flat': False,
                'writethumbnail': False,
                'writeinfojson': True
            }

            success, _ = self.proxy_manager.download_with_proxy(
                video_id, 
                ydl_opts,
                self.storage
            )
            return success

        except Exception as e:
            logging.error(f"Failed to process video {video_id}: {e}")
            return False