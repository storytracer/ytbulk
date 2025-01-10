import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List
from tqdm import tqdm
import yt_dlp

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
        work_dir: Path,
        bucket: str,
        proxy_manager: YTBulkProxyManager
    ):
        self.config = config
        self.storage = YTBulkStorage(work_dir, bucket)
        self.proxy_manager = proxy_manager
        self.progress_bar = None

    def _get_format_selector(self, resolution: YTBulkResolution):
        """Create format selector based on resolution."""
        max_height = resolution.height
        return (
            f'bestvideo[height<={max_height}][ext=mp4]'
            '+bestaudio[ext=m4a]/best[height<={max_height}]'
        )

    async def process_video(
        self,
        video_id: str,
        proxy_url: str,
        download_video: bool = True,
        download_audio: bool = True,
        merge: bool = True
    ) -> bool:
        """Process a single video using yt-dlp."""
        try:
            # Prepare download options
            # In YTBulkDownloader.process_video method:
            format_spec = []
            if download_video and download_audio:
                format_str = f'bestvideo[height<={self.config.default_resolution.height}][ext=mp4]+bestaudio[ext=m4a]'
            elif download_video:
                format_str = f'bestvideo[height<={self.config.default_resolution.height}][ext=mp4]'
            else:
                format_str = 'bestaudio[ext=m4a]'

            ydl_opts = {
                'format': format_str,
                'proxy': proxy_url,
                'outtmpl': str(self.storage.get_work_path('%(uploader_id)s', '%(id)s', '%(id)s.%(ext)s')),
                'keepvideo': False,  # This ensures intermediate files are deleted
                'retries': self.config.max_retries,
                'quiet': True,
                'no_warnings': True,
                'extract_flat': False,
                'writethumbnail': False,
                'progress_hooks': [self._progress_hook] if self.progress_bar else []
            }

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(f'https://www.youtube.com/watch?v={video_id}', download=False)
                
                # Store metadata first
                metadata = VideoMetadata(
                    video_id=video_id,
                    channel_id=info.get('uploader_id', 'unknown'),
                    title=info.get('title', 'unknown')
                )
                await self.storage.save_metadata(metadata)

                # Download the video
                result = ydl.download([f'https://www.youtube.com/watch?v={video_id}'])
                if result != 0:
                    raise Exception("Download failed")

                # Upload to S3
                await self.storage.finalize_video(metadata.channel_id, metadata.video_id)
                self.proxy_manager.mark_proxy_success(proxy_url)
                return True

        except yt_dlp.utils.ExtractorError as e:
            error_str = str(e).lower()
            if "http error 403" in error_str or "sign in to confirm" in error_str:
                self.proxy_manager.mark_proxy_failure(proxy_url, "blocked")
            else:
                self.proxy_manager.mark_proxy_failure(proxy_url, "error")
            logging.error(f"Failed to process video {video_id}: {e}")
            return False

        except Exception as e:
            logging.error(f"Failed to process video {video_id}: {e}")
            self.proxy_manager.mark_proxy_failure(proxy_url, "error")
            return False

    def _progress_hook(self, d):
        """Update progress bar based on yt-dlp progress."""
        if self.progress_bar and d['status'] == 'downloading':
            if 'total_bytes' in d:
                self.progress_bar.total = d['total_bytes']
                self.progress_bar.n = d['downloaded_bytes']
                self.progress_bar.refresh()

    async def process_video_list(self, video_ids: List[str], **kwargs) -> None:
        """Process videos sequentially using available proxies."""
        # Get list of already processed videos
        to_process = await self.storage.list_unprocessed_videos(
            video_ids,
            audio=kwargs.get('download_audio', True),
            video=kwargs.get('download_video', True),
            merged=kwargs.get('merge', True)
        )
        
        if not to_process:
            logging.info("All videos already processed")
            return

        logging.info(f"Found {len(to_process)} videos to process out of {len(video_ids)} total")
        
        failed_consecutive = 0
        
        for video_id in to_process:
            proxy_url = self.proxy_manager.get_next_proxy()
            if not proxy_url:
                logging.error("No working proxies available")
                break

            success = await self.process_video(video_id, proxy_url, **kwargs)
            if success:
                failed_consecutive = 0
                if self.progress_bar:
                    self.progress_bar.update(1)
            else:
                failed_consecutive += 1
                if failed_consecutive >= self.config.error_threshold:
                    logging.error(f"Too many consecutive failures ({failed_consecutive})")
                    break