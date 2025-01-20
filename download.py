import logging
import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List
from tqdm import tqdm

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
        self.progress_bar = None
        self.download_lock = asyncio.Lock()

    async def process_video(
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
                'outtmpl': str(self.storage.downloads_dir) + '/%(channel_id)s/%(id)s/%(id)s.%(ext)s',
                'keepvideo': False,
                'retries': self.config.max_retries,
                'quiet': True,
                'no_warnings': True,
                'extract_flat': False,
                'writethumbnail': False,
                'writeinfojson': True
            }

            success, _ = await self.proxy_manager.download_with_proxy(
                video_id, 
                ydl_opts,
                self.storage
            )
            return success

        except Exception as e:
            logging.error(f"Failed to process video {video_id}: {e}")
            return False

    async def process_video_list(self, video_ids: List[str], **kwargs) -> None:
        """Process videos with true concurrency."""
        total_videos = len(video_ids)
        
        # Initialize progress bar
        
        try:
            # Initialize proxy manager
            await self.proxy_manager.initialize()
            
            # Get list of already processed videos
            to_process = await self.storage.list_unprocessed_videos(
                video_ids,
                audio=kwargs.get('download_audio', True),
                video=kwargs.get('download_video', True)
            )
            
            already_processed = total_videos - len(to_process)
            
            progress_bar = tqdm(total=total_videos, initial=already_processed, desc="Processing videos")

            if not to_process:
                logging.info("All videos already processed")
                return

            logging.info(f"Found {len(to_process)} videos to process out of {total_videos} total")
            
            failure_count = 0
            queue = asyncio.Queue()
            for vid in to_process:
                await queue.put(vid)

            async def worker():
                nonlocal failure_count
                while True:
                    try:
                        video_id = await queue.get()
                        success = await self.process_video(video_id, **kwargs)
                        
                        async with self.download_lock:
                            if success:
                                failure_count = 0
                                progress_bar.update(1)
                            else:
                                failure_count += 1
                                if failure_count >= self.config.error_threshold:
                                    logging.error(f"Error threshold reached ({failure_count})")
                                    return
                        
                        queue.task_done()
                        
                        if failure_count >= self.config.error_threshold:
                            return
                            
                    except asyncio.CancelledError:
                        break
                    except Exception as e:
                        logging.error(f"Worker error: {e}")
                        queue.task_done()

            # Create workers
            workers = []
            for _ in range(self.config.max_concurrent):
                task = asyncio.create_task(worker())
                workers.append(task)

            # Wait for queue to be processed
            await queue.join()

            # Cancel workers
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

        except asyncio.CancelledError:
            logging.info("\nReceived cancellation request")
            raise
        except KeyboardInterrupt:
            logging.info("\nReceived keyboard interrupt")
            raise
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            raise
        finally:
            progress_bar.close()