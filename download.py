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
                'writeinfojson': True,
                'writesubtitles': True,
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
        """Process videos with controlled concurrency."""
        total_videos = len(video_ids)
        
        # Initialize progress bar
        progress_bar = tqdm(total=total_videos, desc="Processing videos")
        
        try:
            # Get list of already processed videos
            to_process = await self.storage.list_unprocessed_videos(
                video_ids,
                audio=kwargs.get('download_audio', True),
                video=kwargs.get('download_video', True)
            )
            
            already_processed = total_videos - len(to_process)
            progress_bar.update(already_processed)

            if not to_process:
                logging.info("All videos already processed")
                return

            logging.info(f"Found {len(to_process)} videos to process out of {total_videos} total")
            
            # Create semaphore for concurrency control
            semaphore = asyncio.Semaphore(self.config.max_concurrent)
            failed_consecutive = 0
            should_stop = False
            
            async def process_with_semaphore(video_id: str):
                nonlocal failed_consecutive, should_stop
                
                if should_stop:
                    return False
                    
                async with semaphore:
                    try:
                        success = await self.process_video(video_id, **kwargs)
                        if success:
                            failed_consecutive = 0
                            progress_bar.update(1)
                        else:
                            failed_consecutive += 1
                            if failed_consecutive >= self.config.error_threshold:
                                should_stop = True
                                logging.error(f"Too many consecutive failures ({failed_consecutive})")
                                raise asyncio.CancelledError("Error threshold reached")
                        return success
                    except asyncio.CancelledError:
                        logging.debug(f"Cancelling download of {video_id}")
                        raise
                    except Exception as e:
                        logging.error(f"Error processing {video_id}: {e}")
                        return False

            async def shutdown(tasks):
                """Gracefully shutdown running tasks."""
                for task in tasks:
                    if not task.done():
                        task.cancel()
                
                if tasks:
                    logging.info("Waiting for running downloads to finish...")
                    await asyncio.gather(*tasks, return_exceptions=True)

            # Create and track tasks
            tasks = [asyncio.create_task(process_with_semaphore(vid)) for vid in to_process]
            
            try:
                # Wait for all tasks or until cancellation
                await asyncio.gather(*tasks)
                
            except asyncio.CancelledError:
                logging.info("\nReceived cancellation request, shutting down...")
                await shutdown(tasks)
                raise
                
            except KeyboardInterrupt:
                logging.info("\nReceived keyboard interrupt, shutting down...")
                await shutdown(tasks)
                raise
                
            except Exception as e:
                logging.error(f"Unexpected error: {e}")
                await shutdown(tasks)
                raise
                        
        finally:
            progress_bar.close()