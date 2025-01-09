import asyncio
import aiohttp
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Dict
from tqdm import tqdm

from .config import YTBulkConfig
from .storage import YTBulkStorage
from .resolutions import YTBulkResolution

@dataclass
class VideoMetadata:
    """Metadata for a single video."""
    video_id: str
    channel_id: str
    title: str
    video_url: Optional[str] = None
    audio_url: Optional[str] = None
    api_response: Optional[dict] = None

class YTBulkDownloader:
    """Handles the downloading of videos in bulk."""
    
    def __init__(
        self,
        config: YTBulkConfig,
        work_dir: Path,
        bucket: str
    ):
        self.config = config
        self.storage = YTBulkStorage(work_dir, bucket)
        self.progress_bar = None
        self.consecutive_errors = 0

    async def fetch_metadata(self, video_id: str) -> Optional[VideoMetadata]:
        """Fetch video metadata from API."""
        headers = {
            "x-rapidapi-key": self.config.api_key,
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.config.api_endpoint,
                    headers=headers,
                    params={"id": video_id}
                ) as response:
                    if response.status != 200:
                        raise Exception(f"API request failed with status {response.status}")
                    
                    data = await response.json()
                    
                    # Validate required fields
                    channel_id = data.get("channelId")
                    title = data.get("title")
                    if not channel_id or not title:
                        raise ValueError(f"Missing required metadata for video {video_id}")

                    # Select best formats
                    video_url = audio_url = None
                    best_height = best_bitrate = 0
                    max_height = self.config.default_resolution.height
                    
                    for fmt in data.get("adaptiveFormats", []):
                        mime_type = fmt.get("mimeType", "")
                        if "video/mp4" in mime_type:
                            height = fmt.get("height", 0)
                            if height <= max_height and height > best_height:
                                best_height = height
                                video_url = fmt["url"]
                        elif "audio/mp4" in mime_type:
                            bitrate = fmt.get("bitrate", 0)
                            if bitrate > best_bitrate:
                                best_bitrate = bitrate
                                audio_url = fmt["url"]

                    return VideoMetadata(
                        video_id=video_id,
                        channel_id=channel_id,
                        title=title,
                        video_url=video_url,
                        audio_url=audio_url,
                        api_response=data
                    )

        except Exception as e:
            logging.error(f"Failed to fetch metadata for video {video_id}: {e}")
            self.consecutive_errors += 1
            if self.consecutive_errors >= self.config.error_threshold:
                raise Exception(f"Error threshold reached ({self.config.error_threshold} consecutive errors)")
            return None

    async def process_video_list(
        self,
        video_ids: List[str],
        download_video: bool = True,
        download_audio: bool = True,
        merge: bool = True
    ) -> None:
        """Process a list of video IDs concurrently."""
        # Get list of already processed videos
        processed = await self.storage.list_processed_videos()
        to_process = [vid for vid in video_ids if vid not in processed]
        
        if not to_process:
            logging.info("All videos already processed")
            return

        logging.info(f"Found {len(to_process)} videos to process out of {len(video_ids)} total")
        
        semaphore = asyncio.Semaphore(self.config.max_concurrent)
        
        async def process_one(video_id: str):
            async with semaphore:
                metadata = await self.fetch_metadata(video_id)
                if metadata:
                    await self.process_video(
                        metadata,
                        download_video=download_video,
                        download_audio=download_audio,
                        merge=merge
                    )
                    if self.progress_bar:
                        self.progress_bar.update(1)

        tasks = [process_one(vid) for vid in to_process]
        await asyncio.gather(*tasks)

    async def process_video(
        self,
        metadata: VideoMetadata,
        download_video: bool = True,
        download_audio: bool = True,
        merge: bool = True
    ) -> bool:
        """Process a single video."""
        try:
            # Save metadata
            await self.storage.save_metadata(metadata)
            
            # Download files
            downloads = []
            if download_video and metadata.video_url:
                downloads.append(
                    self.storage.download_video(
                        metadata.channel_id,
                        metadata.video_id,
                        metadata.video_url
                    )
                )
                
            if download_audio and metadata.audio_url:
                downloads.append(
                    self.storage.download_audio(
                        metadata.channel_id,
                        metadata.video_id,
                        metadata.audio_url
                    )
                )

            if not all(await asyncio.gather(*downloads)):
                raise Exception("Download failed")

            # Merge if requested
            if merge and download_video and download_audio:
                if not await self.storage.merge_audio_video(metadata.channel_id, metadata.video_id):
                    raise Exception("Merge failed")

            # Upload to S3
            await self.storage.finalize_video(metadata.channel_id, metadata.video_id)
            return True

        except Exception as e:
            logging.error(f"Failed to process video {metadata.video_id}: {e}")
            return False