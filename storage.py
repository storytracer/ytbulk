import asyncio
import aioboto3
import aiohttp
import aiofiles
import json
from pathlib import Path
from typing import Dict, Set
import logging
from botocore.exceptions import ClientError

class YTBulkStorage:
    """Handles working directory and S3 storage operations."""
    
    def __init__(self, work_dir: Path, bucket: str):
        self.work_dir = Path(work_dir)
        self.cache_dir = self.work_dir / "cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.processed_videos_cache = self.cache_dir / "processed_videos.json"
        self.bucket = bucket
        self.session = aioboto3.Session()

    def get_work_path(self, channel_id: str, video_id: str, filename: str) -> Path:
        """Get path for temporary working files."""
        path = self.work_dir / channel_id / video_id / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def get_s3_key(self, channel_id: str, video_id: str, filename: str) -> str:
        """Get S3 key for a file."""
        return f"downloads/{channel_id}/{video_id}/{filename}"

    async def list_processed_videos(self) -> Set[str]:
        """Get list of completely processed videos from S3."""
        try:
            # Try local cache first
            if self.processed_videos_cache.exists():
                with open(self.processed_videos_cache) as f:
                    return set(json.load(f))
        except Exception:
            pass

        # If cache miss or error, check S3
        processed = set()
        async with self.session.client("s3") as s3:
            paginator = s3.get_paginator('list_objects_v2')
            try:
                async for page in paginator.paginate(Bucket=self.bucket, Prefix="downloads/"):
                    for obj in page.get('Contents', []):
                        # Parse the S3 key: downloads/channel_id/video_id/filename
                        parts = obj['Key'].split('/')
                        if len(parts) >= 4:
                            processed.add(parts[2])  # video_id

                # Update cache
                self.processed_videos_cache.write_text(json.dumps(list(processed)))
                return processed

            except ClientError as e:
                logging.error(f"Failed to list S3 objects: {e}")
                return set()

    async def download_file(self, url: str, path: Path, chunk_size: int = 1024 * 1024) -> bool:
        """Download a file from URL to path."""
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            async with aiofiles.open(path, 'wb') as f:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        if response.status != 200:
                            return False
                        async for chunk in response.content.iter_chunked(chunk_size):
                            await f.write(chunk)
                return True
        except Exception as e:
            logging.error(f"Download failed: {e}")
            return False

    async def save_metadata(self, metadata) -> bool:
        """Save video metadata to working directory."""
        try:
            path = self.get_work_path(
                metadata.channel_id,
                metadata.video_id,
                f"{metadata.video_id}.json"
            )
            async with aiofiles.open(path, 'w') as f:
                await f.write(json.dumps(metadata.api_response, indent=2))
            return True
        except Exception as e:
            logging.error(f"Failed to save metadata: {e}")
            return False

    async def download_video(self, channel_id: str, video_id: str, url: str) -> bool:
        """Download video file to working directory."""
        return await self.download_file(
            url,
            self.get_work_path(channel_id, video_id, f"{video_id}.video.mp4")
        )

    async def download_audio(self, channel_id: str, video_id: str, url: str) -> bool:
        """Download audio file to working directory."""
        return await self.download_file(
            url,
            self.get_work_path(channel_id, video_id, f"{video_id}.audio.m4a")
        )

    async def merge_audio_video(self, channel_id: str, video_id: str) -> bool:
        """Merge video and audio files using ffmpeg."""
        work_dir = self.get_work_path(channel_id, video_id, "")
        video_path = work_dir / f"{video_id}.video.mp4"
        audio_path = work_dir / f"{video_id}.audio.m4a"
        output_path = work_dir / f"{video_id}.mp4"

        try:
            process = await asyncio.create_subprocess_exec(
                'ffmpeg',
                '-i', str(video_path),
                '-i', str(audio_path),
                '-c:v', 'copy',
                '-c:a', 'aac',
                str(output_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            return process.returncode == 0

        except Exception as e:
            logging.error(f"Failed to merge files: {e}")
            return False

    async def finalize_video(self, channel_id: str, video_id: str) -> bool:
        """Upload processed files from work directory to S3."""
        work_dir = self.get_work_path(channel_id, video_id, "").parent
        
        try:
            async with self.session.client("s3") as s3:
                for file_path in work_dir.iterdir():
                    if file_path.name.startswith(video_id):
                        s3_key = self.get_s3_key(channel_id, video_id, file_path.name)
                        await s3.upload_file(str(file_path), self.bucket, s3_key)
                        file_path.unlink()  # Remove after successful upload

            if work_dir.exists():
                work_dir.rmdir()  # Remove empty work directory
                
            return True

        except Exception as e:
            logging.error(f"Failed to finalize video: {e}")
            return False