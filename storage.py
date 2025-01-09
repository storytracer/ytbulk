import asyncio
import aioboto3
import aiohttp
import aiofiles
import json
from pathlib import Path
from typing import Dict, Set, List
import logging
from botocore.exceptions import ClientError

class YTBulkStorage:
    """Handles working directory and S3 storage operations."""
    
    def __init__(self, work_dir: Path, bucket: str):
        self.work_dir = Path(work_dir)
        self.cache_dir = self.work_dir / "cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.bucket = bucket
        self.session = aioboto3.Session()
        self.downloads_prefix = "downloads"
    
    def get_metadata_filename(self, video_id: str) -> str:
        """Generate metadata filename."""
        return f"{video_id}.ytapi.json"

    def get_video_filename(self, video_id: str) -> str:
        """Generate video filename."""
        return f"{video_id}.video.mp4"

    def get_audio_filename(self, video_id: str) -> str:
        """Generate audio filename."""
        return f"{video_id}.audio.m4a"

    def get_merged_filename(self, video_id: str) -> str:
        """Generate merged output filename."""
        return f"{video_id}.mp4"

    def get_work_path(self, channel_id: str, video_id: str, filename: str) -> Path:
        """Get path for temporary working files."""
        path = self.work_dir / self.downloads_prefix / channel_id / video_id / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def get_s3_key(self, channel_id: str, video_id: str, filename: str) -> str:
        """Get S3 key for a file."""
        return f"{self.downloads_prefix}/{channel_id}/{video_id}/{filename}"

    async def list_s3_files(self) -> Set[str]:
        """Get set of all files currently in S3 downloads directory."""
        try:
            async with self.session.client("s3") as s3:
                paginator = s3.get_paginator('list_objects_v2')
                all_files = set()
                async for page in paginator.paginate(Bucket=self.bucket, Prefix=self.downloads_prefix):
                    for obj in page.get('Contents', []):
                        all_files.add(obj['Key'])
                return all_files
        except ClientError as e:
            logging.error(f"Failed to list S3 objects: {e}")
            return set()
        
    async def list_unprocessed_videos(self, video_ids: List[str], audio: bool, video: bool, merged: bool) -> List[str]:
        """
        Return list of video IDs that don't have all required files in S3.
        
        Args:
            video_ids: List of video IDs to check
            audio: Check for audio file existence
            video: Check for video file existence
            merged: Check for merged file existence
        """
        s3_files = await self.list_s3_files()
        s3_filenames = {Path(path).name for path in s3_files}
        unprocessed_videos = []
        
        for video_id in video_ids:
            required_files = []
            
            required_files.append(self.get_metadata_filename(video_id))
            
            if audio:
                required_files.append(self.get_audio_filename(video_id))
                
            if video:
                required_files.append(self.get_video_filename(video_id))
                
            if merged:
                required_files.append(self.get_merged_filename(video_id))
                
            if any(req_file not in s3_filenames for req_file in required_files):
                unprocessed_videos.append(video_id)
            
        return unprocessed_videos
    
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
                self.get_metadata_filename(metadata.video_id)
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
            self.get_work_path(channel_id, video_id, self.get_video_filename(video_id))
        )

    async def download_audio(self, channel_id: str, video_id: str, url: str) -> bool:
        """Download audio file to working directory."""
        return await self.download_file(
            url,
            self.get_work_path(channel_id, video_id, self.get_audio_filename(video_id))
        )

    async def merge_audio_video(self, channel_id: str, video_id: str) -> bool:
        """Merge video and audio files using ffmpeg."""
        work_dir = self.get_work_path(channel_id, video_id, "")
        video_path = work_dir / self.get_video_filename(video_id)
        audio_path = work_dir / self.get_audio_filename(video_id)
        output_path = work_dir / self.get_merged_filename(video_id)

        try:
            process = await asyncio.create_subprocess_exec(
                'ffmpeg',
                '-i', str(video_path),
                '-i', str(audio_path),
                '-c:v', 'copy',
                '-c:a', 'copy',
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
        video_dir = self.get_work_path(channel_id, video_id, "")
        
        try:
            async with self.session.client("s3") as s3:
                for file_path in video_dir.iterdir():
                    if file_path.name.startswith(video_id):
                        s3_key = self.get_s3_key(channel_id, video_id, file_path.name)
                        await s3.upload_file(str(file_path), self.bucket, s3_key)
                        file_path.unlink()  # Remove after successful upload

            if video_dir.exists():
                video_dir.rmdir()  # Remove empty video directory
                
            return True

        except Exception as e:
            logging.error(f"Failed to finalize video: {e}")
            return False