import boto3
import json
import logging
from pathlib import Path
from typing import Dict, Set, List
from botocore.exceptions import ClientError
from botocore.config import Config

class YTBulkStorage:
    """Handles working directory and S3 storage operations."""
    
    def __init__(self, work_dir: Path, bucket: str, max_concurrent_requests: int):
        self.work_dir = Path(work_dir)
        self.cache_dir = self.work_dir / "cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.bucket = bucket
        
        # Create config with max pool connections
        self.config = Config(
            max_pool_connections=max_concurrent_requests
        )
        
        # Initialize session and create configured client
        self.session = boto3.Session()
        self._s3_client = self.session.client("s3", config=self.config)
        
        self.downloads_prefix = "downloads"
        self.downloads_dir = self.work_dir / self.downloads_prefix
        self.downloads_dir.mkdir(parents=True, exist_ok=True)
    
    def get_metadata_filename(self, video_id: str) -> str:
        """Generate metadata filename."""
        return f"{video_id}.info.json"

    def get_video_filename(self, video_id: str) -> str:
        """Generate video filename."""
        return f"{video_id}.mp4"

    def get_audio_filename(self, video_id: str) -> str:
        """Generate audio filename."""
        return f"{video_id}.m4a"

    def get_work_path(self, channel_id: str, video_id: str, filename: str) -> Path:
        """Get path for temporary working files."""
        path = self.downloads_dir / channel_id / video_id
        path.mkdir(parents=True, exist_ok=True)
        return path / filename

    def get_s3_key(self, channel_id: str, video_id: str, filename: str) -> str:
        """Get S3 key for a file."""
        return f"{self.downloads_prefix}/{channel_id}/{video_id}/{filename}"

    def list_s3_files(self) -> Set[str]:
        """Get set of all files currently in S3 downloads directory."""
        try:
            paginator = self._s3_client.get_paginator('list_objects_v2')
            all_files = set()
            
            for page in paginator.paginate(Bucket=self.bucket, Prefix=self.downloads_prefix):
                for obj in page.get('Contents', []):
                    all_files.add(obj['Key'])
                    
            return all_files
        except ClientError as e:
            logging.error(f"Failed to list S3 objects: {e}")
            return set()

    def list_unprocessed_videos(self, video_ids: List[str], audio: bool, video: bool) -> List[str]:
        """Return list of video IDs that don't have all required files in S3."""
        s3_files = self.list_s3_files()
        s3_filenames = {Path(path).name for path in s3_files}
        unprocessed_videos = []
        
        for video_id in video_ids:
            required_files = [self.get_metadata_filename(video_id)]
            
            if video and audio:
                required_files.append(self.get_video_filename(video_id))
            elif audio:
                required_files.append(self.get_audio_filename(video_id))
            
            if any(req_file not in s3_filenames for req_file in required_files):
                unprocessed_videos.append(video_id)
            
        return unprocessed_videos

    def save_metadata(self, metadata) -> bool:
        """Save video metadata to working directory."""
        try:
            path = self.get_work_path(
                metadata.channel_id,
                metadata.video_id,
                self.get_metadata_filename(metadata.video_id)
            )
            
            with open(path, 'w') as f:
                json.dump({
                    'id': metadata.video_id,
                    'channel_id': metadata.channel_id,
                    'title': metadata.title
                }, f, indent=2)
            return True
        except Exception as e:
            logging.error(f"Failed to save metadata: {e}")
            return False

    def finalize_video(self, info_dict: dict) -> bool:
        """Upload processed files from work directory to S3."""
        channel_id = info_dict.get('channel_id')
        video_id = info_dict.get('id')
        video_dir = self.get_work_path(channel_id, video_id, "")
        
        try:
            # Upload all files that start with video_id
            for file_path in video_dir.iterdir():
                if file_path.name.startswith(video_id):
                    s3_key = self.get_s3_key(channel_id, video_id, file_path.name)
                    self._s3_client.upload_file(str(file_path), self.bucket, s3_key)
                    file_path.unlink()  # Remove after successful upload

            # Clean up empty directory
            if video_dir.exists():
                video_dir.rmdir()
                
            return True

        except Exception as e:
            logging.error(f"Failed to finalize video: {e}")
            return False