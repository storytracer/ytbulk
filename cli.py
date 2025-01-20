# cli.py
import logging

import click
from pathlib import Path
from typing import List
import csv
import re
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from threading import Lock
from tqdm import tqdm

from config import YTBulkConfig
from resolutions import YTBulkResolution
from download import YTBulkDownloader
from proxies import YTBulkProxyManager
from storage import YTBulkStorage

logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class YTBulkCLI:
    """Command line interface for YTBulk."""
    
    @staticmethod
    def is_valid_youtube_id(video_id: str) -> bool:
        """Validate YouTube video ID format."""
        return bool(re.match(r'^[A-Za-z0-9_-]{11}$', video_id))

    @staticmethod
    def read_video_ids(file_path: Path, id_column: str) -> List[str]:
        """Read and validate video IDs from CSV file."""
        video_ids = []
        with open(file_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                if id_column in row and row[id_column].strip():
                    video_id = row[id_column].strip()
                    if YTBulkCLI.is_valid_youtube_id(video_id):
                        video_ids.append(video_id)
                    else:
                        logging.warning(f"Invalid YouTube ID format: {video_id}")
        return video_ids

def setup_logging():
    """Set up basic logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    logging.info("Logging initialized")

def process_video_with_progress(video_id: str, downloader: YTBulkDownloader, 
                              progress_bar: tqdm, progress_lock: Lock,
                              download_video: bool = True, 
                              download_audio: bool = True) -> bool:
    """Process a single video and update progress bar."""
    try:
        success = downloader.process_video(
            video_id,
            download_video=download_video,
            download_audio=download_audio
        )
        if success:
            with progress_lock:
                progress_bar.update(1)
        return success
    except Exception as e:
        logging.error(f"Error processing video {video_id}: {e}")
        return False

@click.command()
@click.argument('csv_file', type=click.Path(exists=True))
@click.argument('id_column', type=str)
@click.option('--work-dir', type=click.Path(dir_okay=True), required=True, help='Working directory for downloads')
@click.option('--bucket', required=True, help='S3 bucket name')
@click.option('--max-resolution', 
              type=click.Choice([res.value for res in YTBulkResolution], case_sensitive=False),
              help='Maximum video resolution')
@click.option('--video/--no-video', default=True, help='Download video')
@click.option('--audio/--no-audio', default=True, help='Download audio')
def main(
    csv_file: str,
    id_column: str,
    work_dir: str,
    bucket: str,
    max_resolution: str,
    video: bool,
    audio: bool
):
    """Download YouTube videos from a file containing video IDs."""
    
    setup_logging()
    config = YTBulkConfig()
    config.validate()

    if max_resolution:
        config.default_resolution = YTBulkResolution(max_resolution)

    proxy_manager = YTBulkProxyManager(
        config=config,
        work_dir=Path(work_dir),
        max_concurrent_requests=config.max_concurrent
    )
    
    storage_manager = YTBulkStorage(
        work_dir=Path(work_dir),
        bucket=bucket
    )
    
    downloader = YTBulkDownloader(
        config=config,
        proxy_manager=proxy_manager,
        storage_manager=storage_manager
    )

    try:
        # Read video IDs
        video_ids = YTBulkCLI.read_video_ids(Path(csv_file), id_column)
        total = len(video_ids)
        
        if total == 0:
            logging.warning("No video IDs found in input file")
            click.echo("No video IDs found in input file", err=True)
            return

        # Get list of unprocessed videos
        to_process = storage_manager.list_unprocessed_videos(
            video_ids,
            audio=audio,
            video=video
        )
        
        already_processed = total - len(to_process)
        
        if not to_process:
            logging.info("All videos already processed")
            return

        logging.info(f"Found {len(to_process)} videos to process out of {total} total")

        # Create progress bar and lock
        progress_bar = tqdm(total=total, 
                          initial=already_processed, 
                          desc="Processing videos",
                          unit="video")
        progress_lock = Lock()

        # Process videos with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=config.max_concurrent) as executor:
            process_func = partial(
                process_video_with_progress,
                downloader=downloader,
                progress_bar=progress_bar,
                progress_lock=progress_lock,
                download_video=video,
                download_audio=audio
            )
            results = list(executor.map(process_func, to_process))

        # Close progress bar
        progress_bar.close()

        # Log results
        success_count = sum(1 for r in results if r)
        logging.info(f"Successfully processed {success_count} out of {len(to_process)} videos")
        click.echo(f"\nSuccessfully processed {success_count} out of {len(to_process)} videos")

    except KeyboardInterrupt:
        logging.info("Download interrupted by user")
        click.echo("\nDownload interrupted by user")
    except Exception as e:
        logging.critical(f"Fatal error: {e}")
        click.echo(f"\nFatal error: {e}", err=True)
        exit(1)

if __name__ == '__main__':
    main()