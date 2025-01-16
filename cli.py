import asyncio
import click
from pathlib import Path
from typing import List
import aiofiles
import csv
import re
from tqdm import tqdm

from config import YTBulkConfig
from resolutions import YTBulkResolution
from download import YTBulkDownloader
from proxies import YTBulkProxyManager
from storage import YTBulkStorage

class YTBulkCLI:
    """Command line interface for YTBulk."""
    
    @staticmethod
    def is_valid_youtube_id(video_id: str) -> bool:
        """Validate YouTube video ID format."""
        return bool(re.match(r'^[A-Za-z0-9_-]{11}$', video_id))

    @staticmethod
    async def read_video_ids(file_path: Path, id_column: str) -> List[str]:
        """Read and validate video IDs from CSV file."""
        video_ids = []
        async with aiofiles.open(file_path) as f:
            content = await f.read()
            reader = csv.DictReader(content.splitlines())
            for row in reader:
                if id_column in row and row[id_column].strip():
                    video_id = row[id_column].strip()
                    if YTBulkCLI.is_valid_youtube_id(video_id):
                        video_ids.append(video_id)
                    else:
                        click.echo(f"Warning: Invalid YouTube ID format: {video_id}", err=True)
        return video_ids

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
@click.option('--merge/--no-merge', default=True, help='Merge video and audio')
@click.option('--max-consecutive-failures', type=int, default=3, help='Maximum consecutive proxy failures')
@click.option('--proxy-cooldown', type=int, default=30, help='Proxy cooldown time in minutes')
def main(
    csv_file: str,
    id_column: str,
    work_dir: str,
    bucket: str,
    max_resolution: str,
    video: bool,
    audio: bool,
    merge: bool,
    max_consecutive_failures: int,
    proxy_cooldown: int
):
    """Download YouTube videos from a file containing video IDs."""
    
    # Load and validate configuration
    config = YTBulkConfig()
    config.validate()

    # Override config with CLI options if provided
    if max_resolution:
        config.default_resolution = YTBulkResolution(max_resolution)

    # Initialize proxy manager
    proxy_manager = YTBulkProxyManager(
        config=config,
        work_dir=Path(work_dir)
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

    async def run():
        # Read video IDs
        video_ids = await YTBulkCLI.read_video_ids(Path(csv_file), id_column)
        total = len(video_ids)
        
        if total == 0:
            click.echo("No video IDs found in input file", err=True)
            return

        # Initialize progress bar
        with tqdm(total=total, desc="Processing videos") as pbar:
            downloader.progress_bar = pbar
            try:
                await downloader.process_video_list(
                    video_ids,
                    download_video=video,
                    download_audio=audio
                )
            except Exception as e:
                click.echo(f"\nError: {e}", err=True)
                raise

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        click.echo("\nDownload interrupted by user")
    except Exception as e:
        click.echo(f"\nFatal error: {e}", err=True)
        exit(1)

if __name__ == '__main__':
    main()