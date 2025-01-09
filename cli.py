import asyncio
import click
from pathlib import Path
from typing import List
import aiofiles
from tqdm import tqdm

from .config import YTBulkConfig
from .resolutions import YTBulkResolution
from .download import YTBulkDownloader

class YTBulkCLI:
    """Command line interface for YTBulk."""

    @staticmethod
    async def read_video_ids(file_path: Path) -> List[str]:
        """Read and validate video IDs from file."""
        async with aiofiles.open(file_path) as f:
            return [line.strip() for line in await f.readlines() if line.strip()]

@click.command()
@click.argument('id_file', type=click.Path(exists=True))
@click.option('--work-dir', type=click.Path(required=True), help='Working directory for downloads')
@click.option('--bucket', required=True, help='S3 bucket name')
@click.option('--max-resolution', 
              type=click.Choice([res.value for res in YTBulkResolution], case_sensitive=False),
              help='Maximum video resolution')
@click.option('--video/--no-video', default=True, help='Download video')
@click.option('--audio/--no-audio', default=True, help='Download audio')
@click.option('--merge/--no-merge', default=True, help='Merge video and audio')
@click.option('--max-concurrent', type=int, help='Maximum concurrent downloads')
def main(
    id_file: str,
    work_dir: str,
    bucket: str,
    max_resolution: str,
    video: bool,
    audio: bool,
    merge: bool,
    max_concurrent: int
):
    """Download YouTube videos from a file containing video IDs."""
    
    # Load and validate configuration
    config = YTBulkConfig()
    config.validate()

    # Override config with CLI options if provided
    if max_concurrent:
        config.max_concurrent = max_concurrent
    if max_resolution:
        config.default_resolution = YTBulkResolution(max_resolution)

    downloader = YTBulkDownloader(
        config=config,
        work_dir=Path(work_dir),
        bucket=bucket
    )

    async def run():
        # Read video IDs
        video_ids = await YTBulkCLI.read_video_ids(Path(id_file))
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
                    download_audio=audio,
                    merge=merge
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