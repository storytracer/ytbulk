import asyncio
import json
import logging
from typing import Optional, List, Tuple, Set
from pathlib import Path
import aiohttp
import aiofiles
import yt_dlp
from yt_dlp.utils import ExtractorError

from config import YTBulkConfig
from storage import YTBulkStorage


class YTBulkProxyManager:
    def __init__(self, config: YTBulkConfig, work_dir: Path):
        self.work_dir = work_dir
        self.proxy_list_url = config.proxy_list_url
        self.failed_proxies_file = work_dir / "cache" / "failed_proxies.jsonl"
        self.failed_proxies_file.parent.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__ )

        # Proxies
        self.failed_proxies: Set[str] = set()  # Set of failed proxies
        self.proxies: List[str] = []  # List of working proxies
        self.current_index = 0  # Index for round-robin proxy selection

    async def load_proxy_list(self) -> None:
        """Load the list of proxies from the remote URL."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.proxy_list_url) as response:
                    if response.status != 200:
                        raise Exception(f"Failed to download proxy list: {response.status}")
                    content = await response.text()
                    self.proxies = [
                        line.strip() for line in content.splitlines() if line.strip()
                    ]
        except Exception as e:
            self.logger.error(f"Error loading proxy list: {e}")
            raise

    async def load_failed_proxies(self) -> None:
        """Load failed proxies from the JSONL file."""
        if not self.failed_proxies_file.exists():
            return

        try:
            async with aiofiles.open(self.failed_proxies_file, "r") as f:
                async for line in f:
                    try:
                        proxy_data = json.loads(line.strip())
                        if "proxy" in proxy_data:
                            self.failed_proxies.add(proxy_data["proxy"])
                    except json.JSONDecodeError as e:
                        self.logger.warning(f"Error parsing failed proxy JSON: {line.strip()} - {e}")
        except Exception as e:
            self.logger.error(f"Error loading failed proxies: {e}")

    async def save_failed_proxy(self, proxy_url: str) -> None:
        """Save a single failed proxy to the JSONL file."""
        try:
            async with aiofiles.open(self.failed_proxies_file, "a") as f:
                await f.write(json.dumps({"proxy": proxy_url}) + "\n")
        except Exception as e:
            self.logger.error(f"Error saving failed proxy: {proxy_url} - {e}")

    async def initialize(self) -> None:
        """Initialize the proxy manager."""
        # Load failed proxies
        await self.load_failed_proxies()

        # Load and filter the proxy list
        await self.load_proxy_list()
        self.proxies = [proxy for proxy in self.proxies if proxy not in self.failed_proxies]

        if not self.proxies:
            raise Exception("No working proxies available after filtering failed proxies.")

    def get_next_proxy(self) -> Optional[str]:
        """Get the next available proxy using round-robin selection."""
        if not self.proxies:
            return None
        proxy = self.proxies[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.proxies)
        return proxy

    async def mark_proxy_failed(self, proxy_url: str) -> None:
        """Mark a proxy as failed and save it to the failed list."""
        if proxy_url not in self.failed_proxies:
            self.failed_proxies.add(proxy_url)
            await self.save_failed_proxy(proxy_url)

    async def download_with_proxy(
        self, 
        video_id: str, 
        ydl_opts: dict, 
        storage_manager: YTBulkStorage
    ) -> Tuple[bool, Optional[str]]:
        """
        Attempt to download a video using available proxies.
        Returns (success, proxy_url used).
        """
        while self.proxies:
            proxy_url = self.get_next_proxy()
            if not proxy_url:
                self.logger.error("No working proxies available.")
                return False, None

            ydl_opts["proxy"] = proxy_url

            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=True)
                    if not info:
                        raise Exception("Download failed")

                    # Finalize the video after successful download
                    if await storage_manager.finalize_video(info):
                        return True, proxy_url
                    else:
                        return False, proxy_url

            except Exception as e:
                error_str = str(e).lower()
                self.logger.error(f"Error with proxy {proxy_url}: {e}")
                if ("http error 403" in error_str or 
                    "sign in to confirm" in error_str or 
                    "unable to download" in error_str):
                    self.logger.warning(f"Marking proxy as failed: {proxy_url} due to error: {e}")
                    await self.mark_proxy_failed(proxy_url)
                    self.proxies.remove(proxy_url)
                return False, proxy_url

        self.logger.error("Exhausted all proxies without success.")
        return False, None