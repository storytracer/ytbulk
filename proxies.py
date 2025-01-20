import asyncio
import json
import logging
import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

import aiohttp
import yt_dlp

from config import YTBulkConfig
from storage import YTBulkStorage


@dataclass
class ProxyStatus:
    """Simple proxy status tracking."""
    url: str
    is_good: bool = False
    download_speed: Optional[float] = None  # Speed in MB/s


class YTBulkProxyManager:
    def __init__(self, config: YTBulkConfig, work_dir: Path, max_concurrent_requests: int):
        self.work_dir = work_dir
        self.proxy_list_url = config.proxy_list_url
        self.status_file = work_dir / "cache" / "proxies.json"
        self.test_video_id = config.proxy_test_video
        self.max_concurrent_requests = max_concurrent_requests

        self.status_file.parent.mkdir(parents=True, exist_ok=True)
        self.good_proxies: List[str] = []
        self.bad_proxies: List[str] = []
        self.proxy_list: List[str] = []

        asyncio.run(self.initialize_proxies())

    async def initialize_proxies(self):
        """Load and test the required number of proxies."""
        self.proxy_list = await self._load_proxy_list()
        status = self._load_proxy_status()

        for proxy in self.proxy_list:
            if len(self.good_proxies) >= self.max_concurrent_requests:
                break
            if proxy not in status:
                status[proxy] = await self._test_and_save_status(proxy)

            if status[proxy]["is_good"]:
                self.good_proxies.append(proxy)
            else:
                self.bad_proxies.append(proxy)

        self._save_proxy_status(status)

    async def _load_proxy_list(self) -> List[str]:
        """Fetch the proxy list from the remote URL."""
        async with aiohttp.ClientSession() as session:
            async with session.get(self.proxy_list_url) as response:
                if response.status != 200:
                    raise Exception(f"Failed to fetch proxy list: {response.status}")
                content = await response.text()  # Await the coroutine
                return list(set(line.strip() for line in content.splitlines() if line.strip()))

    def _load_proxy_status(self) -> Dict[str, Dict[str, Optional[float]]]:
        """Load proxy statuses from the local file."""
        if self.status_file.exists():
            with open(self.status_file) as f:
                return json.load(f)
        return {}

    def _save_proxy_status(self, status: Dict[str, Dict[str, Optional[float]]]):
        """Save proxy statuses to the local file."""
        with open(self.status_file, "w") as f:
            json.dump(status, f, indent=2)

    async def get_working_proxy(self) -> Optional[str]:
        """Return the next available good proxy, testing more if needed."""
        while not self.good_proxies:
            logging.info("No good proxies available. Testing more...")
            status = self._load_proxy_status()

            for proxy in self.proxy_list:
                if proxy not in status or not status[proxy]["is_good"]:
                    status[proxy] = await self._test_and_save_status(proxy)

                    if status[proxy]["is_good"]:
                        self.good_proxies.append(proxy)
                        break

            self._save_proxy_status(status)

            if not self.good_proxies:
                logging.error("No good proxies found after testing.")
                return None

        return self.good_proxies[0]

    async def _test_and_save_status(self, proxy_url: str) -> Dict[str, Optional[float]]:
        """Test a proxy and return its status."""
        is_good, speed = await self._test_proxy(proxy_url)
        return {"is_good": is_good, "download_speed": speed}

    async def _test_proxy(self, proxy_url: str) -> Tuple[bool, Optional[float]]:
        """Test a proxy by downloading a YouTube video."""
        test_dir = self.work_dir / "proxy_test"
        test_dir.mkdir(parents=True, exist_ok=True)

        download_start_time = [None]
        bytes_downloaded = [0]

        def progress_hook(d):
            if d["status"] == "downloading":
                if download_start_time[0] is None:
                    download_start_time[0] = time.time()
                bytes_downloaded[0] = d.get("downloaded_bytes", 0)

        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "proxy": proxy_url,
            "format": "worst",
            "outtmpl": str(test_dir / "%(id)s.%(ext)s"),
            "progress_hooks": [progress_hook],
        }

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([f"https://www.youtube.com/watch?v={self.test_video_id}"])

            if download_start_time[0] is None:
                logging.warning(f"No download progress tracked for proxy {proxy_url}.")
                return False, None

            elapsed_time = time.time() - download_start_time[0]
            download_speed = (bytes_downloaded[0] / (1024 * 1024)) / elapsed_time  # Speed in MB/s

            if download_speed >= 1.0:
                logging.info(f"Proxy {proxy_url} passed with speed {download_speed:.2f} MB/s.")
                return True, download_speed
            else:
                logging.info(f"Proxy {proxy_url} failed due to low speed: {download_speed:.2f} MB/s.")
                return False, download_speed
        except Exception as e:
            logging.debug(f"Proxy {proxy_url} test failed: {e}")
            return False, None
        finally:
            for file in test_dir.glob("*"):
                try:
                    file.unlink()
                except Exception:
                    pass
            try:
                test_dir.rmdir()
            except Exception:
                pass

    async def download_with_proxy(
        self, video_id: str, ydl_opts: dict, storage_manager: YTBulkStorage
    ) -> Tuple[bool, Optional[str]]:
        """Attempt to download using a working proxy."""
        while True:
            proxy_url = await self.get_working_proxy()
            if not proxy_url:
                logging.error("No working proxies available.")
                return False, None

            ydl_opts["proxy"] = proxy_url

            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=True)
                    if not info:
                        raise Exception("Download failed")

                    if await storage_manager.finalize_video(info):
                        return True, proxy_url
            except Exception as e:
                error_msg = str(e).lower()
                if "not a bot" in error_msg or "403" in error_msg:
                    logging.info(f"Marking proxy {proxy_url} as bad: {e}")
                    self._mark_proxy_bad(proxy_url)
                else:
                    logging.error(f"Download error with proxy {proxy_url}: {e}")
                return False, None

    def _mark_proxy_bad(self, proxy_url: str):
        """Mark a proxy as bad in the status file."""
        status = self._load_proxy_status()
        status[proxy_url] = {"is_good": False, "download_speed": None}
        self._save_proxy_status(status)
        self.bad_proxies.append(proxy_url)
