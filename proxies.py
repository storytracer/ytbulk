# proxies.py
import json
import logging
import time
import random
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass
import requests
import yt_dlp
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        self.test_video_id = config.test_video
        self.max_concurrent_requests = max_concurrent_requests
        self.proxy_lock = Lock()

        self.status_file.parent.mkdir(parents=True, exist_ok=True)
        self.good_proxies: Set[str] = set()
        self.bad_proxies: Set[str] = set()
        self.untested_proxies: Set[str] = set()

        self.initialize_proxies()

    def initialize_proxies(self):
        """Load and test the initial set of proxies."""
        with self.proxy_lock:
            self.untested_proxies = set(self._load_proxy_list())
            status = self._load_proxy_status()

            # Initialize sets based on previous status
            for proxy, proxy_status in status.items():
                if proxy in self.untested_proxies:
                    self.untested_proxies.remove(proxy)
                    if proxy_status["is_good"]:
                        self.good_proxies.add(proxy)
                    else:
                        self.bad_proxies.add(proxy)

            # Test random proxies if more good ones are needed
            while len(self.good_proxies) < self.max_concurrent_requests and self.untested_proxies:
                proxy = random.choice(list(self.untested_proxies))
                self.untested_proxies.remove(proxy)
                
                status[proxy] = self._test_and_save_status(proxy)
                if status[proxy]["is_good"]:
                    self.good_proxies.add(proxy)

            # If we still don't have enough good proxies and have more to test, continue testing
            if len(self.good_proxies) < self.max_concurrent_requests and (self.untested_proxies or self.bad_proxies):
                return True  # Signal that we should continue testing
            return False  # Signal that we're done testing

    def _load_proxy_list(self) -> List[str]:
        """Fetch the proxy list from the remote URL."""
        response = requests.get(self.proxy_list_url)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch proxy list: {response.status_code}")
        return list(set(line.strip() for line in response.text.splitlines() if line.strip()))

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

    def get_working_proxy(self) -> Optional[str]:
        """Return a randomly selected good proxy, testing more if needed."""
        with self.proxy_lock:
            while not self.good_proxies and self.untested_proxies:
                logging.info("Testing more proxies...")
                status = self._load_proxy_status()

                # Try untested proxies
                proxy = random.choice(list(self.untested_proxies))
                self.untested_proxies.remove(proxy)

                status[proxy] = self._test_and_save_status(proxy)
                if status[proxy]["is_good"]:
                    self.good_proxies.add(proxy)

                self._save_proxy_status(status)

        with self.proxy_lock:
            if not self.good_proxies:
                logging.error("No working proxies available.")
                return None

            # Randomly select a good proxy
            return random.choice(list(self.good_proxies))

    def _test_and_save_status(self, proxy_url: str) -> Dict[str, Optional[float]]:
        """Test a proxy and return its status."""
        is_good, speed = self._test_proxy(proxy_url)
        return {"is_good": is_good, "download_speed": speed}

    def _test_proxy(self, proxy_url: str) -> Tuple[bool, Optional[float]]:
        """Test a proxy by downloading a YouTube video."""
        from tempfile import TemporaryDirectory

        download_start_time = [None]
        bytes_downloaded = [0]

        def progress_hook(d):
            if d["status"] == "downloading":
                if download_start_time[0] is None:
                    download_start_time[0] = time.time()
                bytes_downloaded[0] = d.get("downloaded_bytes", 0)

        with TemporaryDirectory() as temp_dir:
            ydl_opts = {
                "quiet": True,
                "no_warnings": True,
                "proxy": proxy_url,
                "format": "worst",
                "outtmpl": str(Path(temp_dir) / "%(id)s.%(ext)s"),
                "progress_hooks": [progress_hook],
            }

        with TemporaryDirectory() as temp_dir:
            ydl_opts["outtmpl"] = str(Path(temp_dir) / "%(id)s.%(ext)s")
            
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

    def download_with_proxy(
        self, video_id: str, ydl_opts: dict, storage_manager: YTBulkStorage
    ) -> Tuple[bool, Optional[str]]:
        """Attempt to download using a randomly selected working proxy."""
        while True:
            proxy_url = self.get_working_proxy()
            if not proxy_url:
                logging.error("No working proxies available.")
                return False, None

            ydl_opts["proxy"] = proxy_url

            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=True)
                    if not info:
                        raise Exception("Download failed")

                    if storage_manager.finalize_video(info):
                        return True, proxy_url
            except Exception as e:
                error_msg = str(e).lower()
                if any(error_pattern in error_msg for error_pattern in ("not a bot", "403")):
                    logging.info(f"Marking proxy {proxy_url} as bad: {e}")
                    self._mark_proxy_bad(proxy_url)
                else:
                    logging.error(f"Download error with proxy {proxy_url}: {e}")
                return False, None

    def _mark_proxy_bad(self, proxy_url: str):
        """Mark a proxy as bad and permanently remove it."""
        with self.proxy_lock:
            status = self._load_proxy_status()
            status[proxy_url] = {"is_good": False, "download_speed": None}
            self._save_proxy_status(status)
            self.good_proxies.discard(proxy_url)
            self.bad_proxies.add(proxy_url)
            # Remove from untested list to ensure it won't be retried
            self.untested_proxies.discard(proxy_url)