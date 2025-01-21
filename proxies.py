# proxies.py
import json
import logging
import random
import time
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Dict, Set
from threading import Lock
from tempfile import TemporaryDirectory
from enum import Enum
from tqdm import tqdm
import requests
import yt_dlp

from config import YTBulkConfig
from storage import YTBulkStorage

class ProxyState(str, Enum):
    """Proxy operational states."""
    UNTESTED = "untested"
    VERIFIED = "verified"  # Working with good speed
    WORKING = "working"    # Working but below speed threshold
    FAILED = "failed"      # Not working

@dataclass
class ProxyStatus:
    """Status and performance metrics for a proxy."""
    url: str
    state: ProxyState = ProxyState.UNTESTED
    download_speed: Optional[float] = None  # Speed in MB/s
    last_tested: float = 0.0

class YTBulkProxyManager:
    """Manages a pool of proxies with persistent status tracking."""
    
    def __init__(self, config: YTBulkConfig, work_dir: Path, max_concurrent_requests: int):
        self.config = config
        self.status_file = work_dir / "cache" / "proxies.json"
        self.status_file.parent.mkdir(parents=True, exist_ok=True)
        self.max_concurrent = max_concurrent_requests
        self.proxy_lock = Lock()
        
        # Single source of truth for proxy status
        self.status_cache: Dict[str, ProxyStatus] = {}
        
        # Start proxy initialization
        self._initialize_proxies()

    def _load_status_file(self) -> Dict[str, Dict]:
        """Load proxy status from file with error handling."""
        try:
            if self.status_file.exists():
                with open(self.status_file) as f:
                    return json.load(f)
        except Exception as e:
            logging.error(f"Error loading proxy status file: {e}")
        return {}

    def _save_status_file(self) -> None:
        """Save proxy status to file with error handling."""
        try:
            status_dict = {
                proxy: {
                    "state": status.state.value,
                    "download_speed": status.download_speed,
                    "last_tested": status.last_tested
                }
                for proxy, status in self.status_cache.items()
            }
            with open(self.status_file, "w") as f:
                json.dump(status_dict, f, indent=2)
        except Exception as e:
            logging.error(f"Error saving proxy status file: {e}")

    def _fetch_proxy_list(self) -> Set[str]:
        """Fetch and validate current proxy list."""
        try:
            response = requests.get(self.config.proxy_list_url)
            response.raise_for_status()
            return {line.strip() for line in response.text.splitlines() if line.strip()}
        except Exception as e:
            logging.error(f"Error fetching proxy list: {e}")
            return set()

    def _test_proxy(self, proxy_url: str) -> ProxyStatus:
        """Test proxy performance and reliability using isolated temp directory."""
        status = ProxyStatus(url=proxy_url, last_tested=time.time())
        
        def progress_hook(d):
            if d["status"] == "downloading":
                if not hasattr(progress_hook, "start_time"):
                    progress_hook.start_time = time.time()
                    progress_hook.bytes_downloaded = 0
                progress_hook.bytes_downloaded = d.get("downloaded_bytes", 0)

        with TemporaryDirectory() as temp_dir:
            ydl_opts = {
                "quiet": True,
                "no_warnings": True,
                "proxy": proxy_url,
                "format": "worst",
                "progress_hooks": [progress_hook],
                "outtmpl": str(Path(temp_dir) / "%(id)s.%(ext)s"),
                "noprogress": True
            }

            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([f"https://www.youtube.com/watch?v={self.config.test_video}"])

                if hasattr(progress_hook, "start_time"):
                    elapsed = time.time() - progress_hook.start_time
                    status.download_speed = (progress_hook.bytes_downloaded / (1024 * 1024)) / elapsed
                    if status.download_speed >= self.config.proxy_min_speed:
                        status.state = ProxyState.VERIFIED
                    else:
                        status.state = ProxyState.WORKING
                return status

            except Exception as e:
                logging.debug(f"Proxy test failed - {proxy_url}: {e}")
                status.state = ProxyState.FAILED
                return status

    def _initialize_proxies(self) -> None:
        """Initialize and test proxies with persistent state management."""
        with self.proxy_lock:
            # Load current state
            current_proxies = self._fetch_proxy_list()
            saved_status = self._load_status_file()
            
            # Load previous states for ordering
            proxy_states = {}
            for proxy, data in saved_status.items():
                if proxy in current_proxies:
                    proxy_states[proxy] = ProxyState(data.get("state", ProxyState.UNTESTED.value))
            
            # Mark any new proxies as untested
            for proxy in current_proxies:
                if proxy not in proxy_states:
                    proxy_states[proxy] = ProxyState.UNTESTED

            # Order proxies for testing based on previous state
            to_test = [
                proxy for proxy in current_proxies
                if proxy_states[proxy] == ProxyState.WORKING
            ]
            
            untested = [
                proxy for proxy in current_proxies
                if proxy_states[proxy] == ProxyState.UNTESTED
            ]
            random.shuffle(untested)
            to_test.extend(untested)
            
            to_test.extend(
                proxy for proxy in current_proxies
                if proxy_states[proxy] == ProxyState.VERIFIED
            )
            to_test.extend(
                proxy for proxy in current_proxies
                if proxy_states[proxy] == ProxyState.FAILED
            )

        # Test proxies sequentially
        if to_test:
            # Track only newly tested proxies in progress bar
            state_counts = {state: 0 for state in ProxyState}

            def get_description():
                return f"Testing [V:{state_counts[ProxyState.VERIFIED]} W:{state_counts[ProxyState.WORKING]} F:{state_counts[ProxyState.FAILED]}]"

            pbar = tqdm(
                total=len(to_test),
                initial=0,
                desc=get_description(),
                ncols=75,
            )

            for proxy in to_test:
                try:
                    status = self._test_proxy(proxy)
                    with self.proxy_lock:
                        self.status_cache[proxy] = status
                        state_counts[status.state] += 1
                        self._save_status_file()
                        pbar.set_description_str(get_description())
                        pbar.update(1)
                        
                        if self._count_usable_proxies() >= self.max_concurrent:
                            break
                except Exception as e:
                    logging.error(f"Error testing proxy {proxy}: {e}")
                    state_counts[ProxyState.FAILED] += 1
                    pbar.set_description_str(get_description())
                    pbar.update(1)

            pbar.close()

    def _count_usable_proxies(self) -> int:
        """Count proxies that are either verified or working."""
        return sum(
            1 for status in self.status_cache.values()
            if status.state in (ProxyState.VERIFIED, ProxyState.WORKING)
        )

    def get_working_proxy(self) -> Optional[str]:
        """Get a verified proxy if available, otherwise a working proxy."""
        with self.proxy_lock:
            if self._count_usable_proxies() == 0 and (self.status_cache or self._fetch_proxy_list()):
                self._initialize_proxies()
            
            # Try verified proxies first
            for proxy, status in self.status_cache.items():
                if status.state == ProxyState.VERIFIED:
                    return proxy
            
            # Fall back to working proxies
            for proxy, status in self.status_cache.items():
                if status.state == ProxyState.WORKING:
                    return proxy
            
            return None

    def _mark_proxy_failed(self, proxy_url: str) -> None:
        """Mark a proxy as failed and update persistent state."""
        with self.proxy_lock:
            if proxy_url in self.status_cache:
                status = self.status_cache[proxy_url]
                status.state = ProxyState.FAILED
                status.last_tested = time.time()
                self._save_status_file()

    def download_with_proxy(self, video_id: str, ydl_opts: dict, storage_manager: YTBulkStorage) -> bool:
        """Download video using proxy with automatic failover."""
        while True:
            proxy_url = self.get_working_proxy()
            if not proxy_url:
                logging.error("No working proxies available")
                return False

            ydl_opts["proxy"] = proxy_url
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=True)
                    if info and storage_manager.finalize_video(info):
                        return True
                    raise Exception("Download or finalization failed")
            except Exception as e:
                self._mark_proxy_failed(proxy_url)
                if self._count_usable_proxies() == 0:
                    logging.error("All proxies exhausted")
                    return False