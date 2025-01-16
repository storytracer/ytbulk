import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, Optional, List, Tuple
from pathlib import Path
import aiohttp
import json
import yt_dlp
from yt_dlp.utils import ExtractorError

from config import YTBulkConfig
from storage import YTBulkStorage

@dataclass
class ProxyStatus:
    """Simple proxy status tracking."""
    url: str
    is_good: bool = False

class YTBulkProxyManager:
    def __init__(
        self,
        config: YTBulkConfig,
        work_dir: Path
    ):
        self.work_dir = work_dir
        self.proxy_list_url = config.proxy_list_url
        self.status_file = work_dir / "cache" / "proxies.json"
        self.status_file.parent.mkdir(parents=True, exist_ok=True)
        self.test_video_id = config.proxy_test_video
        self.current_good_proxy: Optional[str] = None

    async def load_proxy_list(self) -> List[str]:
        """Download current proxy list from URL."""
        async with aiohttp.ClientSession() as session:
            async with session.get(self.proxy_list_url) as response:
                if response.status != 200:
                    raise Exception(f"Failed to download proxy list: {response.status}")
                content = await response.text()
                return [line.strip() for line in content.splitlines() if line.strip()]

    def load_proxy_status(self) -> Dict[str, bool]:
        """Load proxy status from JSON file."""
        if self.status_file.exists():
            with open(self.status_file) as f:
                return json.load(f)
        return {}

    def save_proxy_status(self, status: Dict[str, bool]):
        """Save proxy status to JSON file."""
        with open(self.status_file, 'w') as f:
            json.dump(status, f, indent=2)

    async def test_proxy(self, proxy_url: str) -> bool:
        """Test if proxy works with YouTube."""
        test_dir = self.work_dir / "proxy_test"
        test_dir.mkdir(parents=True, exist_ok=True)

        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'proxy': proxy_url,
            'format': 'worst',
            'outtmpl': str(test_dir / '%(id)s.%(ext)s'),
        }

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([f'https://www.youtube.com/watch?v={self.test_video_id}'])
                return True

        except Exception as e:
            logging.debug(f"Proxy test failed for {proxy_url}: {str(e)}")
            return False

        finally:
            if test_dir.exists():
                for file in test_dir.glob('*'):
                    try:
                        file.unlink()
                    except Exception:
                        pass
                try:
                    test_dir.rmdir()
                except Exception:
                    pass

    async def download_with_proxy(
        self, 
        video_id: str, 
        ydl_opts: dict,
        storage_manager: YTBulkStorage
    ) -> Tuple[bool, Optional[str]]:
        """
        Attempt to download using a working proxy.
        Returns (success, proxy_url used).
        """
        while True:
            proxy_url = await self.get_working_proxy()
            if not proxy_url:
                logging.error("No working proxies available")
                return False, None

            # Add proxy to options
            ydl_opts['proxy'] = proxy_url

            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    # Try the download
                    info = ydl.extract_info(f'https://www.youtube.com/watch?v={video_id}', download=True)
                    if not info:
                        raise Exception("Download failed")
                    
                    # Finalize the video if download was successful
                    if await storage_manager.finalize_video(info):
                        return True, proxy_url
                    else:
                        return False, proxy_url

            except yt_dlp.utils.ExtractorError as e:
                error_str = str(e).lower()
                if "sign in to confirm" in error_str:
                    self.mark_proxy_bad(proxy_url)
                else:
                    # Other extractor errors might not be proxy-related
                    return False, proxy_url

            except Exception as e:
                logging.error(f"Download error: {e}")
                return False, proxy_url

    async def get_working_proxy(self) -> Optional[str]:
        """Get a working proxy to use."""
        # If we have a current good proxy, use it
        if self.current_good_proxy:
            return self.current_good_proxy

        # Load current status
        status = self.load_proxy_status()
        
        # Try previously good proxies first
        good_proxies = [url for url, is_good in status.items() if is_good]
        for proxy in good_proxies:
            if await self.test_proxy(proxy):
                self.current_good_proxy = proxy
                return proxy
            else:
                status[proxy] = False

        # Try proxies from the proxy list
        proxy_list = await self.load_proxy_list()
        for proxy in proxy_list:
            if proxy not in status and await self.test_proxy(proxy):
                status[proxy] = True
                self.current_good_proxy = proxy
                self.save_proxy_status(status)
                return proxy
            status[proxy] = False

        # No working proxy found
        self.save_proxy_status(status)
        return None

    def mark_proxy_bad(self, proxy_url: str):
        """Mark a proxy as bad in the status file."""
        if proxy_url == self.current_good_proxy:
            self.current_good_proxy = None
        
        status = self.load_proxy_status()
        status[proxy_url] = False
        self.save_proxy_status(status)