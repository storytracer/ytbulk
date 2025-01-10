import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
from pathlib import Path
import aiofiles
import yt_dlp
from yt_dlp.utils import ExtractorError

@dataclass
class ProxyStatus:
    """Proxy status tracking."""
    url: str
    last_used: datetime
    last_success: Optional[datetime] = None
    last_failure: Optional[datetime] = None
    consecutive_failures: int = 0
    is_blocked: bool = False
    cooldown_until: Optional[datetime] = None

class YTBulkProxyManager:
    def __init__(
        self,
        config: YTBulkConfig,
        work_dir: Path,
        max_consecutive_failures: int = 3,
        cooldown_minutes: int = 30
    ):
        self.proxy_file = proxy_file
        self.work_dir = work_dir
        self.max_consecutive_failures = max_consecutive_failures
        self.cooldown_duration = timedelta(minutes=cooldown_minutes)
        self.proxies: List[ProxyStatus] = []
        self.current_proxy_index = 0
        self.test_video_id = config.proxy_test_video

    async def load_proxies(self):
        """Load proxies from file."""
        async with aiofiles.open(self.proxy_file) as f:
            proxy_list = [line.strip() for line in await f.readlines()]
            self.proxies = [
                ProxyStatus(url=url, last_used=datetime.min)
                for url in proxy_list if url and not url.startswith('#')
            ]
        logging.info(f"Loaded {len(self.proxies)} proxies")

    async def check_proxy(self, proxy_url: str) -> Tuple[bool, Optional[str]]:
        """
        Check if proxy works with YouTube by attempting a download.
        Returns (success, error_type).
        """
        test_dir = self.work_dir / "proxy_test"
        test_dir.mkdir(parents=True, exist_ok=True)

        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'proxy': proxy_url,
            'format': 'worst',
            'outtmpl': str(test_dir / '%(id)s.%(ext)s'),
            'progress_hooks': [],
        }

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([f'https://www.youtube.com/watch?v={self.test_video_id}'])
                return True, None

        except ExtractorError as e:
            error_str = str(e.orig_msg).lower()
            
            # Check for specific blocking patterns
            if "http error 403: forbidden" in error_str or "unable to download video data: http error 403" in error_str:
                logging.warning(f"Proxy {proxy_url} blocked (HTTP 403)")
                return False, "blocked_403"

            if any(indicator in error_str for indicator in [
                "confirm you're not a bot",
                "sign in to confirm"
            ]):
                logging.warning(f"Proxy {proxy_url} blocked (bot detection)")
                return False, "blocked_bot"

            if "unable to download" in error_str and "403" not in error_str:
                logging.debug(f"Proxy {proxy_url} connection failed: {error_str}")
                return False, "connection_failed"

            logging.warning(f"Extraction error with proxy {proxy_url}: {error_str}")
            return False, "extractor_error"

        except Exception as e:
            logging.error(f"Unexpected error checking proxy {proxy_url}: {e}")
            return False, "error"

        finally:
            # Clean up test files
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

    def get_next_proxy(self) -> Optional[str]:
        """Get next available proxy."""
        start_idx = self.current_proxy_index
        
        while True:
            proxy = self.proxies[self.current_proxy_index]
            now = datetime.now()
            
            # Skip blocked proxies that haven't cooled down
            if proxy.is_blocked and proxy.cooldown_until:
                if now >= proxy.cooldown_until:
                    # Proxy has cooled down, verify it
                    success, error_type = asyncio.run(self.check_proxy(proxy.url))
                    if success:
                        proxy.is_blocked = False
                        proxy.consecutive_failures = 0
                        proxy.cooldown_until = None
                        return proxy.url
                    else:
                        # Still blocked, extend cooldown
                        proxy.cooldown_until = now + self.cooldown_duration
                
                # Move to next proxy
                self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxies)
                if self.current_proxy_index == start_idx:
                    return None
                continue
            
            return proxy.url

    def mark_proxy_success(self, proxy_url: str):
        """Mark a proxy as successfully used."""
        for proxy in self.proxies:
            if proxy.url == proxy_url:
                proxy.last_success = datetime.now()
                proxy.last_used = datetime.now()
                proxy.consecutive_failures = 0
                proxy.is_blocked = False
                proxy.cooldown_until = None
                break

    def mark_proxy_failure(self, proxy_url: str, error_type: str = None):
        """Mark a proxy as failed with specific error type."""
        for proxy in self.proxies:
            if proxy.url == proxy_url:
                now = datetime.now()
                proxy.last_failure = now
                proxy.last_used = now
                proxy.consecutive_failures += 1

                # Immediate block for certain error types
                if error_type in {'blocked_403', 'blocked_bot'}:
                    proxy.is_blocked = True
                    proxy.cooldown_until = now + self.cooldown_duration
                    logging.warning(f"Proxy {proxy_url} blocked due to {error_type}")
                    return

                # Progressive block for consecutive failures
                if proxy.consecutive_failures >= self.max_consecutive_failures:
                    proxy.is_blocked = True
                    proxy.cooldown_until = now + self.cooldown_duration
                    logging.warning(f"Proxy {proxy_url} blocked due to consecutive failures")
                break