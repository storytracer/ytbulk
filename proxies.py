import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
from pathlib import Path
import aiohttp
import aiofiles
import json
import yt_dlp
from yt_dlp.utils import ExtractorError

from config import YTBulkConfig

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
        self.work_dir = work_dir
        self.proxy_list_url = config.proxy_list_url
        self.proxy_state_file = work_dir / "cache" / "proxy_state.json"
        self.max_consecutive_failures = max_consecutive_failures
        self.cooldown_duration = timedelta(minutes=cooldown_minutes)
        self.proxies: List[ProxyStatus] = []
        self.current_proxy_index = 0
        self.test_video_id = config.proxy_test_video

    async def load_proxy_list(self) -> List[str]:
        """Download current proxy list from URL."""
        async with aiohttp.ClientSession() as session:
            async with session.get(self.proxy_list_url) as response:
                if response.status != 200:
                    raise Exception(f"Failed to download proxy list: {response.status}")
                content = await response.text()
                return [line.strip() for line in content.splitlines() if line.strip()]

    async def load_proxy_state(self):
        """Load proxy state from persistent storage."""
        if self.proxy_state_file.exists():
            async with aiofiles.open(self.proxy_state_file, 'r') as f:
                state_data = json.loads(await f.read())
                
                # Convert stored strings back to datetime objects
                for proxy_data in state_data:
                    if proxy_data.get('last_used'):
                        proxy_data['last_used'] = datetime.fromisoformat(proxy_data['last_used'])
                    if proxy_data.get('last_success'):
                        proxy_data['last_success'] = datetime.fromisoformat(proxy_data['last_success'])
                    if proxy_data.get('last_failure'):
                        proxy_data['last_failure'] = datetime.fromisoformat(proxy_data['last_failure'])
                    if proxy_data.get('cooldown_until'):
                        proxy_data['cooldown_until'] = datetime.fromisoformat(proxy_data['cooldown_until'])
                
                return [ProxyStatus(**data) for data in state_data]
        return []

    async def save_proxy_state(self):
        """Save current proxy state to persistent storage."""
        state_data = [
            {
                'url': p.url,
                'last_used': p.last_used.isoformat() if p.last_used else None,
                'last_success': p.last_success.isoformat() if p.last_success else None,
                'last_failure': p.last_failure.isoformat() if p.last_failure else None,
                'consecutive_failures': p.consecutive_failures,
                'is_blocked': p.is_blocked,
                'cooldown_until': p.cooldown_until.isoformat() if p.cooldown_until else None
            }
            for p in self.proxies
        ]
        
        async with aiofiles.open(self.proxy_state_file, 'w') as f:
            await f.write(json.dumps(state_data, indent=2))

    async def initialize(self):
        """Initialize proxy manager with fresh list and stored state."""
        # Load current proxy list
        proxy_urls = await self.load_proxy_list()
        
        # Load stored state
        stored_proxies = {p.url: p for p in await self.load_proxy_state()}
        
        # Create new proxy list combining stored state with current URLs
        self.proxies = []
        for url in proxy_urls:
            if url in stored_proxies:
                self.proxies.append(stored_proxies[url])
            else:
                self.proxies.append(ProxyStatus(url=url, last_used=datetime.min))
        
        logging.info(f"Initialized {len(self.proxies)} proxies")
        await self.save_proxy_state()

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
                asyncio.create_task(self.save_proxy_state())
                break

    def mark_proxy_failure(self, proxy_url: str, error_type: str = None):
        """Mark a proxy as failed with specific error type."""
        for proxy in self.proxies:
            if proxy.url == proxy_url:
                now = datetime.now()
                proxy.last_failure = now
                proxy.last_used = now
                proxy.consecutive_failures += 1

                if error_type in {'blocked_403', 'blocked_bot'}:
                    proxy.is_blocked = True
                    proxy.cooldown_until = now + self.cooldown_duration
                elif proxy.consecutive_failures >= self.max_consecutive_failures:
                    proxy.is_blocked = True
                    proxy.cooldown_until = now + self.cooldown_duration

                asyncio.create_task(self.save_proxy_state())
                break