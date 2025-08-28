import aiohttp
import asyncio
import logging
from typing import List, Callable
import re
from functools import lru_cache

logger = logging.getLogger(__name__)

class SourceFetcher:
    """订阅源获取器（带大小检查和智能重试）"""
    
    def __init__(self, timeout: float, concurrency: int, retries: int = 2, config=None):
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.semaphore = asyncio.Semaphore(concurrency)
        self.retries = retries
        self.config = config or {}
        self.common_encodings = ['utf-8', 'gbk', 'latin-1']
        self.max_size = int(self.config.get('FETCHER', 'max_source_size', fallback=50 * 1024 * 1024))

    async def fetch_all(self, urls: List[str], progress_cb: Callable) -> List[str]:
        """批量获取订阅源（带并发控制）"""
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            tasks = [self._fetch_with_retry(session, url, progress_cb) for url in urls]
            return await asyncio.gather(*tasks)

    async def _fetch_with_retry(self, session: aiohttp.ClientSession, url: str, progress_cb: Callable) -> str:
        """带重试机制的请求处理"""
        for attempt in range(self.retries + 1):
            try:
                result = await self._fetch(session, url)
                progress_cb()
                return result
            except Exception as e:
                logger.warning(f"Attempt {attempt+1}/{self.retries+1} failed: {url} - {str(e)}")
                if attempt == self.retries:
                    return ""
                await asyncio.sleep(1 + attempt)
            finally:
                progress_cb()

    async def _fetch(self, session: aiohttp.ClientSession, url: str) -> str:
        """执行单次请求（带大小检查）"""
        async with self.semaphore:
            headers = {'User-Agent': 'Mozilla/5.0'}
            async with session.get(url, headers=headers) as resp:
                # 检查状态码
                if resp.status != 200:
                    raise ValueError(f"HTTP status {resp.status}")
                
                # 处理内容编码
                raw_content = await resp.read()
                
                # 检查实际下载大小
                if len(raw_content) > self.max_size:
                    raise ValueError(
                        f"Content too large ({len(raw_content)/1024/1024:.1f}MB > {self.max_size/1024/1024:.1f}MB)"
                    )
                
                encoding = self._detect_encoding(resp.headers.get('Content-Type', ''), raw_content)
                return raw_content.decode(encoding, errors='replace')

    @lru_cache(maxsize=128)
    def _detect_encoding(self, content_type: str, raw_content: bytes) -> str:
        """检测内容编码（带缓存）"""
        if 'charset=' in content_type:
            if match := re.search(r'charset=([\w-]+)', content_type, re.IGNORECASE):
                return match.group(1).lower()
        
        for enc in self.common_encodings:
            try:
                raw_content.decode(enc)
                return enc
            except UnicodeDecodeError:
                continue
        
        return 'utf-8'