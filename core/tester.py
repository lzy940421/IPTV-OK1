import asyncio
import aiohttp
import time
import logging
from typing import List, Set, Tuple, Optional, Callable
from configparser import ConfigParser
from pathlib import Path
from .models import Channel

logger = logging.getLogger(__name__)

class SpeedTester:
    """HTTP测速引擎（完整支持失败URL记录）"""

    def __init__(self, 
                 timeout: float, 
                 concurrency: int, 
                 max_attempts: int,
                 min_download_speed: float, 
                 enable_logging: bool = True,
                 config: Optional[ConfigParser] = None):
        self.timeout = timeout
        self.concurrency = max(1, concurrency)
        self.max_attempts = max(1, max_attempts)
        self.min_download_speed = min_download_speed
        self._enable_logging = enable_logging
        self.config = config or ConfigParser()
        self.max_latency = self.config.getfloat('TESTER', 'max_latency', fallback=1000.0)
        self._init_logger()
        self.semaphore = asyncio.BoundedSemaphore(self.concurrency)
        self.success_count = 0
        self.total_count = 0
        self.start_time = 0.0

    def _init_logger(self):
        self.logger = logging.getLogger('core.tester')
        self.logger.disabled = not self._enable_logging
        self.log = self._create_log_method()

    def _create_log_method(self):
        def make_log_method(level):
            def log_method(msg, *args, **kwargs):
                if self._enable_logging:
                    getattr(self.logger, level)(msg, *args, **kwargs)
            return log_method
        
        return type('LogMethod', (), {
            'debug': make_log_method('debug'),
            'info': make_log_method('info'),
            'warning': make_log_method('warning'),
            'error': make_log_method('error'),
            'exception': make_log_method('exception')
        })

    async def test_channels(self, 
                          channels: List[Channel], 
                          progress_cb: Callable,
                          failed_urls: Set[str], 
                          white_list: Set[str]) -> None:
        """批量测试HTTP频道（自动记录失败URL）"""
        self.total_count = len(channels)
        self.success_count = 0
        self.start_time = time.time()
        
        self.log.info(
            "▶️ 开始测速 | 总数: %d | 并发: %d | 延迟阈值: %.0fms | 最低速度: %.1fKB/s",
            self.total_count, self.concurrency, self.max_latency, self.min_download_speed
        )

        connector = aiohttp.TCPConnector(
            limit=self.concurrency,
            force_close=False,
            enable_cleanup_closed=True,
            ssl=False
        )

        try:
            async with aiohttp.ClientSession(
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=self.timeout)
            ) as session:
                tasks = [
                    self._test_single_channel(session, channel, progress_cb, failed_urls, white_list)
                    for channel in channels
                ]
                await asyncio.gather(*tasks)
        except Exception as e:
            self.log.error("测速过程中发生错误: %s", str(e))
            raise
        finally:
            await connector.close()
            
            # 保存失败URL到配置文件指定路径
            if failed_urls:
                failed_path = Path(self.config.get(
                    'PATHS', 
                    'failed_urls_path', 
                    fallback='config/failed_urls.txt'
                ))
                failed_path.parent.mkdir(parents=True, exist_ok=True)
                
                with open(failed_path, 'w', encoding='utf-8') as f:
                    f.writelines(f"{url}\n" for url in failed_urls)
                
                self.log.info(f"失败URL已保存到: {failed_path.absolute()}")

        elapsed = time.time() - self.start_time
        success_rate = (self.success_count / self.total_count) * 100 if self.total_count > 0 else 0
        self.log.info(
            "✅ 测速完成 | 成功: %d(%.1f%%) | 失败: %d | 用时: %.1fs",
            self.success_count, success_rate,
            self.total_count - self.success_count,
            elapsed
        )

    async def _test_single_channel(self,
                                 session: aiohttp.ClientSession,
                                 channel: Channel,
                                 progress_cb: Callable,
                                 failed_urls: Set[str],
                                 white_list: Set[str]) -> None:
        async with self.semaphore:
            if self._is_in_white_list(channel, white_list):
                channel.status = 'online'
                self.log.debug("🟢 白名单跳过 %s", channel.name)
                progress_cb()
                return

            for attempt in range(1, self.max_attempts + 1):
                try:
                    success, speed, latency = await self._unified_test(session, channel)
                    if success:
                        self._handle_success(channel, speed, latency)
                        break
                    elif attempt == self.max_attempts:
                        self._handle_failure(channel, failed_urls, speed, latency)
                except Exception as e:
                    if attempt == self.max_attempts:
                        self._handle_error(channel, failed_urls, e)
                finally:
                    if attempt == self.max_attempts:
                        progress_cb()

    async def _unified_test(self,
                          session: aiohttp.ClientSession,
                          channel: Channel) -> Tuple[bool, float, float]:
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            
            # 测延迟
            latency_start = time.perf_counter()
            async with session.head(channel.url, headers=headers, timeout=self.timeout) as resp:
                latency = (time.perf_counter() - latency_start) * 1000
                if latency > self.max_latency or resp.status != 200:
                    return False, 0.0, latency

            # 测速度
            start = time.perf_counter()
            content_size = 0
            async with session.get(channel.url, headers=headers, timeout=self.timeout) as resp:
                async for chunk in resp.content.iter_chunked(1024 * 4):
                    content_size += len(chunk)
                
                duration = time.perf_counter() - start
                speed = content_size / duration / 1024 if duration > 0 else 0
                return speed >= self.min_download_speed, speed, latency

        except (asyncio.TimeoutError, aiohttp.ClientError):
            return False, 0.0, 0.0
        except Exception as e:
            self.log.error("测试错误 %s: %s", channel.url, str(e)[:100])
            return False, 0.0, 0.0

    def _handle_success(self, channel: Channel, speed: float, latency: float):
        self.success_count += 1
        channel.status = 'online'
        channel.response_time = latency
        channel.download_speed = speed
        self.log.info(
            "✅ 成功 | %-30s | %6.1fKB/s | %4.0fms | %s",
            channel.name[:30], speed, latency, self._simplify_url(channel.url)
        )

    def _handle_failure(self, channel: Channel, failed_urls: Set[str], speed: float, latency: float):
        failed_urls.add(channel.url)
        channel.status = 'offline'
        reason = "速度不足" if speed > 0 else "延迟过高" if latency > 0 else "连接失败"
        self.log.warning(
            "❌ 失败 | %-30s | %6.1fKB/s | %4.0fms | %-8s | %s",
            channel.name[:30], speed, latency, reason, self._simplify_url(channel.url)
        )

    def _handle_error(self, channel: Channel, failed_urls: Set[str], error: Exception):
        failed_urls.add(channel.url)
        channel.status = 'offline'
        self.log.error(
            "‼️ 异常 | %-30s | %-20s | %s",
            channel.name[:30], str(error)[:20], self._simplify_url(channel.url)
        )

    def _simplify_url(self, url: str) -> str:
        return url[:100] + '...' if len(url) > 100 else url

    def _is_in_white_list(self, channel: Channel, white_list: Set[str]) -> bool:
        return channel.name.lower() in white_list if white_list else False