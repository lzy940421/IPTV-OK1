import asyncio
import aiohttp
import time
import re
import logging
from typing import List, Set, Tuple, Optional, Dict, Callable
from collections import defaultdict
from urllib.parse import urlparse
from configparser import ConfigParser
from .models import Channel

logger = logging.getLogger(__name__)

class SpeedTester:
    """高性能流媒体测速引擎（完整优化版）"""

    def __init__(self, 
                 timeout: float, 
                 concurrency: int, 
                 max_attempts: int,
                 min_download_speed: float, 
                 enable_logging: bool = True,
                 config: Optional[ConfigParser] = None):
        """
        初始化测速器
        
        参数:
            timeout: 基础超时时间(秒)
            concurrency: 最大并发数
            max_attempts: 最大重试次数
            min_download_speed: HTTP最低速度要求(KB/s)
            enable_logging: 是否启用日志
            config: 配置对象
        """
        # 基础配置
        self.timeout = timeout
        self.concurrency = max(1, concurrency)
        self.max_attempts = max(1, max_attempts)
        self.min_download_speed = max(0.1, min_download_speed)
        self._enable_logging = enable_logging
        self.config = config or ConfigParser()

        # 新增下载大小限制配置
        self.max_download_size = self.config.getint(
            'TESTER', 
            'max_download_size', 
            fallback=100 * 1024  # 默认100KB
        )
        
        # 初始化日志系统
        self._init_logger()

        # 协议检测
        self.rtp_udp_pattern = re.compile(r'/(rtp|udp)/', re.IGNORECASE)
        
        # 协议特定配置
        self.udp_timeout = self.config.getfloat('TESTER', 'udp_timeout', fallback=max(0.5, timeout * 0.3))
        self.http_timeout = self.config.getfloat('TESTER', 'http_timeout', fallback=timeout)
        self.min_udp_download_speed = self.config.getfloat('TESTER', 'min_udp_download_speed', fallback=30)
        self.max_udp_latency = self.config.getint('TESTER', 'max_udp_latency', fallback=300)
        self.max_http_latency = self.config.getint('TESTER', 'max_http_latency', fallback=1000)
        self.max_channels_per_ip = self.config.getint('TESTER', 'max_channels_per_ip', fallback=100)
        
        # IP防护机制
        self.failed_ips: Dict[str, int] = defaultdict(int)
        self.max_failures_per_ip = self.config.getint('PROTECTION', 'max_failures_per_ip', fallback=5)
        self.blocked_ips: Set[str] = set()
        self.ip_cooldown: Dict[str, float] = {}  # IP冷却时间记录
        self.min_ip_interval = self.config.getfloat('PROTECTION', 'min_ip_interval', fallback=0.5)
        
        # 并发控制
        self.semaphore = asyncio.BoundedSemaphore(self.concurrency)
        
        # 统计
        self.success_count = 0
        self.total_count = 0
        self.start_time = 0.0

    def _init_logger(self):
        """初始化日志记录器"""
        self.logger = logging.getLogger('core.tester')
        self.logger.disabled = not self._enable_logging
        
        # 创建安全的日志方法
        self.log = self._create_log_method()

    def _create_log_method(self):
        """创建带开关的日志方法"""
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
        """
        批量测试频道（安全版本）
        
        参数:
            channels: 频道列表
            progress_cb: 进度回调函数
            failed_urls: 存储失败URL的集合
            white_list: 白名单集合
        """
        self.total_count = len(channels)
        self.success_count = 0
        self.start_time = time.time()
        
        self.log.info(
            "▶️ 开始测速 | 总数: %d | 并发: %d | 单IP最大频道: %d | 最大下载量: %dKB",
            self.total_count, self.concurrency, self.max_channels_per_ip,
            self.max_download_size // 1024
        )

        # 智能IP分组
        ip_groups = self._group_channels_by_ip(channels, white_list)
        
        self.log.info(
            "📊 IP分组完成 | 总组数: %d | 最大组: %d | 平均组: %.1f",
            len(ip_groups), max(len(g) for g in ip_groups.values()), 
            sum(len(g) for g in ip_groups.values())/len(ip_groups)
        )

        # 创建自定义connector
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
                # 动态批处理
                batch_size = self._calculate_batch_size(len(ip_groups))
                tasks = []
                
                for ip, group in ip_groups.items():
                    if ip not in self.blocked_ips:
                        task = self._process_ip_group(
                            session, ip, group, progress_cb, failed_urls, white_list)
                        tasks.append(task)
                        
                        if len(tasks) >= batch_size:
                            await self._safe_gather(tasks)
                            progress_cb(len(tasks))
                            tasks = []
                
                if tasks:
                    await self._safe_gather(tasks)
                    progress_cb(len(tasks))
        except Exception as e:
            self.log.error("测试过程中发生错误: %s", str(e))
            if "_abort" not in str(e):
                raise
        finally:
            await connector.close()
        
        elapsed = time.time() - self.start_time
        success_rate = (self.success_count / self.total_count) * 100 if self.total_count > 0 else 0
        self.log.info(
            "✅ 测速完成 | 成功: %d(%.1f%%) | 失败: %d | 屏蔽IP: %d | 用时: %.1fs",
            self.success_count, success_rate,
            self.total_count - self.success_count,
            len(self.blocked_ips),
            elapsed
        )

    async def _safe_gather(self, tasks):
        """安全执行gather操作"""
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            self.log.warning("批处理任务执行异常: %s", str(e))

    def _calculate_batch_size(self, total_groups: int) -> int:
        """动态计算批处理大小"""
        if total_groups <= 100:
            return total_groups
        elif total_groups <= 1000:
            return 100
        return min(500, max(50, total_groups // 20))

    def _group_channels_by_ip(self, 
                            channels: List[Channel],
                            white_list: Set[str]) -> Dict[str, List[Channel]]:
        """
        改进版IP分组逻辑
        返回: { "ip_0": [ch1,ch2...], "ip_1": [...] }
        """
        groups = defaultdict(list)
        ip_counter = defaultdict(int)
        
        # 白名单独立分组
        whitelist_group = [ch for ch in channels if self._is_in_white_list(ch, white_list)]
        if whitelist_group:
            groups["whitelist"] = whitelist_group
        
        # 常规分组
        for ch in channels:
            if self._is_in_white_list(ch, white_list):
                continue
                
            ip = self._extract_ip_from_url(ch.url)
            group_idx = ip_counter[ip] // self.max_channels_per_ip
            group_key = f"{ip}_{group_idx}"
            
            groups[group_key].append(ch)
            ip_counter[ip] += 1
            
            self.log.debug("IP %s 频道数超过 %d，创建分组 %s", 
                         ip, self.max_channels_per_ip, group_key)
        
        return groups

    async def _process_ip_group(self, 
                              session: aiohttp.ClientSession,
                              ip: str, 
                              channels: List[Channel],
                              progress_cb: Callable,
                              failed_urls: Set[str],
                              white_list: Set[str]) -> None:
        """处理IP分组（带动态冷却）"""
        # 动态冷却计算
        current_time = time.time()
        if ip in self.ip_cooldown:
            elapsed = current_time - self.ip_cooldown[ip]
            cool_down = max(0, self.min_ip_interval * (1 + len(channels)/self.max_channels_per_ip) - elapsed)
            if cool_down > 0:
                self.log.debug("⏳ IP %s 冷却中 (%.2fs)", ip, cool_down)
                await asyncio.sleep(cool_down)
        
        try:
            # 动态调整组内并发
            group_concurrency = max(1, min(
                self.concurrency,
                self.concurrency // (len(channels) // self.max_channels_per_ip + 1)
            ))
            group_semaphore = asyncio.BoundedSemaphore(group_concurrency)
            
            async def process_channel(channel):
                async with group_semaphore:
                    await self._test_single_channel(
                        session, channel, progress_cb, failed_urls, white_list)
            
            await self._safe_gather([process_channel(ch) for ch in channels])
            
            # 成功则重置失败计数
            if ip in self.failed_ips:
                del self.failed_ips[ip]
                
        except Exception as e:
            self.failed_ips[ip] += 1
            self.log.error("❌ IP组 %s 测试异常: %s", ip, str(e))
            
            if self.failed_ips[ip] >= self.max_failures_per_ip:
                self.blocked_ips.add(ip)
                self.log.warning("🛑 屏蔽IP %s (连续失败 %d 次)", ip, self.failed_ips[ip])
        finally:
            self.ip_cooldown[ip] = time.time()

    async def _test_single_channel(self,
                                 session: aiohttp.ClientSession,
                                 channel: Channel,
                                 progress_cb: Callable,
                                 failed_urls: Set[str],
                                 white_list: Set[str]) -> None:
        """测试单个频道"""
        if self._is_in_white_list(channel, white_list):
            channel.status = 'online'
            self.log.debug("🟢 白名单跳过 %s", channel.name)
            progress_cb()
            return

        async with self.semaphore:
            try:
                self.log.debug("🔍 开始测试 %s", channel.name)

                success, speed, latency = await self._unified_test(session, channel)
                
                if success:
                    self._handle_success(channel, speed, latency)
                else:
                    self._handle_failure(channel, failed_urls, speed, latency)
                    
            except Exception as e:
                self._handle_error(channel, failed_urls, e)
            finally:
                progress_cb()

    async def _unified_test(self,
                          session: aiohttp.ClientSession,
                          channel: Channel) -> Tuple[bool, float, float]:
        """统一测试方法（支持UDP/HTTP协议）"""
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            is_udp = self._is_udp_url(channel.url)
            timeout_val = self.udp_timeout if is_udp else self.http_timeout
            timeout = aiohttp.ClientTimeout(total=timeout_val)
            
            # 协议阈值
            min_speed = self.min_udp_download_speed if is_udp else self.min_download_speed
            max_latency = self.max_udp_latency if is_udp else self.max_http_latency

            # 阶段1：快速HEAD请求测延迟
            latency_start = time.perf_counter()
            async with session.head(channel.url, headers=headers, timeout=timeout) as resp:
                latency = (time.perf_counter() - latency_start) * 1000
                if latency > max_latency or resp.status != 200:
                    return False, 0.0, latency

            # 阶段2：GET请求测速度（复用连接）
            start = time.perf_counter()
            content_size = 0
            
            # 使用iter_chunked分块读取，避免一次性加载大文件
            async with session.get(channel.url, headers=headers, timeout=timeout) as resp:
                async for chunk in resp.content.iter_chunked(1024 * 4):  # 4KB chunks
                    content_size += len(chunk)
                    # 达到最大下载量时提前结束
                    if content_size >= self.max_download_size:
                        break
                
                duration = time.perf_counter() - start
                speed = content_size / duration / 1024 if duration > 0 else 0
                return speed >= min_speed, speed, latency

        except asyncio.TimeoutError:
            return False, 0.0, 0.0
        except aiohttp.ClientError as e:
            return False, 0.0, 0.0
        except Exception as e:
            self.log.error("测试错误 %s: %s", channel.url, str(e)[:100])
            return False, 0.0, 0.0

    def _handle_success(self,
                      channel: Channel,
                      speed: float,
                      latency: float) -> None:
        """处理成功结果"""
        self.success_count += 1
        channel.status = 'online'
        channel.response_time = latency
        channel.download_speed = speed
        
        protocol = "UDP" if self._is_udp_url(channel.url) else "HTTP"
        self.log.info(
            "✅ 成功 | %-5s | %-30s | %6.1fKB/s | %4.0fms | %s",
            protocol, channel.name[:30], speed, latency,
            self._simplify_url(channel.url)
        )

    def _handle_failure(self,
                       channel: Channel,
                       failed_urls: Set[str],
                       speed: float,
                       latency: float) -> None:
        """处理失败结果"""
        failed_urls.add(channel.url)
        channel.status = 'offline'
        ip = self._extract_ip_from_url(channel.url)
        self.failed_ips[ip] += 1
        
        is_udp = self._is_udp_url(channel.url)
        reason = (
            "速度不足" if speed > 0 and speed < (
                self.min_udp_download_speed if is_udp else self.min_download_speed
            ) else
            "延迟过高" if latency > (
                self.max_udp_latency if is_udp else self.max_http_latency
            ) else
            "连接失败"
        )
        
        self.log.warning(
            "❌ 失败 | %-5s | %-30s | %6.1fKB/s | %4.0fms | %-8s | %s",
            "UDP" if is_udp else "HTTP",
            channel.name[:30], speed, latency, reason,
            self._simplify_url(channel.url)
        )

    def _handle_error(self,
                     channel: Channel,
                     failed_urls: Set[str],
                     error: Exception) -> None:
        """处理异常"""
        failed_urls.add(channel.url)
        channel.status = 'offline'
        ip = self._extract_ip_from_url(channel.url)
        self.failed_ips[ip] += 1
        
        self.log.error(
            "‼️ 异常 | %-30s | %-20s | %s",
            channel.name[:30], str(error)[:20],
            self._simplify_url(channel.url)
        )

    def _is_udp_url(self, url: str) -> bool:
        """判断是否为UDP协议URL"""
        url_lower = url.lower()
        return (url_lower.startswith(('udp://', 'rtp://')) or 
                bool(self.rtp_udp_pattern.search(url_lower)))

    def _extract_ip_from_url(self, url: str) -> str:
        """从URL提取IP地址"""
        try:
            parsed = urlparse(url)
            netloc = parsed.netloc.split('@')[-1]
            if '[' in netloc and ']' in netloc:  # IPv6
                return netloc.split(']')[0] + ']'
            return netloc.split(':')[0]  # IPv4
        except:
            return "unknown"

    def _simplify_url(self, url: str) -> str:
        """简化URL显示"""
        return url[:100] + '...' if len(url) > 100 else url

    def _is_in_white_list(self,
                        channel: Channel,
                        white_list: Set[str]) -> bool:
        """检查是否在白名单中"""
        if not white_list:
            return False
        return channel.name.lower() in white_list