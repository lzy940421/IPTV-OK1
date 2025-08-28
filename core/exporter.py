import logging
from pathlib import Path
from datetime import datetime
from typing import List, Callable, Set, Dict, Tuple
from .models import Channel
import csv
from urllib.parse import quote
import re
from collections import defaultdict

logger = logging.getLogger(__name__)

class ResultExporter:
    """结果导出器（修复版：精确统计+分类导出）"""
    
    def __init__(self, 
                 output_dir: str, 
                 enable_history: bool, 
                 template_path: str, 
                 config, 
                 matcher):
        """
        初始化导出器
        
        参数:
            output_dir: 输出目录路径
            enable_history: 是否启用历史记录
            template_path: 分类模板路径
            config: 配置对象
            matcher: 分类匹配器实例
        """
        self.output_dir = Path(output_dir)
        self.enable_history = enable_history
        self.template_path = template_path
        self.config = config
        self.matcher = matcher
        self._ensure_dirs()

    def _ensure_dirs(self):
        """确保输出目录存在"""
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export(self, 
              channels: List[Channel], 
              whitelist: Set[str],
              progress_cb: Callable) -> None:
        """
        导出所有结果（修复：精确统计）
        
        参数:
            channels: 频道列表
            whitelist: 预加载的白名单集合
            progress_cb: 进度回调函数
        """
        # 按模板排序（使用传入的白名单）
        sorted_channels = self.matcher.sort_channels_by_template(channels, whitelist)
        
        # 收集未分类频道（按原始分类分组）
        uncategorized = defaultdict(list)
        for channel in sorted_channels:
            if channel.category == "未分类":
                clean_name = self.matcher.normalize_channel_name(channel.name)
                uncategorized[channel.original_category].append((clean_name, channel.url))
        
        # 导出主文件
        self._export_all(sorted_channels)
        
        # 导出协议特定文件（修复：正确分类）
        ipv4, ipv6 = self._classify_channels(sorted_channels)
        self._export_channels(ipv4, "ipv4")
        self._export_channels(ipv6, "ipv6")
        
        # 导出未分类频道（标准格式）
        if uncategorized:
            self._export_uncategorized(uncategorized)
        
        # 历史记录功能（保持原有逻辑）
        if self.enable_history:
            self._export_history(sorted_channels)
        
        progress_cb(1)

    def _export_uncategorized(self, uncategorized: Dict[str, List[Tuple[str, str]]]) -> None:
        """导出未分类频道"""
        output_path = self.output_dir / "uncategorized.txt"
        
        with open(output_path, 'w', encoding='utf-8') as f:
            for original_category, channels in uncategorized.items():
                # 写入分类行和频道列表
                f.write(f"{original_category},#genre#\n")
                f.writelines(f"{name},{url}\n" for name, url in channels)
                f.write("\n")  # 分组间空行
        
        logger.info(f"未分类频道已导出到: {output_path}")

    def _classify_channels(self, channels: List[Channel]) -> Tuple[List[Channel], List[Channel]]:
        """
        分类频道为IPv4和IPv6（修复：精确统计）
        返回: (ipv4_channels, ipv6_channels)
        """
        ipv4_channels = []
        ipv6_channels = []
        
        for channel in channels:
            if channel.status == 'online':  # 只处理在线频道
                if Channel.classify_ip_type(channel.url) == "ipv6":
                    ipv6_channels.append(channel)
                else:
                    ipv4_channels.append(channel)
        
        logger.info(f"频道协议分类完成: IPv4({len(ipv4_channels)}) | IPv6({len(ipv6_channels)})")
        return ipv4_channels, ipv6_channels

    def _export_all(self, channels: List[Channel]) -> None:
        """导出主文件（修复：正确统计）"""
        # 从配置读取文件名
        m3u_filename = self.config.get('EXPORTER', 'm3u_filename', fallback='all.m3u')
        txt_filename = self.config.get('EXPORTER', 'txt_filename', fallback='all.txt')
        
        # 导出M3U
        self._export_m3u(
            channels=channels,
            file_path=self.output_dir / m3u_filename
        )
        
        # 导出TXT
        self._export_txt(
            channels=channels,
            file_path=self.output_dir / txt_filename
        )

    def _export_m3u(self, channels: List[Channel], file_path: Path) -> int:
        """导出M3U文件（修复：返回实际数量）"""
        online_channels = [c for c in channels if c.status == 'online']
        count = len(online_channels)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            # 写入文件头
            f.write(self._get_m3u_header())
            
            # 批量写入频道
            for channel in online_channels:
                logo_url = self.config.get(
                    'EXPORTER', 'm3u_logo_url', fallback=''
                ).format(
                    name=quote(channel.name),
                    name_encoded=quote(channel.name),
                    category=quote(channel.category)
                )
                
                f.write(
                    f'#EXTINF:-1 tvg-name="{channel.name}" '
                    f'group-title="{channel.category}" '
                    f'tvg-logo="{logo_url}",{channel.name}\n'
                    f"{channel.url}\n"
                )
                
        logger.info(f"M3U文件已生成: {file_path} (包含 {count} 个频道)")
        return count

    def _export_txt(self, channels: List[Channel], file_path: Path) -> int:
        """导出TXT文件（修复：返回实际数量）"""
        seen_urls = set()
        current_category = None
        count = 0
        
        with open(file_path, 'w', encoding='utf-8') as f:
            for channel in channels:
                if channel.status != 'online' or channel.url in seen_urls:
                    continue
                    
                seen_urls.add(channel.url)
                
                # 分类变化时写入新标题
                if channel.category != current_category:
                    if current_category is not None:
                        f.write("\n")
                    f.write(f"{channel.category},#genre#\n")
                    current_category = channel.category
                
                f.write(f"{channel.name},{channel.url}\n")
                count += 1
                
        logger.info(f"TXT文件已生成: {file_path} (包含 {count} 个频道)")
        return count

    def _export_channels(self, channels: List[Channel], type_name: str) -> None:
        """导出指定协议类型的文件（修复：精确统计）"""
        # 从配置获取输出路径
        output_txt = Path(self.config.get(
            'PATHS', 
            f'{type_name}_output_path', 
            fallback=f'{type_name}.txt'
        ))
        output_m3u = output_txt.with_suffix('.m3u')

        # 导出TXT（修复：接收返回值）
        txt_count = self._export_txt(
            channels=channels,
            file_path=self.output_dir / output_txt
        )
        
        # 导出M3U（修复：接收返回值）
        m3u_count = self._export_m3u(
            channels=channels,
            file_path=self.output_dir / output_m3u
        )

        logger.info(
            f"{type_name.upper()}导出完成: "
            f"TXT({txt_count}个) | M3U({m3u_count}个)"
        )

    def _export_history(self, channels: List[Channel]) -> None:
        """导出历史记录（保持原有逻辑）"""
        if not self.enable_history:
            return
            
        history_file = self.output_dir / f"history_{datetime.now().strftime('%Y%m%d')}.csv"
        with open(history_file, 'w', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Name', 'URL', 'Category', 'Status', 'Speed(KB/s)', 'Response(ms)'])
            for ch in channels:
                writer.writerow([
                    ch.name, ch.url, ch.category, 
                    ch.status, ch.download_speed, ch.response_time
                ])
        logger.info(f"历史记录已保存到: {history_file}")

    def _get_m3u_header(self) -> str:
        """生成M3U文件头（从配置读取EPG地址）"""
        epg_url = self.config.get(
            'EXPORTER', 
            'm3u_epg_url', 
            fallback='http://epg.51zmt.top:8000/cc.xml.gz'
        )
        return (
            f'#EXTM3U x-tvg-url="{epg_url}" '
            'catchup="append" '
            'catchup-source="?playseek=${{(b)yyyyMMddHHmmss}}-${{(e)yyyyMMddHHmmss}}"\n'
        )