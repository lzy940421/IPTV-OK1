import logging
from pathlib import Path
from datetime import datetime
from typing import List, Callable, Set, Dict, Tuple, Optional
from .models import Channel
import csv
from urllib.parse import quote
import re
from collections import defaultdict
import gzip
import shutil

logger = logging.getLogger(__name__)

class ResultExporter:
    """增强版结果导出器（支持分组写入未分类频道）"""

    def __init__(self, 
                output_dir: str, 
                template_path: str, 
                config, 
                matcher):
        self.output_dir = Path(output_dir)
        self.template_path = template_path
        self.config = config
        self.matcher = matcher
        self._ensure_dirs()

    def _ensure_dirs(self):
        """确保所有输出目录存在"""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        Path(self.config.get('PATHS', 'failed_urls_path', fallback='config/failed_urls.txt')).parent.mkdir(exist_ok=True)
        Path(self.config.get('PATHS', 'uncategorized_channels_path', fallback='config/uncategorized.txt')).parent.mkdir(exist_ok=True)
        # 确保CSV输出目录存在
        Path(self.config.get('PATHS', 'csv_output_path', fallback=str(self.output_dir / "history"))).mkdir(exist_ok=True)

    def export(self, 
              channels: List[Channel], 
              whitelist: Set[str],
              progress_cb: Callable) -> None:
        """导出所有结果（严格按原始分组保存未分类频道）"""
        sorted_channels = self.matcher.sort_channels_by_template(channels, whitelist)
        
        # 按原始分组收集未分类频道
        uncategorized = defaultdict(list)
        for channel in sorted_channels:
            if channel.category == "未分类":
                clean_name = self.matcher.normalize_channel_name(channel.name)
                uncategorized[channel.original_category].append((clean_name, channel.url))

        # 导出主文件
        self._export_all(sorted_channels)
        
        # 导出协议特定文件
        ipv4, ipv6 = self._classify_channels(sorted_channels)
        self._export_channels(ipv4, "ipv4")
        self._export_channels(ipv6, "ipv6")
        
        # 导出未分类频道（按原始分组）
        if uncategorized:
            self._export_uncategorized(uncategorized)
        
        # 历史记录（包含所有频道）
        if self.config.getboolean('EXPORTER', 'enable_history', fallback=False):
            self._export_history(sorted_channels)
        
        progress_cb(1)

    def _export_uncategorized(self, uncategorized: Dict[str, List[Tuple[str, str]]]) -> None:
        """导出未分类频道（按原始分组结构）"""
        output_path = Path(self.config.get(
            'PATHS', 
            'uncategorized_channels_path', 
            fallback=str(self.output_dir / "uncategorized.txt")
        ))
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                # 按原始分组排序写入
                for original_category in sorted(uncategorized.keys()):
                    channels = uncategorized[original_category]
                    if not channels:
                        continue
                        
                    # 写入分组头
                    f.write(f"{original_category},#genre#\n")
                    
                    # 写入该分组下的所有频道（按名称排序）
                    for name, url in sorted(channels, key=lambda x: x[0].lower()):
                        f.write(f"{name},{url}\n")
                    
                    # 分组间空行分隔
                    f.write("\n")
            
            logger.info(
                f"未分类频道已按原始分组保存 | 文件: {output_path} | "
                f"分组数: {len(uncategorized)} | "
                f"频道总数: {sum(len(v) for v in uncategorized.values())}"
            )
        except Exception as e:
            logger.error(f"未分类频道导出失败: {str(e)}", exc_info=True)

    def _classify_channels(self, channels: List[Channel]) -> Tuple[List[Channel], List[Channel]]:
        """分类频道为IPv4/IPv6"""
        ipv4_channels = [c for c in channels if c.status == 'online' and Channel.classify_ip_type(c.url) != "ipv6"]
        ipv6_channels = [c for c in channels if c.status == 'online' and Channel.classify_ip_type(c.url) == "ipv6"]
        return ipv4_channels, ipv6_channels

    def _export_all(self, channels: List[Channel]) -> None:
        """导出主文件"""
        m3u_path = self.output_dir / self.config.get('EXPORTER', 'm3u_filename', fallback='all.m3u')
        txt_path = self.output_dir / self.config.get('EXPORTER', 'txt_filename', fallback='all.txt')
        
        m3u_count = self._export_m3u(channels, m3u_path)
        txt_count = self._export_txt(channels, txt_path)
        
        logger.info(
            f"主文件导出完成 | M3U: {m3u_path} ({m3u_count}) | "
            f"TXT: {txt_path} ({txt_count})"
        )

    def _export_m3u(self, channels: List[Channel], file_path: Path) -> int:
        """导出M3U文件"""
        online_channels = [c for c in channels if c.status == 'online']
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(self._get_m3u_header())
            for channel in online_channels:
                logo_url = self.config.get('EXPORTER', 'm3u_logo_url', fallback='').format(
                    name=quote(channel.name),
                    category=quote(channel.category)
                )
                f.write(f'#EXTINF:-1 tvg-name="{channel.name}" group-title="{channel.category}" tvg-logo="{logo_url}",{channel.name}\n')
                f.write(f"{channel.url}\n")
        
        return len(online_channels)

    def _export_txt(self, channels: List[Channel], file_path: Path) -> int:
        """导出TXT文件"""
        seen_urls = set()
        current_category = None
        count = 0
        
        with open(file_path, 'w', encoding='utf-8') as f:
            for channel in channels:
                if channel.status != 'online' or channel.url in seen_urls:
                    continue
                    
                seen_urls.add(channel.url)
                
                if channel.category != current_category:
                    if current_category is not None:
                        f.write("\n")
                    f.write(f"{channel.category},#genre#\n")
                    current_category = channel.category
                
                f.write(f"{channel.name},{channel.url}\n")
                count += 1
                
        return count

    def _export_channels(self, channels: List[Channel], type_name: str) -> None:
        """导出指定协议类型的文件"""
        output_txt = Path(self.config.get('PATHS', f'{type_name}_output_path', fallback=f'{type_name}.txt'))
        output_m3u = output_txt.with_suffix('.m3u')

        txt_count = self._export_txt(channels, self.output_dir / output_txt)
        m3u_count = self._export_m3u(channels, self.output_dir / output_m3u)
        
        logger.info(
            f"{type_name.upper()}频道导出完成 | "
            f"TXT: {output_txt} ({txt_count}) | "
            f"M3U: {output_m3u} ({m3u_count})"
        )

    def _export_history(self, channels: List[Channel]) -> None:
        """导出历史记录（包含所有频道，无论在线或离线）"""
        # 获取配置的CSV输出路径，默认为output_dir/history
        csv_output_path = Path(self.config.get(
            'PATHS', 
            'csv_output_path', 
            fallback=str(self.output_dir / "history")
        ))
        
        # 确保目录存在
        csv_output_path.mkdir(parents=True, exist_ok=True)
        
        # 构建完整文件路径（带时间戳）
        history_file = csv_output_path / f"history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        try:
            with open(history_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                # 表头
                writer.writerow([
                    'Name', 'URL', 'Category', 'OriginalCategory',
                    'Status', 'Speed(KB/s)', 'Response(ms)'
                ])
                # 写入所有频道数据
                for ch in channels:
                    writer.writerow([
                        ch.name, ch.url, ch.category, ch.original_category,
                        ch.status, ch.download_speed, ch.response_time
                    ])
            
            # 根据配置决定是否压缩
            if self.config.getboolean('EXPORTER', 'compress_history', fallback=True):
                with open(history_file, 'rb') as f_in:
                    with gzip.open(f"{history_file}.gz", 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                history_file.unlink()
                logger.info(f"历史记录已压缩: {history_file}.gz | 总频道数: {len(channels)}")
            else:
                logger.info(f"历史记录已保存: {history_file} | 总频道数: {len(channels)}")
        except Exception as e:
            logger.error(f"历史记录导出失败: {str(e)}")

    def _get_m3u_header(self) -> str:
        """生成M3U文件头"""
        epg_url = self.config.get('EXPORTER', 'm3u_epg_url', fallback='')
        return f'#EXTM3U x-tvg-url="{epg_url}"\n'