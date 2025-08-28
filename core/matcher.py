import re
import time
import logging
from typing import Dict, List, Set, Tuple
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
from .models import Channel
import configparser
from collections import defaultdict
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class MatchCache:
    """分类匹配缓存数据结构（优化内存使用）"""
    category: str
    normalized_name: str

class AutoCategoryMatcher:
    """智能分类匹配器（高性能优化版）"""

    def __init__(self, template_path: str, config=None):
        """
        初始化分类匹配器
        
        参数:
            template_path: 分类模板文件路径
            config: 配置对象
        """
        self.template_path = template_path
        self.config = config or configparser.ConfigParser()
        self.enable_space_clean = self.config.getboolean('MATCHER', 'enable_space_clean', fallback=True)
        
        # 初始化缓存和统计
        self.match_cache: Dict[str, MatchCache] = {}
        self.name_normalization_cache: Dict[str, str] = {}
        self.template_order_cache: Dict[tuple, Dict[str, int]] = {}  # {channel_names_tuple: {name: index}}
        
        # 加载模板数据
        self.categories, self.standard_names = self._parse_template()
        self.suffixes = self._extract_suffixes()
        self.template_order = self._load_template_order()
        
        logger.info(f"分类器初始化完成 | 模板规则: {sum(len(p) for p in self.categories.values())}条")

    def _clean_channel_name(self, name: str) -> str:
        """清理频道名称（优化：使用缓存+批量处理）"""
        if not name or not self.enable_space_clean:
            return name
            
        # 1. 去除首尾空格和特殊字符
        cleaned = name.strip().replace('_', ' ').replace('-', ' ')
        # 2. 移除字母/数字与汉字之间的空格
        cleaned = re.sub(r'([a-zA-Z0-9]+)\s+([\u4e00-\u9fa5])', r'\1\2', cleaned)
        # 3. 合并多余空格
        return re.sub(r'\s+', ' ', cleaned)

    def _extract_suffixes(self) -> List[str]:
        """从模板中提取后缀配置（优化：使用正则预编译）"""
        suffix_pattern = re.compile(r'#suffixes:(.*)')
        try:
            with open(self.template_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if match := suffix_pattern.search(line):
                        return [s.strip().lower() for s in match.group(1).split(',') if s.strip()]
        except Exception as e:
            logger.error(f"后缀提取失败: {str(e)}")
        return ["高清", "hd", "综合"]  # 默认后缀

    def _parse_template(self) -> Tuple[Dict[str, List[re.Pattern]], Dict[str, str]]:
        """
        解析模板文件（优化：并行处理+预编译正则）
        返回: (categories, standard_names)
        """
        categories = defaultdict(list)
        standard_names = {}
        current_category = None
        
        try:
            with open(self.template_path, 'r', encoding='utf-8') as f:
                lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]
                
                # 并行处理正则编译
                with ThreadPoolExecutor() as executor:
                    futures = []
                    for line in lines:
                        if line.endswith(',#genre#'):
                            current_category = line.split(',')[0]
                            continue
                            
                        if current_category:
                            parts = line.split('|')
                            standard_name = parts[0].strip()
                            futures.append(
                                executor.submit(
                                    self._compile_patterns,
                                    parts,
                                    current_category,
                                    standard_name
                                )
                            )
                    
                    for future in as_completed(futures):
                        category, patterns, name_mappings = future.result()
                        categories[category].extend(patterns)
                        standard_names.update(name_mappings)
                        
        except Exception as e:
            logger.error(f"模板解析失败: {str(e)}")
            raise
            
        return dict(categories), standard_names

    def _compile_patterns(self, parts: List[str], category: str, standard_name: str) -> Tuple[str, List[re.Pattern], Dict[str, str]]:
        """编译正则模式并构建名称映射（子任务函数）"""
        patterns = []
        name_mappings = {}
        
        for name in parts:
            name = name.strip()
            if not name:
                continue
                
            try:
                # 编译正则并缓存
                pattern = re.compile(name)
                patterns.append(pattern)
                
                # 构建标准化名称映射
                clean_name = self._clean_channel_name(name)
                name_mappings[clean_name.lower()] = standard_name
            except re.error as e:
                logger.warning(f"正则编译跳过: {name} ({str(e)})")
                
        return category, patterns, name_mappings

    def batch_match(self, channel_names: List[str]) -> Dict[str, str]:
        """
        批量匹配分类（优化：并行处理+缓存）
        返回: {channel_name: category}
        """
        if not channel_names:
            return {}
            
        # 小批量直接处理
        if len(channel_names) <= 1000:
            return {name: self.match(name) for name in channel_names}
        
        # 并行处理
        threads = self.config.getint('PERFORMANCE', 'classification_threads', fallback=4)
        batch_size = self.config.getint('PERFORMANCE', 'classification_batch_size', fallback=1000)
        
        logger.debug(f"启动并行分类 | 总数: {len(channel_names)} | 线程: {threads}")
        
        results = {}
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = {
                executor.submit(
                    self._process_batch, 
                    channel_names[i:i+batch_size]
                ): i for i in range(0, len(channel_names), batch_size)
            }
            
            for future in as_completed(futures):
                results.update(future.result())
                
        return results

    def _process_batch(self, names: List[str]) -> Dict[str, str]:
        """处理单个批次的分类匹配"""
        return {name: self.match(name) for name in names}

    def match(self, channel_name: str) -> str:
        """
        匹配单个频道分类（优化：三级缓存）
        返回: 分类名称
        """
        # 第一级缓存检查
        if channel_name in self.match_cache:
            return self.match_cache[channel_name].category
            
        # 清理名称并检查第二级缓存
        clean_name = self._clean_channel_name(channel_name)
        normalized_name = self.normalize_channel_name(clean_name)
        
        # 第三级缓存：模板匹配
        for category, patterns in self.categories.items():
            for pattern in patterns:
                if pattern.search(normalized_name):
                    self.match_cache[channel_name] = MatchCache(category, normalized_name)
                    return category
                    
        # 未匹配情况
        self.match_cache[channel_name] = MatchCache("未分类", normalized_name)
        return "未分类"

    def normalize_channel_name(self, name: str) -> str:
        """标准化频道名称（优化：缓存+后缀处理）"""
        if name in self.name_normalization_cache:
            return self.name_normalization_cache[name]
            
        clean_name = self._clean_channel_name(name)
        
        # 应用标准化名称映射
        normalized_name = self.standard_names.get(clean_name.lower(), clean_name)
        
        # 处理后缀（如"CCTV1高清" -> "CCTV1"）
        for suffix in self.suffixes:
            if normalized_name.lower().endswith(suffix):
                normalized_name = normalized_name[:-len(suffix)]
                break
                
        self.name_normalization_cache[name] = normalized_name
        return normalized_name

    def sort_channels_by_template(self, 
                                channels: List[Channel], 
                                whitelist: Set[str]) -> List[Channel]:
        """
        按模板顺序排序频道（优化：预构建索引）
        返回: 排序后的频道列表
        """
        # 白名单频道优先
        whitelist_channels = [
            c for c in channels 
            if c.name.lower() in whitelist
        ]
        
        # 按模板顺序排序其他频道
        sorted_channels = []
        for category in self.template_order:
            category_channels = [
                c for c in channels 
                if c not in whitelist_channels and c.category == category
            ]
            sorted_channels.extend(
                sorted(
                    category_channels,
                    key=lambda c: self._get_channel_order(c, self.template_order[category])
                )
            )
        
        # 添加未分类频道
        uncategorized = [
            c for c in channels 
            if c not in whitelist_channels 
            and c.category not in self.template_order
        ]
        sorted_channels.extend(uncategorized)
        
        return whitelist_channels + sorted_channels

    def _get_channel_order(self, 
                         channel: Channel, 
                         channel_names: List[str]) -> int:
        """获取频道在模板中的顺序（优化：缓存索引）"""
        cache_key = tuple(channel_names)
        if cache_key not in self.template_order_cache:
            self.template_order_cache[cache_key] = {
                name: i for i, name in enumerate(channel_names)
            }
            
        clean_name = self.normalize_channel_name(channel.name)
        return self.template_order_cache[cache_key].get(clean_name, len(channel_names))

    def _load_template_order(self) -> Dict[str, List[str]]:
        """加载模板中的频道顺序（优化：按需加载）"""
        template_order = {}
        current_category = None
        
        try:
            with open(self.template_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                        
                    if line.endswith(',#genre#'):
                        current_category = line.split(',')[0]
                        template_order[current_category] = []
                        continue
                        
                    if current_category:
                        parts = line.split('|')
                        if parts:
                            template_order[current_category].append(parts[0].strip())
        except Exception as e:
            logger.error(f"模板顺序加载失败: {str(e)}")
            
        return template_order

    def clear_cache(self):
        """清空缓存（用于长时间运行的服务）"""
        self.match_cache.clear()
        self.name_normalization_cache.clear()
        self.template_order_cache.clear()
        logger.info("分类器缓存已清空")