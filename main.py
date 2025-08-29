#!/usr/bin/env python3
import os
import asyncio
import configparser
from pathlib import Path
from typing import List, Set, Dict, Optional
import re
import logging
import gc
import sys
from datetime import datetime
from collections import defaultdict
from core import (
    SourceFetcher,
    PlaylistParser,
    AutoCategoryMatcher,
    SpeedTester,
    ResultExporter,
    Channel
)
from core.progress import SmartProgress

def print_start_page(config: configparser.ConfigParser, logger: logging.Logger):
    """打印启动页面（带配置概览）"""
    title = r"""
   ____   _   _   _       ___   _   _  __  __
  / ___| | \ | | | |     |_ _| | | | | \ \/ /
 | |     |  \| | | |      | |  | | | |  \  / 
 | |___  | |\  | | |___   | |  | |_| |  /  \ 
  \____| |_| \_| |_____| |___|  \___/  /_/\_\
                                             
    """
    
    # 获取关键配置
    urls_path = config.get('PATHS', 'urls_path', fallback='config/urls.txt')
    templates_path = config.get('PATHS', 'templates_path', fallback='config/templates.txt')
    output_dir = config.get('MAIN', 'output_dir', fallback='outputs')
    uncategorized_path = config.get('PATHS', 'uncategorized_channels_path', fallback='config/uncategorized.txt')
    blacklist_path = config.get('BLACKLIST', 'blacklist_path', fallback='config/blacklist.txt')
    whitelist_path = config.get('WHITELIST', 'whitelist_path', fallback='config/whitelist.txt')
    failed_urls_path = config.get('PATHS', 'failed_urls_path', fallback='config/failed_urls.txt')
    log_file_path = config.get('LOGGING', 'log_file_path', fallback='outputs/debug.log')
    
    fetcher_timeout = config.getfloat('FETCHER', 'timeout', fallback=15)
    fetcher_concurrency = config.getint('FETCHER', 'concurrency', fallback=5)
    tester_timeout = config.getfloat('TESTER', 'timeout', fallback=10)
    tester_concurrency = config.getint('TESTER', 'concurrency', fallback=8)
    enable_history = config.getboolean('EXPORTER', 'enable_history', fallback=False)
    log_level = config.get('LOGGING', 'log_level', fallback='INFO').upper()
    
    # 获取版本信息
    try:
        from core import __version__
        version = f"v{__version__}"
    except ImportError:
        version = "v1.0.0"
    
    # 构建启动信息
    start_info = f"""
{title}
╔╔╔╔══════════════════════════════════════════════════╗╗╗╗
║║║║ IPTV智能处理系统 {version}                          ║║║║║║║
║║║║ 启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}                    ║║║║║║║
╟╟╟╟──────────────────────────────────────────────────╢╢╢╢
╟╟╟╟──────────────────────────────────────────────────╢╢╢╢
║║║║ 处理流程:                                        ║║║║║║║
║║║║   1. 获取订阅源 → 2. 解析频道 → 3. 去重          ║║║║║║║
║║║║   4. 黑名单过滤 → 5. 智能分类 → 6. 测速测试      ║║║║║║║
║║║║   7. 结果导出 → 8. 完成!                         ║║║║║║║
╚╚╚╚══════════════════════════════════════════════════╝╝╝╝
核心配置概览:                                    
• 订阅源路径: {urls_path:<25} 
• 分类模板: {templates_path:<25} 
• 未分类频道路径: {uncategorized_path:<25}
• 黑名单路径: {blacklist_path:<25}
• 白名单路径: {whitelist_path:<25}
• 失败URL路径: {failed_urls_path:<25}
• 日志文件路径: {log_file_path:<25}
• 输出目录: {output_dir:<25} 
• 抓取并发数: {fetcher_concurrency:<3} 超时: {fetcher_timeout:<4}秒 
• 测速并发数: {tester_concurrency:<3} 超时: {tester_timeout:<4}秒 
• 历史记录: {'启用' if enable_history else '禁用':<8}          
• 日志级别: {log_level:<8}                     

"""
    
    # 打印到日志和控制台
    if logger:
        logger.info(start_info)
    else:
        print(start_info)

def setup_logging(config: configparser.ConfigParser) -> Optional[logging.Logger]:
    """配置日志系统（每次运行覆盖日志文件）"""
    enable_logging = config.getboolean('LOGGING', 'enable_logging', fallback=True)
    if not enable_logging:
        logging.disable(logging.CRITICAL)
        return None

    log_level = config.get('LOGGING', 'log_level', fallback='INFO').upper()
    log_to_file = config.getboolean('LOGGING', 'log_to_file', fallback=False)

    logger = logging.getLogger()
    logger.setLevel(log_level)

    # 清除所有现有处理器
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # 控制台处理器（始终添加）
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # 文件处理器（覆盖模式）
    if log_to_file:
        log_file = Path(config.get('LOGGING', 'log_file_path', fallback='outputs/debug.log'))
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    return logger

def is_blacklisted(channel: Channel, blacklist: Set[str]) -> bool:
    """检查频道是否在黑名单中"""
    channel_name = channel.name.lower()
    channel_url = channel.url.lower()
    
    for entry in blacklist:
        if '*' in entry:
            if entry.startswith('*.'):
                suffix = entry[2:].lower()
                if channel_url.endswith(suffix) or channel_name.endswith(suffix):
                    return True
        elif entry in channel_name or entry in channel_url:
            return True
    return False

async def main():
    """主工作流程（带启动页面）"""
    logger = None  # 关键修复：确保logger变量始终存在
    
    try:
        # 初始化配置
        config = configparser.ConfigParser()
        config_path = Path('config/config.ini')
        if not config_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {config_path}")
        config.read(config_path, encoding='utf-8')

        # 设置日志（覆盖模式）
        logger = setup_logging(config)
        
        # 打印启动页面
        print_start_page(config, logger)

        # 初始化输出目录
        output_dir = Path(config.get('MAIN', 'output_dir', fallback='outputs'))
        output_dir.mkdir(parents=True, exist_ok=True)

        # 加载黑名单和白名单
        blacklist = set()
        whitelist = set()
        
        blacklist_path = Path(config.get('BLACKLIST', 'blacklist_path', fallback='config/blacklist.txt'))
        if blacklist_path.exists():
            with open(blacklist_path, 'r', encoding='utf-8') as f:
                blacklist = {line.strip().lower() for line in f if line.strip() and not line.startswith('#')}

        whitelist_path = Path(config.get('WHITELIST', 'whitelist_path', fallback='config/whitelist.txt'))
        if whitelist_path.exists():
            with open(whitelist_path, 'r', encoding='utf-8') as f:
                whitelist = {line.strip().lower() for line in f if line.strip() and not line.startswith('#')}

        # ==================== 阶段1: 获取订阅源 ====================
        urls_path = Path(config.get('PATHS', 'urls_path', fallback='config/urls.txt'))
        try:
            if not urls_path.exists():
                raise FileNotFoundError(f"订阅源文件不存在: {urls_path}")
            
            logger.info(f"📡📡 开始获取订阅源 | 路径: {urls_path}")
            
            with open(urls_path, 'r', encoding='utf-8') as f:
                urls = [line.strip() for line in f if line.strip()]
                logger.debug(f"读取到 {len(urls)} 个订阅源URL | 示例: {urls[:3]}...")
                
            if not urls:
                raise ValueError("订阅源列表为空，请检查urls.txt文件内容")
                
            fetcher = SourceFetcher(
                timeout=config.getfloat('FETCHER', 'timeout', fallback=15),
                concurrency=config.getint('FETCHER', 'concurrency', fallback=5),
                config=config
            )
            
            # 带重试机制的获取（固定重试2次）
            contents = []
            for attempt in range(1, 3):  # 不新增配置项，固定重试2次
                try:
                    fetch_progress = SmartProgress(len(urls), f"获取订阅源(尝试{attempt}/2)")
                    contents = await fetcher.fetch_all(urls, fetch_progress.update)
                    fetch_progress.complete()
                    break
                except Exception as e:
                    if attempt == 2:
                        raise
                    logger.warning(f"第 {attempt} 次获取失败: {str(e)}")
                    await asyncio.sleep(2 ** attempt)  # 指数退避
                    
            # 统计有效内容
            valid_contents = [c for c in contents if c and c.strip()]
            logger.info(
                f"✅ 订阅源获取完成 | 总数: {len(urls)} | 成功: {len(valid_contents)} "
                f"({len(valid_contents)/len(urls)*100:.1f}%)"
            )
            
        except Exception as e:
            logger.error(f"‼️ 订阅源获取阶段失败: {str(e)}", exc_info=True)
            raise

        # ==================== 阶段2: 解析频道 ====================
        try:
            parser = PlaylistParser(config)
            all_channels = []
            total_channels = 0
            
            logger.info(f"🔍🔍 开始解析频道内容 | 有效订阅源: {len(valid_contents)}")
            
            parse_progress = SmartProgress(len(valid_contents), "解析频道")
            for i, content in enumerate(valid_contents, 1):
                try:
                    channels = list(parser.parse(content))
                    all_channels.extend(channels)
                    total_channels += len(channels)
                    
                    # 每解析10个源或5000个频道时输出进度
                    if i % 10 == 0 or len(all_channels) % 5000 == 0:
                        logger.debug(
                            f"解析进度 | 源: {i}/{len(valid_contents)} "
                            f"| 频道: {len(all_channels)} "
                            f"| 最新: {channels[-1].name if channels else 'N/A'}"
                        )
                        gc.collect()
                        
                    parse_progress.update()
                except Exception as e:
                    logger.error(f"解析异常(源#{i}): {str(e)}")
                    continue
                    
            parse_progress.complete()
            
            # 解析结果统计
            unique_sources = len({c.url for c in all_channels})
            logger.info(
                f"✅ 频道解析完成 | 总频道数: {len(all_channels)} "
                f"| 唯一源: {unique_sources} "
                f"| 重复率: {(1 - unique_sources/len(all_channels))*100:.1f}%"
            )
            
        except Exception as e:
            logger.error(f"‼️ 频道解析阶段失败: {str(e)}", exc_info=True)
            raise

        # ==================== 阶段3: 去重 ====================
        duplicate_progress = SmartProgress(len(all_channels), "去重处理")
        unique_channels = {channel.url: channel for channel in all_channels}
        duplicate_progress.update(len(all_channels))
        unique_channels = list(unique_channels.values())
        duplicate_progress.complete()
        logger.info(f"去重完成 | 原始: {len(all_channels)} | 去重后: {len(unique_channels)}")

        # ==================== 阶段4: 黑名单过滤 ====================
        filter_progress = SmartProgress(len(unique_channels), "黑名单过滤")
        filtered_channels = [
            channel for channel in unique_channels
            if not is_blacklisted(channel, blacklist)
        ]
        filter_progress.update(len(unique_channels))
        filter_progress.complete()
        logger.info(f"黑名单过滤完成 | 过滤前: {len(unique_channels)} | 过滤后: {len(filtered_channels)}")

        # ==================== 阶段5: 智能分类 ====================
        templates_path = Path(config.get('PATHS', 'templates_path', fallback='config/templates.txt'))
        if not templates_path.exists():
            raise FileNotFoundError(f"分类模板不存在: {templates_path}")
        
        matcher = AutoCategoryMatcher(str(templates_path), config)
        classify_progress = SmartProgress(len(filtered_channels), "分类处理")
        
        # 批量匹配分类
        category_mapping = matcher.batch_match([c.name for c in filtered_channels])
        
        processed_channels = []
        for channel in filtered_channels:
            channel.category = category_mapping[channel.name]
            channel.name = matcher.normalize_channel_name(channel.name)
            processed_channels.append(channel)
            classify_progress.update()
            if len(processed_channels) % 5000 == 0:
                gc.collect()
        
        classify_progress.complete()
        logger.info(f"分类完成 | 已分类: {len([c for c in processed_channels if c.category != '未分类'])} | 未分类: {len(processed_channels) - len([c for c in processed_channels if c.category != '未分类'])}")

        # ==================== 阶段6: 测速测试 ====================
        tester = SpeedTester(
            timeout=config.getfloat('TESTER', 'timeout', fallback=10),
            concurrency=config.getint('TESTER', 'concurrency', fallback=8),
            max_attempts=config.getint('TESTER', 'max_attempts', fallback=2),
            min_download_speed=config.getfloat('TESTER', 'min_download_speed', fallback=0.1),
            enable_logging=config.getboolean('TESTER', 'enable_logging', fallback=True),
            config=config
        )
        
        # 按模板排序
        sorted_channels = matcher.sort_channels_by_template(processed_channels, whitelist)
        
        # 检查是否有频道需要测速
        if not sorted_channels:
            logger.warning("⚠️ 没有需要测速的频道，跳过测速阶段")
        else:
            # 分批测速
            batch_size = min(5000, len(sorted_channels))
            batch_size = max(1, batch_size)
            
            test_progress = SmartProgress(len(sorted_channels), "测速测试")
            failed_urls = set()

            for i in range(0, len(sorted_channels), batch_size):
                batch = sorted_channels[i:i+batch_size]
                await tester.test_channels(batch, test_progress.update, failed_urls, whitelist)
                del batch
                gc.collect()
            
            test_progress.complete()
            logger.info(f"测速完成 | 总数: {len(sorted_channels)} | 失败: {len(failed_urls)}")

        # ==================== 阶段7: 结果导出 ====================
        exporter = ResultExporter(
            output_dir=str(output_dir),
            template_path=str(templates_path),
            config=config,
            matcher=matcher
        )
        
        export_progress = SmartProgress(1, "导出结果")
        exporter.export(sorted_channels, whitelist, export_progress.update)
        export_progress.complete()

        # 完成提示
        online = sum(1 for c in sorted_channels if c.status == 'online') if sorted_channels else 0
        total = len(sorted_channels) if sorted_channels else 0
        logger.info(f"🎉🎉 任务完成! 在线频道: {online}/{total} | 成功率: {online/total*100:.1f}%")

    except Exception as e:
        # 安全异常处理
        if logger:  
            logger.error(f"‼️ 发生严重错误: {str(e)}", exc_info=True)
        else:
            print(f"‼️ 发生严重错误: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    # Windows系统设置事件循环策略
    if os.name == 'nt':
        from asyncio import WindowsSelectorEventLoopPolicy
        asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑🛑🛑 用户中断操作")
        sys.exit(0)
    except Exception as e:
        print(f"💥💥 全局异常: {str(e)}")
        sys.exit(1)