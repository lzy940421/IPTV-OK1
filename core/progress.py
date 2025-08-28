import time
import math
import logging
from typing import Callable, Optional

logger = logging.getLogger('core.progress')

class SmartProgress:
    """智能进度系统（修复版：精确控制+防溢出）"""

    def __init__(self, total: int, description: str = "Processing", min_update_interval: float = 0.5):
        self.total = max(1, total)
        self.description = description
        self.completed = 0
        self.start_time = time.time()
        self.last_update_time = self.start_time
        self.min_update_interval = min_update_interval
        
        # 使用指数加权移动平均 (EWMA) 来平滑速度
        self.alpha = 0.1  # 权重因子
        self.avg_speed = 0.0
        self.last_speed = 0.0

        # 进度条状态控制
        self._update_interval_counter = 0
        self._completed_at_last_update = 0
        self._is_completed = False

        # 自动计算初始更新间隔
        self._calculate_initial_interval()

    def _calculate_initial_interval(self):
        """根据总量计算初始更新频率"""
        if self.total <= 100:
            self.update_interval = 1
        elif self.total <= 1000:
            self.update_interval = 5
        elif self.total <= 10000:
            self.update_interval = 20
        else:
            self.update_interval = max(50, self.total // 200)

    def update(self, n: int = 1):
        """安全更新进度（防溢出）"""
        if self._is_completed:
            return
            
        # 确保不会超过总量
        actual_n = min(n, self.total - self.completed)
        self.completed += actual_n
        
        # 检测是否完成
        if self.completed >= self.total:
            self._is_completed = True
            self.completed = self.total  # 修正为精确值
            self._update_display(force=True)
            return
            
        self._update_interval_counter += actual_n
        if self._update_interval_counter >= self.update_interval:
            self._update_display()
            self._update_interval_counter = 0

    def _update_display(self, force: bool = False):
        """更新进度显示（带强制刷新选项）"""
        current_time = time.time()
        
        # 检查最小更新间隔
        if not force and (current_time - self.last_update_time) < self.min_update_interval:
            return
            
        elapsed = max(0.001, current_time - self.start_time)
        self.last_update_time = current_time
        
        # 计算当前速度（基于上次更新以来的进度）
        if self.completed > self._completed_at_last_update:
            items_since_last = self.completed - self._completed_at_last_update
            time_since_last = current_time - self.last_update_time
            current_speed = items_since_last / time_since_last if time_since_last > 0 else 0
            
            # 更新EWMA平均速度
            if self.avg_speed == 0.0:
                self.avg_speed = current_speed
            else:
                self.avg_speed = self.alpha * current_speed + (1 - self.alpha) * self.avg_speed
                
            self.last_speed = current_speed
            self._completed_at_last_update = self.completed
        
        # 时间格式化
        elapsed_str = self._format_time(elapsed)
        
        # 计算剩余时间（防除零）
        remaining_str = "计算中..."
        if self.avg_speed > 0 and self.completed < self.total:
            remaining_items = self.total - self.completed
            remaining_time = remaining_items / self.avg_speed
            remaining_str = self._format_time(remaining_time)
        elif self.completed >= self.total:
            remaining_str = "即将完成"

        # 确保百分比在0-100%范围内
        percent = min(100.0, (self.completed / self.total) * 100) if self.total > 0 else 0
        
        # 创建进度条（使用Unicode区块元素）
        bar_length = 30
        filled_length = int(bar_length * self.completed // self.total)
        bar = '■' * filled_length + '□' * (bar_length - filled_length)
        
        # 添加速度指示器
        speed_indicator = ""
        if self.last_speed > 0:
            speed_indicator = f" | 速度: {self.last_speed:.1f}项/秒"
        
        # 构建状态信息
        status = (
            f"\r{self.description} {bar} {percent:.1f}% | "
            f"进度: {self.completed}/{self.total} | "
            f"用时: {elapsed_str} | "
            f"预计剩余: {remaining_str}"
            f"{speed_indicator}"
        )
        
        print(status, end='', flush=True)

    def _format_time(self, seconds: float) -> str:
        """智能时间格式转换"""
        if seconds < 60:
            return f"{seconds:.1f}秒"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f}分钟"
        else:
            hours = seconds / 3600
            return f"{hours:.1f}小时"
    
    def complete(self):
        """安全完成进度显示"""
        if not self._is_completed:
            self._is_completed = True
            self.completed = self.total
            self._update_display(force=True)
            
            elapsed = max(0.001, time.time() - self.start_time)
            avg_speed = self.total / elapsed if elapsed > 0 else 0
            
            logger.info(
                f"{self.description} 完成! "
                f"总数: {self.total} | "
                f"用时: {self._format_time(elapsed)} | "
                f"平均速度: {avg_speed:.1f}项/秒"
            )