"""Performance monitoring utilities for script execution."""

import logging
import time
from contextlib import contextmanager
from typing import Any, Callable, Dict, Optional

from dagster import MetadataValue

logger = logging.getLogger(__name__)

# Try to import psutil for memory monitoring
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logger.debug("psutil not available - memory monitoring disabled")


class PerformanceMonitor:
    """Monitor script execution performance.

    Tracks:
    - Execution time (always)
    - Memory usage (if psutil available)
    - CPU usage (if psutil available)
    """

    @staticmethod
    @contextmanager
    def track_performance(context_log: Optional[Any] = None):
        """Context manager to track performance metrics.

        Usage:
        ```python
        with PerformanceMonitor.track_performance(context.log) as perf:
            # Your code here
            result = expensive_operation()

        # Access metrics
        metadata = perf.get_metadata()
        ```

        Args:
            context_log: Optional Dagster context log for logging

        Yields:
            PerformanceTracker instance
        """
        tracker = PerformanceTracker(context_log)
        tracker.start()

        try:
            yield tracker
        finally:
            tracker.stop()

    @staticmethod
    def wrap_callable(
        func: Callable,
        context_log: Optional[Any] = None
    ) -> Callable:
        """Wrap a callable to track its performance.

        Usage:
        ```python
        monitored_func = PerformanceMonitor.wrap_callable(my_function)
        result, metadata = monitored_func()
        ```

        Args:
            func: Function to wrap
            context_log: Optional Dagster context log

        Returns:
            Wrapped function that returns (result, metadata)
        """
        def wrapper(*args, **kwargs):
            with PerformanceMonitor.track_performance(context_log) as perf:
                result = func(*args, **kwargs)
            return result, perf.get_metadata()

        return wrapper


class PerformanceTracker:
    """Tracks performance metrics during execution."""

    def __init__(self, context_log: Optional[Any] = None):
        """Initialize the tracker.

        Args:
            context_log: Optional Dagster context log for logging
        """
        self.context_log = context_log
        self.start_time = None
        self.end_time = None
        self.start_memory = None
        self.end_memory = None
        self.start_cpu = None
        self.end_cpu = None
        self.process = None

        if PSUTIL_AVAILABLE:
            try:
                self.process = psutil.Process()
            except Exception as e:
                logger.debug(f"Could not get psutil process: {e}")

    def start(self):
        """Start tracking."""
        self.start_time = time.time()

        if self.process:
            try:
                # Memory in bytes
                self.start_memory = self.process.memory_info().rss
                # CPU percent (need to call once to initialize)
                self.process.cpu_percent()
            except Exception as e:
                logger.debug(f"Could not get start metrics: {e}")
                self.process = None

        if self.context_log:
            self.context_log.info("Performance monitoring started")

    def stop(self):
        """Stop tracking."""
        self.end_time = time.time()

        if self.process:
            try:
                self.end_memory = self.process.memory_info().rss
                # CPU percent since last call
                self.end_cpu = self.process.cpu_percent()
            except Exception as e:
                logger.debug(f"Could not get end metrics: {e}")

        if self.context_log:
            execution_time = self.get_execution_time()
            self.context_log.info(f"Performance monitoring stopped - Execution time: {execution_time:.2f}s")

            if self.start_memory and self.end_memory:
                memory_used = self.get_memory_used_mb()
                self.context_log.info(f"Memory used: {memory_used:.2f} MB")

    def get_execution_time(self) -> float:
        """Get execution time in seconds.

        Returns:
            Execution time in seconds
        """
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0

    def get_memory_used_mb(self) -> float:
        """Get memory used in MB.

        Returns:
            Memory used in MB, or 0 if not available
        """
        if self.start_memory and self.end_memory:
            return (self.end_memory - self.start_memory) / (1024 * 1024)
        return 0.0

    def get_memory_peak_mb(self) -> Optional[float]:
        """Get peak memory usage in MB.

        Returns:
            Peak memory in MB, or None if not available
        """
        if self.end_memory:
            return self.end_memory / (1024 * 1024)
        return None

    def get_cpu_percent(self) -> Optional[float]:
        """Get CPU usage percentage.

        Returns:
            CPU percentage, or None if not available
        """
        return self.end_cpu

    def get_metadata(self) -> Dict[str, MetadataValue]:
        """Get performance metrics as Dagster metadata.

        Returns:
            Dict of MetadataValue objects for Dagster
        """
        metadata = {}

        # Always include execution time
        execution_time = self.get_execution_time()
        metadata['execution_time_seconds'] = MetadataValue.float(
            round(execution_time, 2)
        )

        # Add human-readable time
        if execution_time < 60:
            time_str = f"{execution_time:.1f}s"
        elif execution_time < 3600:
            time_str = f"{execution_time/60:.1f}m"
        else:
            time_str = f"{execution_time/3600:.1f}h"

        metadata['execution_time'] = MetadataValue.text(time_str)

        # Add memory metrics if available
        memory_used = self.get_memory_used_mb()
        if memory_used != 0.0:
            metadata['memory_used_mb'] = MetadataValue.float(
                round(memory_used, 2)
            )

        memory_peak = self.get_memory_peak_mb()
        if memory_peak:
            metadata['memory_peak_mb'] = MetadataValue.float(
                round(memory_peak, 2)
            )

        # Add CPU metrics if available
        cpu_percent = self.get_cpu_percent()
        if cpu_percent is not None:
            metadata['cpu_percent'] = MetadataValue.float(
                round(cpu_percent, 2)
            )

        # Add monitoring status
        metadata['performance_monitoring'] = MetadataValue.text(
            "✅ Full monitoring" if PSUTIL_AVAILABLE else "⚠️ Limited (time only)"
        )

        return metadata

    def get_summary(self) -> str:
        """Get a human-readable summary of performance.

        Returns:
            Summary string
        """
        parts = []

        execution_time = self.get_execution_time()
        if execution_time < 60:
            parts.append(f"⏱️  {execution_time:.1f}s")
        elif execution_time < 3600:
            parts.append(f"⏱️  {execution_time/60:.1f}m")
        else:
            parts.append(f"⏱️  {execution_time/3600:.1f}h")

        memory_used = self.get_memory_used_mb()
        if memory_used != 0.0:
            parts.append(f"💾 {memory_used:.1f}MB")

        cpu_percent = self.get_cpu_percent()
        if cpu_percent:
            parts.append(f"⚡ {cpu_percent:.1f}% CPU")

        return " | ".join(parts) if parts else "No metrics available"


# Example usage:
"""
from .utils import PerformanceMonitor

# Method 1: Context manager
@asset
def my_asset(context: AssetExecutionContext):
    with PerformanceMonitor.track_performance(context.log) as perf:
        # Your expensive operation
        result = process_data()

    return Output(
        result,
        metadata=perf.get_metadata()
    )

# Method 2: Wrap a function
monitored_func = PerformanceMonitor.wrap_callable(my_expensive_function)
result, metadata = monitored_func()
"""
