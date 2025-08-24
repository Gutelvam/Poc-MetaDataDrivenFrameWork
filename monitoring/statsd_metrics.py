# dags/monitoring/statsd_metrics.py
"""
Simplified StatsD Metrics Integration for Framework
Uses Airflow's built-in StatsD support + custom framework metrics
"""

import logging
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

def safe_get_statsd():
    """Safely get StatsD client from Airflow"""
    try:
        from airflow import settings
        from airflow.stats import Stats
        return Stats
    except Exception as e:
        logger.debug(f"StatsD not available: {e}")
        return None

class FrameworkStatsD:
    """Enhanced StatsD metrics for the framework"""
    
    def __init__(self):
        self.stats = safe_get_statsd()
        self.enabled = self.stats is not None
        
        if self.enabled:
            logger.info("✅ Framework StatsD metrics enabled")
        else:
            logger.warning("⚠️ StatsD not available - metrics disabled")
    
    def incr(self, metric_name: str, count: int = 1, tags: Dict[str, str] = None):
        """Increment a counter metric"""
        if not self.enabled:
            return
        
        try:
            # Format metric name for framework
            full_metric = f"framework.{metric_name}"
            
            # Airflow's Stats.incr doesn't support tags directly in older versions
            # We'll encode tags in metric name for compatibility
            if tags:
                tag_suffix = ".".join([f"{k}_{v}" for k, v in tags.items()])
                full_metric = f"{full_metric}.{tag_suffix}"
            
            self.stats.incr(full_metric, count)
            logger.debug(f"StatsD incr: {full_metric} +{count}")
            
        except Exception as e:
            logger.error(f"StatsD incr failed: {e}")
    
    def gauge(self, metric_name: str, value: float, tags: Dict[str, str] = None):
        """Set a gauge metric"""
        if not self.enabled:
            return
        
        try:
            full_metric = f"framework.{metric_name}"
            
            if tags:
                tag_suffix = ".".join([f"{k}_{v}" for k, v in tags.items()])
                full_metric = f"{full_metric}.{tag_suffix}"
            
            self.stats.gauge(full_metric, value)
            logger.debug(f"StatsD gauge: {full_metric} = {value}")
            
        except Exception as e:
            logger.error(f"StatsD gauge failed: {e}")
    
    def timing(self, metric_name: str, duration_ms: float, tags: Dict[str, str] = None):
        """Record a timing metric"""
        if not self.enabled:
            return
        
        try:
            full_metric = f"framework.{metric_name}"
            
            if tags:
                tag_suffix = ".".join([f"{k}_{v}" for k, v in tags.items()])
                full_metric = f"{full_metric}.{tag_suffix}"
            
            self.stats.timing(full_metric, duration_ms)
            logger.debug(f"StatsD timing: {full_metric} = {duration_ms}ms")
            
        except Exception as e:
            logger.error(f"StatsD timing failed: {e}")
    
    def histogram(self, metric_name: str, value: float, tags: Dict[str, str] = None):
        """Record a histogram metric (using timing for compatibility)"""
        self.timing(metric_name, value, tags)

# Global instance
framework_stats = FrameworkStatsD()

class FrameworkMetricsCollector:
    """Collects and sends framework-specific metrics via StatsD"""
    
    def __init__(self):
        self.stats = framework_stats
    
    def record_data_quality_score(self, dag_id: str, task_id: str, rule_name: str, score: float):
        """Record data quality score"""
        self.stats.gauge(
            "data_quality.score",
            score,
            tags={"dag_id": dag_id, "task_id": task_id, "rule": rule_name}
        )
    
    def record_data_quality_failure(self, dag_id: str, task_id: str, rule_name: str):
        """Record data quality failure"""
        self.stats.incr(
            "data_quality.failed",
            tags={"dag_id": dag_id, "task_id": task_id, "rule": rule_name}
        )
    
    def record_records_processed(self, dag_id: str, task_id: str, count: int):
        """Record number of records processed"""
        self.stats.gauge(
            "records_processed",
            count,
            tags={"dag_id": dag_id, "task_id": task_id}
        )
    
    def record_pipeline_duration(self, dag_id: str, duration_seconds: float):
        """Record end-to-end pipeline duration"""
        self.stats.timing(
            "pipeline_duration",
            duration_seconds * 1000,  # Convert to milliseconds
            tags={"dag_id": dag_id}
        )
    
    def record_extraction_metrics(self, dag_id: str, task_id: str, source_type: str, 
                                records_extracted: int, duration_seconds: float):
        """Record extraction metrics"""
        self.stats.gauge(
            "extraction.records",
            records_extracted,
            tags={"dag_id": dag_id, "task_id": task_id, "source_type": source_type}
        )
        
        self.stats.timing(
            "extraction.duration",
            duration_seconds * 1000,
            tags={"dag_id": dag_id, "task_id": task_id, "source_type": source_type}
        )
    
    def record_loading_metrics(self, dag_id: str, task_id: str, sink_type: str,
                             records_loaded: int, duration_seconds: float):
        """Record loading metrics"""
        self.stats.gauge(
            "loading.records",
            records_loaded,
            tags={"dag_id": dag_id, "task_id": task_id, "sink_type": sink_type}
        )
        
        self.stats.timing(
            "loading.duration",
            duration_seconds * 1000,
            tags={"dag_id": dag_id, "task_id": task_id, "sink_type": sink_type}
        )
    
    def record_transform_metrics(self, dag_id: str, task_id: str, transform_type: str,
                               input_records: int, output_records: int, duration_seconds: float):
        """Record transformation metrics"""
        self.stats.gauge(
            "transform.input_records",
            input_records,
            tags={"dag_id": dag_id, "task_id": task_id, "type": transform_type}
        )
        
        self.stats.gauge(
            "transform.output_records", 
            output_records,
            tags={"dag_id": dag_id, "task_id": task_id, "type": transform_type}
        )
        
        self.stats.timing(
            "transform.duration",
            duration_seconds * 1000,
            tags={"dag_id": dag_id, "task_id": task_id, "type": transform_type}
        )
        
        # Calculate transformation ratio
        if input_records > 0:
            ratio = output_records / input_records
            self.stats.gauge(
                "transform.ratio",
                ratio,
                tags={"dag_id": dag_id, "task_id": task_id, "type": transform_type}
            )

# Global metrics collector
metrics_collector = FrameworkMetricsCollector()

class TimingContext:
    """Context manager for timing operations"""
    
    def __init__(self, metric_name: str, tags: Dict[str, str] = None):
        self.metric_name = metric_name
        self.tags = tags or {}
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            duration = time.time() - self.start_time
            framework_stats.timing(self.metric_name, duration * 1000, self.tags)

def time_operation(metric_name: str, tags: Dict[str, str] = None):
    """Decorator for timing operations"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            with TimingContext(metric_name, tags):
                return func(*args, **kwargs)
        return wrapper
    return decorator

def get_monitoring_callbacks() -> Dict[str, Any]:
    """Get monitoring callbacks that integrate with StatsD"""
    
    def on_success_callback(context):
        """Task success callback with StatsD metrics"""
        try:
            dag_id = context['dag'].dag_id
            task_id = context['task'].task_id
            
            # Airflow will automatically send basic metrics via StatsD
            # We just add any custom framework metrics here
            logger.info(f"✅ Task succeeded: {dag_id}.{task_id}")
            
            # Custom framework success metrics
            framework_stats.incr(
                "task.framework_success",
                tags={"dag_id": dag_id, "task_id": task_id}
            )
            
        except Exception as e:
            logger.error(f"Monitoring callback error: {e}")
    
    def on_failure_callback(context):
        """Task failure callback with StatsD metrics"""
        try:
            dag_id = context['dag'].dag_id
            task_id = context['task'].task_id
            exception = context.get('exception', 'Unknown error')
            
            logger.error(f"❌ Task failed: {dag_id}.{task_id} - {exception}")
            
            # Custom framework failure metrics
            framework_stats.incr(
                "task.framework_failure",
                tags={"dag_id": dag_id, "task_id": task_id}
            )
            
        except Exception as e:
            logger.error(f"Monitoring callback error: {e}")
    
    def on_retry_callback(context):
        """Task retry callback with StatsD metrics"""
        try:
            dag_id = context['dag'].dag_id
            task_id = context['task'].task_id
            
            logger.warning(f"🔄 Task retrying: {dag_id}.{task_id}")
            
            # Custom framework retry metrics
            framework_stats.incr(
                "task.framework_retry",
                tags={"dag_id": dag_id, "task_id": task_id}
            )
            
        except Exception as e:
            logger.error(f"Monitoring callback error: {e}")
    
    return {
        'on_success_callback': on_success_callback,
        'on_failure_callback': on_failure_callback,
        'on_retry_callback': on_retry_callback,
    }

def get_framework_health() -> Dict[str, Any]:
    """Get framework health status with StatsD integration"""
    try:
        health_status = {
            'timestamp': datetime.now().isoformat(),
            'status': 'healthy',
            'statsd_enabled': framework_stats.enabled,
            'metrics_collector_enabled': True,
        }
        
        # Record health check metric
        framework_stats.incr("health.check")
        framework_stats.gauge("health.status", 1.0)  # 1.0 = healthy
        
        return health_status
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        framework_stats.gauge("health.status", 0.0)  # 0.0 = unhealthy
        return {
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }

# Export key components
__all__ = [
    'FrameworkStatsD',
    'FrameworkMetricsCollector',
    'TimingContext',
    'time_operation',
    'get_monitoring_callbacks',
    'get_framework_health',
    'framework_stats',
    'metrics_collector'
]

logger.info("✅ StatsD metrics integration loaded successfully")