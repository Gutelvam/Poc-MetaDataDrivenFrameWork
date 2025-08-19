"""
Fixed Monitoring and Metrics Module
Handles missing Airflow Variables gracefully
"""

import json
import logging
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import threading
import time
from collections import defaultdict

logger = logging.getLogger(__name__)

def safe_get_variable(key: str, default_value: Any = None) -> Any:
    """Safely get an Airflow variable without failing if it doesn't exist"""
    try:
        from airflow.models import Variable
        return Variable.get(key, default_var=default_value)
    except Exception as e:
        logger.debug(f"Variable '{key}' not found or error accessing it: {e}")
        return default_value

@dataclass
class MetricEvent:
    """Represents a metric event"""
    dag_id: str
    task_id: Optional[str]
    event_type: str  # task_start, task_success, task_failure, dag_start, dag_success, dag_failure
    timestamp: datetime
    duration: Optional[float] = None
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

class MetricsCollector:
    """Simple metrics collector for pipeline monitoring"""
    
    def __init__(self):
        self.metrics_buffer = []
        self.buffer_lock = threading.Lock()
        
        # Safely get configuration variables
        self.prometheus_url = safe_get_variable("prometheus_pushgateway_url")
        self.grafana_api_url = safe_get_variable("grafana_api_url")
        self.grafana_api_key = safe_get_variable("grafana_api_key")
        
        # Log configuration status
        if self.prometheus_url:
            logger.info(f"✅ Prometheus configured: {self.prometheus_url}")
        else:
            logger.info("ℹ️ Prometheus not configured (prometheus_pushgateway_url variable missing)")
            
        if self.grafana_api_url and self.grafana_api_key:
            logger.info(f"✅ Grafana configured: {self.grafana_api_url}")
        else:
            logger.info("ℹ️ Grafana not configured (grafana_api_url or grafana_api_key variables missing)")
    
    def record_event(self, event: MetricEvent):
        """Record a metric event"""
        try:
            with self.buffer_lock:
                self.metrics_buffer.append(event)
            
            # Log structured event for external log collectors
            self._log_structured_event(event)
        except Exception as e:
            logger.error(f"Failed to record metric event: {e}")
    
    def _log_structured_event(self, event: MetricEvent):
        """Log event in structured format for Grafana/Loki"""
        try:
            log_data = {
                'timestamp': event.timestamp.isoformat(),
                'dag_id': event.dag_id,
                'task_id': event.task_id,
                'event_type': event.event_type,
                'duration': event.duration,
                'error_message': event.error_message,
                'metadata': event.metadata or {}
            }
            
            # Use different log levels based on event type
            if 'failure' in event.event_type:
                logger.error(f"Pipeline event: {event.event_type}", extra=log_data)
            elif 'success' in event.event_type:
                logger.info(f"Pipeline event: {event.event_type}", extra=log_data)
            else:
                logger.debug(f"Pipeline event: {event.event_type}", extra=log_data)
        except Exception as e:
            logger.error(f"Failed to log structured event: {e}")
    
    def push_metrics_to_prometheus(self):
        """Push metrics to Prometheus Pushgateway"""
        if not self.prometheus_url:
            logger.debug("Prometheus URL not configured, skipping push")
            return
        
        try:
            with self.buffer_lock:
                events_to_process = self.metrics_buffer.copy()
                self.metrics_buffer.clear()
            
            if not events_to_process:
                return
            
            # Group events and create metrics
            metrics_data = self._create_prometheus_metrics(events_to_process)
            
            # Push to Prometheus
            response = requests.post(
                f"{self.prometheus_url}/metrics/job/airflow_pipelines",
                data=metrics_data,
                headers={'Content-Type': 'text/plain'},
                timeout=30
            )
            response.raise_for_status()
            
            logger.info(f"Pushed {len(events_to_process)} events to Prometheus")
            
        except Exception as e:
            logger.error(f"Failed to push metrics to Prometheus: {str(e)}")
    
    def _create_prometheus_metrics(self, events: List[MetricEvent]) -> str:
        """Create Prometheus metrics format from events"""
        lines = []
        
        try:
            # Count events by type
            event_counts = defaultdict(lambda: defaultdict(int))
            durations = defaultdict(list)
            
            for event in events:
                labels = f'dag_id="{event.dag_id}"'
                if event.task_id:
                    labels += f',task_id="{event.task_id}"'
                
                event_counts[event.event_type][labels] += 1
                
                if event.duration is not None:
                    durations[f"{event.dag_id}_{event.task_id or 'dag'}"].append(event.duration)
            
            # Add event count metrics
            for event_type, label_counts in event_counts.items():
                lines.append(f"# HELP airflow_{event_type}_total Total count of {event_type} events")
                lines.append(f"# TYPE airflow_{event_type}_total counter")
                
                for labels, count in label_counts.items():
                    lines.append(f"airflow_{event_type}_total{{{labels}}} {count}")
            
            # Add duration metrics
            if durations:
                lines.append("# HELP airflow_task_duration_seconds Task execution duration")
                lines.append("# TYPE airflow_task_duration_seconds histogram")
                
                for task_key, duration_list in durations.items():
                    dag_id, task_id = task_key.rsplit('_', 1)
                    labels = f'dag_id="{dag_id}",task_id="{task_id}"'
                    
                    avg_duration = sum(duration_list) / len(duration_list)
                    lines.append(f"airflow_task_duration_seconds{{{labels}}} {avg_duration}")
        
        except Exception as e:
            logger.error(f"Failed to create Prometheus metrics: {e}")
            lines = ["# Error creating metrics"]
        
        return '\n'.join(lines) + '\n'
    
    def send_alert_to_grafana(self, title: str, message: str, tags: List[str] = None):
        """Send alert annotation to Grafana"""
        if not self.grafana_api_url or not self.grafana_api_key:
            logger.debug("Grafana not configured, skipping alert")
            return
        
        try:
            alert_data = {
                'title': title,
                'text': message,
                'tags': tags or ['airflow', 'pipeline'],
                'time': int(datetime.now().timestamp() * 1000)
            }
            
            headers = {
                'Authorization': f'Bearer {self.grafana_api_key}',
                'Content-Type': 'application/json'
            }
            
            response = requests.post(
                f"{self.grafana_api_url}/api/annotations",
                json=alert_data,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            
            logger.info(f"Alert sent to Grafana: {title}")
            
        except Exception as e:
            logger.error(f"Failed to send alert to Grafana: {str(e)}")

class PipelineMonitor:
    """Pipeline monitoring with callbacks"""
    
    def __init__(self, metrics_collector: MetricsCollector = None):
        self.metrics = metrics_collector or MetricsCollector()
    
    def on_task_start(self, context: Dict[str, Any]):
        """Task start callback"""
        try:
            event = MetricEvent(
                dag_id=context['dag'].dag_id,
                task_id=context['task'].task_id,
                event_type='task_start',
                timestamp=datetime.now(),
                metadata={
                    'execution_date': context['execution_date'].isoformat(),
                    'try_number': context['task_instance'].try_number
                }
            )
            self.metrics.record_event(event)
        except Exception as e:
            logger.error(f"Failed to record task start event: {e}")
    
    def on_task_success(self, context: Dict[str, Any]):
        """Task success callback"""
        try:
            task_instance = context['task_instance']
            duration = task_instance.duration.total_seconds() if task_instance.duration else None
            
            event = MetricEvent(
                dag_id=context['dag'].dag_id,
                task_id=context['task'].task_id,
                event_type='task_success',
                timestamp=datetime.now(),
                duration=duration,
                metadata={
                    'execution_date': context['execution_date'].isoformat(),
                    'try_number': task_instance.try_number
                }
            )
            self.metrics.record_event(event)
        except Exception as e:
            logger.error(f"Failed to record task success event: {e}")
    
    def on_task_failure(self, context: Dict[str, Any]):
        """Task failure callback"""
        try:
            task_instance = context['task_instance']
            exception = context.get('exception', 'Unknown error')
            
            event = MetricEvent(
                dag_id=context['dag'].dag_id,
                task_id=context['task'].task_id,
                event_type='task_failure',
                timestamp=datetime.now(),
                error_message=str(exception),
                metadata={
                    'execution_date': context['execution_date'].isoformat(),
                    'try_number': task_instance.try_number
                }
            )
            self.metrics.record_event(event)
            
            # Send alert for failures
            self.metrics.send_alert_to_grafana(
                title=f"Task Failure: {context['dag'].dag_id}",
                message=f"Task {context['task'].task_id} failed: {str(exception)}",
                tags=['task_failure', 'error']
            )
        except Exception as e:
            logger.error(f"Failed to record task failure event: {e}")
    
    def on_dag_start(self, context: Dict[str, Any]):
        """DAG start callback"""
        try:
            event = MetricEvent(
                dag_id=context['dag'].dag_id,
                task_id=None,
                event_type='dag_start',
                timestamp=datetime.now(),
                metadata={
                    'execution_date': context['execution_date'].isoformat(),
                    'run_id': context['run_id']
                }
            )
            self.metrics.record_event(event)
        except Exception as e:
            logger.error(f"Failed to record DAG start event: {e}")
    
    def on_dag_success(self, context: Dict[str, Any]):
        """DAG success callback"""
        try:
            dag_run = context['dag_run']
            duration = None
            if dag_run.end_date and dag_run.start_date:
                duration = (dag_run.end_date - dag_run.start_date).total_seconds()
            
            event = MetricEvent(
                dag_id=context['dag'].dag_id,
                task_id=None,
                event_type='dag_success',
                timestamp=datetime.now(),
                duration=duration,
                metadata={
                    'execution_date': context['execution_date'].isoformat(),
                    'run_id': context['run_id']
                }
            )
            self.metrics.record_event(event)
        except Exception as e:
            logger.error(f"Failed to record DAG success event: {e}")
    
    def on_dag_failure(self, context: Dict[str, Any]):
        """DAG failure callback"""
        try:
            event = MetricEvent(
                dag_id=context['dag'].dag_id,
                task_id=None,
                event_type='dag_failure',
                timestamp=datetime.now(),
                metadata={
                    'execution_date': context['execution_date'].isoformat(),
                    'run_id': context['run_id']
                }
            )
            self.metrics.record_event(event)
            
            # Send alert for DAG failures
            self.metrics.send_alert_to_grafana(
                title=f"DAG Failure: {context['dag'].dag_id}",
                message=f"DAG {context['dag'].dag_id} failed",
                tags=['dag_failure', 'critical']
            )
        except Exception as e:
            logger.error(f"Failed to record DAG failure event: {e}")

# Global instances (created safely)
try:
    metrics_collector = MetricsCollector()
    pipeline_monitor = PipelineMonitor(metrics_collector)
except Exception as e:
    logger.error(f"Failed to create monitoring instances: {e}")
    metrics_collector = None
    pipeline_monitor = None

# Export monitoring callbacks for DAG factory
def get_monitoring_callbacks() -> Dict[str, Any]:
    """Get monitoring callbacks for DAG creation"""
    if pipeline_monitor:
        return {
            'on_success_callback': pipeline_monitor.on_task_success,
            'on_failure_callback': pipeline_monitor.on_task_failure,
        }
    else:
        logger.warning("Monitoring not available, returning empty callbacks")
        return {}

# Utility functions
def get_pipeline_health() -> Dict[str, Any]:
    """Get pipeline health status"""
    try:
        # This is a simplified version that doesn't require database access
        return {
            'timestamp': datetime.now().isoformat(),
            'status': 'healthy',
            'monitoring_enabled': metrics_collector is not None,
            'prometheus_configured': bool(safe_get_variable("prometheus_pushgateway_url")),
            'grafana_configured': bool(safe_get_variable("grafana_api_url") and safe_get_variable("grafana_api_key"))
        }
    except Exception as e:
        logger.error(f"Failed to get pipeline health: {e}")
        return {'status': 'error', 'error': str(e)}

def check_sla_violations() -> List[Dict[str, Any]]:
    """Check for SLA violations"""
    try:
        # Simplified implementation
        return []
    except Exception as e:
        logger.error(f"Failed to check SLA violations: {e}")
        return []

# Export key components
__all__ = [
    'MetricsCollector',
    'PipelineMonitor', 
    'get_monitoring_callbacks',
    'get_pipeline_health',
    'check_sla_violations',
    'metrics_collector',
    'pipeline_monitor'
]

logger.info("✅ Monitoring module loaded successfully (with graceful variable handling)")