"""
Monitoring and Metrics Module
Simplified monitoring with Grafana integration
"""

import json
import logging
import requests
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import threading
import time
from collections import defaultdict

from airflow.models import Variable
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

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
        self.prometheus_url = Variable.get("prometheus_pushgateway_url", default_var=None)
        self.grafana_api_url = Variable.get("grafana_api_url", default_var=None)
        self.grafana_api_key = Variable.get("grafana_api_key", default_var=None)
    
    def record_event(self, event: MetricEvent):
        """Record a metric event"""
        with self.buffer_lock:
            self.metrics_buffer.append(event)
        
        # Log structured event for external log collectors
        self._log_structured_event(event)
    
    def _log_structured_event(self, event: MetricEvent):
        """Log event in structured format for Grafana/Loki"""
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
    
    def push_metrics_to_prometheus(self):
        """Push metrics to Prometheus Pushgateway"""
        if not self.prometheus_url:
            logger.debug("Prometheus URL not configured, skipping push")
            return
        
        with self.buffer_lock:
            events_to_process = self.metrics_buffer.copy()
            self.metrics_buffer.clear()
        
        if not events_to_process:
            return
        
        try:
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
        
        return '\n'.join(lines) + '\n'
    
    def send_alert_to_grafana(self, title: str, message: str, tags: List[str] = None):
        """Send alert annotation to Grafana"""
        if not self.grafana_api_url or not self.grafana_api_key:
            logger.debug("Grafana not configured, skipping alert")
            return
        
        alert_data = {
            'title': title,
            'text': message,
            'tags': tags or ['airflow', 'pipeline'],
            'time': int(datetime.now().timestamp() * 1000)
        }
        
        try:
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
    
    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics = metrics_collector
    
    def on_task_start(self, context: Dict[str, Any]):
        """Task start callback"""
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
    
    def on_task_success(self, context: Dict[str, Any]):
        """Task success callback"""
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
    
    def on_task_failure(self, context: Dict[str, Any]):
        """Task failure callback"""
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
    
    def on_dag_start(self, context: Dict[str, Any]):
        """DAG start callback"""
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
    
    def on_dag_success(self, context: Dict[str, Any]):
        """DAG success callback"""
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
    
    def on_dag_failure(self, context: Dict[str, Any]):
        """DAG failure callback"""
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

class HealthChecker:
    """Pipeline health monitoring"""
    
    @staticmethod
    def get_pipeline_health() -> Dict[str, Any]:
        """Get overall pipeline health status"""
        from airflow.models import DagRun, TaskInstance
        from airflow.utils.db import provide_session
        from airflow.utils.state import State
        
        @provide_session
        def _get_health_data(session=None):
            # Get recent DAG runs (last 24 hours)
            recent_runs = session.query(DagRun).filter(
                DagRun.execution_date >= datetime.now() - timedelta(days=1)
            ).all()
            
            # Calculate health metrics
            total_runs = len(recent_runs)
            successful_runs = len([r for r in recent_runs if r.state == State.SUCCESS])
            failed_runs = len([r for r in recent_runs if r.state == State.FAILED])
            running_runs = len([r for r in recent_runs if r.state == State.RUNNING])
            
            success_rate = (successful_runs / total_runs * 100) if total_runs > 0 else 0
            
            # Get pipeline status by DAG
            pipeline_status = {}
            dag_groups = defaultdict(list)
            for run in recent_runs:
                dag_groups[run.dag_id].append(run)
            
            for dag_id, runs in dag_groups.items():
                latest_run = max(runs, key=lambda r: r.execution_date)
                pipeline_status[dag_id] = {
                    'state': latest_run.state,
                    'execution_date': latest_run.execution_date.isoformat(),
                    'duration': latest_run.duration.total_seconds() if latest_run.duration else None,
                    'runs_today': len(runs)
                }
            
            return {
                'timestamp': datetime.now().isoformat(),
                'summary': {
                    'total_runs': total_runs,
                    'successful_runs': successful_runs,
                    'failed_runs': failed_runs,
                    'running_runs': running_runs,
                    'success_rate': success_rate
                },
                'pipelines': pipeline_status
            }
        
        return _get_health_data()
    
    @staticmethod
    def check_pipeline_sla_violations() -> List[Dict[str, Any]]:
        """Check for SLA violations"""
        from airflow.models import SlaMiss
        from airflow.utils.db import provide_session
        
        @provide_session
        def _get_sla_violations(session=None):
            violations = session.query(SlaMiss).filter(
                SlaMiss.timestamp >= datetime.now() - timedelta(hours=24)
            ).all()
            
            return [
                {
                    'dag_id': v.dag_id,
                    'task_id': v.task_id,
                    'execution_date': v.execution_date.isoformat(),
                    'timestamp': v.timestamp.isoformat()
                }
                for v in violations
            ]
        
        return _get_sla_violations()

class ReprocessingManager:
    """Manages pipeline reprocessing operations"""
    
    @staticmethod
    def trigger_reprocessing(dag_id: str, start_date: str, end_date: str, 
                           reason: str = "Manual reprocessing") -> Dict[str, Any]:
        """Trigger reprocessing for a date range"""
        from airflow.api.common.experimental.trigger_dag import trigger_dag
        from datetime import datetime, timedelta
        
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            if start_dt > end_dt:
                raise ValueError("Start date must be before end date")
            
            # Generate execution dates
            current_date = start_dt
            triggered_runs = []
            
            while current_date <= end_dt:
                try:
                    conf = {
                        'is_reprocessing': True,
                        'reprocessing_reason': reason,
                        'original_execution_date': current_date.isoformat()
                    }
                    
                    dag_run = trigger_dag(
                        dag_id=dag_id,
                        execution_date=current_date,
                        conf=conf,
                        replace_microseconds=False
                    )
                    
                    triggered_runs.append({
                        'execution_date': current_date.isoformat(),
                        'dag_run_id': dag_run.run_id,
                        'status': 'triggered'
                    })
                    
                except Exception as e:
                    triggered_runs.append({
                        'execution_date': current_date.isoformat(),
                        'error': str(e),
                        'status': 'failed'
                    })
                
                current_date += timedelta(days=1)
            
            successful_count = len([r for r in triggered_runs if r['status'] == 'triggered'])
            
            logger.info(f"Reprocessing triggered for {dag_id}: {successful_count}/{len(triggered_runs)} runs")
            
            return {
                'dag_id': dag_id,
                'start_date': start_date,
                'end_date': end_date,
                'reason': reason,
                'total_runs': len(triggered_runs),
                'successful_triggers': successful_count,
                'runs': triggered_runs,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Reprocessing failed for {dag_id}: {str(e)}")
            raise AirflowException(f"Reprocessing failed: {str(e)}")

# Background metrics pushing
class MetricsPusher:
    """Background thread for pushing metrics"""
    
    def __init__(self, metrics_collector: MetricsCollector, push_interval: int = 60):
        self.metrics_collector = metrics_collector
        self.push_interval = push_interval
        self.running = False
        self.thread = None
    
    def start(self):
        """Start metrics pushing thread"""
        if self.running:
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._push_loop, daemon=True)
        self.thread.start()
        logger.info("Metrics pusher started")
    
    def stop(self):
        """Stop metrics pushing thread"""
        self.running = False
        if self.thread:
            self.thread.join()
        logger.info("Metrics pusher stopped")
    
    def _push_loop(self):
        """Main push loop"""
        while self.running:
            try:
                self.metrics_collector.push_metrics_to_prometheus()
                time.sleep(self.push_interval)
            except Exception as e:
                logger.error(f"Error in metrics push loop: {str(e)}")
                time.sleep(self.push_interval)

# Global instances
metrics_collector = MetricsCollector()
pipeline_monitor = PipelineMonitor(metrics_collector)
metrics_pusher = MetricsPusher(metrics_collector)

# Start metrics pusher
metrics_pusher.start()

# Export monitoring callbacks for DAG factory
def get_monitoring_callbacks() -> Dict[str, Any]:
    """Get monitoring callbacks for DAG creation"""
    return {
        'on_success_callback': pipeline_monitor.on_task_success,
        'on_failure_callback': pipeline_monitor.on_task_failure,
        'on_dag_success_callback': pipeline_monitor.on_dag_success,
        'on_dag_failure_callback': pipeline_monitor.on_dag_failure,
    }

# Utility functions
def get_pipeline_health() -> Dict[str, Any]:
    """Get pipeline health status"""
    return HealthChecker.get_pipeline_health()

def trigger_pipeline_reprocessing(dag_id: str, start_date: str, end_date: str, 
                                reason: str = "Manual reprocessing") -> Dict[str, Any]:
    """Trigger pipeline reprocessing"""
    return ReprocessingManager.trigger_reprocessing(dag_id, start_date, end_date, reason)

def check_sla_violations() -> List[Dict[str, Any]]:
    """Check for SLA violations"""
    return HealthChecker.check_pipeline_sla_violations()

# Export key components
__all__ = [
    'MetricsCollector',
    'PipelineMonitor',
    'HealthChecker',
    'ReprocessingManager',
    'get_monitoring_callbacks',
    'get_pipeline_health',
    'trigger_pipeline_reprocessing',
    'check_sla_violations',
    'metrics_collector',
    'pipeline_monitor'
]