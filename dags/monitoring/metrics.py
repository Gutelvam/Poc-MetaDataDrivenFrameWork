# dags/monitoring/enhanced_metrics.py
"""
Enhanced Metrics Collection for Framework - FIXED for Airflow 3.x
Exposes comprehensive metrics via HTTP endpoint for Prometheus/Grafana
"""

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
import threading
from collections import defaultdict, deque
from flask import Flask, Response
import prometheus_client
from prometheus_client import Counter, Histogram, Gauge, Summary, CollectorRegistry, generate_latest

# Safe imports for Airflow compatibility
try:
    from airflow.models import DagRun, TaskInstance
    from airflow.utils.db import provide_session
    from airflow.utils.state import State
    from sqlalchemy import func, and_
    AIRFLOW_DB_AVAILABLE = True
except ImportError as e:
    AIRFLOW_DB_AVAILABLE = False
    logging.warning(f"Airflow DB imports not available: {e}")

logger = logging.getLogger(__name__)

# Global metrics registry
METRICS_REGISTRY = CollectorRegistry()

# ==== ENHANCED PROMETHEUS METRICS FOR GRAFANA ====

# DAG-level metrics
DAG_RUNS_TOTAL = Counter(
    'airflow_dag_runs_total',
    'Total number of DAG runs',
    ['dag_id', 'status'],
    registry=METRICS_REGISTRY
)

DAG_SUCCESS_RATE = Gauge(
    'airflow_dag_success_rate',
    'DAG success rate (percentage)',
    ['dag_id'],
    registry=METRICS_REGISTRY
)

DAG_DURATION = Histogram(
    'airflow_dag_duration_seconds',
    'DAG execution duration in seconds',
    ['dag_id'],
    buckets=[30, 60, 300, 600, 1800, 3600, 7200, 14400, 28800],
    registry=METRICS_REGISTRY
)

DAG_LAST_SUCCESS = Gauge(
    'airflow_dag_last_success_timestamp',
    'Timestamp of last successful DAG run',
    ['dag_id'],
    registry=METRICS_REGISTRY
)

DAG_LAST_FAILURE = Gauge(
    'airflow_dag_last_failure_timestamp', 
    'Timestamp of last failed DAG run',
    ['dag_id'],
    registry=METRICS_REGISTRY
)

DAG_CONSECUTIVE_FAILURES = Gauge(
    'airflow_dag_consecutive_failures',
    'Number of consecutive failures',
    ['dag_id'],
    registry=METRICS_REGISTRY
)

# Task-level metrics
TASK_RUNS_TOTAL = Counter(
    'airflow_task_runs_total',
    'Total number of task runs',
    ['dag_id', 'task_id', 'status'],
    registry=METRICS_REGISTRY
)

TASK_DURATION = Histogram(
    'airflow_task_duration_seconds',
    'Task execution duration in seconds',
    ['dag_id', 'task_id'],
    buckets=[1, 5, 10, 30, 60, 300, 600, 1800, 3600],
    registry=METRICS_REGISTRY
)

TASK_SUCCESS_RATE = Gauge(
    'airflow_task_success_rate',
    'Task success rate (percentage)',
    ['dag_id', 'task_id'],
    registry=METRICS_REGISTRY
)

# Data processing metrics
RECORDS_PROCESSED = Counter(
    'airflow_records_processed_total',
    'Total records processed',
    ['dag_id', 'task_id', 'source_type'],
    registry=METRICS_REGISTRY
)

DATA_QUALITY_SCORE = Gauge(
    'airflow_data_quality_score',
    'Data quality score (0-1)',
    ['dag_id', 'task_id', 'rule_name'],
    registry=METRICS_REGISTRY
)

# System metrics
ACTIVE_TASKS = Gauge(
    'airflow_active_tasks',
    'Number of currently active tasks',
    ['dag_id', 'status'],
    registry=METRICS_REGISTRY
)

FRAMEWORK_HEALTH = Gauge(
    'airflow_framework_health',
    'Framework health status (1=healthy, 0=unhealthy)',
    ['component'],
    registry=METRICS_REGISTRY
)

# Error metrics
ERROR_COUNT = Counter(
    'airflow_errors_total',
    'Total errors by type',
    ['dag_id', 'task_id', 'error_type'],
    registry=METRICS_REGISTRY
)

SLA_VIOLATIONS = Counter(
    'airflow_sla_violations_total',
    'Total SLA violations',
    ['dag_id'],
    registry=METRICS_REGISTRY
)

# Performance metrics
PIPELINE_THROUGHPUT = Gauge(
    'airflow_pipeline_throughput_records_per_second',
    'Pipeline throughput in records per second',
    ['dag_id'],
    registry=METRICS_REGISTRY
)

QUEUE_SIZE = Gauge(
    'airflow_queue_size',
    'Number of tasks in queue',
    ['dag_id'],
    registry=METRICS_REGISTRY
)

@dataclass
class MetricEvent:
    """Enhanced metric event with comprehensive metadata"""
    timestamp: datetime
    dag_id: str
    task_id: Optional[str]
    event_type: str
    status: str
    duration: Optional[float] = None
    records_processed: Optional[int] = None
    data_quality_score: Optional[float] = None
    error_type: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

class EnhancedMetricsCollector:
    """Enhanced metrics collector with Airflow 3.x compatibility"""
    
    def __init__(self):
        self.events_buffer = deque(maxlen=10000)
        self.buffer_lock = threading.Lock()
        self.last_db_update = datetime.now()
        
        # Enhanced tracking for success rates
        self.dag_counters = defaultdict(lambda: {
            'total_runs': 0,
            'successful_runs': 0,
            'failed_runs': 0,
            'consecutive_failures': 0,
            'last_success': None,
            'last_failure': None,
            'durations': deque(maxlen=100)  # Track last 100 runs
        })
        
        self.task_counters = defaultdict(lambda: {
            'total_runs': 0,
            'successful_runs': 0,
            'failed_runs': 0,
            'durations': deque(maxlen=50)
        })
        
        # Task start times for duration calculation
        self.task_start_times = {}
        
        # Start background thread for database updates
        if AIRFLOW_DB_AVAILABLE:
            self.update_thread = threading.Thread(target=self._update_loop, daemon=True)
            self.update_thread.start()
    
    def record_task_start(self, dag_id: str, task_id: str, run_id: str, **context):
        """Record task start time for duration calculation"""
        try:
            start_time = datetime.now()
            key = f"{dag_id}_{task_id}_{run_id}"
            self.task_start_times[key] = start_time
            
            event = MetricEvent(
                timestamp=start_time,
                dag_id=dag_id,
                task_id=task_id,
                event_type='task_start',
                status='running',
                metadata={
                    'run_id': run_id,
                    'try_number': context.get('task_instance', {}).get('try_number', 1)
                }
            )
            
            self.record_event(event)
            logger.debug(f"Recorded task start: {dag_id}.{task_id}")
            
        except Exception as e:
            logger.warning(f"Failed to record task start: {str(e)}")
    
    def record_event(self, event: MetricEvent):
        """Record a metric event and update all metrics"""
        try:
            with self.buffer_lock:
                self.events_buffer.append(event)
            
            # Update Prometheus metrics immediately
            self._update_prometheus_metrics(event)
            
            # Update in-memory counters
            self._update_counters(event)
            
        except Exception as e:
            logger.error(f"Failed to record metric event: {e}")
    
    def _update_prometheus_metrics(self, event: MetricEvent):
        """Update Prometheus metrics based on event - FIXED for Airflow 3.x"""
        try:
            # DAG-level metrics
            if event.task_id is None:  # DAG-level event
                DAG_RUNS_TOTAL.labels(
                    dag_id=event.dag_id,
                    status=event.status
                ).inc()
                
                if event.duration:
                    DAG_DURATION.labels(dag_id=event.dag_id).observe(event.duration)
                
                # Update success/failure timestamps
                current_time = time.time()
                if event.status == 'success':
                    DAG_LAST_SUCCESS.labels(dag_id=event.dag_id).set(current_time)
                elif event.status == 'failed':
                    DAG_LAST_FAILURE.labels(dag_id=event.dag_id).set(current_time)
                    SLA_VIOLATIONS.labels(dag_id=event.dag_id).inc()
            
            # Task-level metrics
            else:
                TASK_RUNS_TOTAL.labels(
                    dag_id=event.dag_id,
                    task_id=event.task_id,
                    status=event.status
                ).inc()
                
                if event.duration:
                    TASK_DURATION.labels(
                        dag_id=event.dag_id,
                        task_id=event.task_id
                    ).observe(event.duration)
                
                if event.records_processed:
                    source_type = event.metadata.get('source_type', 'unknown') if event.metadata else 'unknown'
                    RECORDS_PROCESSED.labels(
                        dag_id=event.dag_id,
                        task_id=event.task_id,
                        source_type=source_type
                    ).inc(event.records_processed)
                
                if event.data_quality_score is not None:
                    rule_name = event.metadata.get('rule_name', 'overall') if event.metadata else 'overall'
                    DATA_QUALITY_SCORE.labels(
                        dag_id=event.dag_id,
                        task_id=event.task_id,
                        rule_name=rule_name
                    ).set(event.data_quality_score)
                
                if event.error_type:
                    ERROR_COUNT.labels(
                        dag_id=event.dag_id,
                        task_id=event.task_id,
                        error_type=event.error_type
                    ).inc()
        
        except Exception as e:
            logger.error(f"Failed to update Prometheus metrics: {e}")
    
    def _update_counters(self, event: MetricEvent):
        """Update in-memory counters for success rates"""
        try:
            # DAG counters
            if event.task_id is None:
                dag_counter = self.dag_counters[event.dag_id]
                dag_counter['total_runs'] += 1
                
                if event.status == 'success':
                    dag_counter['successful_runs'] += 1
                    dag_counter['consecutive_failures'] = 0
                    dag_counter['last_success'] = event.timestamp
                elif event.status == 'failed':
                    dag_counter['failed_runs'] += 1
                    dag_counter['consecutive_failures'] += 1
                    dag_counter['last_failure'] = event.timestamp
                
                if event.duration:
                    dag_counter['durations'].append(event.duration)
                
                # Update success rate
                if dag_counter['total_runs'] > 0:
                    success_rate = (dag_counter['successful_runs'] / dag_counter['total_runs']) * 100
                    DAG_SUCCESS_RATE.labels(dag_id=event.dag_id).set(success_rate)
                
                # Update consecutive failures
                DAG_CONSECUTIVE_FAILURES.labels(dag_id=event.dag_id).set(dag_counter['consecutive_failures'])
            
            # Task counters
            else:
                task_key = f"{event.dag_id}.{event.task_id}"
                task_counter = self.task_counters[task_key]
                task_counter['total_runs'] += 1
                
                if event.status == 'success':
                    task_counter['successful_runs'] += 1
                elif event.status == 'failed':
                    task_counter['failed_runs'] += 1
                
                if event.duration:
                    task_counter['durations'].append(event.duration)
                
                # Update task success rate
                if task_counter['total_runs'] > 0:
                    success_rate = (task_counter['successful_runs'] / task_counter['total_runs']) * 100
                    TASK_SUCCESS_RATE.labels(
                        dag_id=event.dag_id,
                        task_id=event.task_id
                    ).set(success_rate)
        
        except Exception as e:
            logger.error(f"Failed to update counters: {e}")
    
    @provide_session
    def _update_from_database(self, session=None):
        """Update metrics from Airflow database"""
        if not AIRFLOW_DB_AVAILABLE:
            return
            
        try:
            # Update active tasks by status
            active_tasks_query = session.query(
                TaskInstance.dag_id,
                TaskInstance.state,
                func.count(TaskInstance.task_id).label('count')
            ).filter(
                TaskInstance.state.in_([State.RUNNING, State.QUEUED, State.SCHEDULED])
            ).group_by(TaskInstance.dag_id, TaskInstance.state)
            
            # Reset active task counters
            active_task_counts = defaultdict(lambda: defaultdict(int))
            
            for result in active_tasks_query:
                active_task_counts[result.dag_id][result.state] = result.count
                ACTIVE_TASKS.labels(
                    dag_id=result.dag_id,
                    status=result.state
                ).set(result.count)
            
            # Update queue sizes
            for dag_id, states in active_task_counts.items():
                queue_size = states.get(State.QUEUED, 0) + states.get(State.SCHEDULED, 0)
                QUEUE_SIZE.labels(dag_id=dag_id).set(queue_size)
            
            # Update framework health based on recent failures
            recent_failures = session.query(func.count(DagRun.dag_id)).filter(
                and_(
                    DagRun.state == State.FAILED,
                    DagRun.execution_date >= datetime.now() - timedelta(hours=1)
                )
            ).scalar()
            
            # Health metrics
            scheduler_health = 1 if recent_failures < 5 else 0
            FRAMEWORK_HEALTH.labels(component='scheduler').set(scheduler_health)
            
            db_health = 1  # If we got here, DB is working
            FRAMEWORK_HEALTH.labels(component='database').set(db_health)
            
            # Calculate throughput for recent DAG runs
            recent_runs = session.query(DagRun).filter(
                DagRun.execution_date >= datetime.now() - timedelta(minutes=10)
            ).all()
            
            for run in recent_runs:
                if run.end_date and run.start_date:
                    duration = (run.end_date - run.start_date).total_seconds()
                    # Estimate throughput (this would need actual record counts)
                    estimated_records = 1000  # Placeholder - you'd get this from task results
                    if duration > 0:
                        throughput = estimated_records / duration
                        PIPELINE_THROUGHPUT.labels(dag_id=run.dag_id).set(throughput)
            
        except Exception as e:
            logger.error(f"Failed to update from database: {e}")
            # Set framework health to unhealthy
            FRAMEWORK_HEALTH.labels(component='database').set(0)
    
    def _update_loop(self):
        """Background thread for periodic database updates"""
        while True:
            try:
                time.sleep(30)  # Update every 30 seconds
                self._update_from_database()
            except Exception as e:
                logger.error(f"Error in update loop: {e}")
                time.sleep(60)  # Longer sleep on error
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get comprehensive summary for Grafana dashboards"""
        summary = {
            'timestamp': datetime.now().isoformat(),
            'dag_metrics': {},
            'task_metrics': {},
            'system_metrics': {
                'events_buffered': len(self.events_buffer),
                'active_task_trackers': len(self.task_start_times),
                'framework_health': 'healthy' if len(self.events_buffer) < 9000 else 'warning'
            }
        }
        
        # DAG metrics summary
        for dag_id, counters in self.dag_counters.items():
            success_rate = 0
            if counters['total_runs'] > 0:
                success_rate = (counters['successful_runs'] / counters['total_runs']) * 100
            
            avg_duration = 0
            if counters['durations']:
                avg_duration = sum(counters['durations']) / len(counters['durations'])
            
            summary['dag_metrics'][dag_id] = {
                'total_runs': counters['total_runs'],
                'success_rate': round(success_rate, 2),
                'consecutive_failures': counters['consecutive_failures'],
                'avg_duration': round(avg_duration, 2),
                'last_success': counters['last_success'].isoformat() if counters['last_success'] else None,
                'last_failure': counters['last_failure'].isoformat() if counters['last_failure'] else None
            }
        
        # Task metrics summary  
        for task_key, counters in self.task_counters.items():
            success_rate = 0
            if counters['total_runs'] > 0:
                success_rate = (counters['successful_runs'] / counters['total_runs']) * 100
            
            avg_duration = 0
            if counters['durations']:
                avg_duration = sum(counters['durations']) / len(counters['durations'])
            
            summary['task_metrics'][task_key] = {
                'total_runs': counters['total_runs'],
                'success_rate': round(success_rate, 2),
                'avg_duration': round(avg_duration, 2)
            }
        
        return summary

class EnhancedPipelineMonitor:
    """Enhanced pipeline monitor with Airflow 3.x compatibility"""
    
    def __init__(self, metrics_collector: EnhancedMetricsCollector = None):
        self.metrics = metrics_collector or EnhancedMetricsCollector()
    
    def on_dag_start(self, context):
        """DAG start callback"""
        try:
            dag_id = context.get('dag', {}).get('dag_id', 'unknown')
            run_id = context.get('run_id', 'unknown')
            
            event = MetricEvent(
                timestamp=datetime.now(),
                dag_id=dag_id,
                task_id=None,
                event_type='dag_start',
                status='running',
                metadata={
                    'execution_date': context.get('execution_date', datetime.now()).isoformat(),
                    'run_id': run_id
                }
            )
            self.metrics.record_event(event)
        except Exception as e:
            logger.error(f"Failed to record DAG start: {e}")
    
    def on_dag_success(self, context):
        """DAG success callback - FIXED for Airflow 3.x"""
        try:
            dag_id = context.get('dag', {}).get('dag_id', 'unknown')
            run_id = context.get('run_id', 'unknown')
            
            # Safe duration calculation
            duration = self._calculate_dag_duration_safely(context)
            
            event = MetricEvent(
                timestamp=datetime.now(),
                dag_id=dag_id,
                task_id=None,
                event_type='dag_success',
                status='success',
                duration=duration,
                metadata={
                    'execution_date': context.get('execution_date', datetime.now()).isoformat(),
                    'run_id': run_id
                }
            )
            self.metrics.record_event(event)
            logger.info(f"DAG {dag_id} completed successfully in {duration:.2f}s")
        except Exception as e:
            logger.error(f"Failed to record DAG success: {e}")
    
    def on_dag_failure(self, context):
        """DAG failure callback"""
        try:
            dag_id = context.get('dag', {}).get('dag_id', 'unknown')
            run_id = context.get('run_id', 'unknown')
            exception = context.get('exception', 'Unknown error')
            
            event = MetricEvent(
                timestamp=datetime.now(),
                dag_id=dag_id,
                task_id=None,
                event_type='dag_failure',
                status='failed',
                error_type=type(exception).__name__ if exception else 'Unknown',
                metadata={
                    'execution_date': context.get('execution_date', datetime.now()).isoformat(),
                    'run_id': run_id,
                    'error_message': str(exception)[:200]
                }
            )
            self.metrics.record_event(event)
            logger.warning(f"DAG {dag_id} failed: {str(exception)[:100]}")
        except Exception as e:
            logger.error(f"Failed to record DAG failure: {e}")
    
    def on_task_start(self, context):
        """Task start callback"""
        try:
            dag_id = context.get('dag', {}).get('dag_id', 'unknown')
            task_id = context.get('task', {}).get('task_id', 'unknown')
            run_id = context.get('run_id', 'unknown')
            
            self.metrics.record_task_start(dag_id, task_id, run_id, **context)
        except Exception as e:
            logger.error(f"Failed to record task start: {e}")
    
    def on_task_success(self, context):
        """Task success callback - FIXED for Airflow 3.x"""
        try:
            dag_id = context.get('dag', {}).get('dag_id', 'unknown')
            task_id = context.get('task', {}).get('task_id', 'unknown')
            run_id = context.get('run_id', 'unknown')
            
            # Safe duration calculation for Airflow 3.x
            duration = self._calculate_task_duration_safely(dag_id, task_id, run_id, context)
            
            # Try to get records processed from XCom
            records_processed = self._extract_records_processed(context)
            
            event = MetricEvent(
                timestamp=datetime.now(),
                dag_id=dag_id,
                task_id=task_id,
                event_type='task_success',
                status='success',
                duration=duration,
                records_processed=records_processed,
                metadata={
                    'execution_date': context.get('execution_date', datetime.now()).isoformat(),
                    'run_id': run_id,
                    'operator': context.get('task', {}).__class__.__name__
                }
            )
            self.metrics.record_event(event)
            logger.info(f"Task {dag_id}.{task_id} completed successfully in {duration:.2f}s")
        except Exception as e:
            logger.error(f"Failed to record task success: {e}")
    
    def on_task_failure(self, context):
        """Task failure callback"""
        try:
            dag_id = context.get('dag', {}).get('dag_id', 'unknown')
            task_id = context.get('task', {}).get('task_id', 'unknown')
            run_id = context.get('run_id', 'unknown')
            exception = context.get('exception', 'Unknown error')
            
            # Safe duration calculation
            duration = self._calculate_task_duration_safely(dag_id, task_id, run_id, context)
            
            event = MetricEvent(
                timestamp=datetime.now(),
                dag_id=dag_id,
                task_id=task_id,
                event_type='task_failure',
                status='failed',
                duration=duration,
                error_type=type(exception).__name__ if exception else 'Unknown',
                metadata={
                    'execution_date': context.get('execution_date', datetime.now()).isoformat(),
                    'run_id': run_id,
                    'error_message': str(exception)[:200]
                }
            )
            self.metrics.record_event(event)
            logger.warning(f"Task {dag_id}.{task_id} failed: {str(exception)[:100]}")
        except Exception as e:
            logger.error(f"Failed to record task failure: {e}")
    
    def _calculate_dag_duration_safely(self, context) -> float:
        """Safely calculate DAG duration for Airflow 3.x"""
        try:
            dag_run = context.get('dag_run')
            if dag_run and hasattr(dag_run, 'end_date') and hasattr(dag_run, 'start_date'):
                if dag_run.end_date and dag_run.start_date:
                    return (dag_run.end_date - dag_run.start_date).total_seconds()
            
            # Fallback: use execution context
            execution_date = context.get('execution_date')
            if execution_date:
                return (datetime.now() - execution_date).total_seconds()
            
            return 0.0
        except Exception as e:
            logger.warning(f"Error calculating DAG duration: {e}")
            return 0.0
    
    def _calculate_task_duration_safely(self, dag_id: str, task_id: str, run_id: str, context) -> float:
        """Safely calculate task duration for Airflow 3.x - FIXED"""
        try:
            # Method 1: Use our tracked start time
            key = f"{dag_id}_{task_id}_{run_id}"
            start_time = self.metrics.task_start_times.get(key)
            
            if start_time:
                duration = (datetime.now() - start_time).total_seconds()
                # Clean up the start time
                del self.metrics.task_start_times[key]
                return duration
            
            # Method 2: Try to get from task instance (if available)
            task_instance = context.get('task_instance')
            if task_instance:
                # Check if it has duration attribute (older Airflow)
                if hasattr(task_instance, 'duration') and task_instance.duration:
                    if hasattr(task_instance.duration, 'total_seconds'):
                        return task_instance.duration.total_seconds()
                    return float(task_instance.duration)
                
                # Check for start/end dates
                if hasattr(task_instance, 'start_date') and hasattr(task_instance, 'end_date'):
                    start = getattr(task_instance, 'start_date', None)
                    end = getattr(task_instance, 'end_date', None)
                    
                    if start and end:
                        return (end - start).total_seconds()
            
            # Method 3: Use execution context as fallback
            execution_date = context.get('execution_date')
            if execution_date:
                return (datetime.now() - execution_date).total_seconds()
            
            return 0.0
            
        except Exception as e:
            logger.warning(f"Error calculating task duration: {e}")
            return 0.0
    
    def _extract_records_processed(self, context) -> Optional[int]:
        """Extract number of records processed from task result"""
        try:
            task_instance = context.get('task_instance')
            task_id = context.get('task', {}).get('task_id')
            
            if not task_instance or not task_id:
                return None
            
            # Try to get result from XCom
            result = task_instance.xcom_pull(task_ids=task_id)
            
            # Handle different result types
            if isinstance(result, int):
                return result  # Sink operators return record counts
            elif isinstance(result, dict):
                return result.get('records_processed') or len(result.get('data', []))
            elif isinstance(result, list):
                return len(result)
            
            return None
            
        except Exception as e:
            logger.debug(f"Could not extract records processed: {e}")
            return None
    
    def record_data_quality_result(self, dag_id: str, task_id: str, rule_name: str, score: float):
        """Record data quality metrics"""
        try:
            event = MetricEvent(
                timestamp=datetime.now(),
                dag_id=dag_id,
                task_id=task_id,
                event_type='quality_check',
                status='completed',
                data_quality_score=score,
                metadata={'rule_name': rule_name}
            )
            self.metrics.record_event(event)
        except Exception as e:
            logger.error(f"Failed to record data quality result: {e}")

class MetricsExporter:
    """HTTP server to expose metrics to Prometheus/Grafana"""
    
    def __init__(self, collector: EnhancedMetricsCollector, port: int = 8090):
        self.collector = collector
        self.port = port
        self.app = Flask(__name__)
        self._setup_routes()
    
    def _setup_routes(self):
        """Setup Flask routes for metrics"""
        
        @self.app.route('/metrics')
        def metrics():
            """Prometheus metrics endpoint"""
            return Response(
                generate_latest(METRICS_REGISTRY),
                mimetype='text/plain'
            )
        
        @self.app.route('/health')
        def health():
            """Health check endpoint"""
            return {'status': 'healthy', 'timestamp': datetime.now().isoformat()}
        
        @self.app.route('/stats')
        def stats():
            """Human-readable stats endpoint for Grafana"""
            return self.collector.get_metrics_summary()
        
        @self.app.route('/dag_stats/<dag_id>')
        def dag_stats(dag_id):
            """Get detailed stats for specific DAG"""
            counters = self.collector.dag_counters.get(dag_id, {})
            return {
                'dag_id': dag_id,
                'stats': dict(counters),
                'timestamp': datetime.now().isoformat()
            }
        
        @self.app.route('/grafana/test')
        def grafana_test():
            """Test endpoint for Grafana connectivity"""
            return {
                'status': 'ok',
                'message': 'Grafana can connect to metrics',
                'timestamp': datetime.now().isoformat(),
                'available_metrics': [
                    'airflow_dag_runs_total',
                    'airflow_dag_success_rate', 
                    'airflow_dag_duration_seconds',
                    'airflow_task_runs_total',
                    'airflow_task_success_rate',
                    'airflow_records_processed_total',
                    'airflow_data_quality_score'
                ]
            }
    
    def run(self):
        """Run the metrics server"""
        try:
            logger.info(f"Starting metrics server on port {self.port}")
            self.app.run(host='0.0.0.0', port=self.port, threaded=True)
        except Exception as e:
            logger.error(f"Failed to start metrics server: {e}")

# Global instances
enhanced_metrics_collector = EnhancedMetricsCollector()
enhanced_pipeline_monitor = EnhancedPipelineMonitor(enhanced_metrics_collector)

# Safe utility functions for DAG creation
def get_enhanced_monitoring_callbacks() -> Dict[str, Any]:
    """Get enhanced monitoring callbacks for DAG creation - SAFE"""
    try:
        return {
            'on_success_callback': enhanced_pipeline_monitor.on_dag_success,
            'on_failure_callback': enhanced_pipeline_monitor.on_dag_failure,
        }
    except Exception as e:
        logger.warning(f"Failed to get DAG monitoring callbacks: {e}")
        return {}

def get_task_monitoring_callbacks() -> Dict[str, Any]:
    """Get task-level monitoring callbacks - SAFE"""
    try:
        return {
            'on_success_callback': enhanced_pipeline_monitor.on_task_success,
            'on_failure_callback': enhanced_pipeline_monitor.on_task_failure,
        }
    except Exception as e:
        logger.warning(f"Failed to get task monitoring callbacks: {e}")
        return {}

# Function to get framework status for health checks
def get_framework_status() -> Dict[str, Any]:
    """Get comprehensive framework status"""
    try:
        return enhanced_metrics_collector.get_metrics_summary()
    except Exception as e:
        logger.error(f"Failed to get framework status: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }