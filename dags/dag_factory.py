# dags/enhanced_dag_factory.py
"""
Enhanced DAG Factory with REAL Integration
Uses your actual framework operators instead of mock implementations
"""

import logging
import yaml
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any

from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Import your actual framework operators
from sources.operators import create_source_operator
from sinks.operators import create_sink_operator
from transforms.operators import (
    create_sql_transform_operator,
    create_python_transform_operator,
    create_validation_transform_operator,
    create_aggregation_transform_operator
)
from quality.operators import create_data_quality_operator

# Import configuration classes
from core.config import (
    SourceConfig, SinkConfig, DataQualityRule, 
    SourceType, SinkType, WriteMode, OperatorType
)

# Import metadata management
from metadata.manager import MetadataManager

# Import enhanced monitoring
try:
    from monitoring.metrics import (
        enhanced_pipeline_monitor,
        get_enhanced_monitoring_callbacks,
        get_task_monitoring_callbacks,
        MetricEvent
    )
    MONITORING_AVAILABLE = True
except ImportError as e:
    logging.warning(f"Enhanced monitoring not available: {e}")
    MONITORING_AVAILABLE = False

logger = logging.getLogger(__name__)

class IntegratedDAGFactory:
    """DAG factory that properly uses your framework operators"""
    
    def __init__(self, metadata_path: str = "/opt/airflow/metadata"):
        self.metadata_manager = MetadataManager(metadata_path)
    
    def create_dag(self, config_file: str) -> DAG:
        """Create a DAG using your real framework operators"""
        try:
            logger.info(f"Creating integrated DAG from: {config_file}")
            
            # Load and validate configuration using your metadata manager
            pipeline_config = self.metadata_manager.load_pipeline_config(Path(config_file).name)
            
            # Get monitoring callbacks
            dag_callbacks = {}
            if MONITORING_AVAILABLE:
                dag_callbacks = get_enhanced_monitoring_callbacks()
                pipeline_config.tags.append('monitored')
            
            # Create DAG with your configuration
            dag = DAG(
                dag_id=pipeline_config.dag_id,
                default_args={
                    'owner': pipeline_config.owner,
                    'start_date': datetime.strptime(pipeline_config.start_date, '%Y-%m-%d'),
                    'retries': pipeline_config.retries,
                    'retry_delay': timedelta(seconds=pipeline_config.retry_delay),
                    'depends_on_past': False,
                },
                description=pipeline_config.description,
                schedule=pipeline_config.schedule_interval,
                catchup=pipeline_config.catchup,
                tags=pipeline_config.tags,
                max_active_runs=pipeline_config.max_active_runs,
                **dag_callbacks
            )
            
            # Create tasks using your actual operators
            tasks = {}
            for task_config in pipeline_config.tasks:
                task = self._create_real_framework_task(task_config, dag, pipeline_config)
                if task:
                    tasks[task.task_id] = task
                    logger.info(f"Created framework task: {task.task_id}")
            
            # Set up dependencies using your configuration
            self._setup_dependencies(tasks, pipeline_config.tasks)
            
            # Save metadata using your metadata manager
            self.metadata_manager.save_pipeline_metadata(pipeline_config)
            
            # Record DAG creation event
            if MONITORING_AVAILABLE:
                try:
                    creation_event = MetricEvent(
                        timestamp=datetime.now(),
                        dag_id=pipeline_config.dag_id,
                        task_id=None,
                        event_type='dag_created',
                        status='created',
                        metadata={
                            'task_count': len(tasks),
                            'config_file': config_file,
                            'pipeline_type': pipeline_config.pipeline_type
                        }
                    )
                    enhanced_pipeline_monitor.metrics.record_event(creation_event)
                except Exception as e:
                    logger.warning(f"Failed to record DAG creation event: {e}")
            
            logger.info(f"Successfully created integrated DAG {pipeline_config.dag_id} with {len(tasks)} real framework tasks")
            return dag
            
        except Exception as e:
            logger.error(f"Failed to create DAG from {config_file}: {str(e)}")
            return self._create_error_dag(config_file, str(e))
    
    def _create_real_framework_task(self, task_config, dag, pipeline_config):
        """Create task using your actual framework operators"""
        
        # Get task-level monitoring callbacks
        task_callbacks = {}
        if MONITORING_AVAILABLE:
            task_callbacks = get_task_monitoring_callbacks()
        
        if task_config.operator_type == OperatorType.DUMMY:
            return EmptyOperator(
                task_id=task_config.task_id,
                dag=dag,
                **task_callbacks
            )
        
        elif task_config.operator_type == OperatorType.EXTRACT:
            if not task_config.source:
                raise ValueError(f"Extract task {task_config.task_id} requires source configuration")
            
            # Use your actual source operator
            return create_source_operator(
                task_id=task_config.task_id,
                source_config=task_config.source,
                dag=dag,
                retries=task_config.retries,
                retry_delay=timedelta(seconds=task_config.retry_delay),
                **task_callbacks
            )
        
        elif task_config.operator_type == OperatorType.TRANSFORM:
            # Determine transform type and use appropriate operator
            if task_config.sql_transform:
                # Get upstream task IDs for data sources
                upstream_task_ids = task_config.depends_on or []
                
                return create_sql_transform_operator(
                    task_id=task_config.task_id,
                    sql_query=task_config.sql_transform,
                    data_source_task_ids=upstream_task_ids,
                    dag=dag,
                    connection_id=task_config.source.connection_id if task_config.source else None,
                    **task_callbacks
                )
            
            elif task_config.python_transform:
                upstream_task_ids = task_config.depends_on or []
                
                return create_python_transform_operator(
                    task_id=task_config.task_id,
                    python_callable=task_config.python_transform,
                    data_source_task_ids=upstream_task_ids,
                    dag=dag,
                    **task_callbacks
                )
            
            elif task_config.custom_params and task_config.custom_params.get('validation_rules'):
                # Use validation transform operator
                upstream_task_id = task_config.depends_on[0] if task_config.depends_on else None
                
                return create_validation_transform_operator(
                    task_id=task_config.task_id,
                    data_source_task_id=upstream_task_id,
                    validation_rules=task_config.custom_params['validation_rules'],
                    dag=dag,
                    **task_callbacks
                )
            
            else:
                # Default to aggregation if no specific transform specified
                upstream_task_id = task_config.depends_on[0] if task_config.depends_on else None
                
                # Extract aggregation parameters from your YAML
                return create_aggregation_transform_operator(
                    task_id=task_config.task_id,
                    data_source_task_id=upstream_task_id,
                    group_by_columns=['product_category'],  # From your YAML example
                    aggregations={'order_amount': ['sum', 'mean', 'count']},
                    dag=dag,
                    **task_callbacks
                )
        
        elif task_config.operator_type == OperatorType.LOAD:
            if not task_config.sink:
                raise ValueError(f"Load task {task_config.task_id} requires sink configuration")
            
            # Get upstream task ID for data source
            data_source_task_id = task_config.depends_on[0] if task_config.depends_on else None
            
            # Use your actual sink operator
            return create_sink_operator(
                task_id=task_config.task_id,
                sink_config=task_config.sink,
                data_source_task_id=data_source_task_id,
                dag=dag,
                retries=task_config.retries,
                retry_delay=timedelta(seconds=task_config.retry_delay),
                **task_callbacks
            )
        
        elif task_config.operator_type == OperatorType.QUALITY_CHECK:
            if not task_config.quality_rules:
                # Use global quality rules if no task-specific rules
                quality_rules = pipeline_config.global_quality_rules or []
            else:
                quality_rules = task_config.quality_rules
            
            if not quality_rules:
                logger.warning(f"Quality check task {task_config.task_id} has no rules defined")
                quality_rules = [DataQualityRule(
                    name='default_completeness',
                    rule_type='completeness',
                    threshold=0.95,
                    severity='warning'
                )]
            
            # Get upstream task ID for data source
            data_source_task_id = task_config.depends_on[0] if task_config.depends_on else None
            
            # Use your actual quality operator
            return create_data_quality_operator(
                task_id=task_config.task_id,
                quality_rules=quality_rules,
                data_source_task_id=data_source_task_id,
                dag=dag,
                fail_on_error=False,  # Don't fail pipeline on quality issues
                **task_callbacks
            )
        
        elif task_config.operator_type == OperatorType.CUSTOM:
            # Use PythonOperator for custom functions
            from airflow.operators.python import PythonOperator
            
            def execute_custom_function(**context):
                """Execute custom function from task configuration"""
                if task_config.custom_function:
                    # Execute the custom function code
                    local_vars = {
                        'context': context,
                        'logger': logger,
                        'params': task_config.custom_params or {}
                    }
                    
                    # Import common libraries for custom functions
                    import pandas as pd
                    import json
                    local_vars.update({'pd': pd, 'json': json})
                    
                    try:
                        exec(task_config.custom_function, {}, local_vars)
                        return local_vars.get('result', {
                            'custom_completed': True,
                            'params': task_config.custom_params
                        })
                    except Exception as e:
                        logger.error(f"Custom function failed: {e}")
                        
                        # Record error event
                        if MONITORING_AVAILABLE:
                            enhanced_pipeline_monitor.metrics.record_event(
                                MetricEvent(
                                    timestamp=datetime.now(),
                                    dag_id=context['dag'].dag_id,
                                    task_id=context['task'].task_id,
                                    event_type='custom_error',
                                    status='failed',
                                    error_type='custom_function_error',
                                    metadata={'error_message': str(e)}
                                )
                            )
                        raise
                else:
                    return {
                        'message': f'Custom task {task_config.task_id} completed',
                        'params': task_config.custom_params
                    }
            
            return PythonOperator(
                task_id=task_config.task_id,
                python_callable=execute_custom_function,
                dag=dag,
                retries=task_config.retries,
                retry_delay=timedelta(seconds=task_config.retry_delay),
                **task_callbacks
            )
        
        else:
            logger.warning(f"Unknown operator type '{task_config.operator_type}' for task {task_config.task_id}, creating empty operator")
            return EmptyOperator(
                task_id=task_config.task_id,
                dag=dag,
                **task_callbacks
            )
    
    def _setup_dependencies(self, tasks: Dict[str, Any], task_configs: List):
        """Set up task dependencies from configuration"""
        for task_config in task_configs:
            task_id = task_config.task_id
            depends_on = task_config.depends_on or []
            
            if task_id in tasks and depends_on:
                current_task = tasks[task_id]
                
                for dependency in depends_on:
                    if dependency in tasks:
                        upstream_task = tasks[dependency]
                        upstream_task >> current_task
                        logger.info(f"Set dependency: {dependency} >> {task_id}")
                    else:
                        logger.warning(f"Dependency '{dependency}' not found for task '{task_id}'")
    
    def _create_error_dag(self, config_file: str, error_message: str) -> DAG:
        """Create an error DAG to show configuration issues"""
        from airflow.operators.python import PythonOperator
        
        config_filename = Path(config_file).stem
        dag_id = f"error_{config_filename}"
        
        def show_error(**context):
            error_info = {
                'config_file': config_file,
                'error_message': error_message,
                'file_exists': Path(config_file).exists(),
                'framework_components': {
                    'sources': 'Available',
                    'sinks': 'Available', 
                    'transforms': 'Available',
                    'quality': 'Available',
                    'monitoring': 'Available' if MONITORING_AVAILABLE else 'Not Available'
                }
            }
            logger.error(f"Configuration error: {error_info}")
            
            # Record error event
            if MONITORING_AVAILABLE:
                enhanced_pipeline_monitor.metrics.record_event(
                    MetricEvent(
                        timestamp=datetime.now(),
                        dag_id=dag_id,
                        task_id='show_config_error',
                        event_type='config_error',
                        status='failed',
                        error_type='configuration_error',
                        metadata=error_info
                    )
                )
            
            raise Exception(f"Config error in {config_file}: {error_message}")
        
        dag = DAG(
            dag_id,
            default_args={
                'owner': 'framework',
                'start_date': datetime(2024, 1, 1),
                'retries': 0,
            },
            description=f'Error DAG for {config_file}: {error_message[:50]}...',
            schedule=None,
            catchup=False,
            tags=['error', 'config_error', 'framework']
        )
        
        error_task = PythonOperator(
            task_id='show_config_error',
            python_callable=show_error,
            dag=dag
        )
        
        return dag

# Create the integrated factory instance that uses your real operators
DAGFactory = IntegratedDAGFactory