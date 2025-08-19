# dags/dag_factory.py
"""
Simplified DAG Factory - Airflow 3.x Compatible
Focus on basic functionality that works reliably
"""

import logging
import yaml
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

class SimplifiedDAGFactory:
    """Simplified factory for creating DAGs from YAML configurations"""
    
    def create_dag(self, config_file: str) -> DAG:
        """Create a DAG from a YAML configuration file"""
        try:
            logger.info(f"Creating DAG from: {config_file}")
            
            # Load YAML configuration
            with open(config_file, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            if not config:
                raise ValueError(f"Empty configuration in {config_file}")
            
            # Extract basic DAG configuration
            dag_id = config.get('dag_id')
            if not dag_id:
                raise ValueError("dag_id is required")
            
            description = config.get('description', 'No description')
            schedule_interval = config.get('schedule_interval')
            start_date_str = config.get('start_date', '2024-01-01')
            catchup = config.get('catchup', False)
            owner = config.get('owner', 'framework')
            tags = config.get('tags', [])
            max_active_runs = config.get('max_active_runs', 1)
            
            # Parse start date
            try:
                start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            except ValueError:
                logger.warning(f"Invalid start_date in {config_file}, using default")
                start_date = datetime(2024, 1, 1)
            
            # Create DAG
            dag = DAG(
                dag_id=dag_id,
                default_args={
                    'owner': owner,
                    'start_date': start_date,
                    'retries': config.get('retries', 1),
                    'depends_on_past': False,
                },
                description=description,
                schedule=schedule_interval,
                catchup=catchup,
                tags=tags,
                max_active_runs=max_active_runs
            )
            
            # Create tasks
            tasks = {}
            task_configs = config.get('tasks', [])
            
            if not task_configs:
                # Create a dummy task to show the DAG exists
                dummy_task = EmptyOperator(task_id='no_tasks_defined', dag=dag)
                tasks['no_tasks_defined'] = dummy_task
                logger.warning(f"No tasks defined in {config_file}")
            else:
                # Create tasks from configuration
                for task_config in task_configs:
                    task = self._create_task(task_config, dag)
                    if task:
                        tasks[task.task_id] = task
                        logger.info(f"Created task: {task.task_id}")
                
                # Set up dependencies
                self._setup_dependencies(tasks, task_configs)
            
            logger.info(f"Successfully created DAG {dag_id} with {len(tasks)} tasks")
            return dag
            
        except Exception as e:
            logger.error(f"Failed to create DAG from {config_file}: {str(e)}")
            return self._create_error_dag(config_file, str(e))
    
    def _create_task(self, task_config: Dict[str, Any], dag: DAG):
        """Create a task from configuration"""
        task_id = task_config.get('task_id')
        operator_type = task_config.get('operator_type', 'dummy')
        
        if not task_id:
            raise ValueError("task_id is required")
        
        if operator_type == 'dummy':
            return EmptyOperator(task_id=task_id, dag=dag)
        
        elif operator_type == 'extract':
            def mock_extract(**context):
                source = task_config.get('source', {})
                logger.info(f"Mock extract from {source.get('name', 'unknown')}")
                return {"extracted": True, "records": 100, "source": source.get('name')}
            
            return PythonOperator(
                task_id=task_id,
                python_callable=mock_extract,
                dag=dag
            )
        
        elif operator_type == 'transform':
            def mock_transform(**context):
                logger.info(f"Mock transform in {task_id}")
                # Get data from upstream tasks if available
                upstream_data = []
                for upstream_task_id in task_config.get('depends_on', []):
                    try:
                        data = context['task_instance'].xcom_pull(task_ids=upstream_task_id)
                        if data:
                            upstream_data.append(data)
                    except:
                        pass
                
                return {"transformed": True, "input_sources": len(upstream_data)}
            
            return PythonOperator(
                task_id=task_id,
                python_callable=mock_transform,
                dag=dag
            )
        
        elif operator_type == 'load':
            def mock_load(**context):
                sink = task_config.get('sink', {})
                logger.info(f"Mock load to {sink.get('name', 'unknown')}")
                return {"loaded": True, "records": 100, "sink": sink.get('name')}
            
            return PythonOperator(
                task_id=task_id,
                python_callable=mock_load,
                dag=dag
            )
        
        elif operator_type == 'quality_check':
            def mock_quality_check(**context):
                logger.info(f"Mock quality check in {task_id}")
                return {"quality_passed": True, "score": 0.95, "checks": 5}
            
            return PythonOperator(
                task_id=task_id,
                python_callable=mock_quality_check,
                dag=dag
            )
        
        elif operator_type == 'custom':
            def custom_function(**context):
                params = task_config.get('custom_params', {})
                function_code = task_config.get('custom_function', '')
                
                logger.info(f"Custom task {task_id} with params: {params}")
                
                # If there's custom function code, try to execute it
                if function_code:
                    try:
                        # Simple execution of custom function
                        local_vars = {'context': context, 'logger': logger, 'params': params}
                        exec(function_code, {}, local_vars)
                        return local_vars.get('result', {"custom_executed": True})
                    except Exception as e:
                        logger.error(f"Custom function failed: {e}")
                        return {"custom_failed": True, "error": str(e)}
                
                return {"custom_completed": True, "params": params}
            
            return PythonOperator(
                task_id=task_id,
                python_callable=custom_function,
                dag=dag
            )
        
        else:
            logger.warning(f"Unknown operator type '{operator_type}', creating empty operator")
            return EmptyOperator(task_id=task_id, dag=dag)
    
    def _setup_dependencies(self, tasks: Dict[str, Any], task_configs: List[Dict]):
        """Set up task dependencies"""
        for task_config in task_configs:
            task_id = task_config.get('task_id')
            depends_on = task_config.get('depends_on', [])
            
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
        config_filename = Path(config_file).stem
        dag_id = f"error_{config_filename}"
        
        def show_error(**context):
            error_info = {
                'config_file': config_file,
                'error_message': error_message,
                'file_exists': Path(config_file).exists()
            }
            logger.error(f"Configuration error: {error_info}")
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
            tags=['error', 'config_error']
        )
        
        error_task = PythonOperator(
            task_id='show_config_error',
            python_callable=show_error,
            dag=dag
        )
        
        return dag

# Create a factory instance for backward compatibility
DAGFactory = SimplifiedDAGFactory