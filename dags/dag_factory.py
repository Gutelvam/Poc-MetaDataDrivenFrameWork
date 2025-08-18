# dags/dag_factory.py
"""
Fixed DAG Factory Core Implementation - Airflow 3.x Compatible
Creates DAGs dynamically from metadata configurations with better error handling
"""

import logging
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional
import yaml
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

class DAGFactory:
    """Factory class for creating DAGs from pipeline configurations"""
    
    def __init__(self, metadata_manager=None):
        self.metadata_manager = metadata_manager
    
    def create_dag(self, config_file: str) -> DAG:
        """Create a DAG from a configuration file with comprehensive error handling"""
        try:
            logger.info(f"Creating DAG from config file: {config_file}")
            
            # Verify file exists
            config_path = Path(config_file)
            if not config_path.exists():
                raise FileNotFoundError(f"Configuration file not found: {config_file}")
            
            # Load and validate YAML
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                logger.info(f"Successfully loaded YAML config: {config.get('dag_id', 'unknown')}")
            except yaml.YAMLError as e:
                raise ValueError(f"Invalid YAML in {config_file}: {str(e)}")
            except Exception as e:
                raise ValueError(f"Failed to read config file {config_file}: {str(e)}")
            
            # Validate required fields
            if not config:
                raise ValueError(f"Empty configuration in {config_file}")
            
            required_fields = ['dag_id', 'description', 'tasks']
            missing_fields = [field for field in required_fields if field not in config]
            if missing_fields:
                raise ValueError(f"Missing required fields in {config_file}: {missing_fields}")
            
            # Extract DAG configuration
            dag_id = config.get('dag_id')
            description = config.get('description', 'No description provided')
            schedule_interval = config.get('schedule_interval')
            start_date_str = config.get('start_date', '2024-01-01')
            catchup = config.get('catchup', False)
            owner = config.get('owner', 'airflow')
            tags = config.get('tags', [])
            max_active_runs = config.get('max_active_runs', 1)
            
            # Parse start date
            try:
                if isinstance(start_date_str, str):
                    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
                else:
                    start_date = start_date_str
            except ValueError as e:
                logger.warning(f"Invalid start_date format in {config_file}, using default")
                start_date = datetime(2024, 1, 1)
            
            # Create default args
            default_args = {
                'owner': owner,
                'start_date': start_date,
                'retries': config.get('retries', 1),
                'depends_on_past': False,
            }
            
            # Create DAG
            dag = DAG(
                dag_id=dag_id,
                default_args=default_args,
                description=description,
                schedule=schedule_interval,  # Changed from schedule_interval for Airflow 3.x
                catchup=catchup,
                tags=tags,
                max_active_runs=max_active_runs
            )
            
            logger.info(f"Created DAG object for {dag_id}")
            
            # Create tasks
            tasks = {}
            task_configs = config.get('tasks', [])
            
            if not task_configs:
                logger.warning(f"No tasks defined in {config_file}")
                # Create a dummy task to show the DAG exists
                dummy_task = EmptyOperator(
                    task_id='no_tasks_defined',
                    dag=dag
                )
                tasks['no_tasks_defined'] = dummy_task
            else:
                # Create tasks
                for i, task_config in enumerate(task_configs):
                    try:
                        task = self._create_task(task_config, dag, config_file)
                        if task:
                            tasks[task.task_id] = task
                            logger.info(f"Created task: {task.task_id}")
                    except Exception as task_error:
                        logger.error(f"Failed to create task {i} in {config_file}: {str(task_error)}")
                        # Create error task to show the issue
                        error_task = self._create_error_task(f"task_error_{i}", str(task_error), dag)
                        tasks[error_task.task_id] = error_task
                
                # Set up dependencies
                self._setup_dependencies(tasks, task_configs, dag)
            
            logger.info(f"Successfully created DAG {dag_id} with {len(tasks)} tasks")
            return dag
            
        except Exception as e:
            logger.error(f"Failed to create DAG from {config_file}: {str(e)}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            
            # Create error DAG to show the issue
            return self._create_error_dag(config_file, str(e))
    
    def _create_task(self, task_config: Dict[str, Any], dag: DAG, config_file: str):
        """Create a single task from configuration"""
        task_id = task_config.get('task_id')
        operator_type = task_config.get('operator_type', 'dummy')
        description = task_config.get('description', 'No description provided')
        
        if not task_id:
            raise ValueError("Task must have task_id")
        
        # For now, create tasks based on operator type
        if operator_type == 'dummy':
            return EmptyOperator(
                task_id=task_id,
                dag=dag
            )
        
        elif operator_type == 'extract':
            # Create a mock extract task
            def mock_extract(**context):
                source_config = task_config.get('source', {})
                logger.info(f"Mock extract from {source_config.get('name', 'unknown source')}")
                return {"message": f"Extracted data from {source_config.get('name', 'source')}", "records": 100}
            
            return PythonOperator(
                task_id=task_id,
                python_callable=mock_extract,
                dag=dag
            )
        
        elif operator_type == 'load':
            # Create a mock load task
            def mock_load(**context):
                sink_config = task_config.get('sink', {})
                logger.info(f"Mock load to {sink_config.get('name', 'unknown sink')}")
                return {"message": f"Loaded data to {sink_config.get('name', 'sink')}", "records": 100}
            
            return PythonOperator(
                task_id=task_id,
                python_callable=mock_load,
                dag=dag
            )
        
        elif operator_type == 'transform':
            # Create a mock transform task
            def mock_transform(**context):
                logger.info(f"Mock transform in task {task_id}")
                return {"message": f"Transformed data in {task_id}", "records": 100}
            
            return PythonOperator(
                task_id=task_id,
                python_callable=mock_transform,
                dag=dag
            )
        
        elif operator_type == 'quality_check':
            # Create a mock quality check task
            def mock_quality_check(**context):
                logger.info(f"Mock quality check in task {task_id}")
                return {"message": f"Quality check passed in {task_id}", "score": 0.95}
            
            return PythonOperator(
                task_id=task_id,
                python_callable=mock_quality_check,
                dag=dag
            )
        
        elif operator_type == 'custom':
            # Create a custom task
            def custom_function(**context):
                custom_params = task_config.get('custom_params', {})
                logger.info(f"Custom task {task_id} with params: {custom_params}")
                return {"message": f"Custom task {task_id} completed", "params": custom_params}
            
            return PythonOperator(
                task_id=task_id,
                python_callable=custom_function,
                dag=dag
            )
        
        else:
            # Default to empty operator for unknown types
            logger.warning(f"Unknown operator type '{operator_type}' for task {task_id}, creating EmptyOperator")
            return EmptyOperator(
                task_id=task_id,
                dag=dag
            )
    
    def _create_error_task(self, task_id: str, error_message: str, dag: DAG):
        """Create an error task to show task creation issues"""
        def show_task_error(**context):
            raise Exception(f"Task creation error: {error_message}")
        
        return PythonOperator(
            task_id=task_id,
            python_callable=show_task_error,
            dag=dag
        )
    
    def _setup_dependencies(self, tasks: Dict[str, Any], task_configs: List[Dict], dag: DAG):
        """Set up task dependencies"""
        try:
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
        except Exception as e:
            logger.error(f"Failed to setup dependencies: {str(e)}")
    
    def _create_error_dag(self, config_file: str, error_message: str) -> DAG:
        """Create an error DAG to show configuration issues"""
        config_filename = Path(config_file).stem
        dag_id = f"error_{config_filename}"
        
        def show_config_error(**context):
            error_details = {
                'config_file': config_file,
                'error_message': error_message,
                'file_exists': Path(config_file).exists(),
                'file_size': Path(config_file).stat().st_size if Path(config_file).exists() else 0
            }
            
            # Try to read the file content for debugging
            if Path(config_file).exists():
                try:
                    with open(config_file, 'r') as f:
                        content = f.read()
                        error_details['file_content_preview'] = content[:500] + "..." if len(content) > 500 else content
                except Exception as read_error:
                    error_details['read_error'] = str(read_error)
            
            logger.error(f"Configuration error details: {error_details}")
            raise Exception(f"Configuration error in {config_file}: {error_message}")
        
        error_dag = DAG(
            dag_id,
            default_args={
                'owner': 'framework',
                'depends_on_past': False,
                'start_date': datetime(2024, 1, 1),
                'retries': 0,
            },
            description=f'Error DAG for {config_file} - {error_message[:100]}',
            schedule=None,
            catchup=False,
            tags=['error', 'config_error', config_filename]
        )
        
        error_task = PythonOperator(
            task_id='show_config_error',
            python_callable=show_config_error,
            dag=error_dag
        )
        
        return error_dag