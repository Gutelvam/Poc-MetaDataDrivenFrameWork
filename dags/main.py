"""
Main DAG Entry Point
This file is placed in the main dags/ directory and imports the modular framework
"""

import sys
import os
import logging
from pathlib import Path

# Add framework modules to Python path
framework_path = Path(__file__).parent
sys.path.insert(0, str(framework_path))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    # Import framework components
    from factory.dag_generator import DynamicDAGGenerator
    from monitoring.metrics import get_monitoring_callbacks
    
    # Initialize the framework
    logger.info("Initializing Metadata-Driven Pipeline Framework")
    
    # Create DAG generator
    dag_generator = DynamicDAGGenerator()
    
    # Generate all DAGs from metadata
    generated_dags = dag_generator.generate_dags()
    
    # Add monitoring callbacks to all DAGs
    monitoring_callbacks = get_monitoring_callbacks()
    for dag_id, dag in generated_dags.items():
        # Add callbacks to DAG default_args if not already present
        if 'on_success_callback' not in dag.default_args:
            dag.default_args.update(monitoring_callbacks)
    
    # Make DAGs available to Airflow
    globals().update(generated_dags)
    
    logger.info(f"Successfully loaded {len(generated_dags)} DAGs: {list(generated_dags.keys())}")
    
    # Validate configurations
    validation_results = dag_generator.validate_all_configurations()
    invalid_configs = {k: v for k, v in validation_results.items() if v}
    
    if invalid_configs:
        logger.warning(f"Found invalid configurations: {invalid_configs}")
    else:
        logger.info("All configurations are valid")
    
    # Utility functions for Airflow UI
    def get_framework_info():
        """Get framework information for debugging"""
        return {
            'total_dags': len(generated_dags),
            'dag_ids': list(generated_dags.keys()),
            'validation_results': validation_results,
            'framework_version': '1.0.0'
        }
    
    def regenerate_dag(config_file: str):
        """Regenerate a specific DAG"""
        try:
            dag = dag_generator.dag_factory.create_dag(config_file)
            globals()[dag.dag_id] = dag
            logger.info(f"Successfully regenerated DAG: {dag.dag_id}")
            return dag
        except Exception as e:
            logger.error(f"Failed to regenerate DAG from {config_file}: {str(e)}")
            raise
    
    # Export utility functions
    __all__ = ['get_framework_info', 'regenerate_dag'] + list(generated_dags.keys())

except Exception as e:
    logger.error(f"Failed to initialize framework: {str(e)}")
    
    # Create a dummy DAG to show the error in AirflowUI
    from airflow import DAG
    from airflow.operators.dummy import DummyOperator
    from airflow.operators.python import PythonOperator
    from datetime import datetime, timedelta
    
    def raise_framework_error(**context):
        raise Exception(f"Framework initialization failed: {str(e)}")
    
    error_dag = DAG(
        'framework_initialization_error',
        default_args={
            'owner': 'framework',
            'depends_on_past': False,
            'start_date': datetime(2024, 1, 1),
            'retries': 0,
        },
        description='Framework initialization error - check logs',
        schedule_interval=None,
        catchup=False,
        tags=['error', 'framework']
    )
    
    error_task = PythonOperator(
        task_id='show_error',
        python_callable=raise_framework_error,
        dag=error_dag
    )
    
    # Make error DAG available
    globals()['framework_initialization_error'] = error_dag