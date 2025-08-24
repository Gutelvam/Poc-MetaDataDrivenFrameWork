# dags/load_framework_dags.py
"""
Main DAG Loader - Airflow Entry Point
This file loads all DAGs from your metadata configurations
Place this file directly in the /dags directory (not in subdirectories)
"""

import logging
import sys
from pathlib import Path

# Add the current directory to Python path for framework imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

logger = logging.getLogger(__name__)

def load_all_framework_dags():
    """Load all DAGs from framework metadata configurations"""
    try:
        # Import your integrated DAG factory
        from dag_factory import IntegratedDAGFactory
        
        logger.info("🚀 Loading Framework DAGs from metadata...")
        
        # Create the DAG factory with metadata path
        metadata_path = "/opt/airflow/metadata"
        dag_factory = IntegratedDAGFactory(metadata_path)
        
        # Load configuration files
        config_files = dag_factory.metadata_manager.list_pipeline_configs()
        logger.info(f"📁 Found {len(config_files)} configuration files: {config_files}")
        
        if not config_files:
            logger.warning(f"❌ No YAML configuration files found in {metadata_path}")
            logger.info("📋 Expected files: *.yaml or *.yml in the metadata directory")
            return {}
        
        # Generate DAGs from each configuration
        generated_dags = {}
        
        for config_file in config_files:
            try:
                logger.info(f"🔧 Processing configuration: {config_file}")
                
                # Create DAG using your integrated factory
                full_path = Path(metadata_path) / config_file
                dag = dag_factory.create_dag(str(full_path))
                
                # Add to generated DAGs
                generated_dags[dag.dag_id] = dag
                logger.info(f"✅ Successfully created DAG: {dag.dag_id}")
                
                # Print task details for verification
                task_details = []
                for task in dag.tasks:
                    task_info = f"{task.task_id} ({task.__class__.__name__})"
                    task_details.append(task_info)
                
                logger.info(f"📋 Tasks in {dag.dag_id}: {task_details}")
                
            except Exception as e:
                logger.error(f"❌ Failed to create DAG from {config_file}: {str(e)}")
                
                # Create error DAG to show the issue
                error_dag = dag_factory._create_error_dag(config_file, str(e))
                generated_dags[error_dag.dag_id] = error_dag
                continue
        
        logger.info(f"🎉 Framework DAG loading complete: {len(generated_dags)} DAGs generated")
        return generated_dags
        
    except Exception as e:
        logger.error(f"💥 Critical error in framework DAG loading: {str(e)}")
        
        # Create fallback DAG to show the error
        from airflow import DAG
        from airflow.operators.python import PythonOperator
        from datetime import datetime
        
        def show_framework_error(**context):
            error_info = {
                'error': str(e),
                'metadata_path': '/opt/airflow/metadata',
                'config_files_exist': Path('/opt/airflow/metadata').exists(),
                'python_path': sys.path[:3]  # First 3 entries
            }
            logger.error(f"Framework loading error: {error_info}")
            raise Exception(f"Framework DAG loading failed: {str(e)}")
        
        fallback_dag = DAG(
            'framework_loading_error',
            default_args={
                'owner': 'framework',
                'start_date': datetime(2024, 1, 1),
                'retries': 0,
            },
            description=f'Framework Loading Error: {str(e)[:100]}...',
            schedule=None,
            catchup=False,
            tags=['error', 'framework', 'loading']
        )
        
        error_task = PythonOperator(
            task_id='show_framework_error',
            python_callable=show_framework_error,
            dag=fallback_dag
        )
        
        return {'framework_loading_error': fallback_dag}

# 🚀 MAIN EXECUTION - This runs when Airflow scans this file
logger.info("🔍 Airflow is scanning load_framework_dags.py...")

# Load all DAGs from your framework
all_dags = load_all_framework_dags()

# 📋 Export DAGs to Airflow's global namespace
# This is CRITICAL - Airflow looks for DAG objects in the global namespace
for dag_id, dag in all_dags.items():
    globals()[dag_id] = dag
    logger.info(f"📤 Exported DAG to Airflow: {dag_id}")

# Print summary for logs
logger.info(f"📊 SUMMARY: {len(all_dags)} DAGs loaded and exported to Airflow")
for dag_id in all_dags.keys():
    logger.info(f"   • {dag_id}")

# 🔍 Debug information
logger.info(f"📁 Metadata path: /opt/airflow/metadata")
logger.info(f"🐍 Python path includes: {current_dir}")
logger.info("✅ Framework DAG loading complete - check Airflow UI!")