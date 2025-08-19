# dags/main.py
"""
Fixed Main DAG Entry Point - Airflow 3.x Compatible
This version ensures DAGs are properly exported to globals
"""

import sys
import os
import logging
from pathlib import Path
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# def create_test_dag():
#     """Create a guaranteed working test DAG"""
#     from airflow import DAG
#     from airflow.operators.empty import EmptyOperator
#     from airflow.operators.python import PythonOperator
    
#     def framework_status(**context):
#         logger.info("🎉 Framework main.py is working!")
#         framework_path = Path("/opt/airflow/dags")
#         metadata_path = Path("/opt/airflow/metadata")
        
#         status = {
#             "framework_path_exists": framework_path.exists(),
#             "metadata_path_exists": metadata_path.exists(),
#             "python_path": sys.path[:3],   # First 3 entries
#             "current_file": __file__,
#         }
        
#         if metadata_path.exists():
#             yaml_files = list(metadata_path.glob("*.yaml")) + list(metadata_path.glob("*.yml"))
#             status["yaml_files_found"] = [f.name for f in yaml_files]
        
#         logger.info(f"Framework Status: {status}")
#         return status
    
#     dag = DAG(
#         'framework_main_test',
#         default_args={
#             'owner': 'framework-main',
#             'start_date': datetime(2024, 1, 1),
#             'retries': 0,
#         },
#         description='Test DAG created by main.py to verify framework',
#         schedule=None,
#         catchup=False,
#         tags=['main', 'framework', 'test']
#     )
    
#     start = EmptyOperator(task_id='start', dag=dag)
#     status = PythonOperator(
#         task_id='check_framework_status',
#         python_callable=framework_status,
#         dag=dag
#     )
#     end = EmptyOperator(task_id='end', dag=dag)
    
#     start >> status >> end
#     return dag

def create_metadata_dags():
    """Try to create DAGs from metadata files"""
    created_dags = {}
    
    try:
        # Add framework path to Python path
        framework_path = Path(__file__).parent
        if str(framework_path) not in sys.path:
            sys.path.insert(0, str(framework_path))
        
        # Check metadata directory
        metadata_path = Path("/opt/airflow/metadata")
        if not metadata_path.exists():
            logger.warning(f"Metadata directory not found: {metadata_path}")
            return created_dags
        
        # Find YAML files
        yaml_files = list(metadata_path.glob("*.yaml")) + list(metadata_path.glob("*.yml"))
        logger.info(f"Found {len(yaml_files)} YAML files: {[f.name for f in yaml_files]}")
        
        if not yaml_files:
            logger.info("No YAML configuration files found - skipping metadata DAG creation")
            return created_dags
        
        # Try to import framework modules
        try:
            from dag_factory import DAGFactory, SimplifiedDAGFactory
            from metadata.manager import MetadataManager
            logger.info("✅ Successfully imported framework modules")
        except ImportError as e:
            logger.error(f"❌ Failed to import framework modules: {e}")
            logger.info("Creating error DAG to show import issues")
            error_dag = create_import_error_dag(str(e))
            created_dags[error_dag.dag_id] = error_dag
            return created_dags
        
        # Create DAGs using the factory
        try:
            factory = SimplifiedDAGFactory()   # Use simplified version
            
            for yaml_file in yaml_files:
                try:
                    logger.info(f"Processing {yaml_file.name}...")
                    dag = factory.create_dag(str(yaml_file))
                    created_dags[dag.dag_id] = dag
                    logger.info(f"✅ Created DAG: {dag.dag_id}")
                except Exception as e:
                    logger.error(f"❌ Failed to create DAG from {yaml_file.name}: {e}")
                    # Create error DAG to show the specific issue
                    error_dag = create_config_error_dag(yaml_file.stem, str(e))
                    created_dags[error_dag.dag_id] = error_dag
        
        except Exception as e:
            logger.error(f"❌ DAG factory creation failed: {e}")
            error_dag = create_factory_error_dag(str(e))
            created_dags[error_dag.dag_id] = error_dag
    
    except Exception as e:
        logger.error(f"❌ Metadata processing completely failed: {e}")
        error_dag = create_general_error_dag(str(e))
        created_dags[error_dag.dag_id] = error_dag
    
    return created_dags

# def create_import_error_dag(error_message):
#     """Create DAG to show import errors"""
#     from airflow import DAG
#     from airflow.operators.python import PythonOperator
    
#     def show_import_error(**context):
#         logger.error(f"Import Error: {error_message}")
#         raise Exception(f"Framework module import failed: {error_message}")
    
#     dag = DAG(
#         'framework_import_error',
#         default_args={'owner': 'framework', 'start_date': datetime(2024, 1, 1), 'retries': 0},
#         description=f'Import Error: {error_message[:50]}...',
#         schedule=None,
#         catchup=False,
#         tags=['error', 'import', 'framework']
#     )
    
#     PythonOperator(task_id='show_error', python_callable=show_import_error, dag=dag)
#     return dag

# def create_config_error_dag(config_name, error_message):
#     """Create DAG to show configuration errors"""
#     from airflow import DAG
#     from airflow.operators.python import PythonOperator
    
#     def show_config_error(**context):
#         logger.error(f"Configuration Error in {config_name}: {error_message}")
#         raise Exception(f"Config error: {error_message}")
    
#     dag = DAG(
#         f'config_error_{config_name}',
#         default_args={'owner': 'framework', 'start_date': datetime(2024, 1, 1), 'retries': 0},
#         description=f'Config Error in {config_name}: {error_message[:50]}...',
#         schedule=None,
#         catchup=False,
#         tags=['error', 'config', 'framework']
#     )
    
#     PythonOperator(task_id='show_error', python_callable=show_config_error, dag=dag)
#     return dag

# def create_factory_error_dag(error_message):
#     """Create DAG to show factory errors"""
#     from airflow import DAG
#     from airflow.operators.python import PythonOperator
    
#     def show_factory_error(**context):
#         logger.error(f"DAG Factory Error: {error_message}")
#         raise Exception(f"DAG Factory failed: {error_message}")
    
#     dag = DAG(
#         'framework_factory_error',
#         default_args={'owner': 'framework', 'start_date': datetime(2024, 1, 1), 'retries': 0},
#         description=f'Factory Error: {error_message[:50]}...',
#         schedule=None,
#         catchup=False,
#         tags=['error', 'factory', 'framework']
#     )
    
#     PythonOperator(task_id='show_error', python_callable=show_factory_error, dag=dag)
#     return dag

# def create_general_error_dag(error_message):
#     """Create DAG to show general errors"""
#     from airflow import DAG
#     from airflow.operators.python import PythonOperator
    
#     def show_general_error(**context):
#         logger.error(f"General Framework Error: {error_message}")
#         raise Exception(f"Framework error: {error_message}")
    
#     dag = DAG(
#         'framework_general_error',
#         default_args={'owner': 'framework', 'start_date': datetime(2024, 1, 1), 'retries': 0},
#         description=f'General Error: {error_message[:50]}...',
#         schedule=None,
#         catchup=False,
#         tags=['error', 'general', 'framework']
#     )
    
#     PythonOperator(task_id='show_error', python_callable=show_general_error, dag=dag)
#     return dag

# =============================================================================
# MAIN EXECUTION - This is where DAGs are created and exported
# =============================================================================

logger.info("🚀 Framework main.py starting...")

# Dictionary to collect all DAGs
all_dags = {}

try:
    # 1. Always create a test DAG first (guaranteed to work)
    # test_dag = create_test_dag()
    # all_dags[test_dag.dag_id] = test_dag
    # logger.info(f"✅ Created test DAG: {test_dag.dag_id}")
    
    # 2. Try to create metadata-based DAGs
    metadata_dags = create_metadata_dags()
    all_dags.update(metadata_dags)
    
    if metadata_dags:
        logger.info(f"✅ Created {len(metadata_dags)} metadata DAGs: {list(metadata_dags.keys())}")
    else:
        logger.info("ℹ️ No metadata DAGs created")
    
    logger.info(f"🎉 Total DAGs created: {len(all_dags)}")

except Exception as e:
    logger.error(f"❌ Critical error in main.py: {e}")
    # Create emergency DAG
    # emergency_dag = create_general_error_dag(f"Critical main.py error: {str(e)}")
    # all_dags[emergency_dag.dag_id] = emergency_dag

# =============================================================================
# CRITICAL: Export all DAGs to globals() so Airflow can find them
# =============================================================================

logger.info("📤 Exporting DAGs to global namespace...")
for dag_id, dag_obj in all_dags.items():
    globals()[dag_id] = dag_obj
    logger.info(f"   ✅ Exported: {dag_id}")

# Also export the dictionary for debugging
globals()['framework_all_dags'] = all_dags

logger.info(f"🎯 Final exported DAGs: {list(all_dags.keys())}")
logger.info("📋 Framework main.py completed successfully!")

# =============================================================================
# END OF MAIN.PY
# =============================================================================