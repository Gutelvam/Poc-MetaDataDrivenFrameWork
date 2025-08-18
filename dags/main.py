# dags/main.py
"""
Fixed Main DAG Entry Point - Airflow 3.x Compatible with Better Error Handling
This file is placed in the main dags/ directory and imports the modular framework
"""

import sys
import os
import logging
from pathlib import Path

# Add framework modules to Python path
framework_path = Path(__file__).parent
sys.path.insert(0, str(framework_path))

# Configure logging with more detail
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment check
def check_environment():
    """Check the environment setup"""
    logger.info("=== FRAMEWORK ENVIRONMENT CHECK ===")
    logger.info(f"Python path: {sys.path}")
    logger.info(f"Framework path: {framework_path}")
    
    # Check metadata directory
    metadata_path = Path("/opt/airflow/metadata")
    logger.info(f"Metadata path: {metadata_path}")
    logger.info(f"Metadata path exists: {metadata_path.exists()}")
    
    if metadata_path.exists():
        yaml_files = list(metadata_path.glob("*.yaml")) + list(metadata_path.glob("*.yml"))
        logger.info(f"YAML files found: {[f.name for f in yaml_files]}")
    else:
        logger.warning(f"Metadata directory does not exist: {metadata_path}")
    
    # Check core modules
    core_modules = ['core', 'metadata', 'factory', 'sources', 'sinks', 'transforms', 'quality', 'monitoring']
    for module in core_modules:
        module_path = framework_path / module
        logger.info(f"Module {module}: {module_path.exists()}")
    
    logger.info("=== END ENVIRONMENT CHECK ===")

# Run environment check
check_environment()

try:
    # Import framework components with detailed error tracking
    logger.info("Initializing Metadata-Driven Pipeline Framework...")
    
    # Test core imports first
    try:
        from core.config import PipelineConfig, TaskConfig, OperatorType
        logger.info("✅ Core config imports successful")
    except Exception as e:
        logger.error(f"❌ Core config import failed: {e}")
        raise
    
    try:
        from metadata.manager import MetadataManager
        logger.info("✅ Metadata manager import successful")
    except Exception as e:
        logger.error(f"❌ Metadata manager import failed: {e}")
        raise
    
    try:
        from factory.dag_generator import DynamicDAGGenerator
        logger.info("✅ DAG generator import successful")
    except Exception as e:
        logger.error(f"❌ DAG generator import failed: {e}")
        raise
    
    try:
        from monitoring.metrics import get_monitoring_callbacks
        logger.info("✅ Monitoring imports successful")
    except Exception as e:
        logger.warning(f"⚠️ Monitoring import failed: {e}")
        # Fallback monitoring
        def get_monitoring_callbacks():
            return {}
    
    # Create DAG generator
    logger.info("Creating DAG generator...")
    dag_generator = DynamicDAGGenerator()
    
    # Generate all DAGs from metadata
    logger.info("Generating DAGs from metadata...")
    generated_dags = dag_generator.generate_dags()
    
    if not generated_dags:
        logger.warning("No DAGs were generated!")
    else:
        logger.info(f"Generated {len(generated_dags)} DAGs")
    
    # Add monitoring callbacks to all DAGs
    try:
        monitoring_callbacks = get_monitoring_callbacks()
        for dag_id, dag in generated_dags.items():
            # Add callbacks to DAG default_args if not already present
            if monitoring_callbacks and 'on_success_callback' not in dag.default_args:
                dag.default_args.update(monitoring_callbacks)
                logger.info(f"Added monitoring callbacks to DAG: {dag_id}")
    except Exception as e:
        logger.warning(f"Failed to add monitoring callbacks: {e}")
    
    # Make DAGs available to Airflow
    globals().update(generated_dags)
    
    logger.info(f"Successfully loaded {len(generated_dags)} DAGs: {list(generated_dags.keys())}")
    
    # Validate configurations
    try:
        validation_results = dag_generator.validate_all_configurations()
        invalid_configs = {k: v for k, v in validation_results.items() if v}
        
        if invalid_configs:
            logger.warning(f"Found invalid configurations: {invalid_configs}")
        else:
            logger.info("All configurations are valid")
    except Exception as e:
        logger.warning(f"Configuration validation failed: {e}")
    
    # Utility functions for Airflow UI
    def get_framework_info():
        """Get framework information for debugging"""
        return {
            'total_dags': len(generated_dags),
            'dag_ids': list(generated_dags.keys()),
            'framework_version': '1.0.0',
            'metadata_path': '/opt/airflow/metadata',
            'python_path': sys.path,
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
    
    # Success message
    logger.info("🎉 Framework initialization completed successfully!")

except Exception as e:
    logger.error(f"❌ Failed to initialize framework: {str(e)}")
    logger.error(f"Exception type: {type(e).__name__}")
    
    # Import traceback for detailed error info
    import traceback
    logger.error(f"Full traceback:\n{traceback.format_exc()}")
    
    # Create debugging DAGs to show the error in Airflow UI
    from airflow import DAG
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import PythonOperator
    from datetime import datetime
    
    def raise_framework_error(**context):
        """Show detailed framework error information"""
        error_info = {
            'error_message': str(e),
            'error_type': type(e).__name__,
            'python_path': sys.path,
            'framework_path': str(framework_path),
            'metadata_path_exists': Path("/opt/airflow/metadata").exists(),
            'yaml_files': []
        }
        
        # Check for YAML files
        metadata_path = Path("/opt/airflow/metadata")
        if metadata_path.exists():
            yaml_files = list(metadata_path.glob("*.yaml")) + list(metadata_path.glob("*.yml"))
            error_info['yaml_files'] = [str(f) for f in yaml_files]
        
        logger.error(f"Framework error details: {error_info}")
        raise Exception(f"Framework initialization failed: {error_info}")
    
    def show_environment_info(**context):
        """Show environment information for debugging"""
        env_info = {
            'cwd': os.getcwd(),
            'python_executable': sys.executable,
            'python_version': sys.version,
            'airflow_home': os.environ.get('AIRFLOW_HOME', 'Not set'),
            'framework_path': str(framework_path),
            'metadata_path': '/opt/airflow/metadata',
        }
        
        # Check directory contents
        if framework_path.exists():
            env_info['framework_contents'] = [str(p) for p in framework_path.iterdir()]
        
        metadata_path = Path("/opt/airflow/metadata")
        if metadata_path.exists():
            env_info['metadata_contents'] = [str(p) for p in metadata_path.iterdir()]
        
        logger.info(f"Environment info: {env_info}")
        return env_info
    
    # Create error DAG
    error_dag = DAG(
        'framework_initialization_error',
        default_args={
            'owner': 'framework',
            'depends_on_past': False,
            'start_date': datetime(2024, 1, 1),
            'retries': 0,
        },
        description='Framework initialization error - check logs for details',
        schedule=None,
        catchup=False,
        tags=['error', 'framework', 'debug']
    )
    
    start_task = EmptyOperator(
        task_id='start',
        dag=error_dag
    )
    
    env_task = PythonOperator(
        task_id='show_environment',
        python_callable=show_environment_info,
        dag=error_dag
    )
    
    error_task = PythonOperator(
        task_id='show_error',
        python_callable=raise_framework_error,
        dag=error_dag
    )
    
    start_task >> env_task >> error_task
    
    # Create a simple test DAG to verify basic Airflow functionality
    test_dag = DAG(
        'framework_test_simple',
        default_args={
            'owner': 'framework',
            'depends_on_past': False,
            'start_date': datetime(2024, 1, 1),
            'retries': 0,
        },
        description='Simple test DAG to verify Airflow is working',
        schedule=None,
        catchup=False,
        tags=['test', 'simple']
    )
    
    def test_yaml_files(**context):
        """Test YAML file reading"""
        metadata_path = Path("/opt/airflow/metadata")
        result = {
            'metadata_path_exists': metadata_path.exists(),
            'yaml_files': []
        }
        
        if metadata_path.exists():
            yaml_files = list(metadata_path.glob("*.yaml")) + list(metadata_path.glob("*.yml"))
            result['yaml_files'] = [{'name': f.name, 'size': f.stat().st_size, 'exists': f.exists()} for f in yaml_files]
            
            # Try to read the simple example
            example_file = metadata_path / "exemplo_simples.yaml"
            if example_file.exists():
                try:
                    import yaml
                    with open(example_file, 'r') as f:
                        content = yaml.safe_load(f)
                    result['exemplo_simples_content'] = content
                except Exception as yaml_error:
                    result['exemplo_simples_error'] = str(yaml_error)
        
        logger.info(f"YAML test result: {result}")
        return result
    
    test_yaml_task = PythonOperator(
        task_id='test_yaml_files',
        python_callable=test_yaml_files,
        dag=test_dag
    )
    
    # Make error DAGs available
    globals()['framework_initialization_error'] = error_dag
    globals()['framework_test_simple'] = test_dag