# dags/factory/dag_generator.py
"""
Fixed DAG Generator - Airflow 3.x Compatible
Combines all the modular components to generate DAGs
"""

import sys
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional

# Add parent directories to path for imports
current_dir = Path(__file__).parent
parent_dir = current_dir.parent
sys.path.insert(0, str(parent_dir))

from airflow import DAG
from datetime import datetime, timedelta

# Import framework modules with error handling
try:
    from core.config import PipelineConfig, TaskConfig, OperatorType
    from metadata.manager import MetadataManager
    # Import DAGFactory from parent directory (fixed path)
    from dag_factory import DAGFactory
except ImportError as e:
    logging.error(f"Failed to import framework modules: {e}")
    # Create fallback components
    class MetadataManager:
        def __init__(self, metadata_path):
            self.metadata_path = metadata_path
        
        def list_pipeline_configs(self):
            metadata_dir = Path(self.metadata_path)
            if not metadata_dir.exists():
                logging.warning(f"Metadata directory does not exist: {metadata_dir}")
                return []
            yaml_files = list(metadata_dir.glob("*.yaml")) + list(metadata_dir.glob("*.yml"))
            return [f.name for f in yaml_files]
        
        def validate_all_configs(self):
            return {}
    
    class DAGFactory:
        def __init__(self, metadata_manager):
            self.metadata_manager = metadata_manager
        
        def create_dag(self, config_file):
            # Basic fallback DAG creation
            from airflow.operators.empty import EmptyOperator
            import yaml
            
            dag_id = f"fallback_{Path(config_file).stem}"
            
            dag = DAG(
                dag_id=dag_id,
                default_args={'start_date': datetime(2024, 1, 1)},
                description="Fallback DAG - Framework import failed",
                schedule=None,
                catchup=False,
                tags=['error', 'fallback']
            )
            
            task = EmptyOperator(
                task_id='framework_error',
                dag=dag
            )
            
            return dag

logger = logging.getLogger(__name__)

class DynamicDAGGenerator:
    """Main DAG generator that combines all framework components"""
    
    def __init__(self, metadata_path: str = "/opt/airflow/metadata"):
        self.metadata_path = metadata_path
        self.metadata_manager = MetadataManager(metadata_path)
        self.dag_factory = DAGFactory(self.metadata_manager)
    
    def generate_dags(self) -> Dict[str, DAG]:
        """Generate all DAGs from configuration files"""
        dags = {}
        
        try:
            config_files = self.metadata_manager.list_pipeline_configs()
            logger.info(f"Found {len(config_files)} configuration files in {self.metadata_path}")
            
            if not config_files:
                logger.warning(f"No YAML configuration files found in {self.metadata_path}")
                # Create a test DAG to verify the system is working
                test_dag = self._create_test_dag()
                dags[test_dag.dag_id] = test_dag
                return dags
            
            for config_file in config_files:
                try:
                    full_path = Path(self.metadata_path) / config_file
                    logger.info(f"Processing config file: {full_path}")
                    
                    if not full_path.exists():
                        logger.error(f"Config file does not exist: {full_path}")
                        continue
                    
                    dag = self.dag_factory.create_dag(str(full_path))
                    dags[dag.dag_id] = dag
                    logger.info(f"Successfully generated DAG: {dag.dag_id}")
                    
                except Exception as e:
                    logger.error(f"Failed to generate DAG from {config_file}: {str(e)}")
                    # Create error DAG to show the issue
                    error_dag = self._create_error_dag(config_file, str(e))
                    dags[error_dag.dag_id] = error_dag
                    continue
            
            logger.info(f"Successfully generated {len(dags)} DAGs")
            
        except Exception as e:
            logger.error(f"Failed to generate DAGs: {str(e)}")
            # Create error DAG to show the issue
            error_dag = self._create_error_dag("framework_error", str(e))
            dags[error_dag.dag_id] = error_dag
        
        return dags
    
    def _create_test_dag(self) -> DAG:
        """Create a test DAG when no configuration files are found"""
        from airflow.operators.empty import EmptyOperator
        from airflow.operators.python import PythonOperator
        
        def check_metadata_path(**context):
            metadata_path = Path(self.metadata_path)
            return {
                "metadata_path": str(metadata_path),
                "exists": metadata_path.exists(),
                "files": list(metadata_path.glob("*.yaml")) + list(metadata_path.glob("*.yml")) if metadata_path.exists() else [],
                "message": f"No YAML files found in {metadata_path}"
            }
        
        dag = DAG(
            'framework_test_dag',
            default_args={
                'owner': 'framework',
                'depends_on_past': False,
                'start_date': datetime(2024, 1, 1),
                'retries': 0,
            },
            description='Test DAG - No configuration files found',
            schedule=None,
            catchup=False,
            tags=['test', 'framework']
        )
        
        start = EmptyOperator(task_id='start', dag=dag)
        
        check = PythonOperator(
            task_id='check_metadata_path',
            python_callable=check_metadata_path,
            dag=dag
        )
        
        end = EmptyOperator(task_id='end', dag=dag)
        
        start >> check >> end
        
        return dag
    
    def _create_error_dag(self, config_file: str, error_message: str) -> DAG:
        """Create an error DAG to show configuration issues"""
        from airflow.operators.python import PythonOperator
        
        def show_error(**context):
            raise Exception(f"Configuration error in {config_file}: {error_message}")
        
        dag_id = f"error_{Path(config_file).stem}"
        
        dag = DAG(
            dag_id,
            default_args={
                'owner': 'framework',
                'depends_on_past': False,
                'start_date': datetime(2024, 1, 1),
                'retries': 0,
            },
            description=f'Error DAG for {config_file}',
            schedule=None,
            catchup=False,
            tags=['error', 'framework']
        )
        
        error_task = PythonOperator(
            task_id='show_error',
            python_callable=show_error,
            dag=dag
        )
        
        return dag
    
    def validate_all_configurations(self) -> Dict[str, List[str]]:
        """Validate all configuration files"""
        return self.metadata_manager.validate_all_configs()
    
    def get_dag_info(self, dag_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific DAG"""
        return self.metadata_manager.get_pipeline_metadata(dag_id)
    
    def create_template_config(self, template_type: str) -> str:
        """Create a template configuration"""
        return self.metadata_manager.export_config_template(template_type)

# Utility functions
def get_framework_status() -> Dict[str, Any]:
    """Get overall framework status"""
    try:
        generator = DynamicDAGGenerator()
        validation_results = generator.validate_all_configurations()
        
        total_configs = len(validation_results)
        valid_configs = len([k for k, v in validation_results.items() if not v])
        invalid_configs = total_configs - valid_configs
        
        return {
            'status': 'healthy' if invalid_configs == 0 else 'warning',
            'total_configurations': total_configs,
            'valid_configurations': valid_configs,
            'invalid_configurations': invalid_configs,
            'validation_details': validation_results
        }
    except Exception as e:
        return {
            'status': 'error',
            'error': str(e)
        }

def regenerate_dag_from_config(config_file: str) -> DAG:
    """Regenerate a specific DAG from configuration"""
    generator = DynamicDAGGenerator()
    return generator.dag_factory.create_dag(config_file)

# Export main components
__all__ = [
    'DynamicDAGGenerator',
    'get_framework_status', 
    'regenerate_dag_from_config'
]