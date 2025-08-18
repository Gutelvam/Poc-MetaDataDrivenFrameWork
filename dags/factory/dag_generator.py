"""
Main DAG Generator (factory/dag_generator.py)
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

# Import framework modules
from core.config import PipelineConfig, TaskConfig, OperatorType
from metadata.manager import MetadataManager

logger = logging.getLogger(__name__)

class DynamicDAGGenerator:
    """Main DAG generator that combines all framework components"""
    
    def __init__(self, metadata_path: str = "/opt/airflow/metadata"):
        self.metadata_manager = MetadataManager(metadata_path)
        # Import DAGFactory lazily to avoid circular imports
        from dag_factory import DAGFactory
        self.dag_factory = DAGFactory(self.metadata_manager)
    
    def generate_dags(self) -> Dict[str, DAG]:
        """Generate all DAGs from configuration files"""
        dags = {}
        
        try:
            config_files = self.metadata_manager.list_pipeline_configs()
            logger.info(f"Found {len(config_files)} configuration files")
            
            for config_file in config_files:
                try:
                    full_path = Path(self.metadata_manager.metadata_path) / config_file
                    dag = self.dag_factory.create_dag(str(full_path))
                    dags[dag.dag_id] = dag
                    logger.info(f"Successfully generated DAG: {dag.dag_id}")
                    
                except Exception as e:
                    logger.error(f"Failed to generate DAG from {config_file}: {str(e)}")
                    continue
            
            logger.info(f"Successfully generated {len(dags)} DAGs")
            
        except Exception as e:
            logger.error(f"Failed to generate DAGs: {str(e)}")
        
        return dags
    
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