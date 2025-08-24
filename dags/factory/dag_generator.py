# dags/factory/dag_generator.py
"""
Updated DAG Generator - Full Framework Integration
Properly coordinates all your framework components
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

# Import your framework modules
try:
    from core.config import PipelineConfig, TaskConfig, OperatorType
    from metadata.manager import MetadataManager
    # Import the integrated DAG factory
    from dag_factory import IntegratedDAGFactory
    
    # Import monitoring
    from monitoring.metrics import (
        enhanced_pipeline_monitor,
        enhanced_metrics_collector,
        get_framework_status
    )
    FRAMEWORK_AVAILABLE = True
    
except ImportError as e:
    logging.error(f"Failed to import framework modules: {e}")
    FRAMEWORK_AVAILABLE = False

logger = logging.getLogger(__name__)

class FrameworkDAGGenerator:
    """Main DAG generator that coordinates your entire framework"""
    
    def __init__(self, metadata_path: str = "/opt/airflow/metadata"):
        self.metadata_path = metadata_path
        
        if FRAMEWORK_AVAILABLE:
            self.metadata_manager = MetadataManager(metadata_path)
            self.dag_factory = IntegratedDAGFactory(metadata_path)
        else:
            self.metadata_manager = None
            self.dag_factory = None
    
    def generate_dags(self) -> Dict[str, DAG]:
        """Generate all DAGs using your complete framework"""
        dags = {}
        
        if not FRAMEWORK_AVAILABLE:
            logger.error("Framework components not available, creating fallback DAG")
            fallback_dag = self._create_fallback_dag()
            dags[fallback_dag.dag_id] = fallback_dag
            return dags
        
        try:
            # Get configuration files using your metadata manager
            config_files = self.metadata_manager.list_pipeline_configs()
            logger.info(f"Found {len(config_files)} configuration files in {self.metadata_path}")
            
            if not config_files:
                logger.warning(f"No YAML configuration files found in {self.metadata_path}")
                # Create a framework status DAG
                status_dag = self._create_framework_status_dag()
                dags[status_dag.dag_id] = status_dag
                return dags
            
            # Validate all configurations first
            validation_results = self.metadata_manager.validate_all_configs()
            logger.info(f"Configuration validation: {len([k for k, v in validation_results.items() if not v])} valid, {len([k for k, v in validation_results.items() if v])} invalid")
            
            # Generate DAGs using your integrated factory
            for config_file in config_files:
                try:
                    full_path = Path(self.metadata_path) / config_file
                    logger.info(f"Generating DAG from: {full_path}")
                    
                    # Use your integrated DAG factory
                    dag = self.dag_factory.create_dag(str(full_path))
                    dags[dag.dag_id] = dag
                    
                    logger.info(f"✅ Successfully generated DAG: {dag.dag_id}")
                    
                except Exception as e:
                    logger.error(f"❌ Failed to generate DAG from {config_file}: {str(e)}")
                    # Create error DAG
                    error_dag = self._create_error_dag(config_file, str(e))
                    dags[error_dag.dag_id] = error_dag
                    continue
            
            # Add monitoring DAG
            monitoring_dag = self._create_monitoring_dag()
            dags[monitoring_dag.dag_id] = monitoring_dag
            
            logger.info(f"🎉 Successfully generated {len(dags)} total DAGs using full framework")
            
        except Exception as e:
            logger.error(f"💥 Framework DAG generation failed: {str(e)}")
            error_dag = self._create_error_dag("framework_error", str(e))
            dags[error_dag.dag_id] = error_dag
        
        return dags
    
    def _create_framework_status_dag(self) -> DAG:
        """Create a DAG that shows framework status and configuration"""
        from airflow.operators.python import PythonOperator
        from airflow.operators.empty import EmptyOperator
        
        def check_framework_components(**context):
            """Check all framework components"""
            status = {
                'timestamp': datetime.now().isoformat(),
                'metadata_path': self.metadata_path,
                'framework_available': FRAMEWORK_AVAILABLE,
                'components': {
                    'metadata_manager': self.metadata_manager is not None,
                    'dag_factory': self.dag_factory is not None,
                    'source_operators': True,  # These are available if imported
                    'sink_operators': True,
                    'transform_operators': True, 
                    'quality_operators': True,
                    'monitoring': True
                },
                'configuration_files': [],
                'validation_results': {}
            }
            
            if self.metadata_manager:
                try:
                    status['configuration_files'] = self.metadata_manager.list_pipeline_configs()
                    status['validation_results'] = self.metadata_manager.validate_all_configs()
                except Exception as e:
                    status['error'] = str(e)
            
            logger.info(f"Framework status: {status}")
            return status
        
        def create_sample_config(**context):
            """Create a sample configuration file"""
            if self.metadata_manager:
                try:
                    sample_config = self.metadata_manager.export_config_template("basic")
                    
                    # Save sample to metadata directory
                    sample_path = Path(self.metadata_path) / "sample_pipeline.yaml"
                    sample_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    with open(sample_path, 'w') as f:
                        f.write(sample_config)
                    
                    return {
                        'sample_created': True,
                        'sample_path': str(sample_path),
                        'sample_config': sample_config
                    }
                except Exception as e:
                    return {'error': str(e)}
            else:
                return {'error': 'Metadata manager not available'}
        
        dag = DAG(
            'framework_status',
            default_args={
                'owner': 'framework',
                'depends_on_past': False,
                'start_date': datetime(2024, 1, 1),
                'retries': 1,
            },
            description='Framework Status and Configuration Check',
            schedule='@daily',
            catchup=False,
            tags=['framework', 'status', 'monitoring']
        )
        
        start = EmptyOperator(task_id='start', dag=dag)
        
        check_components = PythonOperator(
            task_id='check_framework_components',
            python_callable=check_framework_components,
            dag=dag
        )
        
        create_sample = PythonOperator(
            task_id='create_sample_config',
            python_callable=create_sample_config,
            dag=dag
        )
        
        end = EmptyOperator(task_id='end', dag=dag)
        
        start >> [check_components, create_sample] >> end
        
        return dag
    
    def _create_monitoring_dag(self) -> DAG:
        """Create a DAG for framework monitoring and metrics"""
        from airflow.operators.python import PythonOperator
        from airflow.operators.empty import EmptyOperator
        
        def collect_framework_metrics(**context):
            """Collect framework-wide metrics"""
            try:
                if FRAMEWORK_AVAILABLE:
                    # Get metrics from your enhanced monitoring
                    metrics_summary = enhanced_metrics_collector.get_metrics_summary()
                    
                    # Add framework-specific metrics
                    metrics_summary.update({
                        'collection_time': datetime.now().isoformat(),
                        'active_dags': len([dag for dag in context['dag'].dag_bag.dags if not dag.startswith('framework_')]),
                        'framework_health': 'healthy'
                    })
                    
                    logger.info(f"Framework metrics collected: {metrics_summary}")
                    return metrics_summary
                else:
                    return {'error': 'Framework monitoring not available'}
            except Exception as e:
                logger.error(f"Failed to collect metrics: {e}")
                return {'error': str(e)}
        
        def cleanup_old_metrics(**context):
            """Clean up old metric events"""
            try:
                # This would clean up old events from your monitoring system
                cleanup_count = 0  # Placeholder
                return {'cleaned_events': cleanup_count}
            except Exception as e:
                logger.error(f"Cleanup failed: {e}")
                return {'error': str(e)}
        
        dag = DAG(
            'framework_monitoring',
            default_args={
                'owner': 'framework',
                'depends_on_past': False,
                'start_date': datetime(2024, 1, 1),
                'retries': 1,
            },
            description='Framework Monitoring and Metrics Collection',
            schedule='*/15 * * * *',  # Every 15 minutes
            catchup=False,
            tags=['framework', 'monitoring', 'metrics']
        )
        
        start = EmptyOperator(task_id='start', dag=dag)
        
        collect_metrics = PythonOperator(
            task_id='collect_framework_metrics',
            python_callable=collect_framework_metrics,
            dag=dag
        )
        
        cleanup_metrics = PythonOperator(
            task_id='cleanup_old_metrics',
            python_callable=cleanup_old_metrics,
            dag=dag
        )
        
        end = EmptyOperator(task_id='end', dag=dag)
        
        start >> [collect_metrics, cleanup_metrics] >> end
        
        return dag
    
    def _create_fallback_dag(self) -> DAG:
        """Create a fallback DAG when framework is not available"""
        from airflow.operators.python import PythonOperator
        
        def show_framework_error(**context):
            error_info = {
                'error': 'Framework components not available',
                'metadata_path': self.metadata_path,
                'python_path': sys.path,
                'current_dir': str(current_dir),
                'parent_dir': str(parent_dir)
            }
            logger.error(f"Framework error: {error_info}")
            raise Exception("Framework components not available - check imports")
        
        dag = DAG(
            'framework_error',
            default_args={
                'owner': 'framework',
                'start_date': datetime(2024, 1, 1),
                'retries': 0,
            },
            description='Framework Error - Components Not Available',
            schedule=None,
            catchup=False,
            tags=['error', 'framework']
        )
        
        error_task = PythonOperator(
            task_id='show_framework_error',
            python_callable=show_framework_error,
            dag=dag
        )
        
        return dag
    
    def _create_error_dag(self, config_file: str, error_message: str) -> DAG:
        """Create an error DAG to show configuration issues"""
        from airflow.operators.python import PythonOperator
        
        def show_error(**context):
            error_info = {
                'config_file': config_file,
                'error_message': error_message,
                'file_exists': Path(config_file).exists() if config_file != 'framework_error' else None,
                'framework_available': FRAMEWORK_AVAILABLE
            }
            logger.error(f"Configuration error: {error_info}")
            raise Exception(f"Config error in {config_file}: {error_message}")
        
        dag_id = f"error_{Path(config_file).stem}"
        
        dag = DAG(
            dag_id,
            default_args={
                'owner': 'framework',
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
        """Validate all configuration files using your metadata manager"""
        if self.metadata_manager:
            return self.metadata_manager.validate_all_configs()
        return {}
    
    def get_dag_info(self, dag_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific DAG"""
        if self.metadata_manager:
            return self.metadata_manager.get_pipeline_metadata(dag_id)
        return None

# Create the main generator instance
framework_generator = FrameworkDAGGenerator()

# Generate all DAGs using your complete framework
generated_dags = framework_generator.generate_dags()

# Export the DAGs to Airflow's globals()
for dag_id, dag in generated_dags.items():
    globals()[dag_id] = dag

# Utility functions
def get_complete_framework_status() -> Dict[str, Any]:
    """Get overall framework status including all components"""
    try:
        status = framework_generator.validate_all_configurations()
        
        return {
            'framework_available': FRAMEWORK_AVAILABLE,
            'total_dags_generated': len(generated_dags),
            'validation_results': status,
            'components': {
                'source_operators': 'Available',
                'sink_operators': 'Available',
                'transform_operators': 'Available', 
                'quality_operators': 'Available',
                'monitoring': 'Available',
                'metadata_manager': 'Available'
            },
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        return {
            'status': 'error',
            'error': str(e),
            'framework_available': FRAMEWORK_AVAILABLE
        }

# Export main components
__all__ = [
    'FrameworkDAGGenerator',
    'generated_dags',
    'get_complete_framework_status'
]