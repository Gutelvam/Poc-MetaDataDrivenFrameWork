# dags/enhanced_main.py
"""
Enhanced Main DAG Entry Point with Monitoring Integration
Replaces main.py with comprehensive monitoring capabilities
"""

import sys
import os
import logging
import threading
from pathlib import Path
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_metadata_dags():
    """Create DAGs from metadata files with monitoring"""
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
        
        # Try to import enhanced monitoring first
        monitoring_available = False
        try:
            from monitoring.metrics import (
                enhanced_metrics_collector,
                enhanced_pipeline_monitor,
                MetricsExporter
            )
            
            # Try to start the comprehensive metrics service
            try:
                from monitoring.metrics_service import AirflowMetricsService
                
                # Start metrics service that populates real data
                def start_comprehensive_metrics():
                    try:
                        service = AirflowMetricsService(update_interval=30)
                        logger.info("🚀 Starting Comprehensive Airflow Metrics Service...")
                        service.start()
                    except Exception as e:
                        logger.error(f"Failed to start comprehensive metrics service: {e}")
                        # Fallback to basic metrics server
                        metrics_server = MetricsExporter(enhanced_metrics_collector, port=8090)
                        logger.info("🚀 Starting Basic Framework Metrics Server on port 8090...")
                        metrics_server.run()
                
                # Start comprehensive metrics in daemon thread
                metrics_thread = threading.Thread(target=start_comprehensive_metrics, daemon=True)
                metrics_thread.start()
                
                monitoring_available = True
                logger.info("✅ Enhanced monitoring system with real data initialized")
                
            except ImportError as e:
                logger.warning(f"Comprehensive metrics service not available: {e}")
                # Fallback to basic metrics server
                def start_basic_metrics_server():
                    try:
                        metrics_server = MetricsExporter(enhanced_metrics_collector, port=8090)
                        logger.info("🚀 Starting Basic Framework Metrics Server on port 8090...")
                        metrics_server.run()
                    except Exception as e:
                        logger.error(f"Failed to start basic metrics server: {e}")
                
                metrics_thread = threading.Thread(target=start_basic_metrics_server, daemon=True)
                metrics_thread.start()
                
                monitoring_available = True
                logger.info("✅ Basic monitoring system initialized")
            
        except ImportError as e:
            logger.warning(f"Enhanced monitoring not available: {e}")
            monitoring_available = False
        
        # Try to import enhanced DAG factory
        try:
            if monitoring_available:
                from dag_factory import IntegratedDAGFactory
                factory = IntegratedDAGFactory()
                logger.info("✅ Using IntegratedDAGFactory with enhanced metrics")
            else:
                from dag_factory import IntegratedDAGFactory
                factory = IntegratedDAGFactory()
                logger.info("✅ Using IntegratedDAGFactory (monitoring disabled)")
                
        except ImportError as e:
            logger.error(f"❌ Failed to import DAG factory: {e}")
            # Create emergency DAG
            emergency_dag = create_import_error_dag(str(e))
            created_dags[emergency_dag.dag_id] = emergency_dag
            return created_dags
        
        # Create DAGs using the factory
        for yaml_file in yaml_files:
            try:
                logger.info(f"Processing {yaml_file.name}...")
                dag = factory.create_dag(str(yaml_file))
                created_dags[dag.dag_id] = dag
                logger.info(f"✅ Created DAG: {dag.dag_id}")
                
                # Record DAG creation event if monitoring is available
                if monitoring_available:
                    try:
                        from monitoring.metrics import MetricEvent
                        creation_event = MetricEvent(
                            timestamp=datetime.now(),
                            dag_id=dag.dag_id,
                            task_id=None,
                            event_type='dag_registered',
                            status='success',
                            metadata={
                                'config_file': yaml_file.name,
                                'task_count': len(dag.tasks),
                                'monitoring_enabled': True
                            }
                        )
                        enhanced_metrics_collector.record_event(creation_event)
                    except Exception as e:
                        logger.warning(f"Failed to record DAG creation event: {e}")
                        
            except Exception as e:
                logger.error(f"❌ Failed to create DAG from {yaml_file.name}: {e}")
                # Create error DAG to show the specific issue
                error_dag = create_config_error_dag(yaml_file.stem, str(e))
                created_dags[error_dag.dag_id] = error_dag
        
        # Record framework startup metrics
        if monitoring_available:
            try:
                startup_event = MetricEvent(
                    timestamp=datetime.now(),
                    dag_id='_framework',
                    task_id=None,
                    event_type='framework_startup',
                    status='success',
                    metadata={
                        'total_dags_created': len(created_dags),
                        'yaml_files_processed': len(yaml_files),
                        'monitoring_enabled': True,
                        'metrics_server_port': 8090
                    }
                )
                enhanced_metrics_collector.record_event(startup_event)
                logger.info("📊 Framework startup metrics recorded")
            except Exception as e:
                logger.warning(f"Failed to record startup metrics: {e}")
    
    except Exception as e:
        logger.error(f"❌ Metadata processing completely failed: {e}")
        error_dag = create_general_error_dag(str(e))
        created_dags[error_dag.dag_id] = error_dag
    
    return created_dags

def create_import_error_dag(error_message):
    """Create DAG to show import errors"""
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    
    def show_import_error(**context):
        logger.error(f"Import Error: {error_message}")
        raise Exception(f"Framework module import failed: {error_message}")
    
    dag = DAG(
        'framework_import_error',
        default_args={'owner': 'framework', 'start_date': datetime(2024, 1, 1), 'retries': 0},
        description=f'Import Error: {error_message[:50]}...',
        schedule=None,
        catchup=False,
        tags=['error', 'import', 'framework']
    )
    
    PythonOperator(task_id='show_error', python_callable=show_import_error, dag=dag)
    return dag

def create_config_error_dag(config_name, error_message):
    """Create DAG to show configuration errors"""
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    
    def show_config_error(**context):
        logger.error(f"Configuration Error in {config_name}: {error_message}")
        raise Exception(f"Config error: {error_message}")
    
    dag = DAG(
        f'config_error_{config_name}',
        default_args={'owner': 'framework', 'start_date': datetime(2024, 1, 1), 'retries': 0},
        description=f'Config Error in {config_name}: {error_message[:50]}...',
        schedule=None,
        catchup=False,
        tags=['error', 'config', 'framework']
    )
    
    PythonOperator(task_id='show_error', python_callable=show_config_error, dag=dag)
    return dag

def create_general_error_dag(error_message):
    """Create DAG to show general errors"""
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    
    def show_general_error(**context):
        logger.error(f"General Framework Error: {error_message}")
        raise Exception(f"Framework error: {error_message}")
    
    dag = DAG(
        'framework_general_error',
        default_args={'owner': 'framework', 'start_date': datetime(2024, 1, 1), 'retries': 0},
        description=f'General Error: {error_message[:50]}...',
        schedule=None,
        catchup=False,
        tags=['error', 'general', 'framework']
    )
    
    PythonOperator(task_id='show_error', python_callable=show_general_error, dag=dag)
    return dag

def create_monitoring_health_dag():
    """Create a DAG to monitor framework health"""
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.operators.empty import EmptyOperator
    
    def check_framework_health(**context):
        """Check overall framework health"""
        health_info = {
            'timestamp': datetime.now().isoformat(),
            'metadata_path_exists': Path("/opt/airflow/metadata").exists(),
            'monitoring_enabled': False,
            'total_dags': 0,
            'metrics_server_running': False
        }
        
        try:
            # Check if monitoring is available
            from monitoring.metrics import enhanced_metrics_collector
            health_info['monitoring_enabled'] = True
            health_info['metrics_buffer_size'] = len(enhanced_metrics_collector.events_buffer)
            
            # Test metrics server
            import requests
            response = requests.get('http://localhost:8090/health', timeout=5)
            health_info['metrics_server_running'] = response.status_code == 200
            
        except Exception as e:
            health_info['monitoring_error'] = str(e)
        
        # Count DAGs in globals
        dag_count = len([k for k in globals().keys() if not k.startswith('_')])
        health_info['total_dags'] = dag_count
        
        logger.info(f"Framework Health Check: {health_info}")
        return health_info
    
    def generate_sample_metrics(**context):
        """Generate sample metrics for testing"""
        try:
            from monitoring.metrics import enhanced_metrics_collector, MetricEvent
            
            # Generate sample events
            sample_events = [
                MetricEvent(
                    timestamp=datetime.now(),
                    dag_id='health_check_dag',
                    task_id='generate_sample_metrics',
                    event_type='health_check',
                    status='success',
                    records_processed=100,
                    data_quality_score=0.95,
                    metadata={'test': True}
                )
            ]
            
            for event in sample_events:
                enhanced_metrics_collector.record_event(event)
            
            logger.info(f"Generated {len(sample_events)} sample metrics")
            return {'sample_events': len(sample_events)}
            
        except Exception as e:
            logger.warning(f"Could not generate sample metrics: {e}")
            return {'error': str(e)}
    
    dag = DAG(
        'framework_health_monitor',
        default_args={
            'owner': 'framework-monitoring',
            'start_date': datetime(2024, 1, 1),
            'retries': 1,
        },
        description='Monitor framework health and generate sample metrics',
        schedule='*/5 * * * *',  # Every 5 minutes
        catchup=False,
        tags=['framework', 'monitoring', 'health']
    )
    
    start = EmptyOperator(task_id='start', dag=dag)
    
    health_check = PythonOperator(
        task_id='check_framework_health',
        python_callable=check_framework_health,
        dag=dag
    )
    
    sample_metrics = PythonOperator(
        task_id='generate_sample_metrics',
        python_callable=generate_sample_metrics,
        dag=dag
    )
    
    end = EmptyOperator(task_id='end', dag=dag)
    
    start >> [health_check, sample_metrics] >> end
    
    return dag

# =============================================================================
# MAIN EXECUTION - This is where DAGs are created and exported
# =============================================================================

logger.info("🚀 Enhanced Framework main.py starting...")

# Dictionary to collect all DAGs
all_dags = {}

try:
    # 1. Create metadata-based DAGs with monitoring
    metadata_dags = create_metadata_dags()
    all_dags.update(metadata_dags)
    
    if metadata_dags:
        logger.info(f"✅ Created {len(metadata_dags)} metadata DAGs: {list(metadata_dags.keys())}")
    else:
        logger.info("ℹ️ No metadata DAGs created")
    
    # 2. Create monitoring health DAG
    health_dag = create_monitoring_health_dag()
    all_dags[health_dag.dag_id] = health_dag
    logger.info(f"✅ Created health monitoring DAG: {health_dag.dag_id}")
    
    logger.info(f"🎉 Total DAGs created: {len(all_dags)}")

except Exception as e:
    logger.error(f"❌ Critical error in enhanced main.py: {e}")
    # Create emergency DAG
    emergency_dag = create_general_error_dag(f"Critical main.py error: {str(e)}")
    all_dags[emergency_dag.dag_id] = emergency_dag

# =============================================================================
# CRITICAL: Export all DAGs to globals() so Airflow can find them
# =============================================================================

logger.info("📤 Exporting DAGs to global namespace...")
for dag_id, dag_obj in all_dags.items():
    globals()[dag_id] = dag_obj
    logger.info(f"   ✅ Exported: {dag_id}")

# Also export the dictionary for debugging
globals()['framework_all_dags'] = all_dags

# Export monitoring utilities for other modules
try:
    from monitoring.metrics import (
        enhanced_metrics_collector,
        enhanced_pipeline_monitor,
        get_enhanced_monitoring_callbacks,
        get_task_monitoring_callbacks
    )
    
    globals()['framework_metrics_collector'] = enhanced_metrics_collector
    globals()['framework_pipeline_monitor'] = enhanced_pipeline_monitor
    globals()['get_monitoring_callbacks'] = get_enhanced_monitoring_callbacks
    globals()['get_task_callbacks'] = get_task_monitoring_callbacks
    
    logger.info("📊 Enhanced monitoring utilities exported")
    
except ImportError:
    logger.warning("⚠️ Enhanced monitoring not available")

logger.info(f"🎯 Final exported DAGs: {list(all_dags.keys())}")
logger.info("📋 Enhanced Framework main.py completed successfully!")

# =============================================================================
# END OF ENHANCED MAIN.PY
# =============================================================================