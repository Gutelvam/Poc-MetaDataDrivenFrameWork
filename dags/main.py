# dags/main.py
"""
Fixed Main DAG Entry Point - Airflow 3.x Compatible
Versão corrigida com melhor tratamento de erros e debug
"""

import sys
import os
import logging
from pathlib import Path
from datetime import datetime
import traceback

# Setup logging mais robusto
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Função para verificar ambiente
def check_environment():
    """Verifica o ambiente de forma robusta"""
    logger.info("=== FRAMEWORK ENVIRONMENT CHECK ===")
    
    # Verificar diretórios importantes
    framework_path = Path(__file__).parent
    metadata_path = Path("/opt/airflow/metadata")
    
    logger.info(f"Framework path: {framework_path}")
    logger.info(f"Framework path exists: {framework_path.exists()}")
    logger.info(f"Metadata path: {metadata_path}")
    logger.info(f"Metadata path exists: {metadata_path.exists()}")
    
    # Listar arquivos YAML
    if metadata_path.exists():
        yaml_files = list(metadata_path.glob("*.yaml")) + list(metadata_path.glob("*.yml"))
        logger.info(f"YAML files found: {[f.name for f in yaml_files]}")
        
        # Verificar se pelo menos um arquivo existe
        if not yaml_files:
            logger.warning("No YAML files found in metadata directory!")
            return False
    else:
        logger.error(f"Metadata directory does not exist: {metadata_path}")
        return False
    
    # Verificar módulos do framework
    required_modules = ['core', 'metadata', 'factory']
    for module in required_modules:
        module_path = framework_path / module
        if not module_path.exists():
            logger.error(f"Required module directory missing: {module_path}")
            return False
        
        # Verificar se tem __init__.py
        init_file = module_path / "__init__.py"
        if not init_file.exists():
            logger.warning(f"__init__.py missing in {module_path}")
    
    return True

# Função para criar DAG de fallback
def create_fallback_dag(dag_id, description, error_message):
    """Cria DAG de fallback em caso de erro"""
    from airflow import DAG
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import PythonOperator
    
    def show_error(**context):
        logger.error(f"Framework error in {dag_id}: {error_message}")
        raise Exception(f"Framework error: {error_message}")
    
    dag = DAG(
        dag_id,
        default_args={
            'owner': 'framework',
            'depends_on_past': False,
            'start_date': datetime(2024, 1, 1),
            'retries': 0,
        },
        description=description,
        schedule=None,
        catchup=False,
        tags=['error', 'framework']
    )
    
    start = EmptyOperator(task_id='start', dag=dag)
    error_task = PythonOperator(
        task_id='show_error',
        python_callable=show_error,
        dag=dag
    )
    
    start >> error_task
    return dag

# Adicionar path do framework
framework_path = Path(__file__).parent
sys.path.insert(0, str(framework_path))

# Verificar ambiente primeiro
if not check_environment():
    logger.error("Environment check failed - creating minimal DAGs")
    
    # Criar DAGs mínimos para mostrar problemas
    error_dag = create_fallback_dag(
        'framework_environment_error',
        'Environment check failed',
        'Metadata directory or required modules missing'
    )
    globals()['framework_environment_error'] = error_dag
    
    # Criar DAG de teste simples
    from airflow import DAG
    from airflow.operators.empty import EmptyOperator
    
    test_dag = DAG(
        'framework_test_simple',
        default_args={
            'owner': 'framework',
            'start_date': datetime(2024, 1, 1),
            'retries': 0,
        },
        description='Simple test DAG - Framework has issues',
        schedule=None,
        catchup=False,
        tags=['test', 'framework']
    )
    
    test_task = EmptyOperator(task_id='test_task', dag=test_dag)
    globals()['framework_test_simple'] = test_dag
    
else:
    # Ambiente OK, tentar carregar framework
    try:
        logger.info("✅ Environment check passed - loading framework...")
        
        # Import gradual com tratamento de erro
        try:
            from core.config import PipelineConfig, TaskConfig, OperatorType
            logger.info("✅ Core config imports successful")
        except Exception as e:
            logger.error(f"❌ Core config import failed: {e}")
            raise ImportError(f"Core config import failed: {e}")
        
        try:
            from metadata.manager import MetadataManager
            logger.info("✅ Metadata manager import successful")
        except Exception as e:
            logger.error(f"❌ Metadata manager import failed: {e}")
            raise ImportError(f"Metadata manager import failed: {e}")
        
        try:
            from dag_factory import DAGFactory
            logger.info("✅ DAG factory import successful")
        except Exception as e:
            logger.error(f"❌ DAG factory import failed: {e}")
            # Tentar import alternativo
            try:
                from factory.dag_generator import DynamicDAGGenerator
                logger.info("✅ Dynamic DAG generator import successful")
            except Exception as e2:
                logger.error(f"❌ Both DAG imports failed: {e}, {e2}")
                raise ImportError(f"DAG factory imports failed: {e}")
        
        # Tentar importar monitoring (opcional)
        try:
            from monitoring.metrics import get_monitoring_callbacks
            logger.info("✅ Monitoring imports successful")
        except Exception as e:
            logger.warning(f"⚠️ Monitoring import failed: {e}")
            def get_monitoring_callbacks():
                return {}
        
        # Método 1: Tentar usar DAGFactory diretamente
        logger.info("Attempting to create DAGs using DAGFactory...")
        generated_dags = {}
        
        try:
            metadata_manager = MetadataManager("/opt/airflow/metadata")
            dag_factory = DAGFactory(metadata_manager)
            
            # Listar arquivos de configuração
            config_files = metadata_manager.list_pipeline_configs()
            logger.info(f"Found {len(config_files)} configuration files: {config_files}")
            
            if not config_files:
                logger.warning("No configuration files found!")
                # Criar DAG de aviso
                warning_dag = create_fallback_dag(
                    'no_configs_found',
                    'No configuration files found',
                    'No YAML files found in /opt/airflow/metadata'
                )
                generated_dags['no_configs_found'] = warning_dag
            else:
                # Processar cada arquivo
                for config_file in config_files:
                    try:
                        full_path = Path("/opt/airflow/metadata") / config_file
                        logger.info(f"Processing config file: {full_path}")
                        
                        if full_path.exists():
                            dag = dag_factory.create_dag(str(full_path))
                            generated_dags[dag.dag_id] = dag
                            logger.info(f"✅ Successfully created DAG: {dag.dag_id}")
                        else:
                            logger.error(f"Config file does not exist: {full_path}")
                            
                    except Exception as e:
                        logger.error(f"Failed to create DAG from {config_file}: {str(e)}")
                        logger.error(f"Error details: {traceback.format_exc()}")
                        
                        # Criar DAG de erro para mostrar o problema
                        error_dag = create_fallback_dag(
                            f"error_{Path(config_file).stem}",
                            f'Error processing {config_file}',
                            str(e)
                        )
                        generated_dags[error_dag.dag_id] = error_dag
            
        except Exception as e:
            logger.error(f"Failed to initialize DAG factory: {str(e)}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            
            # Método 2: Tentar usar DynamicDAGGenerator
            try:
                from factory.dag_generator import DynamicDAGGenerator
                logger.info("Trying DynamicDAGGenerator as fallback...")
                
                generator = DynamicDAGGenerator("/opt/airflow/metadata")
                generated_dags = generator.generate_dags()
                logger.info(f"DynamicDAGGenerator created {len(generated_dags)} DAGs")
                
            except Exception as e2:
                logger.error(f"DynamicDAGGenerator also failed: {str(e2)}")
                
                # Último recurso: criar DAG de erro
                error_dag = create_fallback_dag(
                    'framework_initialization_error',
                    'Framework initialization failed',
                    f"Both DAGFactory and DynamicDAGGenerator failed: {str(e)}"
                )
                generated_dags = {'framework_initialization_error': error_dag}
        
        # Adicionar callbacks de monitoramento
        try:
            monitoring_callbacks = get_monitoring_callbacks()
            for dag_id, dag in generated_dags.items():
                if monitoring_callbacks and hasattr(dag, 'default_args'):
                    dag.default_args.update(monitoring_callbacks)
                    logger.info(f"Added monitoring callbacks to DAG: {dag_id}")
        except Exception as e:
            logger.warning(f"Failed to add monitoring callbacks: {e}")
        
        # Disponibilizar DAGs para o Airflow
        globals().update(generated_dags)
        
        logger.info(f"🎉 Successfully loaded {len(generated_dags)} DAGs: {list(generated_dags.keys())}")
        
        # Criar função de debug
        def get_framework_status():
            """Retorna status do framework para debug"""
            return {
                'total_dags': len(generated_dags),
                'dag_ids': list(generated_dags.keys()),
                'framework_version': '1.0.0',
                'metadata_path': '/opt/airflow/metadata',
                'framework_path': str(framework_path),
                'python_path': sys.path,
            }
        
        # Exportar para debug
        __all__ = ['get_framework_status'] + list(generated_dags.keys())
        
    except Exception as e:
        logger.error(f"❌ Framework initialization failed: {str(e)}")
        logger.error(f"Full traceback:\n{traceback.format_exc()}")
        
        # Criar DAG de erro detalhado
        from airflow import DAG
        from airflow.operators.python import PythonOperator
        
        def show_detailed_error(**context):
            error_info = {
                'error_message': str(e),
                'error_type': type(e).__name__,
                'traceback': traceback.format_exc(),
                'python_path': sys.path,
                'framework_path': str(framework_path),
                'metadata_exists': Path("/opt/airflow/metadata").exists(),
                'working_directory': os.getcwd(),
            }
            logger.error(f"Detailed error info: {error_info}")
            raise Exception(f"Framework failed: {error_info}")
        
        error_dag = DAG(
            'framework_detailed_error',
            default_args={
                'owner': 'framework',
                'start_date': datetime(2024, 1, 1),
                'retries': 0,
            },
            description=f'Framework detailed error: {str(e)[:100]}',
            schedule=None,
            catchup=False,
            tags=['error', 'framework', 'detailed']
        )
        
        error_task = PythonOperator(
            task_id='show_detailed_error',
            python_callable=show_detailed_error,
            dag=error_dag
        )
        
        globals()['framework_detailed_error'] = error_dag
        
        logger.info("Created error DAG for debugging")

# Log final
logger.info("=== FRAMEWORK MAIN.PY LOADING COMPLETED ===")