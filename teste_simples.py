# test_yaml_simple.py
"""
Teste simples para verificar se os arquivos YAML estão sendo lidos
Execute este arquivo para verificar a configuração básica
"""

import yaml
import json
from pathlib import Path
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

def test_yaml_files():
    """Testa se os arquivos YAML podem ser lidos e parseados"""
    
    metadata_path = Path("/opt/airflow/metadata")
    
    print(f"🔍 Testing YAML files in: {metadata_path}")
    print(f"Directory exists: {metadata_path.exists()}")
    
    if not metadata_path.exists():
        print("❌ Metadata directory does not exist!")
        return []
    
    # Encontrar arquivos YAML
    yaml_files = list(metadata_path.glob("*.yaml")) + list(metadata_path.glob("*.yml"))
    print(f"Found {len(yaml_files)} YAML files: {[f.name for f in yaml_files]}")
    
    valid_configs = []
    
    for yaml_file in yaml_files:
        print(f"\n📄 Testing: {yaml_file.name}")
        
        try:
            with open(yaml_file, 'r', encoding='utf-8') as f:
                content = yaml.safe_load(f)
            
            print("  ✅ YAML parsing successful")
            
            # Verificar campos obrigatórios
            required_fields = ['dag_id', 'description', 'tasks']
            missing_fields = [field for field in required_fields if field not in content]
            
            if missing_fields:
                print(f"  ❌ Missing required fields: {missing_fields}")
            else:
                print(f"  ✅ All required fields present")
                print(f"    DAG ID: {content.get('dag_id')}")
                print(f"    Description: {content.get('description', '')[:50]}...")
                print(f"    Tasks: {len(content.get('tasks', []))}")
                
                valid_configs.append((yaml_file, content))
                
        except Exception as e:
            print(f"  ❌ Error parsing {yaml_file.name}: {e}")
    
    return valid_configs

def create_simple_dag_from_yaml(yaml_file, content):
    """Cria um DAG simples a partir da configuração YAML"""
    
    dag_id = content.get('dag_id')
    description = content.get('description', 'No description')
    
    # Criar DAG
    dag = DAG(
        dag_id=dag_id,
        default_args={
            'owner': content.get('owner', 'framework'),
            'depends_on_past': False,
            'start_date': datetime.strptime(content.get('start_date', '2024-01-01'), '%Y-%m-%d'),
            'retries': content.get('retries', 1),
        },
        description=description,
        schedule=content.get('schedule_interval'),
        catchup=content.get('catchup', False),
        tags=content.get('tags', ['framework']),
        max_active_runs=content.get('max_active_runs', 1)
    )
    
    # Criar tasks
    task_objects = {}
    tasks_config = content.get('tasks', [])
    
    for task_config in tasks_config:
        task_id = task_config.get('task_id')
        operator_type = task_config.get('operator_type', 'dummy')
        
        if operator_type == 'dummy':
            task = EmptyOperator(
                task_id=task_id,
                dag=dag
            )
        else:
            # Para outros tipos, criar PythonOperator com função mock
            def mock_task_function(**context):
                return f"Mock execution of {task_id} ({operator_type})"
            
            task = PythonOperator(
                task_id=task_id,
                python_callable=mock_task_function,
                dag=dag
            )
        
        task_objects[task_id] = task
    
    # Configurar dependências
    for task_config in tasks_config:
        task_id = task_config.get('task_id')
        depends_on = task_config.get('depends_on', [])
        
        if depends_on and task_id in task_objects:
            current_task = task_objects[task_id]
            
            for dependency in depends_on:
                if dependency in task_objects:
                    upstream_task = task_objects[dependency]
                    upstream_task >> current_task
    
    return dag

# Executar teste
if __name__ == "__main__":
    print("🧪 YAML Configuration Test")
    print("=" * 40)
    
    valid_configs = test_yaml_files()
    
    if valid_configs:
        print(f"\n✅ Found {len(valid_configs)} valid configurations")
        
        # Tentar criar DAGs
        created_dags = {}
        
        for yaml_file, content in valid_configs:
            try:
                dag = create_simple_dag_from_yaml(yaml_file, content)
                created_dags[dag.dag_id] = dag
                print(f"✅ Created DAG: {dag.dag_id}")
                
            except Exception as e:
                print(f"❌ Failed to create DAG from {yaml_file.name}: {e}")
        
        print(f"\n🎉 Successfully created {len(created_dags)} DAGs")
        
        # Disponibilizar para Airflow se executado como módulo
        if __name__ != "__main__":
            globals().update(created_dags)
            
    else:
        print("\n❌ No valid configurations found")

# Se executado como módulo do Airflow, criar DAGs
try:
    # Verificar se estamos no contexto do Airflow
    import airflow
    
    # Executar teste e criar DAGs
    valid_configs = test_yaml_files()
    
    if valid_configs:
        for yaml_file, content in valid_configs:
            try:
                dag = create_simple_dag_from_yaml(yaml_file, content)
                globals()[dag.dag_id] = dag
                print(f"✅ Registered DAG with Airflow: {dag.dag_id}")
                
            except Exception as e:
                print(f"❌ Failed to register DAG from {yaml_file.name}: {e}")
    
    # Criar DAG de teste se nenhum YAML válido foi encontrado
    if not valid_configs:
        test_dag = DAG(
            'yaml_test_failed',
            default_args={
                'owner': 'framework',
                'start_date': datetime(2024, 1, 1),
                'retries': 0,
            },
            description='YAML test failed - no valid configurations',
            schedule=None,
            catchup=False,
            tags=['test', 'error']
        )
        
        def show_yaml_error(**context):
            metadata_path = Path("/opt/airflow/metadata")
            error_info = {
                'metadata_path': str(metadata_path),
                'exists': metadata_path.exists(),
                'files': list(metadata_path.glob("*")) if metadata_path.exists() else [],
                'message': 'No valid YAML configurations found'
            }
            raise Exception(f"YAML Test Error: {error_info}")
        
        error_task = PythonOperator(
            task_id='show_yaml_error',
            python_callable=show_yaml_error,
            dag=test_dag
        )
        
        globals()['yaml_test_failed'] = test_dag

except ImportError:
    # Não estamos no contexto do Airflow, apenas executar teste
    pass