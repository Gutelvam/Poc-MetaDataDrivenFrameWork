#!/usr/bin/env python3
"""
Script de Debug Completo para o Framework Airflow
Identifica e resolve problemas de geração de DAGs
"""

import sys
import os
import yaml
import logging
import traceback
from pathlib import Path
import subprocess
import json

# Setup logging detalhado
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def debug_framework_complete():
    """Debug completo do framework"""
    
    print("🔍 FRAMEWORK DEBUG COMPLETO")
    print("=" * 60)
    
    # 1. Verificar ambiente Python
    print("\n1️⃣ AMBIENTE PYTHON")
    print("-" * 30)
    print(f"Python version: {sys.version}")
    print(f"Python executable: {sys.executable}")
    print(f"Current working directory: {os.getcwd()}")
    print(f"PYTHONPATH: {os.environ.get('PYTHONPATH', 'Not set')}")
    
    # 2. Verificar Airflow
    print("\n2️⃣ VERIFICAÇÃO AIRFLOW")
    print("-" * 30)
    try:
        import airflow
        print(f"✅ Airflow version: {airflow.__version__}")
        
        # Verificar configuração do Airflow
        from airflow.configuration import conf
        dags_folder = conf.get('core', 'dags_folder')
        print(f"📁 DAGs folder configured: {dags_folder}")
        
        # Verificar outros parâmetros importantes
        load_examples = conf.get('core', 'load_examples')
        print(f"📋 Load examples: {load_examples}")
        
    except ImportError as e:
        print(f"❌ Airflow import failed: {e}")
        return False
    
    # 3. Verificar estrutura de diretórios
    print("\n3️⃣ ESTRUTURA DE DIRETÓRIOS")
    print("-" * 30)
    
    # Possíveis caminhos de metadata
    metadata_paths = [
        "/opt/airflow/metadata",
        "./metadata", 
        "metadata",
        os.path.join(os.getcwd(), "metadata"),
        os.path.join(os.path.dirname(__file__), "metadata")
    ]
    
    metadata_found = None
    for path in metadata_paths:
        path_obj = Path(path)
        exists = path_obj.exists()
        print(f"  📂 {path}: {'✅ EXISTS' if exists else '❌ NOT FOUND'}")
        
        if exists and not metadata_found:
            metadata_found = path_obj
            yaml_files = list(path_obj.glob("*.yaml")) + list(path_obj.glob("*.yml"))
            print(f"    📄 YAML files: {len(yaml_files)}")
            for yaml_file in yaml_files:
                try:
                    size = yaml_file.stat().st_size
                    print(f"      - {yaml_file.name} ({size} bytes)")
                except:
                    print(f"      - {yaml_file.name} (error reading)")
    
    # 4. Verificar DAGs folder
    print("\n4️⃣ DAGS FOLDER")
    print("-" * 30)
    
    dags_paths = [
        "/opt/airflow/dags",
        "./dags",
        "dags",
        os.path.join(os.getcwd(), "dags")
    ]
    
    dags_found = None
    for path in dags_paths:
        path_obj = Path(path)
        exists = path_obj.exists()
        print(f"  📂 {path}: {'✅ EXISTS' if exists else '❌ NOT FOUND'}")
        
        if exists and not dags_found:
            dags_found = path_obj
            py_files = list(path_obj.glob("*.py"))
            print(f"    🐍 Python files: {len(py_files)}")
            for py_file in py_files:
                print(f"      - {py_file.name}")
    
    # 5. Testar YAML parsing
    print("\n5️⃣ TESTE YAML PARSING")
    print("-" * 30)
    
    if metadata_found:
        yaml_files = list(metadata_found.glob("*.yaml")) + list(metadata_found.glob("*.yml"))
        
        for yaml_file in yaml_files:
            print(f"\n🔍 Testando: {yaml_file.name}")
            try:
                with open(yaml_file, 'r', encoding='utf-8') as f:
                    content = yaml.safe_load(f)
                
                print("  ✅ YAML parsing OK")
                
                # Validar campos obrigatórios
                required_fields = ['dag_id', 'description', 'tasks']
                missing = [field for field in required_fields if field not in content]
                
                if missing:
                    print(f"  ❌ Missing fields: {missing}")
                else:
                    print("  ✅ Required fields OK")
                    print(f"    - DAG ID: {content.get('dag_id')}")
                    print(f"    - Tasks: {len(content.get('tasks', []))}")
                
            except Exception as e:
                print(f"  ❌ YAML error: {e}")
    else:
        print("❌ No metadata directory found")
    
    # 6. Testar imports do framework
    print("\n6️⃣ IMPORTS DO FRAMEWORK")
    print("-" * 30)
    
    if dags_found:
        # Adicionar ao path
        sys.path.insert(0, str(dags_found))
        
        framework_modules = [
            'core.config',
            'metadata.manager', 
            'factory.dag_generator',
            'dag_factory'
        ]
        
        for module in framework_modules:
            try:
                __import__(module)
                print(f"  ✅ {module}")
            except ImportError as e:
                print(f"  ❌ {module}: {e}")
                # Tentar diagnosticar o problema
                try:
                    module_path = module.replace('.', '/')
                    module_file = dags_found / f"{module_path}.py"
                    if module_file.exists():
                        print(f"    📁 File exists: {module_file}")
                    else:
                        print(f"    📁 File missing: {module_file}")
                except:
                    pass
    
    # 7. Testar criação de DAGs
    print("\n7️⃣ TESTE CRIAÇÃO DE DAGS")
    print("-" * 30)
    
    try:
        # Testar DAG simples
        from airflow import DAG
        from airflow.operators.empty import EmptyOperator
        from datetime import datetime
        
        test_dag = DAG(
            'debug_test_dag',
            default_args={
                'owner': 'debug',
                'start_date': datetime(2024, 1, 1),
                'retries': 0,
            },
            description='Debug test DAG',
            schedule=None,
            catchup=False,
            tags=['debug', 'test']
        )
        
        test_task = EmptyOperator(
            task_id='test_task',
            dag=test_dag
        )
        
        print("  ✅ Basic DAG creation OK")
        
        # Testar framework DAG generation
        if metadata_found:
            try:
                from factory.dag_generator import DynamicDAGGenerator
                
                generator = DynamicDAGGenerator(str(metadata_found))
                dags = generator.generate_dags()
                
                print(f"  ✅ Framework DAG generation OK")
                print(f"    Generated DAGs: {len(dags)}")
                for dag_id in dags.keys():
                    print(f"      - {dag_id}")
                    
            except Exception as e:
                print(f"  ❌ Framework DAG generation failed: {e}")
                print(f"    Error details: {traceback.format_exc()}")
        
    except Exception as e:
        print(f"  ❌ DAG creation failed: {e}")
    
    # 8. Verificar comando airflow
    print("\n8️⃣ VERIFICAÇÃO COMANDOS AIRFLOW")
    print("-" * 30)
    
    try:
        # Verificar se o comando airflow funciona
        result = subprocess.run(['airflow', 'version'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print(f"  ✅ Airflow CLI: {result.stdout.strip()}")
        else:
            print(f"  ❌ Airflow CLI error: {result.stderr}")
    except Exception as e:
        print(f"  ❌ Airflow CLI not available: {e}")
    
    # 9. Verificar banco de dados
    print("\n9️⃣ VERIFICAÇÃO BANCO DE DADOS")
    print("-" * 30)
    
    try:
        from airflow.configuration import conf
        sql_alchemy_conn = conf.get('database', 'sql_alchemy_conn')
        print(f"  📊 Database connection: {sql_alchemy_conn}")
        
        # Testar conexão com banco
        from airflow.utils.db import check_conn
        check_conn()
        print("  ✅ Database connection OK")
        
    except Exception as e:
        print(f"  ❌ Database connection failed: {e}")
    
    # 10. Sugestões de correção
    print("\n🔧 SUGESTÕES DE CORREÇÃO")
    print("-" * 30)
    
    suggestions = []
    
    if not metadata_found:
        suggestions.append("📁 Criar diretório /opt/airflow/metadata com arquivos YAML")
    
    if not dags_found:
        suggestions.append("📁 Verificar se diretório dags existe e contém main.py")
    
    suggestions.extend([
        "🔄 Reiniciar scheduler: docker-compose restart airflow-scheduler",
        "🔄 Reiniciar webserver: docker-compose restart airflow-webserver", 
        "📋 Verificar logs: docker-compose logs airflow-scheduler",
        "🐍 Testar parsing: airflow dags list",
        "🔍 Debug específico: airflow dags show <dag_id>",
        "⚙️ Verificar configuração: airflow config list"
    ])
    
    for i, suggestion in enumerate(suggestions, 1):
        print(f"  {i}. {suggestion}")
    
    print("\n" + "=" * 60)
    print("🏁 Debug completo finalizado!")
    return True

def test_specific_yaml_file(yaml_path):
    """Testa um arquivo YAML específico"""
    
    print(f"\n🧪 TESTE ESPECÍFICO: {yaml_path}")
    print("-" * 40)
    
    try:
        with open(yaml_path, 'r', encoding='utf-8') as f:
            content = yaml.safe_load(f)
        
        print("✅ YAML parsing successful")
        print(json.dumps(content, indent=2, default=str))
        
        # Testar criação de DAG
        sys.path.insert(0, str(Path(yaml_path).parent.parent / "dags"))
        
        from dag_factory import DAGFactory
        factory = DAGFactory()
        dag = factory.create_dag(yaml_path)
        
        print(f"✅ DAG creation successful: {dag.dag_id}")
        print(f"   Tasks: {len(dag.tasks)}")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        print(f"   Error details: {traceback.format_exc()}")

if __name__ == "__main__":
    # Debug geral
    success = debug_framework_complete()
    
    # Testes específicos se solicitado
    if len(sys.argv) > 1:
        yaml_file = sys.argv[1]
        if Path(yaml_file).exists():
            test_specific_yaml_file(yaml_file)
        else:
            print(f"\n❌ File not found: {yaml_file}")