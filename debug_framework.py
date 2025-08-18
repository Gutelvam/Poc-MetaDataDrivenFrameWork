#!/usr/bin/env python3
# debug_framework.py - Debug script for the framework

import sys
import os
import yaml
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def debug_framework():
    """Debug the framework setup"""
    
    print("🔍 Framework Debug Information")
    print("=" * 50)
    
    # 1. Check Python environment
    print(f"Python version: {sys.version}")
    print(f"Python executable: {sys.executable}")
    print(f"Current working directory: {os.getcwd()}")
    
    # 2. Check environment variables
    print(f"AIRFLOW_HOME: {os.environ.get('AIRFLOW_HOME', 'Not set')}")
    print(f"PYTHONPATH: {os.environ.get('PYTHONPATH', 'Not set')}")
    
    # 3. Check metadata directory
    metadata_paths = [
        "/opt/airflow/metadata",
        "./metadata",
        "metadata",
        os.path.join(os.getcwd(), "metadata")
    ]
    
    print("\n📁 Checking metadata directories:")
    for path in metadata_paths:
        path_obj = Path(path)
        exists = path_obj.exists()
        print(f"  {path}: {'✅ EXISTS' if exists else '❌ NOT FOUND'}")
        
        if exists:
            yaml_files = list(path_obj.glob("*.yaml")) + list(path_obj.glob("*.yml"))
            print(f"    YAML files: {len(yaml_files)}")
            for yaml_file in yaml_files:
                print(f"      - {yaml_file.name} ({yaml_file.stat().st_size} bytes)")
    
    # 4. Test YAML file reading
    print("\n📄 Testing YAML file reading:")
    
    # Find the exemplo_simples.yaml file
    test_file = None
    for path in metadata_paths:
        potential_file = Path(path) / "exemplo_simples.yaml"
        if potential_file.exists():
            test_file = potential_file
            break
    
    if test_file:
        print(f"Found test file: {test_file}")
        try:
            with open(test_file, 'r', encoding='utf-8') as f:
                content = yaml.safe_load(f)
            print("✅ YAML parsing successful")
            print(f"DAG ID: {content.get('dag_id')}")
            print(f"Description: {content.get('description')}")
            print(f"Tasks: {len(content.get('tasks', []))}")
            
            # Validate required fields
            required_fields = ['dag_id', 'description', 'tasks']
            missing = [field for field in required_fields if field not in content]
            if missing:
                print(f"❌ Missing required fields: {missing}")
            else:
                print("✅ All required fields present")
                
        except Exception as e:
            print(f"❌ YAML parsing failed: {e}")
    else:
        print("❌ exemplo_simples.yaml not found in any metadata directory")
    
    # 5. Check Airflow imports
    print("\n🐍 Testing Airflow imports:")
    try:
        import airflow
        print(f"✅ Airflow version: {airflow.__version__}")
    except ImportError as e:
        print(f"❌ Airflow import failed: {e}")
        return
    
    try:
        from airflow import DAG
        print("✅ DAG import successful")
    except ImportError as e:
        print(f"❌ DAG import failed: {e}")
    
    try:
        from airflow.operators.empty import EmptyOperator
        print("✅ EmptyOperator import successful")
    except ImportError as e:
        print(f"❌ EmptyOperator import failed: {e}")
        try:
            from airflow.operators.dummy import DummyOperator
            print("✅ DummyOperator import successful (legacy)")
        except ImportError as e2:
            print(f"❌ DummyOperator import also failed: {e2}")
    
    # 6. Test framework imports (if in DAGs directory)
    print("\n🔧 Testing framework imports:")
    
    # Add current directory to path for testing
    current_dir = Path.cwd()
    if current_dir.name == "dags" or (current_dir / "dags").exists():
        if current_dir.name != "dags":
            current_dir = current_dir / "dags"
        
        sys.path.insert(0, str(current_dir))
        
        framework_modules = [
            'core.config',
            'metadata.manager',
            'factory.dag_generator',
            'dag_factory'
        ]
        
        for module in framework_modules:
            try:
                __import__(module)
                print(f"✅ {module} import successful")
            except ImportError as e:
                print(f"❌ {module} import failed: {e}")
    
    # 7. Create a test DAG
    print("\n🧪 Creating test DAG:")
    try:
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
        
        print("✅ Test DAG creation successful")
        print(f"   DAG ID: {test_dag.dag_id}")
        print(f"   Tasks: {len(test_dag.tasks)}")
        
    except Exception as e:
        print(f"❌ Test DAG creation failed: {e}")
    
    print("\n" + "=" * 50)
    print("🏁 Debug complete!")

if __name__ == "__main__":
    debug_framework()