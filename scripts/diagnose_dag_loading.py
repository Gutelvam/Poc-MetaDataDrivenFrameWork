# scripts/diagnose_dag_loading.py
"""
DAG Loading Diagnostics Script
Run this to identify why your DAGs aren't being created from metadata
"""

import os
import sys
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def diagnose_dag_loading():
    """Comprehensive diagnosis of DAG loading issues"""
    
    print("🔍 DAG LOADING DIAGNOSTICS")
    print("=" * 60)
    
    # 1. Check Docker environment
    print("1. 🐳 DOCKER ENVIRONMENT CHECK")
    if os.path.exists('/.dockerenv'):
        print("   ✅ Running inside Docker container")
    else:
        print("   ⚠️  Not running in Docker (running locally)")
    
    # 2. Check Airflow environment
    print("\n2. 🌬️ AIRFLOW ENVIRONMENT CHECK")
    airflow_home = os.environ.get('AIRFLOW_HOME', 'Not set')
    print(f"   AIRFLOW_HOME: {airflow_home}")
    
    pythonpath = os.environ.get('PYTHONPATH', 'Not set')
    print(f"   PYTHONPATH: {pythonpath}")
    
    # 3. Check directory structure
    print("\n3. 📁 DIRECTORY STRUCTURE CHECK")
    
    # Check if we're in the right location
    current_dir = Path.cwd()
    print(f"   Current directory: {current_dir}")
    
    # Check dags directory
    dags_dir = Path('/opt/airflow/dags')
    if dags_dir.exists():
        print(f"   ✅ DAGs directory exists: {dags_dir}")
        dag_files = list(dags_dir.glob('*.py'))
        print(f"   📄 Python files in dags: {len(dag_files)}")
        for f in dag_files[:5]:  # Show first 5
            print(f"      • {f.name}")
        if len(dag_files) > 5:
            print(f"      ... and {len(dag_files) - 5} more")
    else:
        print(f"   ❌ DAGs directory not found: {dags_dir}")
    
    # Check metadata directory
    metadata_dir = Path('/opt/airflow/metadata')
    if metadata_dir.exists():
        print(f"   ✅ Metadata directory exists: {metadata_dir}")
        yaml_files = list(metadata_dir.glob('*.yaml')) + list(metadata_dir.glob('*.yml'))
        print(f"   📄 YAML files in metadata: {len(yaml_files)}")
        for f in yaml_files:
            print(f"      • {f.name} ({f.stat().st_size} bytes)")
    else:
        print(f"   ❌ Metadata directory not found: {metadata_dir}")
        print("   💡 Check Docker volume mounting in docker-compose.yml")
    
    # 4. Check framework modules
    print("\n4. 🏗️ FRAMEWORK MODULES CHECK")
    
    # Add dags directory to path for testing
    if dags_dir.exists():
        sys.path.insert(0, str(dags_dir))
    
    framework_modules = [
        ('core.config', 'Configuration classes'),
        ('sources.operator', 'Source operators'),
        ('sink.operator', 'Sink operators'),
        ('transformations.operator', 'Transform operators'),
        ('quality.operator', 'Quality operators'),
        ('monitoring.enhanced_metrics', 'Enhanced monitoring'),
        ('metadata.manager', 'Metadata manager'),
        ('enhanced_dag_factory', 'DAG factory')
    ]
    
    framework_status = {}
    for module_name, description in framework_modules:
        try:
            __import__(module_name)
            print(f"   ✅ {description}: {module_name}")
            framework_status[module_name] = True
        except ImportError as e:
            print(f"   ❌ {description}: {module_name} - {str(e)}")
            framework_status[module_name] = False
    
    # 5. Test metadata loading
    print("\n5. 📋 METADATA LOADING TEST")
    
    if framework_status.get('metadata.manager', False) and metadata_dir.exists():
        try:
            from metadata.manager import MetadataManager
            
            metadata_manager = MetadataManager(str(metadata_dir))
            config_files = metadata_manager.list_pipeline_configs()
            
            print(f"   ✅ MetadataManager created successfully")
            print(f"   📁 Found {len(config_files)} configuration files:")
            
            for config_file in config_files:
                print(f"      • {config_file}")
                
                # Try to load each configuration
                try:
                    pipeline_config = metadata_manager.load_pipeline_config(config_file)
                    print(f"        ✅ Loaded: {pipeline_config.dag_id} ({len(pipeline_config.tasks)} tasks)")
                except Exception as e:
                    print(f"        ❌ Failed to load: {str(e)}")
                    
        except Exception as e:
            print(f"   ❌ Metadata loading failed: {str(e)}")
    else:
        print("   ⚠️  Cannot test - metadata manager or directory not available")
    
    # 6. Test DAG factory
    print("\n6. 🏭 DAG FACTORY TEST")
    
    if framework_status.get('enhanced_dag_factory', False):
        try:
            from enhanced_dag_factory import IntegratedDAGFactory
            
            dag_factory = IntegratedDAGFactory(str(metadata_dir))
            print(f"   ✅ IntegratedDAGFactory created successfully")
            
            # Try to create a DAG from the first config file
            if metadata_dir.exists():
                yaml_files = list(metadata_dir.glob('*.yaml')) + list(metadata_dir.glob('*.yml'))
                if yaml_files:
                    test_config = yaml_files[0]
                    print(f"   🧪 Testing DAG creation with: {test_config.name}")
                    
                    try:
                        dag = dag_factory.create_dag(str(test_config))
                        print(f"   ✅ DAG created successfully: {dag.dag_id}")
                        print(f"   📋 Tasks: {[t.task_id for t in dag.tasks]}")
                    except Exception as e:
                        print(f"   ❌ DAG creation failed: {str(e)}")
                else:
                    print("   ⚠️  No YAML files found for testing")
            
        except Exception as e:
            print(f"   ❌ DAG factory test failed: {str(e)}")
    else:
        print("   ⚠️  Cannot test - DAG factory not available")
    
    # 7. Check if main loader exists
    print("\n7. 📤 DAG LOADER FILE CHECK")
    
    loader_file = dags_dir / 'load_framework_dags.py'
    if loader_file.exists():
        print(f"   ✅ Main DAG loader exists: {loader_file}")
        print(f"   📏 File size: {loader_file.stat().st_size} bytes")
        
        # Check if it's executable (basic syntax check)
        try:
            with open(loader_file, 'r') as f:
                content = f.read()
            
            # Basic checks
            if 'load_all_framework_dags' in content:
                print("   ✅ Contains load_all_framework_dags function")
            if 'globals()[dag_id] = dag' in content:
                print("   ✅ Exports DAGs to global namespace")
            if 'IntegratedDAGFactory' in content:
                print("   ✅ Uses IntegratedDAGFactory")
                
        except Exception as e:
            print(f"   ❌ Error reading loader file: {str(e)}")
    else:
        print(f"   ❌ Main DAG loader not found: {loader_file}")
        print("   💡 You need to create this file for Airflow to discover your DAGs")
    
    # 8. Summary and recommendations
    print("\n8. 📊 SUMMARY AND RECOMMENDATIONS")
    print("=" * 60)
    
    all_framework_modules = all(framework_status.values())
    metadata_exists = metadata_dir.exists()
    loader_exists = loader_file.exists() if dags_dir.exists() else False
    
    if all_framework_modules and metadata_exists and loader_exists:
        print("🎉 ALL CHECKS PASSED - Your setup looks good!")
        print("   If DAGs still don't appear, check Airflow scheduler logs:")
        print("   docker-compose logs airflow-scheduler | grep -i error")
    else:
        print("⚠️  ISSUES FOUND - Fix these problems:")
        
        if not all_framework_modules:
            failed_modules = [name for name, status in framework_status.items() if not status]
            print(f"   • Fix framework module imports: {failed_modules}")
            print("   • Check file paths and Python import structure")
        
        if not metadata_exists:
            print("   • Create metadata directory with YAML configuration files")
            print("   • Check Docker volume mounting in docker-compose.yml")
        
        if not loader_exists:
            print("   • Create the main DAG loader file: load_framework_dags.py")
            print("   • Place it directly in the /dags directory (not subdirectories)")
    
    print("\n🔍 NEXT STEPS:")
    print("1. Fix any issues identified above")
    print("2. Restart Airflow: docker-compose restart")
    print("3. Check scheduler logs: docker-compose logs airflow-scheduler")
    print("4. Check webserver logs: docker-compose logs airflow-apiserver")
    print("5. Visit Airflow UI: http://localhost:8080")

if __name__ == "__main__":
    diagnose_dag_loading()