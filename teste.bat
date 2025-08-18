@echo off
REM =====================================================================
REM Framework Setup Script - WINDOWS BATCH VERSION
REM File: setup_framework.bat
REM =====================================================================

echo 🚀 Setting up Metadata-Driven Pipeline Framework for Windows...

REM Check Docker
echo 🐳 Checking Docker...
docker --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Docker not found. Please install Docker Desktop for Windows.
    echo    Download from: https://www.docker.com/products/docker-desktop
    pause
    exit /b 1
)

docker ps >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Docker is not running. Please start Docker Desktop.
    pause
    exit /b 1
)

echo ✅ Docker is running

REM Create directory structure
echo 📁 Creating directory structure...
mkdir dags\core 2>nul
mkdir dags\metadata 2>nul
mkdir dags\sources 2>nul
mkdir dags\sinks 2>nul
mkdir dags\transforms 2>nul
mkdir dags\quality 2>nul
mkdir dags\monitoring 2>nul
mkdir dags\factory 2>nul
mkdir metadata 2>nul
mkdir scripts 2>nul
mkdir config 2>nul
mkdir schemas 2>nul
mkdir logs 2>nul
mkdir plugins 2>nul

REM Create __init__.py files
echo 🐍 Creating Python module files...
echo. > dags\__init__.py
echo. > dags\core\__init__.py
echo. > dags\metadata\__init__.py
echo. > dags\sources\__init__.py
echo. > dags\sinks\__init__.py
echo. > dags\transforms\__init__.py
echo. > dags\quality\__init__.py
echo. > dags\monitoring\__init__.py
echo. > dags\factory\__init__.py

REM Create dag_factory.py
echo 🏭 Creating DAG Factory...
(
echo """
echo DAG Factory Core Implementation - Windows Batch Version
echo Creates DAGs dynamically from metadata configurations
echo """
echo.
echo import logging
echo from datetime import datetime, timedelta
echo from typing import Dict, List, Any, Optional
echo.
echo from airflow import DAG
echo from airflow.operators.dummy import DummyOperator
echo.
echo logger = logging.getLogger^(__name__^)
echo.
echo class DAGFactory:
echo     """Factory class for creating DAGs from pipeline configurations"""
echo.    
echo     def __init__^(self, metadata_manager=None^):
echo         self.metadata_manager = metadata_manager
echo.    
echo     def create_dag^(self, config_file: str^) -^> DAG:
echo         """Create a DAG from a configuration file"""
echo         try:
echo             # For initial setup, create a simple test DAG
echo             dag = DAG^(
echo                 dag_id='framework_test_windows',
echo                 default_args={
echo                     'owner': 'framework',
echo                     'start_date': datetime^(2024, 1, 1^),
echo                     'retries': 1,
echo                 },
echo                 description='Test DAG for Windows framework setup',
echo                 schedule_interval=None,
echo                 catchup=False,
echo                 tags=['framework', 'test', 'windows']
echo             ^)
echo.            
echo             dummy_task = DummyOperator^(
echo                 task_id='test_task_windows',
echo                 dag=dag
echo             ^)
echo.            
echo             return dag
echo.            
echo         except Exception as e:
echo             logger.error^(f"Failed to create DAG from {config_file}: {str^(e^)}"^)
echo             raise
) > dags\dag_factory.py

REM Create CLI for Windows
echo 🔧 Creating CLI Manager...
(
echo #!/usr/bin/env python3
echo """
echo CLI Manager for Metadata-Driven Pipeline Framework - Windows Batch Version
echo """
echo.
echo import argparse
echo import yaml
echo import subprocess
echo import sys
echo import os
echo from pathlib import Path
echo.
echo class FrameworkCLI:
echo     def __init__^(self^):
echo         self.base_dir = Path^(__file__^).parent
echo         self.metadata_dir = self.base_dir / "metadata"
echo.        
echo     def create_pipeline^(self, template: str, dag_id: str, output_file=None^):
echo         """Create a new pipeline from template"""
echo         templates = {
echo             "basic_etl": {
echo                 'dag_id': dag_id,
echo                 'description': f'Pipeline ETL básico - {dag_id}',
echo                 'pipeline_type': 'batch',
echo                 'schedule_interval': '0 2 * * *',
echo                 'start_date': '2024-01-01',
echo                 'catchup': False,
echo                 'owner': 'data-team',
echo                 'tags': ['etl', 'basic', 'windows'],
echo                 'tasks': [
echo                     {
echo                         'task_id': 'start_task',
echo                         'operator_type': 'dummy',
echo                         'description': 'Tarefa inicial'
echo                     },
echo                     {
echo                         'task_id': 'end_task',  
echo                         'operator_type': 'dummy',
echo                         'description': 'Tarefa final',
echo                         'depends_on': ['start_task']
echo                     }
echo                 ]
echo             }
echo         }
echo.        
echo         if template not in templates:
echo             print^(f"❌ Template '{template}' não encontrado"^)
echo             return
echo.        
echo         self.metadata_dir.mkdir^(exist_ok=True^)
echo         output_file = output_file or f"{dag_id}.yaml"
echo         output_path = self.metadata_dir / output_file
echo.        
echo         with open^(output_path, 'w', encoding='utf-8'^) as f:
echo             yaml.dump^(templates[template], f, default_flow_style=False, allow_unicode=True^)
echo.        
echo         print^(f"✅ Pipeline criado: {output_path}"^)
echo.    
echo     def status^(self^):
echo         """Show framework status"""
echo         print^("🔍 Status do Framework - Windows Batch"^)
echo         print^("=" * 40^)
echo         print^("✅ Setup inicial concluído"^)
echo         print^("📋 Use os comandos para criar e gerenciar pipelines"^)
echo.
echo def main^(^):
echo     parser = argparse.ArgumentParser^(description='Framework CLI - Windows'^)
echo     subparsers = parser.add_subparsers^(dest='command'^)
echo.    
echo     create_parser = subparsers.add_parser^('create'^)
echo     create_parser.add_argument^('template', choices=['basic_etl']^)
echo     create_parser.add_argument^('dag_id'^)
echo.    
echo     subparsers.add_parser^('status'^)
echo.    
echo     args = parser.parse_args^(^)
echo     if not args.command:
echo         parser.print_help^(^)
echo         return
echo.    
echo     cli = FrameworkCLI^(^)
echo     if args.command == 'create':
echo         cli.create_pipeline^(args.template, args.dag_id^)
echo     elif args.command == 'status':
echo         cli.status^(^)
echo.
echo if __name__ == '__main__':
echo     main^(^)
) > framework-cli.py

REM Create requirements.txt
echo 📦 Creating requirements.txt...
(
echo # Core Airflow
echo apache-airflow[postgres,celery,redis]==2.8.0
echo psycopg2-binary==2.9.9
echo pandas==2.1.4
echo numpy==1.24.4
echo requests==2.31.0
echo pyyaml==6.0.1
echo python-dateutil==2.8.2
) > requirements.txt

REM Create .env
echo 🔐 Creating .env file...
(
echo # Windows Configuration
echo AIRFLOW_UID=50000
echo POSTGRES_USER=airflow
echo POSTGRES_PASSWORD=airflow
echo POSTGRES_DB=airflow
echo COMPOSE_CONVERT_WINDOWS_PATHS=1
) > .env

REM Create example pipeline
echo 📝 Creating example pipeline...
(
echo dag_id: exemplo_windows_batch
echo description: "Pipeline criado via batch script"
echo pipeline_type: batch
echo schedule_interval: "0 9 * * *"
echo start_date: "2024-01-01"
echo catchup: false
echo owner: data-team
echo tags:
echo   - exemplo
echo   - windows
echo   - batch
echo.
echo tasks:
echo   - task_id: inicio
echo     operator_type: dummy
echo     description: "Tarefa inicial"
echo.  
echo   - task_id: fim
echo     operator_type: dummy
echo     description: "Tarefa final"
echo     depends_on: [inicio]
) > metadata\exemplo_windows_batch.yaml

REM Create test script
echo ✅ Creating test script...
(
echo @echo off
echo echo 🧪 Testing Framework CLI...
echo python framework-cli.py status
echo echo.
echo echo 📋 Testing pipeline creation...
echo python framework-cli.py create basic_etl teste_batch
echo echo.
echo echo ✅ Tests completed!
echo pause
) > test_framework.bat

echo.
echo 🎉 Windows Batch setup completed successfully!
echo.
echo 📁 Created structure:
echo    ├── dags\dag_factory.py
echo    ├── framework-cli.py  
echo    ├── metadata\exemplo_windows_batch.yaml
echo    └── test_framework.bat
echo.
echo 🚀 Next steps:
echo    1. Run: python framework-cli.py status
echo    2. Run: test_framework.bat
echo    3. Create pipeline: python framework-cli.py create basic_etl meu_pipeline
echo.
echo ⚠️  Make sure Docker Desktop is running!
echo.
pause