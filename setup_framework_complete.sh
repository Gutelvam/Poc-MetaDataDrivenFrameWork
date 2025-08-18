#!/bin/bash

# =====================================================================
# Framework Setup Script - VERSÃO COMPLETA
# File: setup_framework_complete.sh
# =====================================================================

set -e

echo "🚀 Setting up Complete Metadata-Driven Pipeline Framework..."

# Create directory structure
echo "📁 Creating directory structure..."
mkdir -p {dags/{core,metadata,sources,sinks,transforms,quality,monitoring,factory},metadata,scripts,config,schemas,logs,plugins,monitoring/{grafana/{dashboards,datasources},prometheus,nginx}}

# Create __init__.py files for Python modules
echo "🐍 Creating Python module files..."
touch dags/__init__.py
touch dags/core/__init__.py
touch dags/metadata/__init__.py
touch dags/sources/__init__.py
touch dags/sinks/__init__.py
touch dags/transforms/__init__.py
touch dags/quality/__init__.py
touch dags/monitoring/__init__.py
touch dags/factory/__init__.py

# Create dag_factory.py in the correct location
echo "🏭 Creating DAG Factory..."
cat > dags/dag_factory.py << 'EOF'
"""
DAG Factory Core Implementation
Creates DAGs dynamically from metadata configurations
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup

from core.config import PipelineConfig, TaskConfig, OperatorType
from metadata.manager import MetadataManager
from sources.operators import create_source_operator
from sinks.operators import create_sink_operator
from transforms.operators import (
    create_sql_transform_operator, create_python_transform_operator,
    create_custom_script_transform_operator, create_validation_transform_operator,
    create_aggregation_transform_operator
)
from quality.operators import create_data_quality_operator, create_data_profile_operator

logger = logging.getLogger(__name__)

class DAGFactory:
    """Factory class for creating DAGs from pipeline configurations"""
    
    def __init__(self, metadata_manager: MetadataManager):
        self.metadata_manager = metadata_manager
    
    def create_dag(self, config_file: str) -> DAG:
        """Create a DAG from a configuration file"""
        try:
            # Load pipeline configuration
            pipeline_config = self.metadata_manager.load_pipeline_config(config_file)
            
            # Create DAG
            dag = self._create_dag_object(pipeline_config)
            
            # Create tasks
            tasks = self._create_tasks(pipeline_config, dag)
            
            # Set task dependencies
            self._set_task_dependencies(pipeline_config, tasks)
            
            # Save pipeline metadata
            self.metadata_manager.save_pipeline_metadata(pipeline_config)
            
            logger.info(f"Successfully created DAG: {pipeline_config.dag_id}")
            return dag
            
        except Exception as e:
            logger.error(f"Failed to create DAG from {config_file}: {str(e)}")
            raise
    
    def _create_dag_object(self, config: PipelineConfig) -> DAG:
        """Create the base DAG object"""
        
        # Parse start date
        start_date = datetime.strptime(config.start_date, '%Y-%m-%d')
        
        # Default args for all tasks
        default_args = {
            'owner': config.owner,
            'depends_on_past': False,
            'start_date': start_date,
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': config.retries,
            'retry_delay': timedelta(seconds=config.retry_delay),
        }
        
        # Add SLA if specified
        if config.sla:
            default_args['sla'] = timedelta(minutes=config.sla)
        
        # Create DAG
        dag = DAG(
            dag_id=config.dag_id,
            default_args=default_args,
            description=config.description,
            schedule_interval=config.schedule_interval,
            start_date=start_date,
            catchup=config.catchup,
            max_active_runs=config.max_active_runs,
            tags=config.tags,
            doc_md=self._generate_dag_documentation(config)
        )
        
        return dag
    
    def _create_tasks(self, config: PipelineConfig, dag: DAG) -> Dict[str, Any]:
        """Create all tasks for the DAG"""
        tasks = {}
        
        for task_config in config.tasks:
            try:
                task = self._create_single_task(task_config, dag, config)
                tasks[task_config.task_id] = task
                
            except Exception as e:
                logger.error(f"Failed to create task {task_config.task_id}: {str(e)}")
                raise
        
        return tasks
    
    def _create_single_task(self, task_config: TaskConfig, dag: DAG, pipeline_config: PipelineConfig):
        """Create a single task based on its configuration"""
        
        # Common task arguments
        task_kwargs = {
            'task_id': task_config.task_id,
            'dag': dag,
            'retries': task_config.retries,
            'retry_delay': timedelta(seconds=task_config.retry_delay),
            'execution_timeout': timedelta(seconds=task_config.timeout),
            'trigger_rule': task_config.trigger_rule,
        }
        
        if task_config.description:
            task_kwargs['doc'] = task_config.description
        
        # Create task based on operator type
        if task_config.operator_type == OperatorType.DUMMY:
            return DummyOperator(**task_kwargs)
        
        elif task_config.operator_type == OperatorType.EXTRACT:
            if not task_config.source:
                raise ValueError(f"Extract task {task_config.task_id} requires source configuration")
            
            return create_source_operator(
                task_id=task_config.task_id,
                source_config=task_config.source,
                dag=dag,
                **task_kwargs
            )
        
        elif task_config.operator_type == OperatorType.LOAD:
            if not task_config.sink:
                raise ValueError(f"Load task {task_config.task_id} requires sink configuration")
            
            # Find upstream task that provides data
            upstream_task_id = self._find_data_providing_task(task_config, pipeline_config)
            
            return create_sink_operator(
                task_id=task_config.task_id,
                sink_config=task_config.sink,
                data_source_task_id=upstream_task_id,
                dag=dag,
                **task_kwargs
            )
        
        elif task_config.operator_type == OperatorType.TRANSFORM:
            return self._create_transform_task(task_config, dag, pipeline_config, task_kwargs)
        
        elif task_config.operator_type == OperatorType.QUALITY_CHECK:
            if not task_config.quality_rules:
                raise ValueError(f"Quality check task {task_config.task_id} requires quality_rules")
            
            upstream_task_id = self._find_data_providing_task(task_config, pipeline_config)
            
            return create_data_quality_operator(
                task_id=task_config.task_id,
                quality_rules=task_config.quality_rules,
                data_source_task_id=upstream_task_id,
                dag=dag,
                fail_on_error=task_config.custom_params.get('fail_on_error', False) if task_config.custom_params else False,
                **task_kwargs
            )
        
        elif task_config.operator_type == OperatorType.CUSTOM:
            return self._create_custom_task(task_config, dag, task_kwargs)
        
        else:
            raise ValueError(f"Unsupported operator type: {task_config.operator_type}")
    
    def _create_transform_task(self, task_config: TaskConfig, dag: DAG, pipeline_config: PipelineConfig, task_kwargs: Dict):
        """Create transformation task"""
        
        # Find upstream data providing tasks
        upstream_task_ids = self._find_upstream_data_tasks(task_config, pipeline_config)
        
        if task_config.sql_transform:
            return create_sql_transform_operator(
                sql_query=task_config.sql_transform,
                data_source_task_ids=upstream_task_ids,
                dag=dag,
                **task_kwargs
            )
        
        elif task_config.python_transform:
            return create_python_transform_operator(
                python_callable=task_config.python_transform,
                data_source_task_ids=upstream_task_ids,
                op_kwargs=task_config.custom_params or {},
                dag=dag,
                **task_kwargs
            )
        
        elif task_config.custom_script:
            return create_custom_script_transform_operator(
                script_path=task_config.custom_script,
                data_source_task_ids=upstream_task_ids,
                script_args=task_config.custom_params.get('script_args', []) if task_config.custom_params else [],
                script_env=task_config.custom_params.get('script_env', {}) if task_config.custom_params else {},
                dag=dag,
                **task_kwargs
            )
        
        else:
            # Default Python transform
            def default_transform(datasets, **context):
                if datasets:
                    return list(datasets.values())[0]
                return []
            
            return create_python_transform_operator(
                python_callable=default_transform,
                data_source_task_ids=upstream_task_ids,
                dag=dag,
                **task_kwargs
            )
    
    def _create_custom_task(self, task_config: TaskConfig, dag: DAG, task_kwargs: Dict):
        """Create custom task"""
        if task_config.custom_function:
            
            def custom_callable(**context):
                return {"message": f"Custom task {task_config.task_id} completed"}
            
            return PythonOperator(
                python_callable=custom_callable,
                op_kwargs=task_config.custom_params or {},
                **task_kwargs
            )
        else:
            return DummyOperator(**task_kwargs)
    
    def _find_data_providing_task(self, task_config: TaskConfig, pipeline_config: PipelineConfig) -> Optional[str]:
        """Find the first upstream task that provides data"""
        if not task_config.depends_on:
            return None
        
        # Look for extract or transform tasks in dependencies
        for dep_task_id in task_config.depends_on:
            dep_task = self._find_task_config(dep_task_id, pipeline_config)
            if dep_task and dep_task.operator_type in [OperatorType.EXTRACT, OperatorType.TRANSFORM]:
                return dep_task_id
        
        return task_config.depends_on[0]
    
    def _find_upstream_data_tasks(self, task_config: TaskConfig, pipeline_config: PipelineConfig) -> List[str]:
        """Find all upstream tasks that provide data"""
        if not task_config.depends_on:
            return []
        
        data_tasks = []
        for dep_task_id in task_config.depends_on:
            dep_task = self._find_task_config(dep_task_id, pipeline_config)
            if dep_task and dep_task.operator_type in [OperatorType.EXTRACT, OperatorType.TRANSFORM]:
                data_tasks.append(dep_task_id)
        
        return data_tasks or task_config.depends_on
    
    def _find_task_config(self, task_id: str, pipeline_config: PipelineConfig) -> Optional[TaskConfig]:
        """Find task configuration by task_id"""
        for task in pipeline_config.tasks:
            if task.task_id == task_id:
                return task
        return None
    
    def _set_task_dependencies(self, config: PipelineConfig, tasks: Dict[str, Any]):
        """Set task dependencies"""
        for task_config in config.tasks:
            if task_config.depends_on:
                current_task = tasks[task_config.task_id]
                
                for dep_task_id in task_config.depends_on:
                    if dep_task_id in tasks:
                        dep_task = tasks[dep_task_id]
                        dep_task >> current_task
                    else:
                        logger.warning(f"Dependency task {dep_task_id} not found for task {task_config.task_id}")
    
    def _generate_dag_documentation(self, config: PipelineConfig) -> str:
        """Generate DAG documentation"""
        doc = f"""
# {config.dag_id}

{config.description}

## Pipeline Configuration

- **Type**: {config.pipeline_type}
- **Owner**: {config.owner}
- **Schedule**: {config.schedule_interval or 'Manual'}
- **Start Date**: {config.start_date}
- **Tags**: {', '.join(config.tags) if config.tags else 'None'}

## Tasks

"""
        
        for task in config.tasks:
            doc += f"### {task.task_id}\n"
            doc += f"- **Type**: {task.operator_type.value}\n"
            if task.description:
                doc += f"- **Description**: {task.description}\n"
            if task.depends_on:
                doc += f"- **Dependencies**: {', '.join(task.depends_on)}\n"
            doc += "\n"
        
        return doc
EOF

echo "🔧 Creating CLI Manager..."
cat > framework-cli.py << 'EOF'
#!/usr/bin/env python3
"""
CLI Manager for Metadata-Driven Pipeline Framework
"""

import argparse
import yaml
import subprocess
import sys
from pathlib import Path
from datetime import datetime

class FrameworkCLI:
    def __init__(self):
        self.base_dir = Path(__file__).parent
        self.metadata_dir = self.base_dir / "metadata"
    
    def create_pipeline(self, template: str, dag_id: str, output_file=None):
        """Create a new pipeline from template"""
        
        templates = {
            "basic_etl": {
                'dag_id': dag_id,
                'description': f'Pipeline ETL básico - {dag_id}',
                'pipeline_type': 'batch',
                'schedule_interval': '0 2 * * *',
                'start_date': '2024-01-01',
                'catchup': False,
                'owner': 'data-team',
                'tags': ['etl', 'basic'],
                'tasks': [
                    {
                        'task_id': 'extract_data',
                        'operator_type': 'extract',
                        'source': {
                            'name': 'source_data',
                            'type': 'postgresql',
                            'connection_id': 'postgres_source',
                            'query': "SELECT * FROM my_table WHERE date = '{{ ds }}'"
                        }
                    },
                    {
                        'task_id': 'quality_check',
                        'operator_type': 'quality_check',
                        'depends_on': ['extract_data'],
                        'quality_rules': [
                            {
                                'name': 'check_not_null',
                                'rule_type': 'not_null',
                                'column': 'id',
                                'threshold': 1.0,
                                'severity': 'critical'
                            }
                        ]
                    },
                    {
                        'task_id': 'load_data',
                        'operator_type': 'load',
                        'depends_on': ['quality_check'],
                        'sink': {
                            'name': 'target_data',
                            'type': 'postgresql',
                            'connection_id': 'postgres_target',
                            'table_name': 'target_table',
                            'write_mode': 'append',
                            'auto_create_table': True
                        }
                    }
                ]
            }
        }
        
        if template not in templates:
            print(f"❌ Template '{template}' não encontrado")
            return
        
        output_file = output_file or f"{dag_id}.yaml"
        output_path = self.metadata_dir / output_file
        
        with open(output_path, 'w', encoding='utf-8') as f:
            yaml.dump(templates[template], f, default_flow_style=False, sort_keys=False, allow_unicode=True)
        
        print(f"✅ Pipeline criado: {output_path}")
    
    def validate_pipeline(self, config_file: str):
        """Validate a pipeline configuration"""
        config_path = self.metadata_dir / config_file
        
        if not config_path.exists():
            print(f"❌ Arquivo não encontrado: {config_path}")
            return False
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            required_fields = ['dag_id', 'description', 'pipeline_type', 'tasks']
            missing_fields = [field for field in required_fields if field not in config]
            
            if missing_fields:
                print(f"❌ Campos obrigatórios ausentes: {missing_fields}")
                return False
            
            print(f"✅ Pipeline '{config['dag_id']}' é válido")
            return True
            
        except Exception as e:
            print(f"❌ Erro de validação: {e}")
            return False
    
    def status(self):
        """Show framework status"""
        print("🔍 Status do Framework")
        print("=" * 40)
        
        try:
            result = subprocess.run(
                ["docker-compose", "ps"],
                capture_output=True, text=True, cwd=self.base_dir
            )
            
            if "Up" in result.stdout:
                print("✅ Serviços Docker rodando")
            else:
                print("❌ Alguns serviços podem estar parados")
            
            yaml_files = list(self.metadata_dir.glob("*.yaml"))
            print(f"📋 Pipelines configurados: {len(yaml_files)}")
            
            print("\n🌐 URLs de Acesso:")
            print("   - Airflow: http://localhost:8080")
            print("   - Grafana: http://localhost:3000")
            print("   - Prometheus: http://localhost:9090")
            
        except Exception as e:
            print(f"❌ Erro ao verificar status: {e}")

def main():
    parser = argparse.ArgumentParser(description='Framework CLI Manager')
    subparsers = parser.add_subparsers(dest='command', help='Comandos disponíveis')
    
    # Create command
    create_parser = subparsers.add_parser('create', help='Criar novo pipeline')
    create_parser.add_argument('template', choices=['basic_etl'], help='Template do pipeline')
    create_parser.add_argument('dag_id', help='ID do DAG')
    create_parser.add_argument('--output', '-o', help='Arquivo de saída')
    
    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validar pipeline')
    validate_parser.add_argument('config_file', help='Arquivo de configuração')
    
    # Status command
    subparsers.add_parser('status', help='Status do framework')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    cli = FrameworkCLI()
    
    if args.command == 'create':
        cli.create_pipeline(args.template, args.dag_id, args.output)
    elif args.command == 'validate':
        cli.validate_pipeline(args.config_file)
    elif args.command == 'status':
        cli.status()

if __name__ == '__main__':
    main()
EOF

chmod +x framework-cli.py

# Create requirements.txt with all dependencies
echo "📦 Creating requirements.txt..."
cat > requirements.txt << 'EOF'
# Core Airflow with extensions
apache-airflow[postgres,celery,redis,http,mongodb,azure,aws,google]==2.8.0

# Database drivers and connectors
psycopg2-binary==2.9.9
pymongo==4.6.1
clickhouse-connect==0.7.7
sqlalchemy==1.4.53

# Cloud storage
azure-storage-blob==12.19.0
boto3==1.34.34
google-cloud-storage==2.13.0

# Data processing
pandas==2.1.4
numpy==1.24.4
pyarrow==14.0.2
scipy==1.11.4

# File transfer
paramiko==3.4.0
pysftp==0.2.9

# HTTP and APIs
requests==2.31.0
urllib3==2.1.0

# SQL processing
pandasql==0.7.3

# Validation and schemas
jsonschema==4.20.0
cerberus==1.3.5

# Machine Learning (optional)
sentence-transformers==2.2.2
scikit-learn==1.3.2

# Utilities
pyyaml==6.0.1
python-dateutil==2.8.2
python-dotenv==1.0.0

# Development and testing
pytest==7.4.3
pytest-cov==4.1.0
black==23.12.0
flake8==6.1.0
EOF

# Create example schemas
echo "📋 Creating schemas..."
cat > schemas/event_schema.json << 'EOF'
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Event Schema",
  "description": "Schema for validating incoming event data",
  "type": "object",
  "required": ["event_id", "event_type", "timestamp", "customer_id"],
  "properties": {
    "event_id": {
      "type": "string",
      "pattern": "^[A-Za-z0-9_-]+$",
      "minLength": 1,
      "maxLength": 100
    },
    "event_type": {
      "type": "string",
      "enum": ["page_view", "purchase", "login", "logout", "support_case", "email_open"]
    },
    "timestamp": {
      "type": "string",
      "format": "date-time"
    },
    "customer_id": {
      "type": "string",
      "pattern": "^[A-Za-z0-9_-]+$",
      "minLength": 1,
      "maxLength": 50
    },
    "properties": {
      "type": "object",
      "additionalProperties": true
    }
  }
}
EOF

# Create simple example pipeline
echo "📝 Creating example pipeline..."
cat > metadata/exemplo_completo.yaml << 'EOF'
dag_id: exemplo_completo
description: "Pipeline de exemplo completo do framework"
pipeline_type: batch
schedule_interval: "0 9 * * *"
start_date: "2024-01-01"
catchup: false
owner: data-team
tags:
  - exemplo
  - completo
  - framework

tasks:
  - task_id: inicio
    operator_type: dummy
    description: "Tarefa inicial"
  
  - task_id: extrair_exemplo
    operator_type: extract
    description: "Extrai dados de exemplo"
    depends_on: [inicio]
    source:
      name: dados_exemplo
      type: postgresql
      connection_id: postgres_dev
      query: |
        SELECT 
          id,
          nome,
          valor,
          data_criacao
        FROM exemplo_tabela 
        WHERE data_criacao >= '{{ ds }}'
        LIMIT 100
  
  - task_id: verificar_qualidade
    operator_type: quality_check
    description: "Verifica qualidade dos dados"
    depends_on: [extrair_exemplo]
    quality_rules:
      - name: id_nao_nulo
        rule_type: not_null
        column: id
        threshold: 1.0
        severity: critical
      
      - name: valor_positivo
        rule_type: range
        column: valor
        parameters:
          min_value: 0
          max_value: 1000000
        threshold: 0.95
        severity: warning
  
  - task_id: transformar_dados
    operator_type: transform
    description: "Aplica transformações simples"
    depends_on: [verificar_qualidade]
    sql_transform: |
      SELECT 
        id,
        UPPER(nome) as nome_upper,
        valor,
        valor * 1.1 as valor_ajustado,
        data_criacao,
        '{{ ds }}' as data_processamento
      FROM extrair_exemplo
  
  - task_id: fim
    operator_type: dummy
    description: "Tarefa final"
    depends_on: [transformar_dados]
EOF

# Create .env file
echo "🔐 Creating environment configuration..."
cat > .env << 'EOF'
# Airflow Configuration
AIRFLOW_UID=50000
AIRFLOW_GID=0

# Database Configuration
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Monitoring Configuration
PROMETHEUS_URL=http://prometheus:9090
GRAFANA_URL=http://grafana:3000

# Development Database
POSTGRES_DEV_HOST=postgres-dev
POSTGRES_DEV_PORT=5432
POSTGRES_DEV_USER=postgres
POSTGRES_DEV_PASSWORD=postgres
POSTGRES_DEV_DB=test_database
EOF

# Create validation script
echo "✅ Creating validation script..."
cat > validate_setup.sh << 'EOF'
#!/bin/bash

echo "🔍 Validating framework setup..."

# Check required directories
REQUIRED_DIRS=(
    "dags/core"
    "dags/metadata"
    "dags/sources"
    "dags/sinks"
    "dags/transforms"
    "dags/quality"
    "dags/monitoring"
    "dags/factory"
    "metadata"
    "scripts"
    "config"
    "schemas"
)

for dir in "${REQUIRED_DIRS[@]}"; do
    if [ ! -d "$dir" ]; then
        echo "❌ Missing directory: $dir"
        exit 1
    else
        echo "✅ Directory exists: $dir"
    fi
done

# Check required files
REQUIRED_FILES=(
    "dags/dag_factory.py"
    "requirements.txt"
    "docker-compose.yml"
    ".env"
    "framework-cli.py"
    "schemas/event_schema.json"
    "metadata/exemplo_completo.yaml"
)

for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        echo "❌ Missing file: $file"
        exit 1
    else
        echo "✅ File exists: $file"
    fi
done

# Test CLI
echo "🧪 Testing CLI..."
python3 framework-cli.py --help > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "✅ CLI is working"
else
    echo "❌ CLI has issues"
    exit 1
fi

echo ""
echo "🎉 Framework validation completed successfully!"
echo ""
echo "🚀 Next steps:"
echo "   1. Run: docker-compose up airflow-init"
echo "   2. Run: docker-compose up -d"
echo "   3. Run: ./framework-cli.py status"
echo "   4. Visit: http://localhost:8080 (admin/admin)"
echo ""
EOF

chmod +x validate_setup.sh

echo ""
echo "🎉 Complete Framework setup completed successfully!"
echo ""
echo "📁 Key files created:"
echo "   - dags/dag_factory.py (Core DAG Factory)"
echo "   - framework-cli.py (CLI Manager)"
echo "   - metadata/exemplo_completo.yaml (Example pipeline)"
echo ""
echo "🚀 Next steps:"
echo "   1. Run: ./validate_setup.sh"
echo "   2. Run: docker-compose up airflow-init"
echo "   3. Run: docker-compose up -d"
echo "   4. Test: ./framework-cli.py status"
echo ""
echo "💡 Quick start:"
echo "   - Create pipeline: ./framework-cli.py create basic_etl my_pipeline"
echo "   - Validate: ./framework-cli.py validate my_pipeline.yaml"
echo "   - Access Airflow: http://localhost:8080"
echo ""
EOF

chmod +x setup_framework_complete.sh

echo ""
echo "✅ **RESUMO - Onde colocar os arquivos:**"
echo ""
echo "1. 📄 **dag_factory.py** → Colocar em: **dags/dag_factory.py**"
echo "2. 📄 **framework-cli.py** → Colocar na: **raiz do projeto**"
echo "3. 📄 **docker-compose.yml** → Colocar na: **raiz do projeto**"
echo "4. 📄 **Dockerfile** → Colocar na: **raiz do projeto**"
echo ""
echo "🚀 **Para instalar tudo automaticamente:**"
echo "```bash"
echo "# Executar o setup completo"
echo "./setup_framework_complete.sh"
echo ""
echo "# Validar instalação"
echo "./validate_setup.sh"
echo ""
echo "# Subir o ambiente"
echo "docker-compose up airflow-init"
echo "docker-compose up -d"
echo ""
echo "# Testar CLI"
echo "./framework-cli.py status"
echo "```"
echo ""
echo "🎯 **O dag_factory.py deve ficar especificamente em:**"
echo "   📂 Projeto/"
echo "   └── 📂 dags/"
echo "       ├── 📄 dag_factory.py  ← AQUI"
echo "       ├── 📄 main.py"
echo "       └── 📂 outros módulos..."
echo ""