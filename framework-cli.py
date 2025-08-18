#!/usr/bin/env python3
"""
CLI Manager for Metadata-Driven Pipeline Framework - Windows Batch Version
"""

import argparse
import yaml
import subprocess
import sys
import os
from pathlib import Path

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
                'tags': ['etl', 'basic', 'windows'],
                'tasks': [
                    {
                        'task_id': 'start_task',
                        'operator_type': 'dummy',
                        'description': 'Tarefa inicial'
                    },
                    {
                        'task_id': 'end_task',  
                        'operator_type': 'dummy',
                        'description': 'Tarefa final',
                        'depends_on': ['start_task']
                    }
                ]
            }
        }
        
        if template not in templates:
            print(f"❌ Template '{template}' não encontrado")
            return
        
        self.metadata_dir.mkdir(exist_ok=True)
        output_file = output_file or f"{dag_id}.yaml"
        output_path = self.metadata_dir / output_file
        
        with open(output_path, 'w', encoding='utf-8') as f:
            yaml.dump(templates[template], f, default_flow_style=False, allow_unicode=True)
        
        print(f"✅ Pipeline criado: {output_path}")
    
    def status(self):
        """Show framework status"""
        print("🔍 Status do Framework - Windows Batch")
        print("=" * 40)
        print("✅ Setup inicial concluído")
        print("📋 Use os comandos para criar e gerenciar pipelines")

def main():
    parser = argparse.ArgumentParser(description='Framework CLI - Windows')
    subparsers = parser.add_subparsers(dest='command')
    
    create_parser = subparsers.add_parser('create')
    create_parser.add_argument('template', choices=['basic_etl'])
    create_parser.add_argument('dag_id')
    
    subparsers.add_parser('status')
    
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return
    
    cli = FrameworkCLI()
    if args.command == 'create':
        cli.create_pipeline(args.template, args.dag_id)
    elif args.command == 'status':
        cli.status()

if __name__ == '__main__':
    main()
