#!/usr/bin/env python3
"""
Script de Verificação do Framework - Airflow 3.x
Verifica se todas as correções foram aplicadas e o framework está funcionando
"""

import subprocess
import sys
import os
import json
import time
from pathlib import Path

class FrameworkChecker:
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.success = []
        
    def run_command(self, cmd, capture_output=True):
        """Execute command and return result"""
        try:
            result = subprocess.run(
                cmd, shell=True, capture_output=capture_output, 
                text=True, timeout=30
            )
            return result.returncode == 0, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            return False, "", "Command timed out"
        except Exception as e:
            return False, "", str(e)
    
    def check_docker_compose(self):
        """Check if docker-compose is running"""
        print("🔍 Verificando Docker Compose...")
        
        success, stdout, stderr = self.run_command("docker-compose ps")
        if not success:
            self.errors.append("Docker Compose não está funcionando")
            return False
        
        # Check specific services
        services = ['airflow-webserver', 'airflow-scheduler', 'postgres']
        running_services = []
        
        for service in services:
            success, stdout, stderr = self.run_command(f"docker-compose ps {service}")
            if "Up" in stdout:
                running_services.append(service)
                self.success.append(f"✅ {service} está rodando")
            else:
                self.errors.append(f"❌ {service} não está rodando")
        
        return len(running_services) >= 2  # At least webserver and scheduler
    
    def check_airflow_version(self):
        """Check Airflow version"""
        print("🔍 Verificando versão do Airflow...")
        
        cmd = "docker-compose exec -T airflow-webserver python -c 'import airflow; print(airflow.__version__)'"
        success, stdout, stderr = self.run_command(cmd)
        
        if success and stdout.strip():
            version = stdout.strip()
            self.success.append(f"✅ Airflow versão: {version}")
            if version.startswith('3.'):
                self.success.append("✅ Airflow 3.x detectado")
                return True
            else:
                self.warnings.append(f"⚠️ Versão do Airflow: {version} (esperado 3.x)")
                return False
        else:
            self.errors.append("❌ Não foi possível verificar versão do Airflow")
            return False
    
    def check_framework_imports(self):
        """Check if framework modules can be imported"""
        print("🔍 Verificando imports do framework...")
        
        imports_to_test = [
            ("airflow.operators.empty", "EmptyOperator"),
            ("core.config", "PipelineConfig"),
            ("factory.dag_generator", "DynamicDAGGenerator"),
            ("monitoring.metrics", "get_monitoring_callbacks"),
        ]
        
        all_good = True
        for module, class_name in imports_to_test:
            cmd = f"docker-compose exec -T airflow-webserver python -c 'import sys; sys.path.insert(0, \"/opt/airflow/dags\"); from {module} import {class_name}; print(\"OK\")'"
            success, stdout, stderr = self.run_command(cmd)
            
            if success and "OK" in stdout:
                self.success.append(f"✅ {module}.{class_name} importado com sucesso")
            else:
                self.errors.append(f"❌ Falha ao importar {module}.{class_name}: {stderr.strip()}")
                all_good = False
        
        return all_good
    
    def check_dag_processing(self):
        """Check if DAGs are being processed without errors"""
        print("🔍 Verificando processamento de DAGs...")
        
        # Check DAG list
        cmd = "docker-compose exec -T airflow api-server airflow dags list"
        success, stdout, stderr = self.run_command(cmd)
        
        if success:
            lines = stdout.strip().split('\n')
            dag_count = len([line for line in lines if not line.startswith('dag_id') and line.strip()])
            self.success.append(f"✅ {dag_count} DAGs detectados")
            
            # Look for framework DAGs
            if 'framework_test_windows' in stdout:
                self.success.append("✅ DAG de teste do framework encontrado")
            else:
                self.warnings.append("⚠️ DAG de teste do framework não encontrado")
            
            return True
        else:
            self.errors.append(f"❌ Erro ao listar DAGs: {stderr.strip()}")
            return False
    
    def check_dag_errors(self):
        """Check for DAG parsing errors"""
        print("🔍 Verificando erros de parsing de DAGs...")
        
        # Check scheduler logs for import errors
        cmd = "docker-compose logs airflow-scheduler 2>/dev/null | grep -i 'error\\|failed' | tail -10"
        success, stdout, stderr = self.run_command(cmd)
        
        if stdout.strip():
            recent_errors = stdout.strip().split('\n')
            if len(recent_errors) > 0:
                self.warnings.append(f"⚠️ {len(recent_errors)} erros recentes nos logs do scheduler")
                for error in recent_errors[-3:]:  # Show last 3 errors
                    self.warnings.append(f"   {error[:100]}...")
            return False
        else:
            self.success.append("✅ Nenhum erro recente nos logs do scheduler")
            return True
    
    def check_web_interface(self):
        """Check if web interface is accessible"""
        print("🔍 Verificando interface web...")
        
        cmd = "curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/health"
        success, stdout, stderr = self.run_command(cmd)
        
        if success and stdout.strip() == '200':
            self.success.append("✅ Interface web acessível (http://localhost:8080)")
            return True
        else:
            self.errors.append("❌ Interface web não acessível")
            return False
    
    def check_file_compatibility(self):
        """Check if key files have been updated for Airflow 3.x"""
        print("🔍 Verificando compatibilidade dos arquivos...")
        
        files_to_check = {
            'dags/dag_factory.py': ['EmptyOperator', 'schedule='],
            'dags/main.py': ['EmptyOperator'],
            'requirements.txt': ['paramiko>=3.0.0'],
            'docker-compose.yml': ['airflow-webserver', 'airflow-scheduler']
        }
        
        all_good = True
        for file_path, required_content in files_to_check.items():
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    content = f.read()
                
                missing = []
                for required in required_content:
                    if required not in content:
                        missing.append(required)
                
                if missing:
                    self.warnings.append(f"⚠️ {file_path} pode precisar de atualizações: {missing}")
                    all_good = False
                else:
                    self.success.append(f"✅ {file_path} atualizado")
            else:
                self.errors.append(f"❌ Arquivo não encontrado: {file_path}")
                all_good = False
        
        return all_good
    
    def run_all_checks(self):
        """Run all verification checks"""
        print("🚀 Iniciando verificação do Framework...")
        print("=" * 50)
        
        checks = [
            ("Docker Compose", self.check_docker_compose),
            ("Versão Airflow", self.check_airflow_version),
            ("Imports Framework", self.check_framework_imports),
            ("Processamento DAGs", self.check_dag_processing),
            ("Erros DAGs", self.check_dag_errors),
            ("Interface Web", self.check_web_interface),
            ("Compatibilidade Arquivos", self.check_file_compatibility),
        ]
        
        results = {}
        for check_name, check_func in checks:
            try:
                results[check_name] = check_func()
                time.sleep(1)  # Small delay between checks
            except Exception as e:
                self.errors.append(f"❌ Erro na verificação {check_name}: {str(e)}")
                results[check_name] = False
        
        # Summary
        print("\n" + "=" * 50)
        print("📊 RESUMO DA VERIFICAÇÃO")
        print("=" * 50)
        
        if self.success:
            print("✅ SUCESSOS:")
            for msg in self.success:
                print(f"   {msg}")
        
        if self.warnings:
            print("\n⚠️ AVISOS:")
            for msg in self.warnings:
                print(f"   {msg}")
        
        if self.errors:
            print("\n❌ ERROS:")
            for msg in self.errors:
                print(f"   {msg}")
        
        # Overall status
        passed_checks = sum(1 for result in results.values() if result)
        total_checks = len(results)
        
        print(f"\n📈 STATUS GERAL: {passed_checks}/{total_checks} verificações passaram")
        
        if passed_checks == total_checks:
            print("🎉 FRAMEWORK FUNCIONANDO PERFEITAMENTE!")
            print("🌐 Acesse: http://localhost:8080")
            return True
        elif passed_checks >= total_checks * 0.7:
            print("⚠️ FRAMEWORK PARCIALMENTE FUNCIONANDO")
            print("🔧 Algumas correções podem ser necessárias")
            return False
        else:
            print("❌ FRAMEWORK COM PROBLEMAS SÉRIOS")
            print("🚨 Correções urgentes necessárias")
            return False
    
    def suggest_fixes(self):
        """Suggest fixes based on found issues"""
        if self.errors:
            print("\n🔧 SUGESTÕES DE CORREÇÃO:")
            print("-" * 30)
            
            if any("Docker Compose" in error for error in self.errors):
                print("1. Reiniciar containers:")
                print("   docker-compose down && docker-compose up -d")
            
            if any("import" in error.lower() for error in self.errors):
                print("2. Reconstruir imagens:")
                print("   docker-compose build --no-cache")
            
            if any("web" in error.lower() for error in self.errors):
                print("3. Verificar logs do webserver:")
                print("   docker-compose logs airflow-webserver")
            
            print("4. Aplicar correções do Airflow 3.x:")
            print("   Seguir o guia de correções fornecido")

def main():
    checker = FrameworkChecker()
    success = checker.run_all_checks()
    
    if not success:
        checker.suggest_fixes()
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()