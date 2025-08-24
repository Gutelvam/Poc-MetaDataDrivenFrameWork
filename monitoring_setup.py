#!/usr/bin/env python3
"""
Monitoring Setup Script for Airflow Framework
Sets up Prometheus and Grafana monitoring for the metadata-driven pipeline framework
"""

import os
import json
import yaml
import requests
import time
import logging
from pathlib import Path
from typing import Dict, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MonitoringSetup:
    """Setup monitoring for Airflow Framework"""
    
    def __init__(self):
        self.prometheus_url = "http://localhost:9090"
        self.grafana_url = "http://localhost:3000"
        self.grafana_user = "admin"
        self.grafana_pass = "admin"
        self.project_root = Path(__file__).parent
    
    def setup_all(self):
        """Setup complete monitoring stack"""
        logger.info("🚀 Setting up Airflow Framework Monitoring")
        
        # 1. Create directory structure
        self.create_monitoring_directories()
        
        # 2. Create configuration files
        self.create_prometheus_config()
        self.create_grafana_config()
        
        # 3. Wait for services to be up
        logger.info("⏳ Waiting for services to start...")
        time.sleep(30)
        
        # 4. Setup Grafana
        self.setup_grafana()
        
        # 5. Verify setup
        self.verify_setup()
        
        logger.info("✅ Monitoring setup complete!")
        self.print_access_info()
    
    def create_monitoring_directories(self):
        """Create necessary monitoring directories"""
        logger.info("📁 Creating monitoring directories...")
        
        directories = [
            "monitoring/prometheus",
            "monitoring/grafana/dashboards",
            "monitoring/grafana/provisioning/dashboards",
            "monitoring/grafana/provisioning/datasources",
            "monitoring/alerts"
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
            logger.info(f"  ✓ {directory}")
    
    def create_prometheus_config(self):
        """Create Prometheus configuration"""
        logger.info("⚙️ Creating Prometheus configuration...")
        
        prometheus_config = {
            'global': {
                'scrape_interval': '15s',
                'evaluation_interval': '15s'
            },
            'rule_files': [
                'airflow_alerts.yml'
            ],
            'scrape_configs': [
                {
                    'job_name': 'prometheus',
                    'static_configs': [{'targets': ['localhost:9090']}]
                },
                {
                    'job_name': 'airflow-apiserver',
                    'static_configs': [{'targets': ['airflow-apiserver:8080']}],
                    'metrics_path': '/admin/metrics',
                    'scrape_interval': '30s'
                },
                {
                    'job_name': 'framework-metrics',
                    'static_configs': [{'targets': ['airflow-apiserver:8090']}],
                    'metrics_path': '/metrics',
                    'scrape_interval': '15s'
                }
            ]
        }
        
        config_path = Path("monitoring/prometheus/prometheus.yml")
        with open(config_path, 'w') as f:
            yaml.dump(prometheus_config, f, default_flow_style=False)
        
        logger.info(f"  ✓ Created {config_path}")
    
    def create_grafana_config(self):
        """Create Grafana provisioning configuration"""
        logger.info("⚙️ Creating Grafana configuration...")
        
        # Datasource configuration
        datasource_config = {
            'apiVersion': 1,
            'datasources': [
                {
                    'name': 'Prometheus',
                    'type': 'prometheus',
                    'access': 'proxy',
                    'url': 'http://prometheus:9090',
                    'isDefault': True,
                    'editable': False,
                    'jsonData': {
                        'httpMethod': 'POST',
                        'timeInterval': '15s',
                        'queryTimeout': '60s'
                    }
                }
            ]
        }
        
        datasource_path = Path("monitoring/grafana/provisioning/datasources/prometheus.yaml")
        with open(datasource_path, 'w') as f:
            yaml.dump(datasource_config, f, default_flow_style=False)
        
        # Dashboard provisioning configuration
        dashboard_config = {
            'apiVersion': 1,
            'providers': [
                {
                    'name': 'default',
                    'orgId': 1,
                    'folder': '',
                    'type': 'file',
                    'disableDeletion': False,
                    'updateIntervalSeconds': 10,
                    'allowUiUpdates': True,
                    'options': {
                        'path': '/var/lib/grafana/dashboards'
                    }
                }
            ]
        }
        
        dashboard_path = Path("monitoring/grafana/provisioning/dashboards/dashboard.yaml")
        with open(dashboard_path, 'w') as f:
            yaml.dump(dashboard_config, f, default_flow_style=False)
        
        logger.info(f"  ✓ Created Grafana configurations")
    
    def wait_for_service(self, url: str, name: str, timeout: int = 60):
        """Wait for a service to be available"""
        logger.info(f"⏳ Waiting for {name} at {url}")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    logger.info(f"  ✓ {name} is available")
                    return True
            except requests.exceptions.RequestException:
                pass
            
            time.sleep(5)
        
        logger.warning(f"  ⚠️ {name} not available after {timeout}s")
        return False
    
    def setup_grafana(self):
        """Setup Grafana datasources and dashboards"""
        logger.info("📊 Setting up Grafana...")
        
        # Wait for Grafana to be available
        if not self.wait_for_service(f"{self.grafana_url}/api/health", "Grafana"):
            logger.error("Grafana not available, skipping setup")
            return
        
        # Create API session
        session = requests.Session()
        session.auth = (self.grafana_user, self.grafana_pass)
        
        # Create datasource
        self.create_grafana_datasource(session)
        
        # Import dashboard
        self.import_grafana_dashboard(session)
    
    def create_grafana_datasource(self, session: requests.Session):
        """Create Prometheus datasource in Grafana"""
        logger.info("📡 Creating Prometheus datasource...")
        
        datasource = {
            "name": "Prometheus",
            "type": "prometheus",
            "url": "http://prometheus:9090",
            "access": "proxy",
            "isDefault": True,
            "jsonData": {
                "httpMethod": "POST",
                "timeInterval": "15s"
            }
        }
        
        try:
            response = session.post(
                f"{self.grafana_url}/api/datasources",
                json=datasource,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code in [200, 409]:  # 409 = already exists
                logger.info("  ✓ Prometheus datasource created/updated")
            else:
                logger.warning(f"  ⚠️ Failed to create datasource: {response.text}")
        
        except Exception as e:
            logger.error(f"  ❌ Error creating datasource: {e}")
    
    def import_grafana_dashboard(self, session: requests.Session):
        """Import Airflow Framework dashboard"""
        logger.info("📈 Importing Airflow Framework dashboard...")
        
        # Create a simplified dashboard
        dashboard = {
            "dashboard": {
                "id": None,
                "title": "Airflow Framework Overview",
                "tags": ["airflow", "framework"],
                "timezone": "browser",
                "refresh": "30s",
                "panels": [
                    {
                        "id": 1,
                        "title": "Pipeline Success Rate",
                        "type": "stat",
                        "targets": [
                            {
                                "expr": "(sum(rate(airflow_pipeline_runs_total{status=\"success\"}[5m])) / sum(rate(airflow_pipeline_runs_total[5m]))) * 100",
                                "legendFormat": "Success Rate %"
                            }
                        ],
                        "fieldConfig": {
                            "defaults": {
                                "unit": "percent",
                                "min": 0,
                                "max": 100
                            }
                        },
                        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0}
                    },
                    {
                        "id": 2,
                        "title": "Pipeline Execution Rate",
                        "type": "timeseries",
                        "targets": [
                            {
                                "expr": "rate(airflow_pipeline_runs_total[5m])",
                                "legendFormat": "{{dag_id}} - {{status}}"
                            }
                        ],
                        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0}
                    },
                    {
                        "id": 3,
                        "title": "Data Quality Scores",
                        "type": "timeseries",
                        "targets": [
                            {
                                "expr": "airflow_data_quality_score",
                                "legendFormat": "{{dag_id}} - {{rule_name}}"
                            }
                        ],
                        "fieldConfig": {
                            "defaults": {
                                "unit": "percentunit",
                                "min": 0,
                                "max": 1
                            }
                        },
                        "gridPos": {"h": 8, "w": 24, "x": 0, "y": 8}
                    }
                ],
                "time": {
                    "from": "now-1h",
                    "to": "now"
                },
                "templating": {
                    "list": [
                        {
                            "name": "dag_id",
                            "type": "query",
                            "query": "label_values(airflow_pipeline_runs_total, dag_id)",
                            "includeAll": True,
                            "multi": True
                        }
                    ]
                }
            },
            "overwrite": True
        }
        
        try:
            response = session.post(
                f"{self.grafana_url}/api/dashboards/db",
                json=dashboard,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"  ✓ Dashboard imported: {result.get('url', 'Unknown URL')}")
            else:
                logger.warning(f"  ⚠️ Failed to import dashboard: {response.text}")
        
        except Exception as e:
            logger.error(f"  ❌ Error importing dashboard: {e}")
    
    def verify_setup(self):
        """Verify monitoring setup"""
        logger.info("🔍 Verifying monitoring setup...")
        
        # Check Prometheus
        try:
            response = requests.get(f"{self.prometheus_url}/api/v1/targets", timeout=10)
            if response.status_code == 200:
                targets = response.json()['data']['activeTargets']
                healthy_targets = [t for t in targets if t['health'] == 'up']
                logger.info(f"  ✓ Prometheus: {len(healthy_targets)}/{len(targets)} targets healthy")
            else:
                logger.warning("  ⚠️ Prometheus API not responding")
        except Exception as e:
            logger.error(f"  ❌ Prometheus check failed: {e}")
        
        # Check Grafana
        try:
            response = requests.get(
                f"{self.grafana_url}/api/datasources",
                auth=(self.grafana_user, self.grafana_pass),
                timeout=10
            )
            if response.status_code == 200:
                datasources = response.json()
                logger.info(f"  ✓ Grafana: {len(datasources)} datasources configured")
            else:
                logger.warning("  ⚠️ Grafana API not responding")
        except Exception as e:
            logger.error(f"  ❌ Grafana check failed: {e}")
    
    def print_access_info(self):
        """Print access information"""
        logger.info("\n" + "="*60)
        logger.info("🎉 MONITORING SETUP COMPLETE!")
        logger.info("="*60)
        logger.info("📊 Access URLs:")
        logger.info(f"  • Grafana:    {self.grafana_url}")
        logger.info(f"  • Prometheus: {self.prometheus_url}")
        logger.info("🔑 Default Credentials:")
        logger.info(f"  • Grafana: {self.grafana_user} / {self.grafana_pass}")
        logger.info("\n📈 Key Metrics to Monitor:")
        logger.info("  • Pipeline success rate")
        logger.info("  • Task execution duration")
        logger.info("  • Data quality scores")
        logger.info("  • Record processing rates")
        logger.info("  • Error rates by type")
        logger.info("\n🔔 Alerting:")
        logger.info("  • High failure rate alerts")
        logger.info("  • Data quality degradation")
        logger.info("  • SLA violations")
        logger.info("  • Infrastructure health")
        logger.info("="*60)
    
    def create_sample_queries(self):
        """Create file with sample Prometheus queries"""
        logger.info("📝 Creating sample queries file...")
        
        queries = {
            "Pipeline Metrics": {
                "Success Rate": "sum(rate(airflow_pipeline_runs_total{status=\"success\"}[5m])) / sum(rate(airflow_pipeline_runs_total[5m]))",
                "Failure Rate": "sum(rate(airflow_pipeline_runs_total{status=\"failed\"}[5m])) / sum(rate(airflow_pipeline_runs_total[5m]))",
                "Average Duration": "avg(rate(airflow_pipeline_duration_seconds_sum[5m]) / rate(airflow_pipeline_duration_seconds_count[5m]))",
                "95th Percentile Duration": "histogram_quantile(0.95, rate(airflow_pipeline_duration_seconds_bucket[5m]))"
            },
            "Task Metrics": {
                "Task Success Rate": "sum by (dag_id) (rate(airflow_task_runs_total{status=\"success\"}[5m])) / sum by (dag_id) (rate(airflow_task_runs_total[5m]))",
                "Average Task Duration": "avg by (dag_id, task_id) (rate(airflow_task_duration_seconds_sum[5m]) / rate(airflow_task_duration_seconds_count[5m]))",
                "Active Tasks": "sum by (dag_id) (airflow_active_tasks)"
            },
            "Data Quality Metrics": {
                "Overall Quality Score": "avg(airflow_data_quality_score)",
                "Quality Score by DAG": "avg by (dag_id) (airflow_data_quality_score)",
                "Failed Quality Checks": "sum(rate(airflow_data_quality_score < 0.8[5m]))"
            },
            "Data Processing Metrics": {
                "Records Processed Rate": "sum(rate(airflow_records_processed_total[5m]))",
                "Records by Source Type": "sum by (source_type) (rate(airflow_records_processed_total[5m]))",
                "Processing Efficiency": "sum(rate(airflow_records_processed_total[5m])) / sum(airflow_active_tasks)"
            },
            "Error Metrics": {
                "Error Rate": "sum(rate(airflow_errors_total[5m]))",
                "Errors by Type": "sum by (error_type) (rate(airflow_errors_total[5m]))",
                "Errors by DAG": "sum by (dag_id) (rate(airflow_errors_total[5m]))"
            }
        }
        
        queries_path = Path("monitoring/sample_queries.yaml")
        with open(queries_path, 'w') as f:
            yaml.dump(queries, f, default_flow_style=False)
        
        logger.info(f"  ✓ Created {queries_path}")

def main():
    """Main setup function"""
    setup = MonitoringSetup()
    
    try:
        setup.setup_all()
        setup.create_sample_queries()
    except KeyboardInterrupt:
        logger.info("\n⚠️ Setup interrupted by user")
    except Exception as e:
        logger.error(f"❌ Setup failed: {e}")
        raise

if __name__ == "__main__":
    main()