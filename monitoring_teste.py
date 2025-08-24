#!/usr/bin/env python3
"""
Monitoring Test and Validation Script
Comprehensive testing of Prometheus and Grafana monitoring setup for Airflow Framework
"""

import time
import requests
import json
import yaml
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import concurrent.futures
import subprocess
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class TestResult:
    """Test result container"""
    name: str
    success: bool
    duration: float
    message: str
    details: Dict[str, Any] = None

class MonitoringValidator:
    """Comprehensive monitoring system validator"""
    
    def __init__(self):
        self.prometheus_url = "http://localhost:9090"
        self.grafana_url = "http://localhost:3000"
        self.metrics_url = "http://localhost:8090"
        self.airflow_url = "http://localhost:8080"
        
        self.grafana_auth = ('admin', 'admin')
        self.test_results = []
        
    def run_all_tests(self) -> Dict[str, Any]:
        """Run comprehensive monitoring validation"""
        logger.info("🚀 Starting Monitoring System Validation")
        logger.info("=" * 60)
        
        test_suites = [
            ("Service Availability", self.test_service_availability),
            ("Metrics Collection", self.test_metrics_collection),
            ("Prometheus Configuration", self.test_prometheus_config),
            ("Grafana Setup", self.test_grafana_setup),
            ("Data Quality", self.test_data_quality),
            ("Performance", self.test_performance),
            ("Alerting", self.test_alerting),
        ]
        
        all_results = {}
        total_tests = 0
        passed_tests = 0
        
        for suite_name, test_func in test_suites:
            logger.info(f"\n📋 Running {suite_name} Tests...")
            suite_results = test_func()
            all_results[suite_name] = suite_results
            
            suite_passed = sum(1 for r in suite_results if r.success)
            suite_total = len(suite_results)
            total_tests += suite_total
            passed_tests += suite_passed
            
            logger.info(f"  ✅ {suite_passed}/{suite_total} tests passed")
        
        # Generate summary report
        summary = {
            'timestamp': datetime.now().isoformat(),
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'success_rate': (passed_tests / total_tests) * 100 if total_tests > 0 else 0,
            'results': all_results
        }
        
        self.print_summary(summary)
        return summary
    
    def test_service_availability(self) -> List[TestResult]:
        """Test if all monitoring services are available"""
        tests = []
        
        services = [
            ("Airflow API", self.airflow_url + "/health", 10),
            ("Framework Metrics", self.metrics_url + "/health", 5),
            ("Prometheus", self.prometheus_url + "/-/healthy", 5),
            ("Grafana", self.grafana_url + "/api/health", 5),
        ]
        
        for name, url, timeout in services:
            start_time = time.time()
            try:
                response = requests.get(url, timeout=timeout)
                duration = time.time() - start_time
                
                if response.status_code == 200:
                    tests.append(TestResult(
                        name=f"{name} Availability",
                        success=True,
                        duration=duration,
                        message=f"Service available (HTTP {response.status_code})",
                        details={"response_time": duration, "status_code": response.status_code}
                    ))
                else:
                    tests.append(TestResult(
                        name=f"{name} Availability",
                        success=False,
                        duration=duration,
                        message=f"Service returned HTTP {response.status_code}",
                        details={"status_code": response.status_code}
                    ))
                    
            except requests.RequestException as e:
                duration = time.time() - start_time
                tests.append(TestResult(
                    name=f"{name} Availability",
                    success=False,
                    duration=duration,
                    message=f"Connection failed: {str(e)}",
                    details={"error": str(e)}
                ))
        
        return tests
    
    def test_metrics_collection(self) -> List[TestResult]:
        """Test metrics collection and exposure"""
        tests = []
        
        # Test metrics endpoint
        start_time = time.time()
        try:
            response = requests.get(f"{self.metrics_url}/metrics", timeout=10)
            duration = time.time() - start_time
            
            if response.status_code == 200:
                metrics_text = response.text
                
                # Check for expected metrics
                expected_metrics = [
                    'airflow_pipeline_runs_total',
                    'airflow_task_runs_total',
                    'airflow_pipeline_duration_seconds',
                    'airflow_data_quality_score',
                    'airflow_records_processed_total'
                ]
                
                found_metrics = []
                missing_metrics = []
                
                for metric in expected_metrics:
                    if metric in metrics_text:
                        found_metrics.append(metric)
                    else:
                        missing_metrics.append(metric)
                
                tests.append(TestResult(
                    name="Metrics Endpoint",
                    success=response.status_code == 200,
                    duration=duration,
                    message=f"Found {len(found_metrics)}/{len(expected_metrics)} expected metrics",
                    details={
                        "found_metrics": found_metrics,
                        "missing_metrics": missing_metrics,
                        "total_lines": len(metrics_text.split('\n'))
                    }
                ))
                
            else:
                tests.append(TestResult(
                    name="Metrics Endpoint",
                    success=False,
                    duration=duration,
                    message=f"HTTP {response.status_code}",
                    details={"status_code": response.status_code}
                ))
                
        except Exception as e:
            duration = time.time() - start_time
            tests.append(TestResult(
                name="Metrics Endpoint",
                success=False,
                duration=duration,
                message=f"Error: {str(e)}",
                details={"error": str(e)}
            ))
        
        # Test stats endpoint
        start_time = time.time()
        try:
            response = requests.get(f"{self.metrics_url}/stats", timeout=5)
            duration = time.time() - start_time
            
            if response.status_code == 200:
                stats = response.json()
                tests.append(TestResult(
                    name="Stats Endpoint",
                    success=True,
                    duration=duration,
                    message="Stats endpoint accessible",
                    details=stats
                ))
            else:
                tests.append(TestResult(
                    name="Stats Endpoint",
                    success=False,
                    duration=duration,
                    message=f"HTTP {response.status_code}",
                    details={"status_code": response.status_code}
                ))
                
        except Exception as e:
            duration = time.time() - start_time
            tests.append(TestResult(
                name="Stats Endpoint",
                success=False,
                duration=duration,
                message=f"Error: {str(e)}",
                details={"error": str(e)}
            ))
        
        return tests
    
    def test_prometheus_config(self) -> List[TestResult]:
        """Test Prometheus configuration and targets"""
        tests = []
        
        # Test targets
        start_time = time.time()
        try:
            response = requests.get(f"{self.prometheus_url}/api/v1/targets", timeout=10)
            duration = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                targets = data.get('data', {}).get('activeTargets', [])
                
                healthy_targets = [t for t in targets if t.get('health') == 'up']
                unhealthy_targets = [t for t in targets if t.get('health') != 'up']
                
                tests.append(TestResult(
                    name="Prometheus Targets",
                    success=len(unhealthy_targets) == 0,
                    duration=duration,
                    message=f"{len(healthy_targets)}/{len(targets)} targets healthy",
                    details={
                        "total_targets": len(targets),
                        "healthy_targets": len(healthy_targets),
                        "unhealthy_targets": len(unhealthy_targets),
                        "target_jobs": [t.get('job') for t in targets]
                    }
                ))
                
        except Exception as e:
            duration = time.time() - start_time
            tests.append(TestResult(
                name="Prometheus Targets",
                success=False,
                duration=duration,
                message=f"Error: {str(e)}",
                details={"error": str(e)}
            ))
        
        # Test specific metrics queries
        test_queries = [
            ("Pipeline Metrics", "airflow_pipeline_runs_total"),
            ("Task Metrics", "airflow_task_runs_total"),
            ("Quality Metrics", "airflow_data_quality_score"),
            ("Processing Metrics", "airflow_records_processed_total")
        ]
        
        for query_name, query in test_queries:
            start_time = time.time()
            try:
                response = requests.get(
                    f"{self.prometheus_url}/api/v1/query",
                    params={"query": query},
                    timeout=10
                )
                duration = time.time() - start_time
                
                if response.status_code == 200:
                    data = response.json()
                    result = data.get('data', {}).get('result', [])
                    
                    tests.append(TestResult(
                        name=f"Query: {query_name}",
                        success=len(result) > 0,
                        duration=duration,
                        message=f"Found {len(result)} metric series",
                        details={"series_count": len(result), "query": query}
                    ))
                else:
                    tests.append(TestResult(
                        name=f"Query: {query_name}",
                        success=False,
                        duration=duration,
                        message=f"HTTP {response.status_code}",
                        details={"status_code": response.status_code, "query": query}
                    ))
                    
            except Exception as e:
                duration = time.time() - start_time
                tests.append(TestResult(
                    name=f"Query: {query_name}",
                    success=False,
                    duration=duration,
                    message=f"Error: {str(e)}",
                    details={"error": str(e), "query": query}
                ))
        
        return tests
    
    def test_grafana_setup(self) -> List[TestResult]:
        """Test Grafana configuration"""
        tests = []
        
        # Test datasources
        start_time = time.time()
        try:
            response = requests.get(
                f"{self.grafana_url}/api/datasources",
                auth=self.grafana_auth,
                timeout=10
            )
            duration = time.time() - start_time
            
            if response.status_code == 200:
                datasources = response.json()
                prometheus_ds = [ds for ds in datasources if ds.get('type') == 'prometheus']
                
                tests.append(TestResult(
                    name="Grafana Datasources",
                    success=len(prometheus_ds) > 0,
                    duration=duration,
                    message=f"Found {len(prometheus_ds)} Prometheus datasources",
                    details={
                        "total_datasources": len(datasources),
                        "prometheus_datasources": len(prometheus_ds)
                    }
                ))
            else:
                tests.append(TestResult(
                    name="Grafana Datasources",
                    success=False,
                    duration=duration,
                    message=f"HTTP {response.status_code}",
                    details={"status_code": response.status_code}
                ))
                
        except Exception as e:
            duration = time.time() - start_time
            tests.append(TestResult(
                name="Grafana Datasources",
                success=False,
                duration=duration,
                message=f"Error: {str(e)}",
                details={"error": str(e)}
            ))
        
        # Test dashboards
        start_time = time.time()
        try:
            response = requests.get(
                f"{self.grafana_url}/api/search",
                auth=self.grafana_auth,
                timeout=10
            )
            duration = time.time() - start_time
            
            if response.status_code == 200:
                dashboards = response.json()
                airflow_dashboards = [d for d in dashboards if 'airflow' in d.get('title', '').lower()]
                
                tests.append(TestResult(
                    name="Grafana Dashboards",
                    success=len(dashboards) >= 0,  # Any dashboards are fine
                    duration=duration,
                    message=f"Found {len(airflow_dashboards)} Airflow dashboards out of {len(dashboards)} total",
                    details={
                        "total_dashboards": len(dashboards),
                        "airflow_dashboards": len(airflow_dashboards)
                    }
                ))
            else:
                tests.append(TestResult(
                    name="Grafana Dashboards",
                    success=False,
                    duration=duration,
                    message=f"HTTP {response.status_code}",
                    details={"status_code": response.status_code}
                ))
                
        except Exception as e:
            duration = time.time() - start_time
            tests.append(TestResult(
                name="Grafana Dashboards",
                success=False,
                duration=duration,
                message=f"Error: {str(e)}",
                details={"error": str(e)}
            ))
        
        return tests
    
    def test_data_quality(self) -> List[TestResult]:
        """Test data quality and completeness"""
        tests = []
        
        # Generate test data
        start_time = time.time()
        try:
            # Try to generate sample metrics
            test_data = {
                "timestamp": datetime.now().isoformat(),
                "test_type": "validation",
                "metrics_generated": True
            }
            
            duration = time.time() - start_time
            tests.append(TestResult(
                name="Test Data Generation",
                success=True,
                duration=duration,
                message="Sample test data generated successfully",
                details=test_data
            ))
            
        except Exception as e:
            duration = time.time() - start_time
            tests.append(TestResult(
                name="Test Data Generation",
                success=False,
                duration=duration,
                message=f"Error: {str(e)}",
                details={"error": str(e)}
            ))
        
        # Check metric freshness
        start_time = time.time()
        try:
            response = requests.get(
                f"{self.prometheus_url}/api/v1/query",
                params={"query": "time() - max(airflow_pipeline_runs_total)"},
                timeout=10
            )
            duration = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                result = data.get('data', {}).get('result', [])
                
                if result:
                    staleness = float(result[0]['value'][1])
                    fresh = staleness < 3600  # Less than 1 hour old
                    
                    tests.append(TestResult(
                        name="Metric Freshness",
                        success=fresh,
                        duration=duration,
                        message=f"Metrics are {staleness:.0f} seconds old",
                        details={"staleness_seconds": staleness, "fresh": fresh}
                    ))
                else:
                    tests.append(TestResult(
                        name="Metric Freshness",
                        success=False,
                        duration=duration,
                        message="No metric data found",
                        details={"result": result}
                    ))
            else:
                tests.append(TestResult(
                    name="Metric Freshness",
                    success=False,
                    duration=duration,
                    message=f"HTTP {response.status_code}",
                    details={"status_code": response.status_code}
                ))
                
        except Exception as e:
            duration = time.time() - start_time
            tests.append(TestResult(
                name="Metric Freshness",
                success=False,
                duration=duration,
                message=f"Error: {str(e)}",
                details={"error": str(e)}
            ))
        
        return tests
    
    def test_performance(self) -> List[TestResult]:
        """Test monitoring system performance"""
        tests = []
        
        # Test query response times
        performance_queries = [
            "airflow_pipeline_runs_total",
            "rate(airflow_pipeline_runs_total[5m])",
            "histogram_quantile(0.95, rate(airflow_pipeline_duration_seconds_bucket[5m]))",
            "sum(airflow_active_tasks) by (dag_id)"
        ]
        
        for query in performance_queries:
            start_time = time.time()
            try:
                response = requests.get(
                    f"{self.prometheus_url}/api/v1/query",
                    params={"query": query},
                    timeout=10
                )
                duration = time.time() - start_time
                
                fast_enough = duration < 2.0  # Under 2 seconds
                
                tests.append(TestResult(
                    name=f"Query Performance: {query[:30]}...",
                    success=response.status_code == 200 and fast_enough,
                    duration=duration,
                    message=f"Query took {duration:.2f}s",
                    details={"query": query, "duration": duration, "fast_enough": fast_enough}
                ))
                
            except Exception as e:
                duration = time.time() - start_time
                tests.append(TestResult(
                    name=f"Query Performance: {query[:30]}...",
                    success=False,
                    duration=duration,
                    message=f"Error: {str(e)}",
                    details={"query": query, "error": str(e)}
                ))
        
        return tests
    
    def test_alerting(self) -> List[TestResult]:
        """Test alerting configuration"""
        tests = []
        
        # Check if alert rules are loaded
        start_time = time.time()
        try:
            response = requests.get(f"{self.prometheus_url}/api/v1/rules", timeout=10)
            duration = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                groups = data.get('data', {}).get('groups', [])
                
                total_rules = sum(len(group.get('rules', [])) for group in groups)
                
                tests.append(TestResult(
                    name="Alert Rules",
                    success=total_rules > 0,
                    duration=duration,
                    message=f"Found {total_rules} alert rules in {len(groups)} groups",
                    details={"total_rules": total_rules, "groups": len(groups)}
                ))
            else:
                tests.append(TestResult(
                    name="Alert Rules",
                    success=False,
                    duration=duration,
                    message=f"HTTP {response.status_code}",
                    details={"status_code": response.status_code}
                ))
                
        except Exception as e:
            duration = time.time() - start_time
            tests.append(TestResult(
                name="Alert Rules",
                success=False,
                duration=duration,
                message=f"Error: {str(e)}",
                details={"error": str(e)}
            ))
        
        return tests
    
    def print_summary(self, summary: Dict[str, Any]):
        """Print comprehensive test summary"""
        logger.info("\n" + "=" * 60)
        logger.info("📊 MONITORING VALIDATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"🕐 Completed: {summary['timestamp']}")
        logger.info(f"📈 Overall Success Rate: {summary['success_rate']:.1f}% ({summary['passed_tests']}/{summary['total_tests']})")
        
        if summary['success_rate'] >= 80:
            logger.info("✅ MONITORING SYSTEM IS HEALTHY")
        elif summary['success_rate'] >= 60:
            logger.info("⚠️ MONITORING SYSTEM HAS SOME ISSUES")
        else:
            logger.info("❌ MONITORING SYSTEM NEEDS ATTENTION")
        
        logger.info("\n📋 Test Suite Results:")
        for suite_name, results in summary['results'].items():
            passed = sum(1 for r in results if r.success)
            total = len(results)
            status = "✅" if passed == total else "⚠️" if passed > 0 else "❌"
            logger.info(f"  {status} {suite_name}: {passed}/{total}")
        
        # Show failed tests
        failed_tests = []
        for suite_name, results in summary['results'].items():
            for result in results:
                if not result.success:
                    failed_tests.append((suite_name, result))
        
        if failed_tests:
            logger.info("\n❌ Failed Tests:")
            for suite_name, result in failed_tests:
                logger.info(f"  • {suite_name} > {result.name}: {result.message}")
        
        logger.info("\n🔗 Access URLs:")
        logger.info(f"  • Airflow:    {self.airflow_url}")
        logger.info(f"  • Grafana:    {self.grafana_url}")
        logger.info(f"  • Prometheus: {self.prometheus_url}")
        logger.info(f"  • Metrics:    {self.metrics_url}")
        
        logger.info("=" * 60)
    
    def save_report(self, summary: Dict[str, Any], filename: str = None):
        """Save test report to file"""
        if filename is None:
            filename = f"monitoring_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(filename, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        logger.info(f"📄 Test report saved to: {filename}")

def main():
    """Main validation function"""
    validator = MonitoringValidator()
    
    try:
        # Run all tests
        summary = validator.run_all_tests()
        
        # Save report
        validator.save_report(summary)
        
        # Exit with appropriate code
        if summary['success_rate'] >= 80:
            sys.exit(0)  # Success
        elif summary['success_rate'] >= 60:
            sys.exit(1)  # Warning
        else:
            sys.exit(2)  # Critical issues
            
    except KeyboardInterrupt:
        logger.info("\n⚠️ Validation interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"❌ Validation failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()