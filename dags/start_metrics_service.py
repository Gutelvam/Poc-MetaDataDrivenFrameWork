#!/usr/bin/env python3
"""
Startup script for Airflow Metrics Service - Located in dags directory
This script runs the comprehensive metrics service that populates Prometheus with real Airflow data
"""

import os
import sys
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def main():
    """Main entry point for the metrics service"""
    logger.info("🚀 Starting Comprehensive Airflow Metrics Service...")
    logger.info(f"📂 Working directory: {os.getcwd()}")
    logger.info(f"📁 Script location: {__file__}")
    
    # Ensure current directory is in Python path
    current_dir = Path(__file__).parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
        logger.info(f"✅ Added current directory to Python path: {current_dir}")
    
    try:
        # Test Airflow availability
        try:
            import airflow
            from airflow.models import DagRun, TaskInstance
            logger.info(f"✅ Airflow {airflow.__version__} is available")
        except ImportError as e:
            logger.error(f"❌ Airflow not available: {e}")
            return 1
        
        # Import our metrics service
        try:
            from monitoring.metrics_service import AirflowMetricsService
            logger.info("✅ Successfully imported AirflowMetricsService")
        except ImportError as e:
            logger.error(f"❌ Failed to import metrics service: {e}")
            logger.info("📋 Trying to list available modules...")
            try:
                monitoring_path = current_dir / "monitoring"
                if monitoring_path.exists():
                    logger.info(f"📁 Files in monitoring directory:")
                    for item in monitoring_path.iterdir():
                        logger.info(f"   - {item.name}")
                else:
                    logger.error("❌ Monitoring directory not found")
            except Exception as list_e:
                logger.error(f"Failed to list modules: {list_e}")
            return 1
        
        # Create and start the metrics service
        logger.info("🚀 Creating Airflow Metrics Service instance...")
        service = AirflowMetricsService(update_interval=30)
        
        if not service.db_available:
            logger.error("❌ Database not available - cannot start metrics service")
            return 1
        
        logger.info("🔥 Starting comprehensive metrics service...")
        service.start()
        
        # Keep the service running
        import time
        while True:
            time.sleep(60)
            logger.info("📊 Metrics service is running... (Updates every 30s)")
        
    except KeyboardInterrupt:
        logger.info("👋 Metrics service stopped by user")
        return 0
    except Exception as e:
        logger.error(f"❌ Failed to start metrics service: {e}")
        import traceback
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return 1

if __name__ == "__main__":
    sys.exit(main())