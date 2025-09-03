#!/usr/bin/env python3
"""
Startup script for Airflow Metrics Service
This script can be run independently to populate Prometheus metrics with real Airflow data
"""

import os
import sys
import logging
from pathlib import Path

# Setup logging first
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def main():
    """Main entry point for the metrics service"""
    logger.info("🚀 Starting Airflow Metrics Service...")
    logger.info(f"📂 Working directory: {os.getcwd()}")
    logger.info(f"🐍 Python path: {sys.path}")
    
    # Add paths to find our modules
    current_dir = Path(__file__).parent
    dags_dir = current_dir / "dags"
    
    if dags_dir.exists():
        sys.path.insert(0, str(dags_dir))
        logger.info(f"✅ Added dags directory to path: {dags_dir}")
    else:
        # Try alternative paths
        alt_dags = Path("/opt/airflow/dags")
        if alt_dags.exists():
            sys.path.insert(0, str(alt_dags))
            logger.info(f"✅ Added alternative dags directory to path: {alt_dags}")
        else:
            logger.warning("⚠️ Could not find dags directory")
    
    try:
        # Check if we have access to Airflow
        try:
            import airflow
            logger.info(f"✅ Airflow version: {airflow.__version__}")
        except ImportError as e:
            logger.error(f"❌ Airflow not available: {e}")
            return 1
        
        # Import and run the metrics service
        try:
            from monitoring.metrics_service import run_metrics_service
            logger.info("✅ Successfully imported metrics service")
            run_metrics_service()
        except ImportError as e:
            logger.error(f"❌ Failed to import metrics service: {e}")
            logger.info("📋 Available modules in current directory:")
            try:
                for item in Path(".").glob("**/*.py"):
                    logger.info(f"   - {item}")
            except:
                pass
            return 1
        
    except KeyboardInterrupt:
        logger.info("👋 Metrics service stopped by user")
        return 0
    except Exception as e:
        logger.error(f"❌ Failed to start metrics service: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())