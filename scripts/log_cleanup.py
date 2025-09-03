#!/usr/bin/env python3
"""
Log Cleanup Script for Airflow Framework
Removes log files older than 1 month from /opt/airflow/logs

This script is designed to be executed by the framework's new Python script operator.
It provides comprehensive log cleanup with disk space monitoring.
"""

import os
import sys
import shutil
import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional

def get_upstream_data() -> Dict[str, List[Dict[str, Any]]]:
    """Get upstream data from framework integration"""
    upstream_data = {}
    
    # Try to get data from environment variable (subprocess mode)
    data_file_path = os.environ.get('FRAMEWORK_UPSTREAM_DATA_FILE')
    if data_file_path and os.path.exists(data_file_path):
        try:
            with open(data_file_path, 'r') as f:
                upstream_data = json.load(f)
            print(f"📊 Loaded upstream data from: {data_file_path}")
        except Exception as e:
            print(f"⚠️ Could not load upstream data: {e}")
    
    return upstream_data

def check_disk_space(logs_path: str = "/opt/airflow/logs") -> Dict[str, Any]:
    """Check available disk space"""
    try:
        # Get disk usage
        statvfs = os.statvfs(logs_path)
        
        # Calculate disk space
        total_space = statvfs.f_frsize * statvfs.f_blocks
        available_space = statvfs.f_frsize * statvfs.f_available
        used_space = total_space - available_space
        
        # Convert to GB
        total_gb = total_space / 1024 / 1024 / 1024
        available_gb = available_space / 1024 / 1024 / 1024
        used_gb = used_space / 1024 / 1024 / 1024
        
        usage_percent = (used_space / total_space) * 100
        
        print(f"💾 Disk Usage for {logs_path}:")
        print(f"   📊 Total: {total_gb:.2f} GB")
        print(f"   📈 Used: {used_gb:.2f} GB ({usage_percent:.1f}%)")
        print(f"   📉 Available: {available_gb:.2f} GB")
        
        return {
            "total_gb": round(total_gb, 2),
            "used_gb": round(used_gb, 2),
            "available_gb": round(available_gb, 2),
            "usage_percent": round(usage_percent, 1),
            "timestamp": datetime.now().isoformat(),
            "path": logs_path
        }
        
    except Exception as e:
        print(f"❌ Failed to check disk space: {e}")
        return {"error": str(e), "timestamp": datetime.now().isoformat()}

def cleanup_airflow_logs(days_old: int = 30, logs_path: str = "/opt/airflow/logs") -> Dict[str, Any]:
    """Clean up Airflow logs older than specified days"""
    
    logs_path = Path(logs_path)
    cutoff_date = datetime.now() - timedelta(days=days_old)
    
    total_deleted_files = 0
    total_deleted_size = 0
    deleted_directories = []
    
    try:
        if not logs_path.exists():
            print(f"❌ Logs directory not found: {logs_path}")
            return {"error": "Logs directory not found", "deleted_files": 0}
        
        print(f"🧹 Starting log cleanup for files older than {cutoff_date.strftime('%Y-%m-%d')}")
        print(f"📂 Logs directory: {logs_path}")
        
        # Walk through all directories in logs
        for root, dirs, files in os.walk(logs_path):
            root_path = Path(root)
            
            # Skip if this is the main logs directory
            if root_path == logs_path:
                continue
            
            # Check if directory represents a DAG run (has pattern: dag_id=*/run_id=*)
            if "dag_id=" in root_path.name or "run_id=" in root_path.name:
                
                # Get directory creation/modification time
                try:
                    dir_mtime = datetime.fromtimestamp(root_path.stat().st_mtime)
                    
                    # If directory is older than cutoff, delete it
                    if dir_mtime < cutoff_date:
                        
                        # Calculate size before deletion
                        dir_size = sum(f.stat().st_size for f in root_path.rglob('*') if f.is_file())
                        file_count = len(list(root_path.rglob('*.log')))
                        
                        print(f"🗑️  Deleting old log directory: {root_path}")
                        print(f"   📊 Size: {dir_size / 1024 / 1024:.2f} MB, Files: {file_count}")
                        
                        # Remove the entire directory
                        shutil.rmtree(root_path)
                        
                        total_deleted_files += file_count
                        total_deleted_size += dir_size
                        deleted_directories.append(str(root_path))
                        
                        # Don't traverse into subdirectories of deleted directory
                        dirs.clear()
                
                except (OSError, PermissionError) as e:
                    print(f"⚠️  Could not process {root_path}: {e}")
                    continue
        
        # Clean up empty parent directories
        cleanup_empty_directories(logs_path)
        
        # Summary
        deleted_size_mb = total_deleted_size / 1024 / 1024
        print(f"✅ Log cleanup completed!")
        print(f"   📁 Deleted directories: {len(deleted_directories)}")
        print(f"   📄 Deleted log files: {total_deleted_files}")
        print(f"   💾 Freed space: {deleted_size_mb:.2f} MB")
        
        return {
            "success": True,
            "deleted_directories": len(deleted_directories),
            "deleted_files": total_deleted_files,
            "freed_space_mb": round(deleted_size_mb, 2),
            "cutoff_date": cutoff_date.isoformat(),
            "directories_sample": deleted_directories[:5],  # First 5 for reference
            "execution_time": datetime.now().isoformat()
        }
        
    except Exception as e:
        print(f"❌ Log cleanup failed: {str(e)}")
        return {"success": False, "error": str(e), "execution_time": datetime.now().isoformat()}

def cleanup_empty_directories(base_path: Path):
    """Remove empty directories after log cleanup"""
    try:
        for root, dirs, files in os.walk(base_path, topdown=False):
            for dir_name in dirs:
                dir_path = Path(root) / dir_name
                try:
                    # Try to remove if empty
                    if dir_path.exists() and not any(dir_path.iterdir()):
                        print(f"🗂️  Removing empty directory: {dir_path}")
                        dir_path.rmdir()
                except OSError:
                    # Directory not empty or permission issue, skip
                    pass
    except Exception as e:
        print(f"⚠️  Error cleaning empty directories: {e}")

def generate_cleanup_summary(
    disk_before: Dict[str, Any], 
    cleanup_result: Dict[str, Any], 
    disk_after: Dict[str, Any]
) -> Dict[str, Any]:
    """Generate comprehensive cleanup summary"""
    
    print("📋 LOG CLEANUP SUMMARY")
    print("=" * 60)
    
    summary = {
        "cleanup_execution_time": datetime.now().isoformat(),
        "overall_success": cleanup_result.get('success', False)
    }
    
    # Disk space comparison
    if disk_before and disk_after:
        space_freed = disk_after.get('available_gb', 0) - disk_before.get('available_gb', 0)
        
        print(f"💾 DISK SPACE ANALYSIS:")
        print(f"   Before: {disk_before.get('available_gb', 0):.2f} GB available ({disk_before.get('usage_percent', 0):.1f}% used)")
        print(f"   After:  {disk_after.get('available_gb', 0):.2f} GB available ({disk_after.get('usage_percent', 0):.1f}% used)")
        print(f"   🆓 Space Freed: {space_freed:.2f} GB")
        
        summary.update({
            "disk_before": disk_before,
            "disk_after": disk_after,
            "space_freed_gb": round(space_freed, 2)
        })
    
    # Cleanup results
    if cleanup_result:
        print(f"\n🧹 CLEANUP RESULTS:")
        print(f"   📁 Deleted Directories: {cleanup_result.get('deleted_directories', 0)}")
        print(f"   📄 Deleted Log Files: {cleanup_result.get('deleted_files', 0)}")
        print(f"   💾 Freed Space (files): {cleanup_result.get('freed_space_mb', 0):.2f} MB")
        print(f"   📅 Cutoff Date: {cleanup_result.get('cutoff_date', 'Unknown')}")
        print(f"   ✅ Success: {cleanup_result.get('success', False)}")
        
        if cleanup_result.get('directories_sample'):
            print(f"   📋 Sample Deleted Dirs:")
            for dir_path in cleanup_result['directories_sample']:
                print(f"      - {dir_path}")
        
        summary.update({
            "cleanup_result": cleanup_result
        })
    
    # Overall status
    cleanup_success = summary.get('overall_success', False)
    space_freed = summary.get('space_freed_gb', 0)
    
    print(f"\n🎯 OVERALL STATUS:")
    print(f"   Status: {'✅ SUCCESS' if cleanup_success else '❌ FAILED'}")
    print(f"   Space Freed: {space_freed:.2f} GB")
    
    print("=" * 60)
    
    summary.update({
        "total_space_freed_gb": space_freed,
        "recommendation": "SUCCESS - Log cleanup completed successfully" if cleanup_success else "ATTENTION - Log cleanup encountered issues"
    })
    
    return summary

def main(upstream_data=None, context=None, script_args=None, **kwargs):
    """
    Main entry point for framework integration
    
    Args:
        upstream_data: Data from upstream tasks (framework integration)
        context: Airflow context (framework integration)  
        script_args: Custom script arguments
        **kwargs: Additional environment variables
    """
    
    print("🚀 Starting Airflow Log Cleanup Script")
    print(f"📅 Execution Time: {datetime.now().isoformat()}")
    
    # Get script arguments
    days_old = 30
    logs_path = "/opt/airflow/logs"
    
    if script_args:
        days_old = script_args.get('days_old', 30)
        logs_path = script_args.get('logs_path', logs_path)
    
    print(f"📋 Configuration:")
    print(f"   Days Old: {days_old}")
    print(f"   Logs Path: {logs_path}")
    
    try:
        # Step 1: Check disk space before cleanup
        print("\n🔍 Step 1: Checking disk space before cleanup...")
        disk_before = check_disk_space(logs_path)
        
        # Step 2: Perform log cleanup
        print("\n🧹 Step 2: Performing log cleanup...")
        cleanup_result = cleanup_airflow_logs(days_old, logs_path)
        
        # Step 3: Check disk space after cleanup
        print("\n🔍 Step 3: Checking disk space after cleanup...")
        disk_after = check_disk_space(logs_path)
        
        # Step 4: Generate summary
        print("\n📊 Step 4: Generating cleanup summary...")
        summary = generate_cleanup_summary(disk_before, cleanup_result, disk_after)
        
        # Return result for framework integration
        result = {
            "log_cleanup_completed": True,
            "summary": summary,
            "disk_before": disk_before,
            "cleanup_result": cleanup_result,
            "disk_after": disk_after,
            "execution_timestamp": datetime.now().isoformat()
        }
        
        # Output JSON result for framework integration
        print(f"\n📤 Framework Integration Result:")
        print(json.dumps(result, indent=2, default=str))
        
        return result
        
    except Exception as e:
        error_result = {
            "log_cleanup_completed": False,
            "error": str(e),
            "execution_timestamp": datetime.now().isoformat()
        }
        
        print(f"❌ Script execution failed: {e}")
        print(json.dumps(error_result, indent=2))
        
        return error_result

if __name__ == "__main__":
    """
    Entry point for direct script execution (subprocess mode)
    """
    
    # Get upstream data if available
    upstream_data = get_upstream_data()
    
    # Get script arguments from environment
    script_args = {}
    script_args_json = os.environ.get('FRAMEWORK_SCRIPT_ARGS', '{}')
    try:
        script_args = json.loads(script_args_json)
    except json.JSONDecodeError:
        pass
    
    # Execute main function
    result = main(upstream_data=upstream_data, script_args=script_args)
    
    # Exit with appropriate code
    if result.get('log_cleanup_completed', False):
        sys.exit(0)
    else:
        sys.exit(1)