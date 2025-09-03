# transforms/python_script_operator.py
"""
Python Script Transform Operator - New Framework Feature
Executes Python scripts from the /scripts/ directory with framework integration
"""

import os
import sys
import logging
import subprocess
import importlib.util
import tempfile
import json
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path

from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.exceptions import AirflowException

# Import framework components for data sharing
try:
    from core.data_registry import query_task_data, get_data_registry
    DATA_REGISTRY_AVAILABLE = True
except ImportError:
    DATA_REGISTRY_AVAILABLE = False

logger = logging.getLogger(__name__)

class PythonScriptTransformOperator(BaseOperator):
    """
    Operator to execute Python scripts from /scripts/ directory with framework integration
    
    Features:
    - Executes Python scripts from /scripts/ directory
    - Provides access to upstream task data via framework's data registry
    - Supports both sink-based data sharing and traditional approaches
    - Automatic error handling and logging
    - Context and configuration passing to scripts
    """
    
    def __init__(
        self,
        python_script_path: str,
        source_task_ids: Optional[List[str]] = None,
        script_args: Optional[Dict[str, Any]] = None,
        working_directory: Optional[str] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.python_script_path = python_script_path
        self.source_task_ids = source_task_ids or []
        self.script_args = script_args or {}
        self.working_directory = working_directory
        
    def execute(self, context: Context) -> Any:
        """Execute the Python script with framework integration"""
        
        try:
            # Validate script path
            script_path = self._validate_script_path()
            
            # Prepare script environment
            script_env = self._prepare_script_environment(context)
            
            # Get upstream data if source tasks are specified
            upstream_data = self._get_upstream_data(context)
            
            # Execute script
            result = self._execute_script(script_path, script_env, upstream_data, context)
            
            logger.info(f"✅ Python script executed successfully: {self.python_script_path}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Failed to execute Python script {self.python_script_path}: {str(e)}")
            raise AirflowException(f"Python script execution failed: {str(e)}")
    
    def _validate_script_path(self) -> Path:
        """Validate and resolve the script path"""
        
        # Base scripts directory
        scripts_base = Path("/opt/airflow/scripts")
        
        # Handle different path formats
        if self.python_script_path.startswith("/scripts/"):
            # Remove leading /scripts/ if present
            script_path = scripts_base / self.python_script_path[9:]
        elif self.python_script_path.startswith("scripts/"):
            # Remove leading scripts/ if present  
            script_path = scripts_base / self.python_script_path[8:]
        else:
            # Direct filename or relative path
            script_path = scripts_base / self.python_script_path
        
        # Ensure .py extension
        if not script_path.suffix:
            script_path = script_path.with_suffix('.py')
        
        # Validate file exists
        if not script_path.exists():
            raise AirflowException(f"Python script not found: {script_path}")
        
        if not script_path.is_file():
            raise AirflowException(f"Script path is not a file: {script_path}")
        
        logger.info(f"📄 Validated script path: {script_path}")
        return script_path
    
    def _prepare_script_environment(self, context: Context) -> Dict[str, Any]:
        """Prepare environment variables and configuration for the script"""
        
        script_env = {
            # Airflow context information
            'AIRFLOW_CONTEXT_DAG_ID': context.get('dag').dag_id if context.get('dag') else 'unknown',
            'AIRFLOW_CONTEXT_TASK_ID': self.task_id,
            'AIRFLOW_CONTEXT_EXECUTION_DATE': str(context.get('execution_date', datetime.now())),
            'AIRFLOW_CONTEXT_RUN_ID': context.get('run_id', 'unknown'),
            
            # Framework-specific environment
            'FRAMEWORK_DATA_REGISTRY_AVAILABLE': str(DATA_REGISTRY_AVAILABLE),
            'FRAMEWORK_SOURCE_TASKS': ','.join(self.source_task_ids),
            'FRAMEWORK_SCRIPT_ARGS': json.dumps(self.script_args),
            
            # Working directory
            'SCRIPT_WORKING_DIR': self.working_directory or '/opt/airflow/scripts',
        }
        
        return script_env
    
    def _get_upstream_data(self, context: Context) -> Dict[str, List[Dict[str, Any]]]:
        """Get data from upstream tasks using framework's data registry"""
        
        upstream_data = {}
        
        if not self.source_task_ids or not DATA_REGISTRY_AVAILABLE:
            return upstream_data
        
        try:
            dag_id = context.get('dag').dag_id if context.get('dag') else 'unknown'
            execution_date = str(context.get('execution_date', datetime.now()))
            
            for task_id in self.source_task_ids:
                try:
                    task_data = query_task_data(task_id, dag_id, execution_date)
                    upstream_data[task_id] = task_data
                    logger.info(f"📊 Retrieved {len(task_data)} records from task: {task_id}")
                    
                except Exception as e:
                    logger.warning(f"⚠️ Could not retrieve data from task {task_id}: {str(e)}")
                    upstream_data[task_id] = []
            
            logger.info(f"📈 Retrieved data from {len(upstream_data)} upstream tasks")
            
        except Exception as e:
            logger.error(f"❌ Failed to get upstream data: {str(e)}")
        
        return upstream_data
    
    def _execute_script(
        self, 
        script_path: Path, 
        script_env: Dict[str, str], 
        upstream_data: Dict[str, List[Dict[str, Any]]], 
        context: Context
    ) -> Any:
        """Execute the Python script with multiple execution methods"""
        
        # Method 1: Try to import and execute as module (preferred)
        try:
            return self._execute_as_module(script_path, script_env, upstream_data, context)
        except Exception as module_error:
            logger.warning(f"⚠️ Module execution failed, trying subprocess: {module_error}")
            
            # Method 2: Execute as subprocess (fallback)
            try:
                return self._execute_as_subprocess(script_path, script_env, upstream_data, context)
            except Exception as subprocess_error:
                logger.error(f"❌ Both execution methods failed")
                raise AirflowException(f"Script execution failed: Module: {module_error}, Subprocess: {subprocess_error}")
    
    def _execute_as_module(
        self, 
        script_path: Path, 
        script_env: Dict[str, str], 
        upstream_data: Dict[str, List[Dict[str, Any]]], 
        context: Context
    ) -> Any:
        """Execute script by importing as Python module"""
        
        # Create module spec
        module_name = f"script_{self.task_id}_{int(datetime.now().timestamp())}"
        spec = importlib.util.spec_from_file_location(module_name, script_path)
        
        if not spec or not spec.loader:
            raise AirflowException(f"Could not create module spec for {script_path}")
        
        # Import module
        script_module = importlib.util.module_from_spec(spec)
        
        # Add to sys.modules temporarily
        sys.modules[module_name] = script_module
        
        try:
            # Execute module
            spec.loader.exec_module(script_module)
            
            # Look for standard entry points
            result = None
            
            if hasattr(script_module, 'main'):
                # Call main() function with framework integration
                if callable(script_module.main):
                    # Try different signatures
                    try:
                        # Signature: main(upstream_data, context, **kwargs)
                        result = script_module.main(
                            upstream_data=upstream_data,
                            context=context,
                            script_args=self.script_args,
                            **script_env
                        )
                    except TypeError:
                        try:
                            # Signature: main(upstream_data, context)
                            result = script_module.main(upstream_data, context)
                        except TypeError:
                            # Signature: main()
                            result = script_module.main()
            
            elif hasattr(script_module, 'run'):
                # Alternative entry point: run()
                if callable(script_module.run):
                    result = script_module.run(upstream_data, context, self.script_args)
            
            elif hasattr(script_module, 'execute'):
                # Alternative entry point: execute()
                if callable(script_module.execute):
                    result = script_module.execute(upstream_data, context)
            
            else:
                # No specific entry point found - script executed during import
                logger.info("📄 Script executed during module import (no specific entry point found)")
                result = {"executed": True, "method": "import", "timestamp": datetime.now().isoformat()}
            
            logger.info(f"✅ Script executed as module with result: {type(result)}")
            return result
            
        finally:
            # Clean up
            if module_name in sys.modules:
                del sys.modules[module_name]
    
    def _execute_as_subprocess(
        self, 
        script_path: Path, 
        script_env: Dict[str, str], 
        upstream_data: Dict[str, List[Dict[str, Any]]], 
        context: Context
    ) -> Any:
        """Execute script as subprocess with data injection"""
        
        # Prepare environment for subprocess
        subprocess_env = os.environ.copy()
        subprocess_env.update(script_env)
        
        # Create temporary file for upstream data
        data_file = None
        try:
            if upstream_data:
                data_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
                json.dump(upstream_data, data_file, default=str, indent=2)
                data_file.close()
                subprocess_env['FRAMEWORK_UPSTREAM_DATA_FILE'] = data_file.name
            
            # Execute script
            logger.info(f"🚀 Executing script as subprocess: {script_path}")
            result = subprocess.run(
                [sys.executable, str(script_path)],
                env=subprocess_env,
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour timeout
                cwd=self.working_directory or script_path.parent
            )
            
            # Check result
            if result.returncode != 0:
                logger.error(f"❌ Script failed with return code {result.returncode}")
                logger.error(f"❌ STDERR: {result.stderr}")
                raise AirflowException(f"Script failed: {result.stderr}")
            
            logger.info(f"✅ Script completed successfully")
            if result.stdout:
                logger.info(f"📄 STDOUT: {result.stdout}")
            
            # Try to parse JSON output, otherwise return stdout
            try:
                return json.loads(result.stdout) if result.stdout.strip() else {"executed": True}
            except json.JSONDecodeError:
                return {"executed": True, "stdout": result.stdout, "method": "subprocess"}
        
        finally:
            # Clean up temporary file
            if data_file and os.path.exists(data_file.name):
                os.unlink(data_file.name)

def create_python_script_transform_operator(
    task_id: str,
    python_script_path: str,
    source_task_ids: Optional[List[str]] = None,
    script_args: Optional[Dict[str, Any]] = None,
    dag=None,
    **kwargs
) -> PythonScriptTransformOperator:
    """Factory function to create Python script transform operator"""
    
    return PythonScriptTransformOperator(
        task_id=task_id,
        python_script_path=python_script_path,
        source_task_ids=source_task_ids,
        script_args=script_args,
        dag=dag,
        **kwargs
    )