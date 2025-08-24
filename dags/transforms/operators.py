"""
Transform Operators Module - Airflow 3.x Compatible
Handles data transformations using SQL, Python, and custom scripts
"""

import pandas as pd
import json
import logging
import subprocess
import sys
import importlib.util
import os
from datetime import datetime
from typing import Dict, List, Any, Optional, Union, Callable
from pathlib import Path

from airflow.models import BaseOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException

from core.config import TaskConfig

logger = logging.getLogger(__name__)


def clean_decimal_types(data):
        """Convert Decimal types to float for SQLite compatibility"""
        import pandas as pd
        from decimal import Decimal
        
        if isinstance(data, list):
            # Convert list of dictionaries
            cleaned_data = []
            for item in data:
                cleaned_item = {}
                for key, value in item.items():
                    if isinstance(value, Decimal):
                        cleaned_item[key] = float(value)
                    else:
                        cleaned_item[key] = value
                cleaned_data.append(cleaned_item)
            return cleaned_data
        
        elif isinstance(data, pd.DataFrame):
            # Convert DataFrame
            df = data.copy()
            for column in df.columns:
                if df[column].dtype == 'object':
                    # Check if column contains Decimal values
                    if any(isinstance(val, Decimal) for val in df[column].dropna()):
                        df[column] = df[column].apply(
                            lambda x: float(x) if isinstance(x, Decimal) else x
                        )
                        df[column] = pd.to_numeric(df[column], errors='coerce')
            return df
        
        return data



class SQLTransformOperator(BaseOperator):
    """Execute SQL transformations on data - Airflow 3.x compatible"""
    
    def __init__(
        self,
        sql_query: str,
        data_source_task_ids: Union[str, List[str]],
        connection_id: str = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.sql_query = sql_query
        self.data_source_task_ids = data_source_task_ids if isinstance(data_source_task_ids, list) else [data_source_task_ids]
        self.connection_id = connection_id
    
    def execute(self, context):
        """Execute SQL transformation"""
        try:
            # Get data from upstream tasks
            datasets = {}
            for task_id in self.data_source_task_ids:
                data = context['task_instance'].xcom_pull(task_ids=task_id)
                if data:
                    datasets[task_id] = pd.DataFrame(data) if isinstance(data, list) else data
                else:
                    logger.warning(f"No data received from task: {task_id}")
            
            if not datasets:
                logger.warning("No data available for SQL transformation")
                return []
            
            # Execute SQL transformation
            if self.connection_id:
                # Execute on database
                return self._execute_sql_on_database(context, datasets)
            else:
                # Execute using pandas (in-memory)
                return self._execute_sql_with_pandas(context, datasets)
                
        except Exception as e:
            logger.error(f"SQL transformation failed: {str(e)}")
            raise
    
    def _execute_sql_on_database(self, context, datasets: Dict[str, pd.DataFrame]) -> List[Dict]:
        """Execute SQL on database engine"""
        connection = BaseHook.get_connection(self.connection_id)
        
        # This is a simplified implementation - in production you'd want to:
        # 1. Load data into temporary tables
        # 2. Execute the SQL query
        # 3. Return results
        # 4. Clean up temporary tables
        
        raise NotImplementedError("Database SQL execution not implemented in this example")
    

    def _execute_sql_with_pandas(self, context, datasets: Dict[str, pd.DataFrame]) -> List[Dict]:
        """Execute SQL using pandas with Decimal handling"""
        import pandasql as ps
        
        # Clean datasets before processing
        cleaned_datasets = {}
        for key, data in datasets.items():
            cleaned_datasets[key] = clean_decimal_types(data)
        
        # Make cleaned datasets available to SQL query
        local_env = cleaned_datasets.copy()
        
        # Add context variables safely
        local_env.update({
            'execution_date': context.get('execution_date'),
            'ds': context.get('ds'),
            'ts': context.get('ts')
        })
        
        # Filter out None values
        local_env = {k: v for k, v in local_env.items() if v is not None}
        
        try:
            # Execute SQL query
            result_df = ps.sqldf(self.sql_query, local_env)
            
            # Clean result as well
            cleaned_result = clean_decimal_types(result_df)
            return cleaned_result.to_dict('records') if isinstance(cleaned_result, pd.DataFrame) else cleaned_result
            
        except Exception as e:
            logger.error(f"pandas SQL execution failed: {str(e)}")
            raise AirflowException(f"SQL transformation failed: {str(e)}")

class PythonTransformOperator(BaseOperator):
    """Execute Python transformations on data - Airflow 3.x compatible"""
    
    def __init__(
        self,
        python_callable: Union[str, Callable],
        data_source_task_ids: Union[str, List[str]],
        op_args: Optional[tuple] = None,
        op_kwargs: Optional[Dict] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.python_callable = python_callable
        self.data_source_task_ids = data_source_task_ids if isinstance(data_source_task_ids, list) else [data_source_task_ids]
        self.op_args = op_args or ()
        self.op_kwargs = op_kwargs or {}
    
    def execute(self, context):
        """Execute Python transformation"""
        try:
            # Get data from upstream tasks
            datasets = {}
            for task_id in self.data_source_task_ids:
                data = context['task_instance'].xcom_pull(task_ids=task_id)
                if data:
                    datasets[task_id] = data
            
            if not datasets:
                logger.warning("No data available for Python transformation")
                return []
            
            # Resolve callable if it's a string
            if isinstance(self.python_callable, str):
                callable_func = self._resolve_callable_from_string(self.python_callable)
            else:
                callable_func = self.python_callable
            
            # Prepare arguments
            args = (datasets,) + self.op_args
            kwargs = {**self.op_kwargs, 'context': context}
            
            # Execute transformation
            result = callable_func(*args, **kwargs)
            
            logger.info(f"Python transformation completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Python transformation failed: {str(e)}")
            raise
    
    def _resolve_callable_from_string(self, callable_string: str) -> Callable:
        """Resolve callable from string (module.function format)"""
        try:
            module_name, function_name = callable_string.rsplit('.', 1)
            module = importlib.import_module(module_name)
            return getattr(module, function_name)
        except Exception as e:
            raise AirflowException(f"Failed to resolve callable '{callable_string}': {str(e)}")

class CustomScriptTransformOperator(BaseOperator):
    """Execute custom scripts for data transformation - Airflow 3.x compatible"""
    
    def __init__(
        self,
        script_path: str,
        data_source_task_ids: Union[str, List[str]],
        script_args: Optional[List[str]] = None,
        script_env: Optional[Dict[str, str]] = None,
        script_type: str = "python",  # python, bash, R
        **kwargs
    ):
        super().__init__(**kwargs)
        self.script_path = script_path
        self.data_source_task_ids = data_source_task_ids if isinstance(data_source_task_ids, list) else [data_source_task_ids]
        self.script_args = script_args or []
        self.script_env = script_env or {}
        self.script_type = script_type
    
    def execute(self, context):
        """Execute custom script transformation"""
        try:
            # Get data from upstream tasks
            datasets = {}
            for task_id in self.data_source_task_ids:
                data = context['task_instance'].xcom_pull(task_ids=task_id)
                if data:
                    datasets[task_id] = data
            
            # Save input data to temporary files
            input_files = self._save_input_data(datasets, context)
            
            # Prepare environment variables
            env = self._prepare_environment(context, input_files)
            
            # Execute script
            if self.script_type == "python":
                result = self._execute_python_script(env)
            elif self.script_type == "bash":
                result = self._execute_bash_script(env)
            else:
                raise ValueError(f"Unsupported script type: {self.script_type}")
            
            # Clean up temporary files
            self._cleanup_temp_files(input_files)
            
            return result
            
        except Exception as e:
            logger.error(f"Custom script transformation failed: {str(e)}")
            raise
    
    def _save_input_data(self, datasets: Dict[str, Any], context) -> Dict[str, str]:
        """Save input datasets to temporary files"""
        temp_dir = Path(f"/tmp/airflow_transform_{context['ts_nodash']}")
        temp_dir.mkdir(exist_ok=True)
        
        input_files = {}
        for task_id, data in datasets.items():
            file_path = temp_dir / f"{task_id}.json"
            
            # Convert data to JSON serializable format
            if isinstance(data, pd.DataFrame):
                json_data = data.to_dict('records')
            elif isinstance(data, list):
                json_data = data
            else:
                json_data = [data]
            
            with open(file_path, 'w') as f:
                json.dump(json_data, f, default=str, indent=2)
            
            input_files[task_id] = str(file_path)
        
        return input_files
    
    def _prepare_environment(self, context, input_files: Dict[str, str]) -> Dict[str, str]:
        """Prepare environment variables for script execution"""
        env = {
            **self.script_env,
            'AIRFLOW_EXECUTION_DATE': context['execution_date'].isoformat(),
            'AIRFLOW_DS': context['ds'],
            'AIRFLOW_TS': context['ts'],
            'AIRFLOW_DAG_ID': context['dag'].dag_id,
            'AIRFLOW_TASK_ID': context['task'].task_id,
        }
        
        # Add input file paths
        for task_id, file_path in input_files.items():
            env[f'INPUT_FILE_{task_id.upper()}'] = file_path
        
        # Output file path
        temp_dir = Path(input_files[list(input_files.keys())[0]]).parent
        output_file = temp_dir / "output.json"
        env['OUTPUT_FILE'] = str(output_file)
        
        return env
    
    def _execute_python_script(self, env: Dict[str, str]) -> Any:
        """Execute Python script"""
        cmd = [sys.executable, self.script_path] + self.script_args
        
        result = subprocess.run(
            cmd,
            env={**dict(os.environ), **env},
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )
        
        if result.returncode != 0:
            raise AirflowException(f"Script failed with exit code {result.returncode}: {result.stderr}")
        
        # Load output data
        output_file = env.get('OUTPUT_FILE')
        if output_file and Path(output_file).exists():
            with open(output_file, 'r') as f:
                return json.load(f)
        
        # If no output file, return script stdout
        if result.stdout:
            try:
                return json.loads(result.stdout)
            except json.JSONDecodeError:
                return {"script_output": result.stdout}
        
        return {"message": "Script completed successfully"}
    
    def _execute_bash_script(self, env: Dict[str, str]) -> Any:
        """Execute Bash script"""
        cmd = ["bash", self.script_path] + self.script_args
        
        result = subprocess.run(
            cmd,
            env={**dict(os.environ), **env},
            capture_output=True,
            text=True,
            timeout=3600
        )
        
        if result.returncode != 0:
            raise AirflowException(f"Script failed with exit code {result.returncode}: {result.stderr}")
        
        # Load output data
        output_file = env.get('OUTPUT_FILE')
        if output_file and Path(output_file).exists():
            with open(output_file, 'r') as f:
                return json.load(f)
        
        return {"script_output": result.stdout, "message": "Script completed successfully"}
    
    def _cleanup_temp_files(self, input_files: Dict[str, str]):
        """Clean up temporary files"""
        try:
            if input_files:
                temp_dir = Path(list(input_files.values())[0]).parent
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception as e:
            logger.warning(f"Failed to cleanup temp files: {str(e)}")

class DataValidationTransformOperator(BaseOperator):
    """Validate and clean data during transformation - Airflow 3.x compatible"""
    
    def __init__(
        self,
        data_source_task_id: str,
        validation_rules: Dict[str, Any],
        drop_invalid: bool = False,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.data_source_task_id = data_source_task_id
        self.validation_rules = validation_rules
        self.drop_invalid = drop_invalid
    
    def execute(self, context):
        """Execute data validation and cleaning"""
        try:
            # Get data from upstream task
            data = context['task_instance'].xcom_pull(task_ids=self.data_source_task_id)
            
            if not data:
                logger.warning("No data available for validation")
                return []
            
            # Convert to DataFrame
            df = pd.DataFrame(data) if isinstance(data, list) else data
            
            if df.empty:
                return []
            
            logger.info(f"Validating {len(df)} records")
            
            # Apply validation rules
            validated_df = self._apply_validation_rules(df)
            
            logger.info(f"Validation completed. {len(validated_df)} valid records remaining")
            
            return validated_df.to_dict('records')
            
        except Exception as e:
            logger.error(f"Data validation failed: {str(e)}")
            raise
    
    def _apply_validation_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply validation rules to DataFrame"""
        original_count = len(df)
        
        # Remove duplicates if specified
        if self.validation_rules.get('remove_duplicates'):
            subset = self.validation_rules.get('duplicate_subset')
            df = df.drop_duplicates(subset=subset)
            logger.info(f"Removed {original_count - len(df)} duplicate records")
        
        # Handle missing values
        missing_rules = self.validation_rules.get('missing_values', {})
        for column, action in missing_rules.items():
            if column in df.columns:
                if action == 'drop':
                    df = df.dropna(subset=[column])
                elif action == 'fill_zero':
                    df[column] = df[column].fillna(0)
                elif action == 'fill_mean' and pd.api.types.is_numeric_dtype(df[column]):
                    df[column] = df[column].fillna(df[column].mean())
                elif isinstance(action, str) and action.startswith('fill_'):
                    fill_value = action.replace('fill_', '')
                    df[column] = df[column].fillna(fill_value)
        
        # Data type conversions
        type_conversions = self.validation_rules.get('type_conversions', {})
        for column, target_type in type_conversions.items():
            if column in df.columns:
                try:
                    if target_type == 'datetime':
                        df[column] = pd.to_datetime(df[column], errors='coerce')
                    elif target_type == 'numeric':
                        df[column] = pd.to_numeric(df[column], errors='coerce')
                    else:
                        df[column] = df[column].astype(target_type)
                except Exception as e:
                    logger.warning(f"Failed to convert column {column} to {target_type}: {str(e)}")
        
        # Range validations
        range_rules = self.validation_rules.get('range_validations', {})
        for column, ranges in range_rules.items():
            if column in df.columns:
                min_val = ranges.get('min')
                max_val = ranges.get('max')
                
                if min_val is not None:
                    invalid_mask = df[column] < min_val
                    if self.drop_invalid:
                        df = df[~invalid_mask]
                    else:
                        df.loc[invalid_mask, column] = min_val
                
                if max_val is not None:
                    invalid_mask = df[column] > max_val
                    if self.drop_invalid:
                        df = df[~invalid_mask]
                    else:
                        df.loc[invalid_mask, column] = max_val
        
        # Pattern validations
        pattern_rules = self.validation_rules.get('pattern_validations', {})
        for column, pattern in pattern_rules.items():
            if column in df.columns:
                import re
                valid_mask = df[column].astype(str).str.match(pattern, na=False)
                if self.drop_invalid:
                    df = df[valid_mask]
                else:
                    # Mark invalid entries
                    df.loc[~valid_mask, f'{column}_valid'] = False
        
        return df

class AggregationTransformOperator(BaseOperator):
    """Perform data aggregations - Airflow 3.x compatible"""
    
    def __init__(
        self,
        data_source_task_id: str,
        group_by_columns: List[str],
        aggregations: Dict[str, Union[str, List[str]]],
        **kwargs
    ):
        super().__init__(**kwargs)
        self.data_source_task_id = data_source_task_id
        self.group_by_columns = group_by_columns
        self.aggregations = aggregations
    
    def execute(self, context):
        """Execute data aggregation"""
        try:
            # Get data from upstream task
            data = context['task_instance'].xcom_pull(task_ids=self.data_source_task_id)
            
            if not data:
                logger.warning("No data available for aggregation")
                return []
            
            # Convert to DataFrame
            df = pd.DataFrame(data) if isinstance(data, list) else data
            
            if df.empty:
                return []
            
            logger.info(f"Aggregating {len(df)} records by {self.group_by_columns}")
            
            # Perform aggregation
            if self.group_by_columns:
                grouped = df.groupby(self.group_by_columns)
                agg_df = grouped.agg(self.aggregations).reset_index()
            else:
                # Aggregate entire dataset
                agg_df = df.agg(self.aggregations).to_frame().T
            
            # Flatten column names if multi-level
            if isinstance(agg_df.columns, pd.MultiIndex):
                agg_df.columns = ['_'.join(col).strip() for col in agg_df.columns]
            
            logger.info(f"Aggregation completed. {len(agg_df)} aggregated records")
            
            return agg_df.to_dict('records')
            
        except Exception as e:
            logger.error(f"Data aggregation failed: {str(e)}")
            raise

def create_sql_transform_operator(
    task_id: str,
    sql_query: str,
    data_source_task_ids: Union[str, List[str]],
    dag,
    connection_id: str = None,
    **kwargs
) -> SQLTransformOperator:
    """Factory function to create SQL transform operator"""
    
    return SQLTransformOperator(
        task_id=task_id,
        sql_query=sql_query,
        data_source_task_ids=data_source_task_ids,
        connection_id=connection_id,
        dag=dag,
        **kwargs
    )

def create_python_transform_operator(
    task_id: str,
    python_callable: Union[str, Callable],
    data_source_task_ids: Union[str, List[str]],
    dag,
    op_args: Optional[tuple] = None,
    op_kwargs: Optional[Dict] = None,
    **kwargs
) -> PythonTransformOperator:
    """Factory function to create Python transform operator"""
    
    return PythonTransformOperator(
        task_id=task_id,
        python_callable=python_callable,
        data_source_task_ids=data_source_task_ids,
        op_args=op_args,
        op_kwargs=op_kwargs,
        dag=dag,
        **kwargs
    )

def create_custom_script_transform_operator(
    task_id: str,
    script_path: str,
    data_source_task_ids: Union[str, List[str]],
    dag,
    script_args: Optional[List[str]] = None,
    script_env: Optional[Dict[str, str]] = None,
    script_type: str = "python",
    **kwargs
) -> CustomScriptTransformOperator:
    """Factory function to create custom script transform operator"""
    
    return CustomScriptTransformOperator(
        task_id=task_id,
        script_path=script_path,
        data_source_task_ids=data_source_task_ids,
        script_args=script_args,
        script_env=script_env,
        script_type=script_type,
        dag=dag,
        **kwargs
    )

def create_validation_transform_operator(
    task_id: str,
    data_source_task_id: str,
    validation_rules: Dict[str, Any],
    dag,
    drop_invalid: bool = False,
    **kwargs
) -> DataValidationTransformOperator:
    """Factory function to create validation transform operator"""
    
    return DataValidationTransformOperator(
        task_id=task_id,
        data_source_task_id=data_source_task_id,
        validation_rules=validation_rules,
        drop_invalid=drop_invalid,
        dag=dag,
        **kwargs
    )

def create_aggregation_transform_operator(
    task_id: str,
    data_source_task_id: str,
    group_by_columns: List[str],
    aggregations: Dict[str, Union[str, List[str]]],
    dag,
    **kwargs
) -> AggregationTransformOperator:
    """Factory function to create aggregation transform operator"""
    
    return AggregationTransformOperator(
        task_id=task_id,
        data_source_task_id=data_source_task_id,
        group_by_columns=group_by_columns,
        aggregations=aggregations,
        dag=dag,
        **kwargs
    )

class FixedPythonTransformOperator(BaseOperator):
    """Execute Python transformations on data - Fixed for inline code support"""
    
    def __init__(
        self,
        python_callable: Union[str, Callable],
        data_source_task_ids: Union[str, List[str]],
        op_args: Optional[tuple] = None,
        op_kwargs: Optional[Dict] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.python_callable = python_callable
        self.data_source_task_ids = data_source_task_ids if isinstance(data_source_task_ids, list) else [data_source_task_ids]
        self.op_args = op_args or ()
        self.op_kwargs = op_kwargs or {}
    
    def execute(self, context):
        """Execute Python transformation"""
        try:
            # Get data from upstream tasks
            datasets = {}
            for task_id in self.data_source_task_ids:
                data = context['task_instance'].xcom_pull(task_ids=task_id)
                if data:
                    datasets[task_id] = data
                else:
                    logger.warning(f"No data received from task: {task_id}")
            
            if not datasets:
                logger.warning("No data available for Python transformation")
                return []
            
            logger.info(f"Available datasets: {list(datasets.keys())}")
            
            # Handle callable resolution
            if isinstance(self.python_callable, str):
                # Check if it's inline code or module path
                if self._is_inline_code(self.python_callable):
                    callable_func = self._execute_inline_code(self.python_callable)
                else:
                    callable_func = self._resolve_callable_from_string(self.python_callable)
            else:
                callable_func = self.python_callable
            
            # Prepare arguments
            args = (datasets,) + self.op_args
            kwargs = {**self.op_kwargs, 'context': context}
            
            # Execute transformation
            result = callable_func(*args, **kwargs)
            
            logger.info(f"Python transformation completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Python transformation failed: {str(e)}")
            raise
    
    def _is_inline_code(self, code_string: str) -> bool:
        """Check if the string is inline Python code vs module path"""
        # If it contains 'def ', 'import ', or starts with whitespace, it's likely inline code
        inline_indicators = ['def ', 'import ', 'from ', 'class ', '\n', '    ']
        return any(indicator in code_string for indicator in inline_indicators)
    
    def _execute_inline_code(self, code_string: str) -> Callable:
        """Execute inline Python code and return the callable"""
        try:
            # Clean up the code string
            code_lines = code_string.strip().split('\n')
            
            # Find the function name from the def statement
            function_name = None
            for line in code_lines:
                if line.strip().startswith('def '):
                    # Extract function name: "def function_name(" -> "function_name"
                    func_def = line.strip()
                    start = func_def.find('def ') + 4
                    end = func_def.find('(')
                    function_name = func_def[start:end].strip()
                    break
            
            if not function_name:
                raise ValueError("No function definition found in inline code")
            
            # Create a local namespace with common imports
            local_namespace = {
                'pd': pd,
                'pandas': pd,
                'json': json,
                'logger': logger,
                'datetime': datetime
            }
            
            # Execute the code in the local namespace
            exec(code_string, {}, local_namespace)
            
            # Return the function from the namespace
            if function_name in local_namespace:
                return local_namespace[function_name]
            else:
                raise ValueError(f"Function '{function_name}' not found after executing inline code")
                
        except Exception as e:
            logger.error(f"Failed to execute inline code: {str(e)}")
            logger.error(f"Code was: {code_string}")
            raise AirflowException(f"Inline code execution failed: {str(e)}")
    
    def _resolve_callable_from_string(self, callable_string: str) -> Callable:
        """Resolve callable from string (module.function format)"""
        try:
            if '.' not in callable_string:
                raise ValueError(f"Callable string '{callable_string}' must be in 'module.function' format")
            
            module_name, function_name = callable_string.rsplit('.', 1)
            module = importlib.import_module(module_name)
            return getattr(module, function_name)
        except Exception as e:
            raise AirflowException(f"Failed to resolve callable '{callable_string}': {str(e)}")

# Enhanced version that can also handle script files
class InlineCodeTransformOperator(BaseOperator):
    """
    Enhanced transform operator that can handle:
    1. Inline Python code from YAML
    2. Python script files
    3. Module.function references
    """
    
    def __init__(
        self,
        python_code: str,
        data_source_task_ids: Union[str, List[str]],
        code_type: str = "auto",  # "auto", "inline", "file", "module"
        **kwargs
    ):
        super().__init__(**kwargs)
        self.python_code = python_code
        self.data_source_task_ids = data_source_task_ids if isinstance(data_source_task_ids, list) else [data_source_task_ids]
        self.code_type = code_type
    
    def execute(self, context):
        """Execute Python transformation with enhanced code handling"""
        try:
            # Get data from upstream tasks
            datasets = {}
            for task_id in self.data_source_task_ids:
                data = context['task_instance'].xcom_pull(task_ids=task_id)
                if data:
                    datasets[task_id] = data
                else:
                    logger.warning(f"No data received from task: {task_id}")
            
            logger.info(f"Processing {len(datasets)} datasets: {list(datasets.keys())}")
            
            # Determine code type if auto
            if self.code_type == "auto":
                self.code_type = self._detect_code_type(self.python_code)
            
            # Execute based on code type
            if self.code_type == "inline":
                result = self._execute_inline_transform(datasets, context)
            elif self.code_type == "file":
                result = self._execute_file_transform(datasets, context)
            elif self.code_type == "module":
                result = self._execute_module_transform(datasets, context)
            else:
                raise ValueError(f"Unknown code type: {self.code_type}")
            
            logger.info(f"Transform completed. Result type: {type(result)}")
            return result
            
        except Exception as e:
            logger.error(f"Enhanced Python transformation failed: {str(e)}")
            raise
    
    def _detect_code_type(self, code: str) -> str:
        """Auto-detect the type of Python code"""
        if code.startswith('/') or code.endswith('.py'):
            return "file"
        elif '.' in code and not any(keyword in code for keyword in ['def ', 'import ', '\n']):
            return "module"
        else:
            return "inline"
    
    def _execute_inline_transform(self, datasets: Dict, context) -> Any:
        """Execute inline Python code"""
        # Create execution environment
        exec_env = {
            'datasets': datasets,
            'context': context,
            'pd': pd,
            'pandas': pd,
            'json': json,
            'logger': logger,
            'datetime': datetime
        }
        
        # Execute the code
        try:
            exec(self.python_code, exec_env)
            
            # Look for result in various possible names
            result_candidates = ['result', 'output', 'data', 'transformed_data']
            
            for candidate in result_candidates:
                if candidate in exec_env:
                    return exec_env[candidate]
            
            # If no explicit result, look for the last function defined and call it
            functions = [name for name, obj in exec_env.items() 
                        if callable(obj) and not name.startswith('_') 
                        and name not in ['pd', 'pandas', 'json', 'logger', 'datetime']]
            
            if functions:
                func_name = functions[-1]  # Use the last defined function
                func = exec_env[func_name]
                logger.info(f"Calling function: {func_name}")
                return func(datasets, **context)
            
            raise ValueError("No result found and no callable function detected")
            
        except Exception as e:
            logger.error(f"Inline code execution failed: {str(e)}")
            logger.error(f"Code: {self.python_code}")
            raise
    
    def _execute_file_transform(self, datasets: Dict, context) -> Any:
        """Execute Python file"""
        if not Path(self.python_code).exists():
            raise FileNotFoundError(f"Python file not found: {self.python_code}")
        
        # Load and execute the file
        spec = importlib.util.spec_from_file_location("transform_module", self.python_code)
        module = importlib.util.module_from_spec(spec)
        
        # Add context to module
        module.datasets = datasets
        module.context = context
        module.pd = pd
        module.logger = logger
        
        spec.loader.exec_module(module)
        
        # Look for transform function or main function
        if hasattr(module, 'transform'):
            return module.transform(datasets, **context)
        elif hasattr(module, 'main'):
            return module.main(datasets, **context)
        else:
            raise ValueError(f"No 'transform' or 'main' function found in {self.python_code}")
    
    def _execute_module_transform(self, datasets: Dict, context) -> Any:
        """Execute module.function reference"""
        try:
            module_name, function_name = self.python_code.rsplit('.', 1)
            module = importlib.import_module(module_name)
            func = getattr(module, function_name)
            return func(datasets, **context)
        except Exception as e:
            raise AirflowException(f"Module execution failed: {str(e)}")

# Factory functions
def create_fixed_python_transform_operator(
    task_id: str,
    python_callable: Union[str, Callable],
    data_source_task_ids: Union[str, List[str]],
    dag,
    op_args: Optional[tuple] = None,
    op_kwargs: Optional[Dict] = None,
    **kwargs
) -> FixedPythonTransformOperator:
    """Factory function to create fixed Python transform operator"""
    
    return FixedPythonTransformOperator(
        task_id=task_id,
        python_callable=python_callable,
        data_source_task_ids=data_source_task_ids,
        op_args=op_args,
        op_kwargs=op_kwargs,
        dag=dag,
        **kwargs
    )

def create_inline_code_transform_operator(
    task_id: str,
    python_code: str,
    data_source_task_ids: Union[str, List[str]],
    dag,
    code_type: str = "auto",
    **kwargs
) -> InlineCodeTransformOperator:
    """Factory function to create inline code transform operator"""
    
    return InlineCodeTransformOperator(
        task_id=task_id,
        python_code=python_code,
        data_source_task_ids=data_source_task_ids,
        code_type=code_type,
        dag=dag,
        **kwargs
    )

# For backward compatibility
def create_python_transform_operator(
    task_id: str,
    python_callable: Union[str, Callable],
    data_source_task_ids: Union[str, List[str]],
    dag,
    op_args: Optional[tuple] = None,
    op_kwargs: Optional[Dict] = None,
    **kwargs
) -> FixedPythonTransformOperator:
    """Factory function to create Python transform operator (fixed version)"""
    
    return FixedPythonTransformOperator(
        task_id=task_id,
        python_callable=python_callable,
        data_source_task_ids=data_source_task_ids,
        op_args=op_args,
        op_kwargs=op_kwargs,
        dag=dag,
        **kwargs
    )