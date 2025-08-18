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
        """Execute SQL using pandas (limited SQL support)"""
        import pandasql as ps
        
        # Make datasets available to SQL query
        local_env = datasets.copy()
        local_env.update({
            'execution_date': context['execution_date'],
            'ds': context['ds'],
            'ts': context['ts']
        })
        
        try:
            # Execute SQL query
            result_df = ps.sqldf(self.sql_query, local_env)
            return result_df.to_dict('records')
            
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