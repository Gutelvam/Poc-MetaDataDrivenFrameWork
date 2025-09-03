"""
Transform with Sink Operator - Combines transformation and sink operations
"""

import logging
from typing import List, Dict, Any, Optional, Union, Callable
import pandas as pd
from datetime import timedelta

from airflow.models import BaseOperator
from transforms.operators import PythonTransformOperator
from sinks.operators import create_sink_operator
from core.config import SinkConfig

logger = logging.getLogger(__name__)

class TransformWithSinkOperator(BaseOperator):
    """
    Operator that performs a transformation and then writes the result to a sink.
    This avoids using XCom for large data transfers between tasks.
    """
    
    def __init__(
        self,
        python_callable: Union[str, Callable],
        data_source_task_ids: List[str],
        sink_config: SinkConfig,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.python_callable = python_callable
        self.data_source_task_ids = data_source_task_ids
        self.sink_config = sink_config
        
    def execute(self, context):
        """Execute transformation and then write to sink"""
        try:
            # Step 1: Get data from upstream tasks
            logger.info(f"🔄 Retrieving data from upstream tasks: {self.data_source_task_ids}")
            
            if self.data_source_task_ids:
                # Get data from XCom
                task_instance = context['task_instance']
                
                if len(self.data_source_task_ids) == 1:
                    # Single source
                    upstream_result = task_instance.xcom_pull(task_ids=self.data_source_task_ids[0])
                    data = self._resolve_data_source(upstream_result, context)
                else:
                    # Multiple sources - combine them
                    all_data = []
                    for task_id in self.data_source_task_ids:
                        upstream_result = task_instance.xcom_pull(task_ids=task_id)
                        resolved_data = self._resolve_data_source(upstream_result, context)
                        if resolved_data:
                            if isinstance(resolved_data, list):
                                all_data.extend(resolved_data)
                            else:
                                all_data.append(resolved_data)
                    data = all_data
            else:
                data = None
            
            # Step 2: Execute transformation
            logger.info(f"🔧 Executing transformation for {self.task_id}")
            logger.info(f"🔍 Data to transform type: {type(data)}, length: {len(data) if hasattr(data, '__len__') else 'N/A'}")
            
            # Execute transformation directly with resolved data (bypass PythonTransformOperator)
            from transforms.operators import PythonTransformOperator
            
            # Create temp transform operator just to resolve the callable
            temp_transform_op = PythonTransformOperator(
                task_id=f"{self.task_id}_transform",
                python_callable=self.python_callable,
                data_source_task_ids=[],
                dag=self.dag
            )
            
            # Resolve the callable
            if isinstance(self.python_callable, str):
                callable_func = temp_transform_op._resolve_callable_from_string(self.python_callable)
            else:
                callable_func = self.python_callable
            
            # Call the function directly with our resolved data
            import inspect
            if callable_func and hasattr(callable_func, '__code__'):
                sig = inspect.signature(callable_func)
                params = list(sig.parameters.keys())
                
                if len(params) >= 2 and params[0] == 'data' and params[1] == 'context':
                    # Function expects (data, context) - pass resolved data directly
                    logger.info(f"🔄 Calling transform function with {len(data)} records")
                    transformed_data = callable_func(data, context)
                else:
                    # Standard format - pass as dict with single key
                    datasets = {'resolved_data': data}
                    logger.info(f"🔄 Calling transform function with datasets dict containing {len(data)} records")
                    transformed_data = callable_func(datasets, context=context)
            else:
                transformed_data = callable_func(data, context)
            
            logger.info(f"🔍 Transform result type: {type(transformed_data)}")
            logger.info(f"🔍 Transform result: {str(transformed_data)[:200]}...")
            
            # Handle case where transform returns metadata instead of data
            if isinstance(transformed_data, int):
                logger.warning(f"Transform returned int ({transformed_data}), likely no data processed")
                return 0
            elif not transformed_data or (isinstance(transformed_data, list) and len(transformed_data) == 0):
                logger.warning(f"No data returned from transformation in {self.task_id}")
                return 0
            
            # Step 3: Write to sink
            logger.info(f"💾 Writing transformed data to sink: {self.sink_config.name}")
            
            # Create sink operator
            sink_op = create_sink_operator(
                task_id=f"{self.task_id}_sink",
                sink_config=self.sink_config,
                data_source_task_id=None,  # We'll pass data directly
                dag=self.dag
            )
            
            # Pass transformed data directly to sink
            context['data'] = transformed_data
            records_written = sink_op.execute(context)
            
            logger.info(f"✅ Successfully transformed and loaded {records_written} records")
            
            # Check if we need to cleanup temp tables
            self._cleanup_temp_tables_if_needed(context)
            
            # Return appropriate response based on sink type
            table_name = self.sink_config.table_name
            is_temp_table = 'temp_' in table_name.lower()
            
            if is_temp_table:
                # Temp table - return metadata for downstream
                return {
                    'records_transformed': len(transformed_data) if isinstance(transformed_data, (list, pd.DataFrame)) else 1,
                    'records_written': records_written,
                    'sink_table': f"{self.sink_config.schema_name}.{self.sink_config.table_name}" if self.sink_config.schema_name else self.sink_config.table_name,
                    'sink_connection': self.sink_config.connection_id,
                    'task_type': 'transform_with_sink',
                    'is_temp_table': True
                }
            else:
                # Final table - return count
                return records_written
            
        except Exception as e:
            logger.error(f"❌ Transform with sink failed for {self.task_id}: {str(e)}")
            raise
    
    def _resolve_data_source(self, upstream_result, context):
        """
        Resolve data source from upstream result.
        Can handle direct data or metadata with table references.
        """
        if upstream_result is None:
            return None
            
        # If it's already a list of records, return it directly
        if isinstance(upstream_result, list):
            return upstream_result
            
        # If it's metadata from extract_with_sink operation
        if isinstance(upstream_result, dict) and upstream_result.get('task_type') == 'extract_with_sink':
            logger.info(f"🔍 Upstream task stored data in table: {upstream_result.get('sink_table')}")
            
            # Read data from the specified table
            connection_id = upstream_result.get('sink_connection')
            table_name = upstream_result.get('sink_table')
            
            if connection_id and table_name:
                return self._read_from_table(connection_id, table_name, context)
            else:
                logger.error(f"Missing connection_id or table_name in metadata: {upstream_result}")
                return None
        
        # If it's a single record, wrap in list
        if isinstance(upstream_result, dict):
            return [upstream_result]
            
        # For any other type, log and return None
        logger.warning(f"Unexpected upstream result type: {type(upstream_result)}")
        return None
    
    def _read_from_table(self, connection_id: str, table_name: str, context):
        """Read data from a PostgreSQL table"""
        try:
            from sources.operators import PostgreSQLSourceOperator
            from core.config import SourceConfig, SourceType
            
            # Create source config to read from the table
            source_config = SourceConfig(
                name=f"temp_read_{table_name}",
                type=SourceType.POSTGRESQL,
                connection_id=connection_id,
                query=f"SELECT * FROM {table_name}"
            )
            
            # Create source operator
            source_op = PostgreSQLSourceOperator(
                task_id=f"temp_read_{self.task_id}",
                source_config=source_config,
                dag=self.dag
            )
            
            # Extract data
            data = source_op.extract_data(context)
            logger.info(f"📊 Read {len(data)} records from table {table_name}")
            return data
            
        except Exception as e:
            logger.error(f"❌ Failed to read from table {table_name}: {str(e)}")
            return None
    
    def _cleanup_temp_tables_if_needed(self, context):
        """Clean up upstream temp tables if this is the final sink"""
        try:
            # Only cleanup if current sink is NOT a temp table (i.e., it's final)
            if 'temp_' not in self.sink_config.table_name.lower():
                logger.info("🧹 Cleaning up upstream temp tables")
                
                # Get upstream results to find temp tables to cleanup
                task_instance = context['task_instance']
                
                for upstream_task_id in self.data_source_task_ids:
                    upstream_result = task_instance.xcom_pull(task_ids=upstream_task_id)
                    
                    if (isinstance(upstream_result, dict) and 
                        upstream_result.get('is_temp_table') and 
                        upstream_result.get('sink_table')):
                        
                        temp_table = upstream_result['sink_table']
                        connection_id = upstream_result['sink_connection']
                        
                        self._drop_table(connection_id, temp_table)
                        logger.info(f"🗑️ Dropped temp table: {temp_table}")
                        
        except Exception as e:
            logger.warning(f"Failed to cleanup temp tables: {str(e)}")
    
    def _drop_table(self, connection_id: str, table_name: str):
        """Drop a table from PostgreSQL"""
        try:
            import psycopg2
            from airflow.hooks.base import BaseHook
            
            connection = BaseHook.get_connection(connection_id)
            
            conn = psycopg2.connect(
                host=connection.host,
                port=connection.port,
                database=connection.schema,
                user=connection.login,
                password=connection.password
            )
            
            cursor = conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.commit()
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.warning(f"Failed to drop table {table_name}: {str(e)}")

def create_transform_with_sink_operator(
    task_id: str,
    python_callable: Union[str, Callable],
    data_source_task_ids: List[str],
    sink_config: SinkConfig,
    dag,
    **kwargs
) -> TransformWithSinkOperator:
    """Factory function to create a transform with sink operator"""
    
    return TransformWithSinkOperator(
        task_id=task_id,
        python_callable=python_callable,
        data_source_task_ids=data_source_task_ids,
        sink_config=sink_config,
        dag=dag,
        **kwargs
    )