"""
Temporary Database Sink Operators
Specialized sinks for temporary data storage with automatic cleanup
"""

import sqlite3
import json
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
from pathlib import Path

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

from core.config import SinkConfig, WriteMode
from core.data_registry import get_data_registry, TaskSinkType

logger = logging.getLogger(__name__)


class TempDatabaseSinkOperator(BaseOperator):
    """
    Temporary SQLite database sink for task data sharing
    Automatically registers task data location in the registry
    """
    
    def __init__(self, sink_config: SinkConfig, data_source_task_id: str = None, 
                 auto_register: bool = True, **kwargs):
        super().__init__(**kwargs)
        self.sink_config = sink_config
        self.data_source_task_id = data_source_task_id
        self.auto_register = auto_register
        self.registry = get_data_registry()
    
    def execute(self, context):
        """Execute data loading to temporary database"""
        try:
            # Get data from upstream task or context
            if self.data_source_task_id:
                data = context['task_instance'].xcom_pull(task_ids=self.data_source_task_id)
            else:
                # For cases where data is passed directly (e.g., from transforms)
                data = context.get('task_data') or context.get('data')
            
            if not data:
                logger.warning(f"No data received for temp sink {self.sink_config.name}")
                if self.auto_register:
                    # Register empty location
                    self.registry.register_task_data_location(
                        task_id=context['task'].task_id,
                        dag_id=context['dag'].dag_id,
                        execution_date=context['execution_date'].isoformat(),
                        sink_config=self.sink_config,
                        records_count=0
                    )
                return 0
            
            # Convert to DataFrame if needed
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, pd.DataFrame):
                df = data
            else:
                # Single record or other data types
                df = pd.DataFrame([data] if not isinstance(data, list) else data)
            
            logger.info(f"Loading {len(df)} records to temp database table: {self.sink_config.table_name}")
            
            # Load data to SQLite
            records_processed = self._load_to_sqlite(df, context)
            
            # Register data location
            if self.auto_register:
                self.registry.register_task_data_location(
                    task_id=context['task'].task_id,
                    dag_id=context['dag'].dag_id,
                    execution_date=context['execution_date'].isoformat(),
                    sink_config=self.sink_config,
                    records_count=records_processed
                )
            
            logger.info(f"Successfully loaded {records_processed} records to temp database")
            return records_processed
            
        except Exception as e:
            logger.error(f"Failed to load data to temp database: {str(e)}")
            raise
    
    def _load_to_sqlite(self, df: pd.DataFrame, context) -> int:
        """Load DataFrame to SQLite database"""
        if df.empty:
            return 0
        
        # Use the registry's temp database
        db_path = self.registry._temp_db_path
        
        with sqlite3.connect(str(db_path)) as conn:
            # Handle write modes
            if self.sink_config.write_mode == WriteMode.OVERWRITE:
                # Drop existing table
                conn.execute(f"DROP TABLE IF EXISTS {self.sink_config.table_name}")
            
            # Write DataFrame to SQLite
            if_exists_mode = 'replace' if self.sink_config.write_mode == WriteMode.OVERWRITE else 'append'
            
            try:
                df.to_sql(
                    self.sink_config.table_name,
                    conn,
                    if_exists=if_exists_mode,
                    index=False,
                    method='multi'  # Faster bulk inserts
                )
                
                # Add metadata columns if they don't exist
                self._add_metadata_columns(conn, self.sink_config.table_name, context)
                
                return len(df)
                
            except Exception as e:
                logger.error(f"SQLite write failed: {str(e)}")
                # Try with individual inserts as fallback
                return self._fallback_insert(conn, df, context)
    
    def _add_metadata_columns(self, conn: sqlite3.Connection, table_name: str, context):
        """Add metadata columns to track data lineage"""
        try:
            # Add execution metadata
            execution_date = context['execution_date'].isoformat()
            dag_id = context['dag'].dag_id
            task_id = context['task'].task_id
            
            # Check if metadata columns exist
            cursor = conn.execute(f"PRAGMA table_info({table_name})")
            columns = [col[1] for col in cursor.fetchall()]
            
            if '_airflow_dag_id' not in columns:
                conn.execute(f"ALTER TABLE {table_name} ADD COLUMN _airflow_dag_id TEXT")
                conn.execute(f"UPDATE {table_name} SET _airflow_dag_id = ?", (dag_id,))
            
            if '_airflow_task_id' not in columns:
                conn.execute(f"ALTER TABLE {table_name} ADD COLUMN _airflow_task_id TEXT")
                conn.execute(f"UPDATE {table_name} SET _airflow_task_id = ?", (task_id,))
            
            if '_airflow_execution_date' not in columns:
                conn.execute(f"ALTER TABLE {table_name} ADD COLUMN _airflow_execution_date TEXT")
                conn.execute(f"UPDATE {table_name} SET _airflow_execution_date = ?", (execution_date,))
            
            if '_created_at' not in columns:
                conn.execute(f"ALTER TABLE {table_name} ADD COLUMN _created_at TEXT")
                conn.execute(f"UPDATE {table_name} SET _created_at = ?", (datetime.utcnow().isoformat(),))
            
            conn.commit()
            
        except Exception as e:
            logger.warning(f"Could not add metadata columns: {str(e)}")
    
    def _fallback_insert(self, conn: sqlite3.Connection, df: pd.DataFrame, context) -> int:
        """Fallback method for inserting data row by row"""
        try:
            records = df.to_dict('records')
            
            if not records:
                return 0
            
            # Get column names
            columns = list(records[0].keys())
            placeholders = ', '.join(['?' for _ in columns])
            columns_str = ', '.join(columns)
            
            # Create table if not exists (inferred schema)
            self._create_table_if_not_exists(conn, self.sink_config.table_name, records[0])
            
            # Insert records
            insert_sql = f"INSERT INTO {self.sink_config.table_name} ({columns_str}) VALUES ({placeholders})"
            
            for record in records:
                values = [self._sanitize_value(record.get(col)) for col in columns]
                conn.execute(insert_sql, values)
            
            conn.commit()
            return len(records)
            
        except Exception as e:
            logger.error(f"Fallback insert failed: {str(e)}")
            raise AirflowException(f"Could not insert data: {str(e)}")
    
    def _create_table_if_not_exists(self, conn: sqlite3.Connection, table_name: str, sample_record: Dict):
        """Create table with inferred schema if it doesn't exist"""
        try:
            # Check if table exists
            cursor = conn.execute("""
                SELECT name FROM sqlite_master WHERE type='table' AND name=?
            """, (table_name,))
            
            if cursor.fetchone():
                return  # Table already exists
            
            # Infer column types from sample record
            columns_def = []
            for key, value in sample_record.items():
                if isinstance(value, bool):
                    col_type = "BOOLEAN"
                elif isinstance(value, int):
                    col_type = "INTEGER"
                elif isinstance(value, float):
                    col_type = "REAL"
                elif isinstance(value, datetime):
                    col_type = "TEXT"  # Store as ISO string
                elif isinstance(value, (dict, list)):
                    col_type = "TEXT"  # Store as JSON string
                else:
                    col_type = "TEXT"
                
                columns_def.append(f"{key} {col_type}")
            
            # Create table
            create_sql = f"""
                CREATE TABLE {table_name} (
                    {', '.join(columns_def)}
                )
            """
            
            conn.execute(create_sql)
            logger.info(f"Created temp table: {table_name}")
            
        except Exception as e:
            logger.error(f"Could not create table {table_name}: {str(e)}")
            raise
    
    def _sanitize_value(self, value):
        """Sanitize values for SQLite storage"""
        if value is None:
            return None
        elif isinstance(value, (dict, list)):
            return json.dumps(value)
        elif isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, pd.Timestamp):
            return value.isoformat()
        else:
            return value


class TempFileSinkOperator(BaseOperator):
    """
    Temporary file sink for task data sharing
    Stores data as JSON/CSV files with automatic registry
    """
    
    def __init__(self, sink_config: SinkConfig, data_source_task_id: str = None,
                 auto_register: bool = True, **kwargs):
        super().__init__(**kwargs)
        self.sink_config = sink_config
        self.data_source_task_id = data_source_task_id
        self.auto_register = auto_register
        self.registry = get_data_registry()
    
    def execute(self, context):
        """Execute data loading to temporary file"""
        try:
            # Get data
            if self.data_source_task_id:
                data = context['task_instance'].xcom_pull(task_ids=self.data_source_task_id)
            else:
                data = context.get('task_data') or context.get('data')
            
            if not data:
                logger.warning(f"No data received for temp file sink {self.sink_config.name}")
                if self.auto_register:
                    self.registry.register_task_data_location(
                        task_id=context['task'].task_id,
                        dag_id=context['dag'].dag_id,
                        execution_date=context['execution_date'].isoformat(),
                        sink_config=self.sink_config,
                        records_count=0
                    )
                return 0
            
            # Ensure directory exists
            file_path = Path(self.sink_config.file_path)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Convert data format
            if isinstance(data, pd.DataFrame):
                records_count = len(data)
                data_to_write = data.to_dict('records')
            elif isinstance(data, list):
                records_count = len(data)
                data_to_write = data
            else:
                records_count = 1
                data_to_write = [data] if not isinstance(data, dict) else data
            
            # Write file based on format
            if self.sink_config.file_format == 'json':
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump({
                        'data': data_to_write,
                        'metadata': {
                            'dag_id': context['dag'].dag_id,
                            'task_id': context['task'].task_id,
                            'execution_date': context['execution_date'].isoformat(),
                            'records_count': records_count,
                            'created_at': datetime.utcnow().isoformat()
                        }
                    }, f, indent=2, default=str)
            
            elif self.sink_config.file_format == 'csv':
                if isinstance(data, pd.DataFrame):
                    data.to_csv(file_path, index=False)
                else:
                    pd.DataFrame(data_to_write).to_csv(file_path, index=False)
            
            else:
                raise ValueError(f"Unsupported temp file format: {self.sink_config.file_format}")
            
            logger.info(f"Saved {records_count} records to temp file: {file_path}")
            
            # Register data location
            if self.auto_register:
                self.registry.register_task_data_location(
                    task_id=context['task'].task_id,
                    dag_id=context['dag'].dag_id,
                    execution_date=context['execution_date'].isoformat(),
                    sink_config=self.sink_config,
                    records_count=records_count
                )
            
            return records_count
            
        except Exception as e:
            logger.error(f"Failed to save temp file: {str(e)}")
            raise


class TempDataLakeSinkOperator(BaseOperator):
    """
    Temporary Azure Data Lake Gen2 sink for task data sharing
    Stores data as Parquet blobs with automatic registry
    """
    
    def __init__(self, sink_config: SinkConfig, data_source_task_id: str = None,
                 auto_register: bool = True, **kwargs):
        super().__init__(**kwargs)
        self.sink_config = sink_config
        self.data_source_task_id = data_source_task_id
        self.auto_register = auto_register
        self.registry = get_data_registry()
    
    def execute(self, context):
        """Execute data loading to temporary Data Lake"""
        try:
            # Get data
            if self.data_source_task_id:
                data = context['task_instance'].xcom_pull(task_ids=self.data_source_task_id)
            else:
                data = context.get('task_data') or context.get('data')
            
            if not data:
                logger.warning(f"No data received for temp Data Lake sink {self.sink_config.name}")
                if self.auto_register:
                    self.registry.register_task_data_location(
                        task_id=context['task'].task_id,
                        dag_id=context['dag'].dag_id,
                        execution_date=context['execution_date'].isoformat(),
                        sink_config=self.sink_config,
                        records_count=0
                    )
                return 0
            
            # Convert to DataFrame
            if isinstance(data, pd.DataFrame):
                df = data
                records_count = len(df)
            elif isinstance(data, list):
                df = pd.DataFrame(data)
                records_count = len(data)
            else:
                df = pd.DataFrame([data])
                records_count = 1
            
            # Save to Data Lake
            records_processed = self._save_to_datalake(df, context)
            
            logger.info(f"Saved {records_processed} records to Data Lake: {self.sink_config.file_path}")
            
            # Register data location
            if self.auto_register:
                self.registry.register_task_data_location(
                    task_id=context['task'].task_id,
                    dag_id=context['dag'].dag_id,
                    execution_date=context['execution_date'].isoformat(),
                    sink_config=self.sink_config,
                    records_count=records_processed
                )
            
            return records_processed
            
        except Exception as e:
            logger.error(f"Failed to save to Data Lake: {str(e)}")
            raise
    
    def _save_to_datalake(self, df: pd.DataFrame, context) -> int:
        """Save DataFrame to Azure Data Lake Gen2"""
        try:
            from azure.storage.blob import BlobServiceClient
            import io
            
            if df.empty:
                return 0
            
            # Get connection
            connection = BaseHook.get_connection(self.sink_config.connection_id)
            
            # Create blob service client
            blob_service_client = BlobServiceClient(
                account_url=f"https://{connection.host}.blob.core.windows.net",
                credential=connection.password
            )
            
            # Get container and blob path
            container = self.sink_config.custom_config.get("container", "airflow-temp-data")
            blob_path = self.sink_config.file_path
            
            # Add metadata to DataFrame
            df_with_metadata = df.copy()
            df_with_metadata['_airflow_dag_id'] = context['dag'].dag_id
            df_with_metadata['_airflow_task_id'] = context['task'].task_id
            df_with_metadata['_airflow_execution_date'] = context['execution_date'].isoformat()
            df_with_metadata['_created_at'] = datetime.utcnow().isoformat()
            
            # Convert to appropriate format
            if self.sink_config.file_format == 'parquet':
                buffer = io.BytesIO()
                df_with_metadata.to_parquet(buffer, index=False)
                content = buffer.getvalue()
                content_type = 'application/octet-stream'
                
            elif self.sink_config.file_format == 'json':
                content = df_with_metadata.to_json(orient='records', indent=2)
                content_type = 'application/json'
                
            elif self.sink_config.file_format == 'csv':
                content = df_with_metadata.to_csv(index=False)
                content_type = 'text/csv'
                
            else:
                # Default to parquet for efficiency
                buffer = io.BytesIO()
                df_with_metadata.to_parquet(buffer, index=False)
                content = buffer.getvalue()
                content_type = 'application/octet-stream'
            
            # Upload blob
            blob_client = blob_service_client.get_blob_client(
                container=container,
                blob=blob_path
            )
            
            # Handle write mode
            overwrite = self.sink_config.write_mode == WriteMode.OVERWRITE
            
            blob_client.upload_blob(
                content, 
                overwrite=overwrite,
                content_settings={'content_type': content_type}
            )
            
            return len(df)
            
        except Exception as e:
            logger.error(f"Failed to save to Data Lake: {str(e)}")
            raise


def create_auto_sink_operator(task_id: str, dag_id: str, execution_date: str,
                            preferred_sink_type: Optional[TaskSinkType] = None,
                            data_source_task_id: str = None,
                            dag=None, **kwargs) -> BaseOperator:
    """
    Factory function to create automatic sink operator for a task
    
    This replaces manual sink configuration with automatic temp storage
    """
    registry = get_data_registry()
    
    # Get automatic sink configuration
    sink_config = registry.get_task_sink_config(
        task_id=task_id,
        dag_id=dag_id,
        execution_date=execution_date,
        preferred_sink_type=preferred_sink_type
    )
    
    # Create appropriate operator
    if sink_config.connection_id == "temp_database":
        return TempDatabaseSinkOperator(
            task_id=f"{task_id}_auto_sink",
            sink_config=sink_config,
            data_source_task_id=data_source_task_id,
            dag=dag,
            **kwargs
        )
    elif sink_config.type.value == "file":
        return TempFileSinkOperator(
            task_id=f"{task_id}_auto_sink",
            sink_config=sink_config,
            data_source_task_id=data_source_task_id,
            dag=dag,
            **kwargs
        )
    elif sink_config.type.value == "datalake_gen2":
        return TempDataLakeSinkOperator(
            task_id=f"{task_id}_auto_sink",
            sink_config=sink_config,
            data_source_task_id=data_source_task_id,
            dag=dag,
            **kwargs
        )
    else:
        raise ValueError(f"Unsupported auto sink type for config: {sink_config}")