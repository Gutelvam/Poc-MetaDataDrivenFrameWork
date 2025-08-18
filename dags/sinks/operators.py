"""
Sink Operators Module
Handles data loading to various data sinks with support for different write modes
"""

import json
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import clickhouse_connect
from pymongo import MongoClient
from azure.storage.blob import BlobServiceClient
import pysftp
import requests

from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.decorators import apply_defaults

from core.config import SinkConfig, SinkType, WriteMode

logger = logging.getLogger(__name__)

class BaseSinkOperator(BaseOperator):
    """Base class for all sink operators"""
    
    @apply_defaults
    def __init__(self, sink_config: SinkConfig, data_source_task_id: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sink_config = sink_config
        self.data_source_task_id = data_source_task_id
    
    def execute(self, context):
        """Execute data loading"""
        try:
            # Get data from upstream task if specified
            if self.data_source_task_id:
                data = context['task_instance'].xcom_pull(task_ids=self.data_source_task_id)
            else:
                # For transform tasks, data might be passed directly
                data = context.get('data')
            
            if not data:
                logger.warning(f"No data received for sink {self.sink_config.name}")
                return 0
            
            logger.info(f"Starting data load to {self.sink_config.name} with {len(data) if isinstance(data, list) else 'data'} records")
            
            # Ensure table exists if auto_create is enabled
            if self.sink_config.auto_create_table:
                self.create_table_if_not_exists(data)
            
            # Load data based on write mode
            records_processed = self.load_data(data, context)
            
            logger.info(f"Successfully loaded {records_processed} records to {self.sink_config.name}")
            return records_processed
            
        except Exception as e:
            logger.error(f"Failed to load data to {self.sink_config.name}: {str(e)}")
            raise
    
    def create_table_if_not_exists(self, data: Union[List[Dict], pd.DataFrame]):
        """Create table if it doesn't exist (to be implemented by subclasses)"""
        pass
    
    def load_data(self, data: Union[List[Dict], pd.DataFrame], context) -> int:
        """Abstract method to be implemented by concrete operators"""
        raise NotImplementedError("Subclasses must implement load_data method")

class PostgreSQLSinkOperator(BaseSinkOperator):
    """Load data into PostgreSQL database"""
    
    def create_table_if_not_exists(self, data: Union[List[Dict], pd.DataFrame]):
        """Create PostgreSQL table if it doesn't exist"""
        if not data:
            return
        
        connection = BaseHook.get_connection(self.sink_config.connection_id)
        
        try:
            conn = psycopg2.connect(
                host=connection.host,
                port=connection.port,
                database=connection.schema,
                user=connection.login,
                password=connection.password
            )
            
            cursor = conn.cursor()
            
            # Build table name with schema
            full_table_name = self._get_full_table_name()
            
            # Check if table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = %s
                );
            """, (self.sink_config.schema_name or 'public', self.sink_config.table_name))
            
            table_exists = cursor.fetchone()[0]
            
            if not table_exists:
                # Infer schema from data
                if self.sink_config.table_schema:
                    columns_def = self._build_columns_definition(self.sink_config.table_schema)
                else:
                    columns_def = self._infer_schema_from_data(data)
                
                create_table_sql = f"""
                    CREATE TABLE {full_table_name} (
                        {columns_def}
                    )
                """
                
                cursor.execute(create_table_sql)
                conn.commit()
                logger.info(f"Created table: {full_table_name}")
            
        finally:
            if 'conn' in locals():
                conn.close()
    
    def load_data(self, data: Union[List[Dict], pd.DataFrame], context) -> int:
        """Load data into PostgreSQL"""
        if isinstance(data, pd.DataFrame):
            data = data.to_dict('records')
        
        if not data:
            return 0
        
        connection = BaseHook.get_connection(self.sink_config.connection_id)
        
        try:
            conn = psycopg2.connect(
                host=connection.host,
                port=connection.port,
                database=connection.schema,
                user=connection.login,
                password=connection.password
            )
            
            cursor = conn.cursor()
            full_table_name = self._get_full_table_name()
            
            if self.sink_config.write_mode == WriteMode.OVERWRITE:
                return self._overwrite_data(cursor, conn, full_table_name, data)
            elif self.sink_config.write_mode == WriteMode.APPEND:
                return self._append_data(cursor, conn, full_table_name, data)
            elif self.sink_config.write_mode == WriteMode.UPSERT:
                return self._upsert_data(cursor, conn, full_table_name, data)
            else:
                raise ValueError(f"Unsupported write mode: {self.sink_config.write_mode}")
            
        finally:
            if 'conn' in locals():
                conn.close()
    
    def _get_full_table_name(self) -> str:
        """Get fully qualified table name"""
        if self.sink_config.schema_name:
            return f"{self.sink_config.schema_name}.{self.sink_config.table_name}"
        return self.sink_config.table_name
    
    def _overwrite_data(self, cursor, conn, table_name: str, data: List[Dict]) -> int:
        """Overwrite table data"""
        # Truncate table
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        
        return self._insert_data(cursor, conn, table_name, data)
    
    def _append_data(self, cursor, conn, table_name: str, data: List[Dict]) -> int:
        """Append data to table"""
        return self._insert_data(cursor, conn, table_name, data)
    
    def _insert_data(self, cursor, conn, table_name: str, data: List[Dict]) -> int:
        """Insert data into table"""
        if not data:
            return 0
        
        # Get column names from first record
        columns = list(data[0].keys())
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Prepare data for batch insert
        values = []
        for record in data:
            values.append(tuple(record.get(col) for col in columns))
        
        # Batch insert
        batch_size = self.sink_config.batch_size or 1000
        total_inserted = 0
        
        for i in range(0, len(values), batch_size):
            batch = values[i:i + batch_size]
            
            insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES %s"
            execute_values(cursor, insert_sql, batch)
            total_inserted += len(batch)
        
        conn.commit()
        return total_inserted
    
    def _upsert_data(self, cursor, conn, table_name: str, data: List[Dict]) -> int:
        """Upsert data (INSERT ... ON CONFLICT)"""
        if not data or not self.sink_config.upsert_keys:
            raise ValueError("Upsert requires data and upsert_keys")
        
        columns = list(data[0].keys())
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Build conflict clause
        conflict_keys = ', '.join(self.sink_config.upsert_keys)
        update_columns = [col for col in columns if col not in self.sink_config.upsert_keys]
        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        
        upsert_sql = f"""
            INSERT INTO {table_name} ({columns_str}) 
            VALUES %s
            ON CONFLICT ({conflict_keys}) 
            DO UPDATE SET {update_clause}
        """
        
        # Prepare data
        values = []
        for record in data:
            values.append(tuple(record.get(col) for col in columns))
        
        # Batch upsert
        batch_size = self.sink_config.batch_size or 1000
        total_upserted = 0
        
        for i in range(0, len(values), batch_size):
            batch = values[i:i + batch_size]
            execute_values(cursor, upsert_sql, batch)
            total_upserted += len(batch)
        
        conn.commit()
        return total_upserted
    
    def _infer_schema_from_data(self, data: Union[List[Dict], pd.DataFrame]) -> str:
        """Infer PostgreSQL schema from data"""
        if isinstance(data, pd.DataFrame):
            sample = data.head(1).to_dict('records')[0]
        else:
            sample = data[0] if data else {}
        
        columns = []
        for col_name, value in sample.items():
            if isinstance(value, bool):
                col_type = "BOOLEAN"
            elif isinstance(value, int):
                col_type = "INTEGER"
            elif isinstance(value, float):
                col_type = "REAL"
            elif isinstance(value, datetime):
                col_type = "TIMESTAMP"
            elif isinstance(value, (list, dict)):
                col_type = "JSONB"
            else:
                col_type = "TEXT"
            
            columns.append(f"{col_name} {col_type}")
        
        return ', '.join(columns)
    
    def _build_columns_definition(self, schema: Dict[str, str]) -> str:
        """Build columns definition from provided schema"""
        columns = []
        for col_name, col_type in schema.items():
            columns.append(f"{col_name} {col_type}")
        return ', '.join(columns)

class MongoDBSinkOperator(BaseSinkOperator):
    """Load data into MongoDB"""
    
    def load_data(self, data: Union[List[Dict], pd.DataFrame], context) -> int:
        """Load data into MongoDB"""
        if isinstance(data, pd.DataFrame):
            data = data.to_dict('records')
        
        if not data:
            return 0
        
        connection = BaseHook.get_connection(self.sink_config.connection_id)
        
        # Build MongoDB URI
        if connection.password:
            uri = f"mongodb://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}"
        else:
            uri = f"mongodb://{connection.host}:{connection.port}/{connection.schema}"
        
        client = MongoClient(uri)
        
        try:
            db = client[connection.schema or self.sink_config.schema_name]
            collection = db[self.sink_config.collection_name]
            
            if self.sink_config.write_mode == WriteMode.OVERWRITE:
                # Drop and recreate collection
                collection.delete_many({})
                result = collection.insert_many(data)
                return len(result.inserted_ids)
            
            elif self.sink_config.write_mode == WriteMode.APPEND:
                result = collection.insert_many(data)
                return len(result.inserted_ids)
            
            elif self.sink_config.write_mode == WriteMode.UPSERT:
                if not self.sink_config.upsert_keys:
                    raise ValueError("MongoDB upsert requires upsert_keys")
                
                upserted_count = 0
                for record in data:
                    # Build filter from upsert keys
                    filter_dict = {key: record[key] for key in self.sink_config.upsert_keys if key in record}
                    
                    # Upsert document
                    result = collection.replace_one(filter_dict, record, upsert=True)
                    upserted_count += 1
                
                return upserted_count
            
            else:
                raise ValueError(f"Unsupported write mode: {self.sink_config.write_mode}")
            
        finally:
            client.close()

class ClickHouseSinkOperator(BaseSinkOperator):
    """Load data into ClickHouse"""
    
    def create_table_if_not_exists(self, data: Union[List[Dict], pd.DataFrame]):
        """Create ClickHouse table if it doesn't exist"""
        if not data:
            return
        
        connection = BaseHook.get_connection(self.sink_config.connection_id)
        
        client = clickhouse_connect.get_client(
            host=connection.host,
            port=connection.port or 8123,
            username=connection.login,
            password=connection.password,
            database=connection.schema or 'default'
        )
        
        try:
            # Check if table exists
            result = client.query(f"EXISTS TABLE {self.sink_config.table_name}")
            table_exists = result.result_rows[0][0] if result.result_rows else False
            
            if not table_exists:
                if self.sink_config.table_schema:
                    columns_def = self._build_clickhouse_columns(self.sink_config.table_schema)
                else:
                    columns_def = self._infer_clickhouse_schema(data)
                
                create_table_sql = f"""
                    CREATE TABLE {self.sink_config.table_name} (
                        {columns_def}
                    ) ENGINE = MergeTree()
                    ORDER BY tuple()
                """
                
                client.query(create_table_sql)
                logger.info(f"Created ClickHouse table: {self.sink_config.table_name}")
        
        finally:
            client.close()
    
    def load_data(self, data: Union[List[Dict], pd.DataFrame], context) -> int:
        """Load data into ClickHouse"""
        if isinstance(data, pd.DataFrame):
            data = data.to_dict('records')
        
        if not data:
            return 0
        
        connection = BaseHook.get_connection(self.sink_config.connection_id)
        
        client = clickhouse_connect.get_client(
            host=connection.host,
            port=connection.port or 8123,
            username=connection.login,
            password=connection.password,
            database=connection.schema or 'default'
        )
        
        try:
            if self.sink_config.write_mode == WriteMode.OVERWRITE:
                # Truncate table
                client.query(f"TRUNCATE TABLE {self.sink_config.table_name}")
            
            # Insert data
            client.insert(self.sink_config.table_name, data)
            return len(data)
        
        finally:
            client.close()
    
    def _infer_clickhouse_schema(self, data: Union[List[Dict], pd.DataFrame]) -> str:
        """Infer ClickHouse schema from data"""
        if isinstance(data, pd.DataFrame):
            sample = data.head(1).to_dict('records')[0]
        else:
            sample = data[0] if data else {}
        
        columns = []
        for col_name, value in sample.items():
            if isinstance(value, bool):
                col_type = "UInt8"
            elif isinstance(value, int):
                col_type = "Int64"
            elif isinstance(value, float):
                col_type = "Float64"
            elif isinstance(value, datetime):
                col_type = "DateTime"
            elif isinstance(value, str):
                col_type = "String"
            else:
                col_type = "String"
            
            columns.append(f"{col_name} {col_type}")
        
        return ', '.join(columns)
    
    def _build_clickhouse_columns(self, schema: Dict[str, str]) -> str:
        """Build ClickHouse columns definition"""
        columns = []
        for col_name, col_type in schema.items():
            columns.append(f"{col_name} {col_type}")
        return ', '.join(columns)

class DataLakeGen2SinkOperator(BaseSinkOperator):
    """Load data into Azure Data Lake Gen2"""
    
    def load_data(self, data: Union[List[Dict], pd.DataFrame], context) -> int:
        """Load data into Data Lake Gen2"""
        if isinstance(data, pd.DataFrame):
            df = data
        else:
            df = pd.DataFrame(data) if data else pd.DataFrame()
        
        if df.empty:
            return 0
        
        connection = BaseHook.get_connection(self.sink_config.connection_id)
        
        blob_service_client = BlobServiceClient(
            account_url=f"https://{connection.host}.blob.core.windows.net",
            credential=connection.password
        )
        
        try:
            # Format file path with execution context
            execution_date = context['execution_date']
            actual_file_path = self.sink_config.file_path.format(
                year=execution_date.year,
                month=execution_date.month,
                day=execution_date.day,
                hour=execution_date.hour,
                ds=context['ds'],
                ts_nodash=context['ts_nodash']
            )
            
            container_name = self.sink_config.custom_config.get('container')
            
            # Handle partitioning
            if self.sink_config.partition_columns:
                return self._write_partitioned_data(blob_service_client, container_name, actual_file_path, df)
            else:
                return self._write_single_file(blob_service_client, container_name, actual_file_path, df)
        
        except Exception as e:
            logger.error(f"Failed to write to Data Lake: {str(e)}")
            raise
    
    def _write_single_file(self, blob_service_client, container_name: str, file_path: str, df: pd.DataFrame) -> int:
        """Write data to a single file"""
        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=file_path
        )
        
        # Convert data to appropriate format
        if self.sink_config.file_format == 'json':
            content = df.to_json(orient='records', indent=2)
        elif self.sink_config.file_format == 'csv':
            content = df.to_csv(index=False)
        elif self.sink_config.file_format == 'parquet':
            import io
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            content = buffer.getvalue()
        else:
            raise ValueError(f"Unsupported file format: {self.sink_config.file_format}")
        
        # Handle write mode
        overwrite = self.sink_config.write_mode == WriteMode.OVERWRITE
        
        blob_client.upload_blob(content, overwrite=overwrite)
        return len(df)
    
    def _write_partitioned_data(self, blob_service_client, container_name: str, base_path: str, df: pd.DataFrame) -> int:
        """Write data with partitioning"""
        total_records = 0
        
        # Group by partition columns
        for partition_values, group_df in df.groupby(self.sink_config.partition_columns):
            if not isinstance(partition_values, tuple):
                partition_values = (partition_values,)
            
            # Build partition path
            partition_parts = []
            for i, col in enumerate(self.sink_config.partition_columns):
                partition_parts.append(f"{col}={partition_values[i]}")
            
            partition_path = f"{base_path}/{'/'.join(partition_parts)}/data.{self.sink_config.file_format}"
            
            total_records += self._write_single_file(blob_service_client, container_name, partition_path, group_df)
        
        return total_records

class SFTPSinkOperator(BaseSinkOperator):
    """Load data to SFTP server"""
    
    def load_data(self, data: Union[List[Dict], pd.DataFrame], context) -> int:
        """Load data to SFTP server"""
        if isinstance(data, pd.DataFrame):
            df = data
        else:
            df = pd.DataFrame(data) if data else pd.DataFrame()
        
        if df.empty:
            return 0
        
        connection = BaseHook.get_connection(self.sink_config.connection_id)
        
        # SFTP connection options
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None  # Disable host key checking for demo
        
        try:
            with pysftp.Connection(
                host=connection.host,
                port=connection.port or 22,
                username=connection.login,
                password=connection.password,
                cnopts=cnopts
            ) as sftp:
                
                # Format file path
                execution_date = context['execution_date']
                actual_file_path = self.sink_config.file_path.format(
                    year=execution_date.year,
                    month=execution_date.month,
                    day=execution_date.day,
                    ds=context['ds']
                )
                
                # Create directory if needed
                directory = str(Path(actual_file_path).parent)
                sftp.makedirs(directory)
                
                # Convert data to appropriate format
                import io
                if self.sink_config.file_format == 'json':
                    content = df.to_json(orient='records', indent=2)
                    file_buffer = io.StringIO(content)
                elif self.sink_config.file_format == 'csv':
                    content = df.to_csv(index=False)
                    file_buffer = io.StringIO(content)
                else:
                    raise ValueError(f"Unsupported file format: {self.sink_config.file_format}")
                
                # Upload file
                sftp.putfo(file_buffer, actual_file_path)
                return len(df)
                
        except Exception as e:
            logger.error(f"SFTP upload failed: {str(e)}")
            raise

class RestAPISinkOperator(BaseSinkOperator):
    """Load data to REST API endpoint"""
    
    def load_data(self, data: Union[List[Dict], pd.DataFrame], context) -> int:
        """Load data to REST API"""
        if isinstance(data, pd.DataFrame):
            data = data.to_dict('records')
        
        if not data:
            return 0
        
        # Build headers
        headers = {'Content-Type': 'application/json'}
        if self.sink_config.custom_config and 'headers' in self.sink_config.custom_config:
            headers.update(self.sink_config.custom_config['headers'])
        
        # Get endpoint from sink config
        endpoint = self.sink_config.custom_config.get('endpoint')
        method = self.sink_config.custom_config.get('method', 'POST')
        
        try:
            batch_size = self.sink_config.batch_size or len(data)
            total_sent = 0
            
            # Send data in batches
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                
                if method.upper() == 'POST':
                    response = requests.post(endpoint, json=batch, headers=headers, timeout=300)
                elif method.upper() == 'PUT':
                    response = requests.put(endpoint, json=batch, headers=headers, timeout=300)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
                
                response.raise_for_status()
                total_sent += len(batch)
            
            return total_sent
            
        except requests.exceptions.RequestException as e:
            logger.error(f"REST API upload failed: {str(e)}")
            raise

# Sink operator factory
SINK_OPERATORS = {
    SinkType.POSTGRESQL: PostgreSQLSinkOperator,
    SinkType.MONGODB: MongoDBSinkOperator,
    SinkType.CLICKHOUSE: ClickHouseSinkOperator,
    SinkType.DATALAKE_GEN2: DataLakeGen2SinkOperator,
    SinkType.SFTP: SFTPSinkOperator,
    SinkType.REST_API: RestAPISinkOperator,
}

def create_sink_operator(task_id: str, sink_config: SinkConfig, data_source_task_id: str, dag, **kwargs) -> BaseSinkOperator:
    """Factory function to create appropriate sink operator"""
    
    operator_class = SINK_OPERATORS.get(sink_config.type)
    
    if not operator_class:
        raise ValueError(f"Unsupported sink type: {sink_config.type}")
    
    return operator_class(
        task_id=task_id,
        sink_config=sink_config,
        data_source_task_id=data_source_task_id,
        dag=dag,
        **kwargs
    )