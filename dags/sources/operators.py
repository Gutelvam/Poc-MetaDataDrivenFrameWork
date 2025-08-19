"""
Source Operators Module - Airflow 3.x Compatible (without pysftp)
Handles data extraction from various data sources
"""

import json
import pandas as pd
import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union
import boto3
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine
import psycopg2
from psycopg2.extras import RealDictCursor
import paramiko
from pymongo import MongoClient

from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook

from core.config import SourceConfig, SourceType

logger = logging.getLogger(__name__)

class BaseSourceOperator(BaseOperator):
    """Base class for all source operators - Airflow 3.x compatible"""
    
    def __init__(self, source_config: SourceConfig, **kwargs):
        super().__init__(**kwargs)
        self.source_config = source_config
    
    def execute(self, context):
        """Execute data extraction"""
        try:
            logger.info(f"Starting data extraction from {self.source_config.name}")
            data = self.extract_data(context)
            logger.info(f"Successfully extracted {len(data) if isinstance(data, list) else 'data'} records from {self.source_config.name}")
            return data
        except Exception as e:
            logger.error(f"Failed to extract data from {self.source_config.name}: {str(e)}")
            raise
    
    def extract_data(self, context) -> Union[List[Dict], pd.DataFrame, Any]:
        """Abstract method to be implemented by concrete operators"""
        raise NotImplementedError("Subclasses must implement extract_data method")

class PostgreSQLSourceOperator(BaseSourceOperator):
    """Extract data from PostgreSQL database"""
    
    def extract_data(self, context) -> List[Dict]:
        connection = BaseHook.get_connection(self.source_config.connection_id)
        
        try:
            # Connect using psycopg2 for better control
            conn = psycopg2.connect(
                host=connection.host,
                port=connection.port,
                database=connection.schema,
                user=connection.login,
                password=connection.password
            )
            
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Build query
            if self.source_config.query:
                query = self.source_config.query
            elif self.source_config.table_name:
                schema_prefix = f"{self.source_config.schema_name}." if self.source_config.schema_name else ""
                query = f"SELECT * FROM {schema_prefix}{self.source_config.table_name}"
            else:
                raise ValueError("Either query or table_name must be provided")
            
            # Execute query with context variables
            cursor.execute(query)
            results = cursor.fetchall()
            
            # Convert to list of dictionaries
            data = [dict(row) for row in results]
            return data
            
        finally:
            if 'conn' in locals():
                conn.close()

class MongoDBSourceOperator(BaseSourceOperator):
    """Extract data from MongoDB"""
    
    def extract_data(self, context) -> List[Dict]:
        connection = BaseHook.get_connection(self.source_config.connection_id)
        
        # Build MongoDB URI
        if connection.password:
            uri = f"mongodb://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}"
        else:
            uri = f"mongodb://{connection.host}:{connection.port}/{connection.schema}"
        
        client = MongoClient(uri)
        
        try:
            db = client[connection.schema or self.source_config.schema_name]
            collection = db[self.source_config.collection_name]
            
            # Build filter
            if self.source_config.filter_condition:
                filter_condition = self.source_config.filter_condition
            else:
                filter_condition = {}
            
            # Execute query
            cursor = collection.find(filter_condition)
            
            # Convert ObjectId to string for JSON serialization
            documents = []
            for doc in cursor:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
                documents.append(doc)
            
            return documents
            
        finally:
            client.close()

class ClickHouseSourceOperator(BaseSourceOperator):
    """Extract data from ClickHouse using clickhouse-sqlalchemy"""
    
    def extract_data(self, context) -> List[Dict]:
        connection = BaseHook.get_connection(self.source_config.connection_id)
        
        # Build the SQLAlchemy connection string for ClickHouse
        # The syntax is 'clickhouse://user:password@host:port/database'
        conn_string = f"clickhouse://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema or 'default'}"
        engine = create_engine(conn_string)
        
        try:
            # Build query
            if self.source_config.query:
                query = self.source_config.query
            elif self.source_config.table_name:
                query = f"SELECT * FROM {self.source_config.table_name}"
            else:
                raise ValueError("Either query or table_name must be provided")
            
            # Execute query and read into a pandas DataFrame
            with engine.connect() as conn:
                df = pd.read_sql_query(query, conn)
            
            return df.to_dict('records')
            
        finally:
            # The engine and its connections are managed by SQLAlchemy
            pass

class PgVectorSourceOperator(BaseSourceOperator):
    """Extract data from PostgreSQL with pgvector support"""
    
    def extract_data(self, context) -> List[Dict]:
        connection = BaseHook.get_connection(self.source_config.connection_id)
        
        try:
            conn = psycopg2.connect(
                host=connection.host,
                port=connection.port,
                database=connection.schema,
                user=connection.login,
                password=connection.password
            )
            
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Build query with vector operations
            if self.source_config.query:
                query = self.source_config.query
            else:
                raise ValueError("PgVector source requires a custom query")
            
            # Execute query
            cursor.execute(query)
            results = cursor.fetchall()
            
            # Convert to list of dictionaries
            data = []
            for row in results:
                row_dict = dict(row)
                # Handle vector columns (convert to list for JSON serialization)
                for key, value in row_dict.items():
                    if hasattr(value, '__iter__') and not isinstance(value, (str, bytes)):
                        try:
                            row_dict[key] = list(value)
                        except:
                            pass
                data.append(row_dict)
            
            return data
            
        finally:
            if 'conn' in locals():
                conn.close()

class XAPISourceOperator(BaseSourceOperator):
    """Extract data from xAPI (Experience API) Learning Record Store"""
    
    def extract_data(self, context) -> List[Dict]:
        # Get xAPI configuration
        auth_token = self.source_config.custom_config.get('auth_token')
        batch_size = self.source_config.custom_config.get('batch_size', 1000)
        
        headers = {
            'Authorization': f'Bearer {auth_token}',
            'Content-Type': 'application/json',
            'X-Experience-API-Version': '1.0.3'
        }
        
        # Build query parameters
        params = self.source_config.params.copy() if self.source_config.params else {}
        params['limit'] = batch_size
        
        # Add date filtering if configured
        if self.source_config.custom_config.get('date_filter'):
            execution_date = context['execution_date']
            if self.source_config.custom_config['date_filter'] == 'daily':
                params['since'] = execution_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                params['until'] = (execution_date + timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        all_statements = []
        
        try:
            while True:
                response = requests.get(
                    f"{self.source_config.endpoint}/statements",
                    headers=headers,
                    params=params,
                    timeout=300
                )
                response.raise_for_status()
                
                data = response.json()
                statements = data.get('statements', [])
                
                if not statements:
                    break
                
                all_statements.extend(statements)
                
                # Check for more statements
                more_url = data.get('more')
                if not more_url:
                    break
                
                # Update params for next batch
                params = {'more': more_url}
            
            return all_statements
            
        except requests.exceptions.RequestException as e:
            logger.error(f"xAPI request failed: {str(e)}")
            raise

class DataLakeGen2SourceOperator(BaseSourceOperator):
    """Extract data from Azure Data Lake Gen2"""
    
    def extract_data(self, context) -> Union[List[Dict], Any]:
        connection = BaseHook.get_connection(self.source_config.connection_id)
        
        # Create blob service client
        blob_service_client = BlobServiceClient(
            account_url=f"https://{connection.host}.blob.core.windows.net",
            credential=connection.password
        )
        
        try:
            # Format file path with execution context
            execution_date = context['execution_date']
            actual_file_path = self.source_config.file_path.format(
                year=execution_date.year,
                month=execution_date.month,
                day=execution_date.day,
                hour=execution_date.hour,
                ds=context['ds'],
                ts=context['ts']
            )
            
            container_name = self.source_config.custom_config.get('container')
            
            # Handle multiple files with wildcard
            if '*' in actual_file_path:
                return self._read_multiple_files(blob_service_client, container_name, actual_file_path)
            else:
                return self._read_single_file(blob_service_client, container_name, actual_file_path)
            
        except Exception as e:
            logger.error(f"Failed to read from Data Lake: {str(e)}")
            raise
    
    def _read_single_file(self, blob_service_client, container_name: str, file_path: str):
        """Read a single file from Data Lake"""
        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=file_path
        )
        
        if not blob_client.exists():
            logger.warning(f"Blob does not exist: {file_path}")
            return []
        
        blob_content = blob_client.download_blob().readall()
        
        if self.source_config.file_format == 'json':
            return json.loads(blob_content.decode('utf-8'))
        elif self.source_config.file_format == 'csv':
            import io
            df = pd.read_csv(io.StringIO(blob_content.decode('utf-8')))
            return df.to_dict('records')
        elif self.source_config.file_format == 'parquet':
            import io
            df = pd.read_parquet(io.BytesIO(blob_content))
            return df.to_dict('records')
        else:
            return blob_content
    
    def _read_multiple_files(self, blob_service_client, container_name: str, file_pattern: str):
        """Read multiple files matching a pattern"""
        container_client = blob_service_client.get_container_client(container_name)
        
        # Get prefix from pattern
        prefix = file_pattern.split('*')[0]
        
        blobs = container_client.list_blobs(name_starts_with=prefix)
        all_data = []
        
        for blob in blobs:
            if self._matches_pattern(blob.name, file_pattern):
                data = self._read_single_file(blob_service_client, container_name, blob.name)
                if isinstance(data, list):
                    all_data.extend(data)
                else:
                    all_data.append(data)
        
        return all_data
    
    def _matches_pattern(self, filename: str, pattern: str) -> bool:
        """Simple pattern matching for wildcards"""
        import fnmatch
        return fnmatch.fnmatch(filename, pattern)

class SFTPSourceOperator(BaseSourceOperator):
    """Extract data from SFTP server using paramiko directly"""
    
    def extract_data(self, context) -> Union[List[Dict], bytes]:
        connection = BaseHook.get_connection(self.source_config.connection_id)
        
        try:
            # Create SSH client
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # Connect to SFTP server
            ssh.connect(
                hostname=connection.host,
                port=connection.port or 22,
                username=connection.login,
                password=connection.password
            )
            
            sftp = ssh.open_sftp()
            
            # Format file path with execution context
            execution_date = context['execution_date']
            actual_file_path = self.source_config.file_path.format(
                year=execution_date.year,
                month=execution_date.month,
                day=execution_date.day,
                ds=context['ds']
            )
            
            # Check if file exists
            try:
                sftp.stat(actual_file_path)
            except FileNotFoundError:
                logger.warning(f"File does not exist: {actual_file_path}")
                return []
            
            # Download file to memory
            import io
            file_content = io.BytesIO()
            sftp.getfo(actual_file_path, file_content)
            file_content.seek(0)
            
            # Parse based on format
            if self.source_config.file_format == 'json':
                return json.loads(file_content.read().decode('utf-8'))
            elif self.source_config.file_format == 'csv':
                df = pd.read_csv(file_content)
                return df.to_dict('records')
            else:
                return file_content.read()
                
        except Exception as e:
            logger.error(f"SFTP extraction failed: {str(e)}")
            raise
        finally:
            if 'sftp' in locals():
                sftp.close()
            if 'ssh' in locals():
                ssh.close()

class RestAPISourceOperator(BaseSourceOperator):
    """Extract data from REST API"""
    
    def extract_data(self, context) -> List[Dict]:
        # Build headers
        headers = self.source_config.headers.copy() if self.source_config.headers else {}
        headers.setdefault('Content-Type', 'application/json')
        
        # Build parameters
        params = self.source_config.params.copy() if self.source_config.params else {}
        
        # Add execution context to parameters if needed
        execution_date = context['execution_date']
        params.update({
            'execution_date': execution_date.isoformat(),
            'ds': context['ds']
        })
        
        try:
            response = requests.get(
                self.source_config.endpoint,
                headers=headers,
                params=params,
                timeout=300
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Handle different response structures
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                # Look for common data keys
                for key in ['data', 'results', 'items', 'records']:
                    if key in data and isinstance(data[key], list):
                        return data[key]
                # If no array found, return single item as list
                return [data]
            else:
                return [{'data': data}]
                
        except requests.exceptions.RequestException as e:
            logger.error(f"REST API request failed: {str(e)}")
            raise

class FileSourceOperator(BaseSourceOperator):
    """Extract data from local files"""
    
    def extract_data(self, context) -> List[Dict]:
        from pathlib import Path
        
        # Format file path with execution context
        execution_date = context['execution_date']
        actual_file_path = self.source_config.file_path.format(
            year=execution_date.year,
            month=execution_date.month,
            day=execution_date.day,
            ds=context['ds']
        )
        
        if not Path(actual_file_path).exists():
            logger.warning(f"File does not exist: {actual_file_path}")
            return []
        
        try:
            if self.source_config.file_format == 'json':
                with open(actual_file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data if isinstance(data, list) else [data]
            
            elif self.source_config.file_format == 'csv':
                df = pd.read_csv(actual_file_path)
                return df.to_dict('records')
            
            elif self.source_config.file_format == 'parquet':
                df = pd.read_parquet(actual_file_path)
                return df.to_dict('records')
            
            else:
                # Read as text
                with open(actual_file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    return [{'content': content, 'filename': actual_file_path}]
                    
        except Exception as e:
            logger.error(f"File extraction failed: {str(e)}")
            raise

# Source operator factory
SOURCE_OPERATORS = {
    SourceType.POSTGRESQL: PostgreSQLSourceOperator,
    SourceType.MONGODB: MongoDBSourceOperator,
    SourceType.CLICKHOUSE: ClickHouseSourceOperator,
    SourceType.PGVECTOR: PgVectorSourceOperator,
    SourceType.XAPI: XAPISourceOperator,
    SourceType.DATALAKE_GEN2: DataLakeGen2SourceOperator,
    SourceType.SFTP: SFTPSourceOperator,
    SourceType.REST_API: RestAPISourceOperator,
    SourceType.FILE: FileSourceOperator,
}

def create_source_operator(task_id: str, source_config: SourceConfig, dag, **kwargs) -> BaseSourceOperator:
    """Factory function to create appropriate source operator"""
    
    operator_class = SOURCE_OPERATORS.get(source_config.type)
    
    if not operator_class:
        raise ValueError(f"Unsupported source type: {source_config.type}")
    
    return operator_class(
        task_id=task_id,
        source_config=source_config,
        dag=dag,
        **kwargs
    )