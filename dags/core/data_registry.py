"""
Data Sink Registry - Manages task data sinks and provides unified data access
Replaces XCom-based data sharing with production-ready sink-based approach
"""

import json
import logging
import sqlite3
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, asdict
from enum import Enum
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from core.config import SinkConfig, SinkType, WriteMode

logger = logging.getLogger(__name__)


class TaskSinkType(Enum):
    """Types of automatic task sinks"""
    TEMP_DATABASE = "temp_database"      # Default SQLite temp database
    TEMP_POSTGRES = "temp_postgres"      # Temporary PostgreSQL schema
    TEMP_FILE = "temp_file"             # Temporary JSON/CSV files
    TEMP_DATALAKE = "temp_datalake"     # Azure Data Lake Gen2 blob storage
    CUSTOM = "custom"                   # User-defined sink
    NONE = "none"                       # No automatic sink (for procedures/API calls)


@dataclass
class TaskDataLocation:
    """Represents where a task's output data is stored"""
    task_id: str
    dag_id: str
    execution_date: str
    sink_type: TaskSinkType
    connection_details: Dict[str, Any]
    table_name: Optional[str] = None
    file_path: Optional[str] = None
    schema_name: Optional[str] = None
    created_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()


class DataSinkRegistry:
    """
    Central registry for managing task data sinks and providing unified data access
    Replaces XCom with production-ready data sharing
    """
    
    def __init__(self, default_sink_type: TaskSinkType = TaskSinkType.TEMP_DATABASE):
        self.default_sink_type = default_sink_type
        self._registry: Dict[str, TaskDataLocation] = {}
        self._temp_db_path = None
        self._initialize_temp_database()
    
    def _initialize_temp_database(self):
        """Initialize default SQLite temporary database"""
        temp_dir = Path("/tmp/airflow_data_sinks")
        temp_dir.mkdir(exist_ok=True)
        self._temp_db_path = temp_dir / "task_data.db"
        
        # Create registry table
        with sqlite3.connect(str(self._temp_db_path)) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS task_registry (
                    task_key TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    dag_id TEXT NOT NULL,
                    execution_date TEXT NOT NULL,
                    sink_type TEXT NOT NULL,
                    connection_details TEXT NOT NULL,
                    table_name TEXT,
                    file_path TEXT,
                    schema_name TEXT,
                    created_at TEXT NOT NULL
                )
            """)
            conn.commit()
    
    def get_task_sink_config(self, task_id: str, dag_id: str, 
                           execution_date: str, 
                           preferred_sink_type: Optional[TaskSinkType] = None) -> SinkConfig:
        """
        Generate automatic sink configuration for a task
        """
        sink_type = preferred_sink_type or self.default_sink_type
        
        if sink_type == TaskSinkType.TEMP_DATABASE:
            return SinkConfig(
                name=f"{task_id}_auto_sink",
                type=SinkType.POSTGRESQL,  # Will use SQLite internally
                connection_id="temp_database",
                table_name=self._get_task_table_name(task_id, dag_id, execution_date),
                write_mode=WriteMode.OVERWRITE,
                auto_create_table=True
            )
        
        elif sink_type == TaskSinkType.TEMP_POSTGRES:
            return SinkConfig(
                name=f"{task_id}_temp_postgres",
                type=SinkType.POSTGRESQL,
                connection_id="temp_postgres",
                schema_name="temp_data",
                table_name=self._get_task_table_name(task_id, dag_id, execution_date),
                write_mode=WriteMode.OVERWRITE,
                auto_create_table=True
            )
        
        elif sink_type == TaskSinkType.TEMP_FILE:
            file_path = f"/tmp/airflow_data_sinks/{dag_id}/{task_id}_{execution_date}.json"
            return SinkConfig(
                name=f"{task_id}_temp_file",
                type=SinkType.FILE,
                connection_id="local_file",
                file_path=file_path,
                file_format="json",
                write_mode=WriteMode.OVERWRITE
            )
        
        elif sink_type == TaskSinkType.TEMP_DATALAKE:
            # Azure Data Lake Gen2 temporary storage
            # Clean execution date for path
            clean_date = execution_date.replace(':', '').replace('-', '').replace('T', '_')[:15]
            file_path = f"temp-data/{dag_id}/{task_id}/{clean_date}/data.parquet"
            return SinkConfig(
                name=f"{task_id}_temp_datalake",
                type=SinkType.DATALAKE_GEN2,
                connection_id="azure_datalake_conn",
                file_path=file_path,
                file_format="parquet",  # More efficient than JSON for large datasets
                write_mode=WriteMode.OVERWRITE,
                custom_config={
                    "container": "airflow-temp-data"  # Dedicated container for temp data
                }
            )
        
        else:
            raise ValueError(f"Unsupported automatic sink type: {sink_type}")
    
    def register_task_data_location(self, task_id: str, dag_id: str, 
                                  execution_date: str, sink_config: SinkConfig,
                                  records_count: int = 0) -> TaskDataLocation:
        """
        Register where a task's data is stored after execution
        """
        # Create task data location
        location = TaskDataLocation(
            task_id=task_id,
            dag_id=dag_id,
            execution_date=execution_date,
            sink_type=self._sink_config_to_task_sink_type(sink_config),
            connection_details=self._extract_connection_details(sink_config),
            table_name=sink_config.table_name,
            file_path=sink_config.file_path,
            schema_name=sink_config.schema_name
        )
        
        # Store in memory registry
        task_key = self._get_task_key(task_id, dag_id, execution_date)
        self._registry[task_key] = location
        
        # Persist to database
        self._persist_location(location, records_count)
        
        logger.info(f"Registered data location for task {task_id}: {location.sink_type.value}")
        return location
    
    def get_task_data_location(self, task_id: str, dag_id: str, 
                             execution_date: str) -> Optional[TaskDataLocation]:
        """
        Get the data location for a specific task
        """
        task_key = self._get_task_key(task_id, dag_id, execution_date)
        
        # Check memory cache first
        if task_key in self._registry:
            return self._registry[task_key]
        
        # Load from database
        return self._load_location_from_db(task_key)
    
    def query_task_data(self, task_id: str, dag_id: str, execution_date: str,
                       query: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Query data from a task's sink. This replaces XCom pulls.
        
        Args:
            task_id: Source task ID
            dag_id: DAG ID  
            execution_date: Execution date
            query: Optional SQL query (defaults to SELECT * FROM task_table)
            
        Returns:
            List of dictionaries representing the data
        """
        location = self.get_task_data_location(task_id, dag_id, execution_date)
        if not location:
            logger.warning(f"No data location found for task {task_id}")
            return []
        
        if location.sink_type == TaskSinkType.TEMP_DATABASE:
            return self._query_sqlite_data(location, query)
        elif location.sink_type == TaskSinkType.TEMP_POSTGRES:
            return self._query_postgres_data(location, query)
        elif location.sink_type == TaskSinkType.TEMP_FILE:
            return self._query_file_data(location, query)
        elif location.sink_type == TaskSinkType.TEMP_DATALAKE:
            return self._query_datalake_data(location, query)
        else:
            raise ValueError(f"Cannot query data from sink type: {location.sink_type}")
    
    def _get_task_table_name(self, task_id: str, dag_id: str, execution_date: str) -> str:
        """Generate a unique table name for a task"""
        # Clean execution date for table name
        clean_date = execution_date.replace(':', '').replace('-', '').replace('T', '_')[:15]
        return f"{dag_id}_{task_id}_{clean_date}".lower().replace('-', '_')
    
    def _get_task_key(self, task_id: str, dag_id: str, execution_date: str) -> str:
        """Generate unique key for task data location"""
        return f"{dag_id}:{task_id}:{execution_date}"
    
    def _sink_config_to_task_sink_type(self, sink_config: SinkConfig) -> TaskSinkType:
        """Convert SinkConfig to TaskSinkType"""
        if sink_config.connection_id == "temp_database":
            return TaskSinkType.TEMP_DATABASE
        elif sink_config.connection_id == "temp_postgres":
            return TaskSinkType.TEMP_POSTGRES
        elif sink_config.type == SinkType.FILE:
            return TaskSinkType.TEMP_FILE
        elif sink_config.type == SinkType.DATALAKE_GEN2:
            return TaskSinkType.TEMP_DATALAKE
        else:
            return TaskSinkType.CUSTOM
    
    def _extract_connection_details(self, sink_config: SinkConfig) -> Dict[str, Any]:
        """Extract connection details from sink config"""
        return {
            "connection_id": sink_config.connection_id,
            "type": sink_config.type.value,
            "write_mode": sink_config.write_mode.value
        }
    
    def _persist_location(self, location: TaskDataLocation, records_count: int):
        """Persist location to registry database"""
        task_key = self._get_task_key(location.task_id, location.dag_id, location.execution_date)
        
        with sqlite3.connect(str(self._temp_db_path)) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO task_registry (
                    task_key, task_id, dag_id, execution_date, sink_type,
                    connection_details, table_name, file_path, schema_name, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                task_key, location.task_id, location.dag_id, location.execution_date,
                location.sink_type.value, json.dumps(location.connection_details),
                location.table_name, location.file_path, location.schema_name,
                location.created_at.isoformat()
            ))
            conn.commit()
    
    def _load_location_from_db(self, task_key: str) -> Optional[TaskDataLocation]:
        """Load location from registry database"""
        with sqlite3.connect(str(self._temp_db_path)) as conn:
            cursor = conn.execute("""
                SELECT * FROM task_registry WHERE task_key = ?
            """, (task_key,))
            
            row = cursor.fetchone()
            if not row:
                return None
            
            return TaskDataLocation(
                task_id=row[1],
                dag_id=row[2],
                execution_date=row[3],
                sink_type=TaskSinkType(row[4]),
                connection_details=json.loads(row[5]),
                table_name=row[6],
                file_path=row[7],
                schema_name=row[8],
                created_at=datetime.fromisoformat(row[9])
            )
    
    def _query_sqlite_data(self, location: TaskDataLocation, query: Optional[str]) -> List[Dict]:
        """Query data from SQLite temporary database"""
        if not query:
            query = f"SELECT * FROM {location.table_name}"
        
        with sqlite3.connect(str(self._temp_db_path)) as conn:
            conn.row_factory = sqlite3.Row  # Enable dict-like access
            cursor = conn.execute(query)
            return [dict(row) for row in cursor.fetchall()]
    
    def _query_postgres_data(self, location: TaskDataLocation, query: Optional[str]) -> List[Dict]:
        """Query data from PostgreSQL temporary schema"""
        if not query:
            full_table_name = f"{location.schema_name}.{location.table_name}"
            query = f"SELECT * FROM {full_table_name}"
        
        connection = BaseHook.get_connection(location.connection_details["connection_id"])
        
        try:
            conn = psycopg2.connect(
                host=connection.host,
                port=connection.port,
                database=connection.schema,
                user=connection.login,
                password=connection.password
            )
            
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(query)
            return [dict(row) for row in cursor.fetchall()]
            
        finally:
            if 'conn' in locals():
                conn.close()
    
    def _query_file_data(self, location: TaskDataLocation, query: Optional[str]) -> List[Dict]:
        """Query data from temporary file (basic filtering support)"""
        file_path = Path(location.file_path)
        
        if not file_path.exists():
            logger.warning(f"Task data file not found: {location.file_path}")
            return []
        
        # Load data from file
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        if not isinstance(data, list):
            data = [data]
        
        # Basic query support (would need more sophisticated parser for full SQL)
        if query and "WHERE" in query.upper():
            logger.warning("File-based querying with WHERE clauses not fully implemented")
            # TODO: Implement basic filtering
        
        return data
    
    def _query_datalake_data(self, location: TaskDataLocation, query: Optional[str]) -> List[Dict]:
        """Query data from Azure Data Lake Gen2 temporary storage"""
        try:
            from azure.storage.blob import BlobServiceClient
            import pandas as pd
            import io
            
            # Get connection details
            connection = BaseHook.get_connection(location.connection_details["connection_id"])
            
            # Create blob service client
            blob_service_client = BlobServiceClient(
                account_url=f"https://{connection.host}.blob.core.windows.net",
                credential=connection.password
            )
            
            # Get container and blob path
            container = location.connection_details.get("container", "airflow-temp-data")
            blob_path = location.file_path
            
            # Download blob
            blob_client = blob_service_client.get_blob_client(
                container=container,
                blob=blob_path
            )
            
            if not blob_client.exists():
                logger.warning(f"Blob does not exist: {blob_path}")
                return []
            
            blob_content = blob_client.download_blob().readall()
            
            # Parse based on file format
            if location.file_path.endswith('.parquet'):
                df = pd.read_parquet(io.BytesIO(blob_content))
            elif location.file_path.endswith('.json'):
                df = pd.read_json(io.StringIO(blob_content.decode('utf-8')))
            elif location.file_path.endswith('.csv'):
                df = pd.read_csv(io.StringIO(blob_content.decode('utf-8')))
            else:
                # Default to JSON
                data = json.loads(blob_content.decode('utf-8'))
                return data if isinstance(data, list) else [data]
            
            # Apply basic query filtering if provided
            if query and "WHERE" in query.upper():
                logger.warning("Complex querying on Data Lake data not fully implemented. Consider using temp_database or temp_postgres for complex SQL.")
                # TODO: Implement basic filtering or use DuckDB for SQL on parquet
            
            return df.to_dict('records')
            
        except Exception as e:
            logger.error(f"Failed to query Data Lake data: {str(e)}")
            raise
    
    def cleanup_old_data(self, days_to_keep: int = 7):
        """Clean up old task data to prevent storage bloat"""
        cutoff_date = datetime.utcnow() - pd.Timedelta(days=days_to_keep)
        
        with sqlite3.connect(str(self._temp_db_path)) as conn:
            # Get old records
            cursor = conn.execute("""
                SELECT task_key, table_name, file_path FROM task_registry 
                WHERE created_at < ?
            """, (cutoff_date.isoformat(),))
            
            old_records = cursor.fetchall()
            
            # Clean up data tables/files
            for task_key, table_name, file_path in old_records:
                try:
                    if table_name:
                        # Drop old table
                        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                    if file_path and Path(file_path).exists():
                        # Delete old file
                        Path(file_path).unlink()
                except Exception as e:
                    logger.warning(f"Failed to cleanup old data: {e}")
            
            # Remove registry entries
            conn.execute("DELETE FROM task_registry WHERE created_at < ?", (cutoff_date.isoformat(),))
            conn.commit()
            
            logger.info(f"Cleaned up {len(old_records)} old task data locations")


# Global registry instance
_global_registry: Optional[DataSinkRegistry] = None

def get_data_registry() -> DataSinkRegistry:
    """Get the global data sink registry instance"""
    global _global_registry
    if _global_registry is None:
        _global_registry = DataSinkRegistry()
    return _global_registry

def query_task_data(task_id: str, dag_id: str, execution_date: str, 
                   query: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Convenience function to query task data - replaces xcom_pull
    
    Usage:
        # Old way:
        data = context['task_instance'].xcom_pull(task_ids='birds')
        
        # New way:
        data = query_task_data('birds', dag_id, execution_date)
        # or with custom query:
        data = query_task_data('birds', dag_id, execution_date, 
                              "SELECT DISTINCT column1 FROM birds WHERE column2 > 100")
    """
    registry = get_data_registry()
    return registry.query_task_data(task_id, dag_id, execution_date, query)