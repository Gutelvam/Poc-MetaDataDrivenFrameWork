"""
Core Configuration and Data Models
Modular framework structure with separate concerns
"""

from dataclasses import dataclass, asdict
from typing import Dict, List, Any, Optional, Union
from enum import Enum
import yaml
import json
from pathlib import Path
from datetime import datetime

class SourceType(Enum):
    MONGODB = "mongodb"
    POSTGRESQL = "postgresql"
    PGVECTOR = "pgvector"
    CLICKHOUSE = "clickhouse"
    XAPI = "xapi"
    DATALAKE_GEN2 = "datalake_gen2"
    SFTP = "sftp"
    REST_API = "rest_api"
    FILE = "file"
    CUSTOM = "custom"

class SinkType(Enum):
    MONGODB = "mongodb"
    POSTGRESQL = "postgresql"
    PGVECTOR = "pgvector"
    CLICKHOUSE = "clickhouse"
    DATALAKE_GEN2 = "datalake_gen2"
    SFTP = "sftp"
    REST_API = "rest_api"
    FILE = "file"
    CUSTOM = "custom"

class WriteMode(Enum):
    OVERWRITE = "overwrite"
    APPEND = "append"
    UPSERT = "upsert"
    UPDATE = "update"
    INSERT_ONLY = "insert_only"

class OperatorType(Enum):
    EXTRACT = "extract"
    TRANSFORM = "transform"
    LOAD = "load"
    QUALITY_CHECK = "quality_check"
    CUSTOM = "custom"
    DUMMY = "dummy"
    SENSOR = "sensor"

@dataclass
class DataQualityRule:
    """Data quality rule configuration"""
    name: str
    rule_type: str  # not_null, unique, range, pattern, custom
    column: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    threshold: float = 1.0  # Percentage threshold for pass/fail
    severity: str = "warning"  # warning, error, critical

@dataclass
class SourceConfig:
    """Configuration for data sources"""
    name: str
    type: SourceType
    connection_id: str
    
    # Database/Collection specific
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    collection_name: Optional[str] = None
    
    # Query/Filter specific
    query: Optional[str] = None
    filter_condition: Optional[Dict[str, Any]] = None
    
    # File specific
    file_path: Optional[str] = None
    file_format: Optional[str] = None  # json, csv, parquet, avro
    
    # API specific
    endpoint: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    params: Optional[Dict[str, Any]] = None
    
    # Custom configuration
    custom_config: Optional[Dict[str, Any]] = None

@dataclass
class SinkConfig:
    """Configuration for data sinks"""
    name: str
    type: SinkType
    connection_id: str
    write_mode: WriteMode = WriteMode.APPEND
    
    # Database/Collection specific
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    collection_name: Optional[str] = None
    
    # Auto table creation
    auto_create_table: bool = True
    table_schema: Optional[Dict[str, str]] = None  # column_name: data_type
    
    # Upsert specific
    upsert_keys: Optional[List[str]] = None  # Keys for upsert operations
    
    # File specific
    file_path: Optional[str] = None
    file_format: Optional[str] = None
    partition_columns: Optional[List[str]] = None
    
    # Batch configuration
    batch_size: Optional[int] = None
    
    # Custom configuration
    custom_config: Optional[Dict[str, Any]] = None

@dataclass
class TaskConfig:
    """Configuration for individual tasks"""
    task_id: str
    operator_type: OperatorType
    description: Optional[str] = None
    
    # Source and Sink configurations
    source: Optional[SourceConfig] = None
    sink: Optional[SinkConfig] = None
    
    # Dependencies
    depends_on: Optional[List[str]] = None
    
    # Execution configuration
    retries: int = 3
    retry_delay: int = 300  # seconds
    timeout: int = 3600  # seconds
    trigger_rule: str = "all_success"
    
    # Custom processing
    custom_script: Optional[str] = None
    custom_function: Optional[str] = None
    custom_params: Optional[Dict[str, Any]] = None
    
    # Data quality
    quality_rules: Optional[List[DataQualityRule]] = None
    
    # Transform configuration
    sql_transform: Optional[str] = None
    python_transform: Optional[str] = None

@dataclass
class PipelineConfig:
    """Main pipeline configuration"""
    dag_id: str
    description: str
    pipeline_type: str  # batch, event_triggered, streaming
    
    # Scheduling
    schedule_interval: Optional[str] = None
    start_date: str = "2024-01-01"
    catchup: bool = False
    max_active_runs: int = 1
    
    # Metadata
    tags: Optional[List[str]] = None
    owner: str = "data-platform"
    
    # Tasks
    tasks: List[TaskConfig] = None
    
    # Dependencies
    dependencies: Optional[List[str]] = None  # Other DAGs this depends on
    
    # SLA and monitoring
    sla: Optional[int] = None  # SLA in minutes
    retries: int = 1
    retry_delay: int = 300
    
    # Event trigger specific
    trigger_config: Optional[Dict[str, Any]] = None
    
    # Reprocessing config
    allow_reprocessing: bool = True
    reprocess_lookback_days: int = 30
    
    # Data quality
    global_quality_rules: Optional[List[DataQualityRule]] = None

class ConfigValidator:
    """Validates pipeline configurations"""
    
    @staticmethod
    def validate_pipeline_config(config: PipelineConfig) -> List[str]:
        """Validate pipeline configuration and return list of errors"""
        errors = []
        
        # Basic validation
        if not config.dag_id:
            errors.append("dag_id is required")
        
        if not config.tasks:
            errors.append("At least one task is required")
        
        # Validate tasks
        task_ids = set()
        for task in config.tasks:
            if task.task_id in task_ids:
                errors.append(f"Duplicate task_id: {task.task_id}")
            task_ids.add(task.task_id)
            
            # Validate task dependencies
            if task.depends_on:
                for dep in task.depends_on:
                    if dep not in task_ids and dep not in [t.task_id for t in config.tasks]:
                        errors.append(f"Task {task.task_id} depends on non-existent task: {dep}")
            
            # Validate source/sink configuration
            if task.operator_type == OperatorType.EXTRACT and not task.source:
                errors.append(f"Extract task {task.task_id} must have source configuration")
            
            if task.operator_type == OperatorType.LOAD and not task.sink:
                errors.append(f"Load task {task.task_id} must have sink configuration")
        
        return errors
    
    @staticmethod
    def validate_source_config(source: SourceConfig) -> List[str]:
        """Validate source configuration"""
        errors = []
        
        if source.type in [SourceType.MONGODB] and not source.collection_name:
            errors.append(f"MongoDB source {source.name} requires collection_name")
        
        if source.type in [SourceType.POSTGRESQL, SourceType.CLICKHOUSE] and not (source.table_name or source.query):
            errors.append(f"Database source {source.name} requires table_name or query")
        
        if source.type == SourceType.FILE and not source.file_path:
            errors.append(f"File source {source.name} requires file_path")
        
        return errors
    
    @staticmethod
    def validate_sink_config(sink: SinkConfig) -> List[str]:
        """Validate sink configuration"""
        errors = []
        
        if sink.type in [SinkType.MONGODB] and not sink.collection_name:
            errors.append(f"MongoDB sink {sink.name} requires collection_name")
        
        if sink.type in [SinkType.POSTGRESQL, SinkType.CLICKHOUSE] and not sink.table_name:
            errors.append(f"Database sink {sink.name} requires table_name")
        
        if sink.write_mode == WriteMode.UPSERT and not sink.upsert_keys:
            errors.append(f"Upsert sink {sink.name} requires upsert_keys")
        
        return errors

def convert_enum_to_str(obj):
    """Convert enum values to strings for JSON serialization"""
    if isinstance(obj, Enum):
        return obj.value
    elif isinstance(obj, dict):
        return {k: convert_enum_to_str(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_enum_to_str(item) for item in obj]
    return obj

def convert_str_to_enum(data, enum_mappings):
    """Convert string values back to enums"""
    if isinstance(data, dict):
        result = {}
        for key, value in data.items():
            if key in enum_mappings and isinstance(value, str):
                result[key] = enum_mappings[key](value)
            else:
                result[key] = convert_str_to_enum(value, enum_mappings)
        return result
    elif isinstance(data, list):
        return [convert_str_to_enum(item, enum_mappings) for item in data]
    return data