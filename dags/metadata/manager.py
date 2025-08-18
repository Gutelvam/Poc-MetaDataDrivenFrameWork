"""
Metadata Manager Module
Handles loading, validation, and management of pipeline configurations
"""

import yaml
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

from core.config import (
    PipelineConfig, TaskConfig, SourceConfig, SinkConfig, DataQualityRule,
    SourceType, SinkType, WriteMode, OperatorType, ConfigValidator,
    convert_enum_to_str, convert_str_to_enum
)

logger = logging.getLogger(__name__)

class MetadataManager:
    """Manages pipeline metadata and configurations"""
    
    def __init__(self, metadata_path: str = "/opt/airflow/metadata"):
        self.metadata_path = Path(metadata_path)
        self.metadata_path.mkdir(parents=True, exist_ok=True)
        self.validator = ConfigValidator()
        
        # Enum mappings for YAML parsing
        self.enum_mappings = {
            'type': SourceType,
            'sink_type': SinkType,
            'write_mode': WriteMode,
            'operator_type': OperatorType
        }
    
    def load_pipeline_config(self, config_file: str) -> PipelineConfig:
        """Load and validate pipeline configuration from YAML file"""
        config_path = self.metadata_path / config_file
        
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            raw_config = yaml.safe_load(f)
        
        # Parse and validate configuration
        try:
            pipeline_config = self._parse_pipeline_config(raw_config)
            
            # Validate configuration
            errors = self.validator.validate_pipeline_config(pipeline_config)
            if errors:
                raise ValueError(f"Configuration validation failed: {', '.join(errors)}")
            
            logger.info(f"Successfully loaded pipeline configuration: {pipeline_config.dag_id}")
            return pipeline_config
            
        except Exception as e:
            logger.error(f"Failed to load configuration from {config_file}: {str(e)}")
            raise
    
    def _parse_pipeline_config(self, raw_config: Dict[str, Any]) -> PipelineConfig:
        """Parse raw configuration dictionary into PipelineConfig object"""
        
        # Parse tasks
        tasks = []
        for task_data in raw_config.get('tasks', []):
            task = self._parse_task_config(task_data)
            tasks.append(task)
        
        # Parse global quality rules
        global_quality_rules = []
        for rule_data in raw_config.get('global_quality_rules', []):
            rule = self._parse_quality_rule(rule_data)
            global_quality_rules.append(rule)
        
        # Create pipeline config
        pipeline_config = PipelineConfig(
            dag_id=raw_config['dag_id'],
            description=raw_config['description'],
            pipeline_type=raw_config['pipeline_type'],
            schedule_interval=raw_config.get('schedule_interval'),
            start_date=raw_config.get('start_date', '2024-01-01'),
            catchup=raw_config.get('catchup', False),
            max_active_runs=raw_config.get('max_active_runs', 1),
            tags=raw_config.get('tags', []),
            owner=raw_config.get('owner', 'data-platform'),
            tasks=tasks,
            dependencies=raw_config.get('dependencies', []),
            sla=raw_config.get('sla'),
            retries=raw_config.get('retries', 1),
            retry_delay=raw_config.get('retry_delay', 300),
            trigger_config=raw_config.get('trigger_config'),
            allow_reprocessing=raw_config.get('allow_reprocessing', True),
            reprocess_lookback_days=raw_config.get('reprocess_lookback_days', 30),
            global_quality_rules=global_quality_rules if global_quality_rules else None
        )
        
        return pipeline_config
    
    def _parse_task_config(self, task_data: Dict[str, Any]) -> TaskConfig:
        """Parse task configuration"""
        
        # Parse source configuration
        source = None
        if 'source' in task_data:
            source = self._parse_source_config(task_data['source'])
        
        # Parse sink configuration
        sink = None
        if 'sink' in task_data:
            sink = self._parse_sink_config(task_data['sink'])
        
        # Parse quality rules
        quality_rules = []
        for rule_data in task_data.get('quality_rules', []):
            rule = self._parse_quality_rule(rule_data)
            quality_rules.append(rule)
        
        # Parse operator type
        operator_type_str = task_data.get('operator_type', 'custom')
        operator_type = OperatorType(operator_type_str)
        
        task_config = TaskConfig(
            task_id=task_data['task_id'],
            operator_type=operator_type,
            description=task_data.get('description'),
            source=source,
            sink=sink,
            depends_on=task_data.get('depends_on'),
            retries=task_data.get('retries', 3),
            retry_delay=task_data.get('retry_delay', 300),
            timeout=task_data.get('timeout', 3600),
            trigger_rule=task_data.get('trigger_rule', 'all_success'),
            custom_script=task_data.get('custom_script'),
            custom_function=task_data.get('custom_function'),
            custom_params=task_data.get('custom_params'),
            quality_rules=quality_rules if quality_rules else None,
            sql_transform=task_data.get('sql_transform'),
            python_transform=task_data.get('python_transform')
        )
        
        return task_config
    
    def _parse_source_config(self, source_data: Dict[str, Any]) -> SourceConfig:
        """Parse source configuration"""
        
        source_type = SourceType(source_data['type'])
        
        source_config = SourceConfig(
            name=source_data['name'],
            type=source_type,
            connection_id=source_data['connection_id'],
            schema_name=source_data.get('schema_name'),
            table_name=source_data.get('table_name'),
            collection_name=source_data.get('collection_name'),
            query=source_data.get('query'),
            filter_condition=source_data.get('filter_condition'),
            file_path=source_data.get('file_path'),
            file_format=source_data.get('file_format'),
            endpoint=source_data.get('endpoint'),
            headers=source_data.get('headers'),
            params=source_data.get('params'),
            custom_config=source_data.get('custom_config')
        )
        
        # Validate source configuration
        errors = self.validator.validate_source_config(source_config)
        if errors:
            raise ValueError(f"Source validation failed for {source_config.name}: {', '.join(errors)}")
        
        return source_config
    
    def _parse_sink_config(self, sink_data: Dict[str, Any]) -> SinkConfig:
        """Parse sink configuration"""
        
        sink_type = SinkType(sink_data['type'])
        write_mode = WriteMode(sink_data.get('write_mode', 'append'))
        
        sink_config = SinkConfig(
            name=sink_data['name'],
            type=sink_type,
            connection_id=sink_data['connection_id'],
            write_mode=write_mode,
            schema_name=sink_data.get('schema_name'),
            table_name=sink_data.get('table_name'),
            collection_name=sink_data.get('collection_name'),
            auto_create_table=sink_data.get('auto_create_table', True),
            table_schema=sink_data.get('table_schema'),
            upsert_keys=sink_data.get('upsert_keys'),
            file_path=sink_data.get('file_path'),
            file_format=sink_data.get('file_format'),
            partition_columns=sink_data.get('partition_columns'),
            batch_size=sink_data.get('batch_size'),
            custom_config=sink_data.get('custom_config')
        )
        
        # Validate sink configuration
        errors = self.validator.validate_sink_config(sink_config)
        if errors:
            raise ValueError(f"Sink validation failed for {sink_config.name}: {', '.join(errors)}")
        
        return sink_config
    
    def _parse_quality_rule(self, rule_data: Dict[str, Any]) -> DataQualityRule:
        """Parse data quality rule configuration"""
        
        return DataQualityRule(
            name=rule_data['name'],
            rule_type=rule_data['rule_type'],
            column=rule_data.get('column'),
            parameters=rule_data.get('parameters'),
            threshold=rule_data.get('threshold', 1.0),
            severity=rule_data.get('severity', 'warning')
        )
    
    def save_pipeline_metadata(self, pipeline_config: PipelineConfig):
        """Save pipeline metadata for tracking and auditing"""
        metadata_file = self.metadata_path / f"{pipeline_config.dag_id}_metadata.json"
        
        # Convert config to dictionary with enum handling
        config_dict = self._config_to_dict(pipeline_config)
        
        metadata = {
            'dag_id': pipeline_config.dag_id,
            'created_at': datetime.now().isoformat(),
            'version': '1.0',
            'config': config_dict,
            'validation_status': 'valid',
            'last_updated': datetime.now().isoformat()
        }
        
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, default=str)
        
        logger.info(f"Saved metadata for pipeline: {pipeline_config.dag_id}")
    
    def _config_to_dict(self, config: PipelineConfig) -> Dict[str, Any]:
        """Convert pipeline config to dictionary with proper enum handling"""
        
        def convert_dataclass(obj):
            if hasattr(obj, '__dataclass_fields__'):
                result = {}
                for field_name, field_value in obj.__dict__.items():
                    if field_value is None:
                        continue
                    elif isinstance(field_value, list):
                        result[field_name] = [convert_dataclass(item) for item in field_value]
                    else:
                        result[field_name] = convert_dataclass(field_value)
                return result
            else:
                return convert_enum_to_str(obj)
        
        return convert_dataclass(config)
    
    def list_pipeline_configs(self) -> List[str]:
        """List all available pipeline configuration files"""
        yaml_files = list(self.metadata_path.glob("*.yaml"))
        yml_files = list(self.metadata_path.glob("*.yml"))
        
        config_files = [f.name for f in yaml_files + yml_files]
        return sorted(config_files)
    
    def get_pipeline_metadata(self, dag_id: str) -> Optional[Dict[str, Any]]:
        """Get saved metadata for a pipeline"""
        metadata_file = self.metadata_path / f"{dag_id}_metadata.json"
        
        if not metadata_file.exists():
            return None
        
        with open(metadata_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def validate_all_configs(self) -> Dict[str, List[str]]:
        """Validate all configuration files and return validation results"""
        results = {}
        
        for config_file in self.list_pipeline_configs():
            try:
                self.load_pipeline_config(config_file)
                results[config_file] = []  # No errors
            except Exception as e:
                results[config_file] = [str(e)]
        
        return results
    
    def export_config_template(self, template_type: str = "basic") -> str:
        """Export configuration template for different pipeline types"""
        
        if template_type == "basic":
            template = {
                'dag_id': 'my_pipeline',
                'description': 'My data pipeline',
                'pipeline_type': 'batch',
                'schedule_interval': '0 2 * * *',
                'start_date': '2024-01-01',
                'tags': ['example'],
                'tasks': [
                    {
                        'task_id': 'extract_data',
                        'operator_type': 'extract',
                        'source': {
                            'name': 'source_data',
                            'type': 'postgresql',
                            'connection_id': 'postgres_default',
                            'table_name': 'source_table',
                            'query': 'SELECT * FROM source_table WHERE date = \'{{ ds }}\''
                        }
                    },
                    {
                        'task_id': 'load_data',
                        'operator_type': 'load',
                        'sink': {
                            'name': 'target_data',
                            'type': 'postgresql',
                            'connection_id': 'postgres_default',
                            'table_name': 'target_table',
                            'write_mode': 'append',
                            'auto_create_table': True
                        },
                        'depends_on': ['extract_data']
                    }
                ]
            }
        else:
            raise ValueError(f"Unknown template type: {template_type}")
        
        return yaml.dump(template, default_flow_style=False, sort_keys=False)