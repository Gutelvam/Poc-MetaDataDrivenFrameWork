"""
Enhanced Load Operator - Supports both upstream data and direct source extraction
This makes LOAD operations much more powerful and flexible
"""

import logging
from typing import Dict, List, Any, Optional, Union

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

from core.config import SourceConfig, SinkConfig
from sources.operators import create_source_operator
from sinks.operators import create_sink_operator

logger = logging.getLogger(__name__)


class EnhancedLoadOperator(BaseOperator):
    """
    Enhanced Load Operator that supports both upstream data and direct source extraction
    
    Modes of Operation:
    1. Traditional: Get data from upstream task → Load to sink
    2. Enhanced: Extract data from source → Load to sink  
    3. Hybrid: Get data from upstream task → Load to sink (with source as backup)
    """
    
    def __init__(
        self,
        sink_config: SinkConfig,
        source_config: Optional[SourceConfig] = None,
        data_source_task_id: Optional[str] = None,
        prefer_upstream: bool = True,  # Prefer upstream data over source extraction
        **kwargs
    ):
        super().__init__(**kwargs)
        self.sink_config = sink_config
        self.source_config = source_config
        self.data_source_task_id = data_source_task_id
        self.prefer_upstream = prefer_upstream
        
        # Validate configuration
        if not source_config and not data_source_task_id:
            raise ValueError("Enhanced Load operator requires either source_config or data_source_task_id")
    
    def execute(self, context):
        """
        Execute enhanced load with flexible data sourcing
        """
        try:
            logger.info(f"🚀 Starting Enhanced Load: {self.task_id}")
            
            # Step 1: Get data using flexible sourcing
            data = self._get_data_flexible(context)
            
            if not data:
                logger.warning(f"No data obtained for load task {self.task_id}")
                return 0
            
            # Step 2: Load data to sink
            records_loaded = self._load_data_to_sink(data, context)
            
            logger.info(f"✅ Enhanced Load completed: {records_loaded} records loaded to {self.sink_config.name}")
            
            # For temp tables (used in ETL chains), return metadata for downstream tasks
            # For final tables, can return just the count
            table_name = self.sink_config.table_name
            is_temp_table = 'temp_' in table_name.lower()
            
            if is_temp_table:
                # Return metadata for downstream ETL tasks
                return {
                    'records_loaded': records_loaded,
                    'sink_table': f"{self.sink_config.schema_name}.{self.sink_config.table_name}" if self.sink_config.schema_name else self.sink_config.table_name,
                    'sink_connection': self.sink_config.connection_id,
                    'total_records': len(data),
                    'task_type': 'extract_with_sink',
                    'is_temp_table': True
                }
            else:
                # Final table - just return count
                return records_loaded
            
        except Exception as e:
            logger.error(f"❌ Enhanced Load failed for {self.task_id}: {str(e)}")
            raise AirflowException(f"Enhanced Load operation failed: {str(e)}")
    
    def _get_data_flexible(self, context) -> List[Dict[str, Any]]:
        """
        Flexible data sourcing with multiple fallback options
        """
        data = None
        data_source = None
        
        try:
            # Option 1: Try upstream task data (if preferred and available)
            if self.prefer_upstream and self.data_source_task_id:
                logger.info(f"📥 Attempting to get data from upstream task: {self.data_source_task_id}")
                data = self._get_upstream_data(context)
                if data:
                    data_source = f"upstream_task:{self.data_source_task_id}"
                    logger.info(f"✅ Got {len(data)} records from upstream task")
                else:
                    logger.info("⚠️  No data from upstream task, trying source extraction...")
            
            # Option 2: Extract from source (if upstream failed or not preferred)
            if not data and self.source_config:
                logger.info(f"📤 Extracting data from source: {self.source_config.name}")
                data = self._extract_from_source(context)
                if data:
                    data_source = f"source:{self.source_config.name}"
                    logger.info(f"✅ Extracted {len(data)} records from source")
            
            # Option 3: Fallback - try the other method if first one failed
            if not data:
                if not self.prefer_upstream and self.data_source_task_id:
                    logger.info("🔄 Fallback: Trying upstream task data...")
                    data = self._get_upstream_data(context)
                    if data:
                        data_source = f"fallback_upstream:{self.data_source_task_id}"
                elif self.prefer_upstream and self.source_config:
                    logger.info("🔄 Fallback: Trying source extraction...")  
                    data = self._extract_from_source(context)
                    if data:
                        data_source = f"fallback_source:{self.source_config.name}"
            
            if data:
                logger.info(f"📊 Data sourced from: {data_source}")
                # Add metadata about data source to context for monitoring
                context['data_source_info'] = {
                    'source': data_source,
                    'record_count': len(data),
                    'task_id': self.task_id
                }
            else:
                logger.warning("⚠️  No data available from any source")
            
            return data or []
            
        except Exception as e:
            logger.error(f"❌ Failed to get data: {str(e)}")
            raise
    
    def _get_upstream_data(self, context) -> Optional[List[Dict[str, Any]]]:
        """Get data from upstream task via XCom"""
        try:
            if not self.data_source_task_id:
                return None
            
            data = context['task_instance'].xcom_pull(task_ids=self.data_source_task_id)
            
            if data is None:
                logger.info(f"No XCom data found for task: {self.data_source_task_id}")
                return None
            
            # Ensure data is in list format
            if isinstance(data, dict):
                return [data]
            elif isinstance(data, list):
                return data
            else:
                logger.warning(f"Unexpected data type from upstream: {type(data)}")
                return [{'data': data}]
                
        except Exception as e:
            logger.warning(f"Failed to get upstream data from {self.data_source_task_id}: {str(e)}")
            return None
    
    def _extract_from_source(self, context) -> Optional[List[Dict[str, Any]]]:
        """Extract data directly from source"""
        try:
            if not self.source_config:
                return None
            
            # Create a temporary source operator for data extraction
            temp_source_operator = create_source_operator(
                task_id=f"temp_extract_{self.task_id}",
                source_config=self.source_config,
                dag=context['dag']
            )
            
            # Execute the source extraction
            extracted_data = temp_source_operator.execute(context)
            
            if isinstance(extracted_data, list):
                return extracted_data
            elif extracted_data is not None:
                return [extracted_data]
            else:
                return None
                
        except Exception as e:
            logger.warning(f"Failed to extract from source {self.source_config.name}: {str(e)}")
            return None
    
    def _load_data_to_sink(self, data: List[Dict[str, Any]], context) -> int:
        """Load data to the configured sink"""
        try:
            # Create sink operator
            sink_operator = create_sink_operator(
                task_id=f"temp_sink_{self.task_id}",
                sink_config=self.sink_config,
                data_source_task_id=None,  # We'll pass data directly
                dag=context['dag']
            )
            
            # Pass data directly to sink operator through context
            temp_context = context.copy()
            temp_context['data'] = data
            
            # Execute sink operation
            records_loaded = sink_operator.execute(temp_context)
            
            return records_loaded if isinstance(records_loaded, int) else len(data)
            
        except Exception as e:
            logger.error(f"Failed to load data to sink {self.sink_config.name}: {str(e)}")
            raise


def create_enhanced_load_operator(
    task_id: str,
    sink_config: SinkConfig,
    source_config: Optional[SourceConfig] = None,
    data_source_task_id: Optional[str] = None,
    prefer_upstream: bool = True,
    dag=None,
    **kwargs
) -> EnhancedLoadOperator:
    """
    Factory function to create Enhanced Load Operator
    
    Args:
        task_id: Task identifier
        sink_config: Sink configuration (required)
        source_config: Source configuration (optional)
        data_source_task_id: Upstream task ID (optional)
        prefer_upstream: Whether to prefer upstream data over source extraction
        dag: DAG reference
        **kwargs: Additional operator parameters
        
    Returns:
        EnhancedLoadOperator instance
    """
    
    return EnhancedLoadOperator(
        task_id=task_id,
        sink_config=sink_config,
        source_config=source_config,
        data_source_task_id=data_source_task_id,
        prefer_upstream=prefer_upstream,
        dag=dag,
        **kwargs
    )


class SmartLoadOperator(EnhancedLoadOperator):
    """
    Smart Load Operator with intelligent data source selection
    Automatically determines the best data source based on availability and performance
    """
    
    def __init__(self, **kwargs):
        # Smart defaults
        kwargs.setdefault('prefer_upstream', True)
        super().__init__(**kwargs)
    
    def _get_data_flexible(self, context) -> List[Dict[str, Any]]:
        """
        Smart data sourcing with performance considerations
        """
        # Check if upstream task completed successfully
        if self.data_source_task_id:
            try:
                upstream_task_state = context['dag_run'].get_task_instance(self.data_source_task_id).state
                if upstream_task_state == 'success':
                    logger.info("🎯 Smart choice: Using upstream data (task completed successfully)")
                    self.prefer_upstream = True
                else:
                    logger.info("🎯 Smart choice: Using source extraction (upstream task not successful)")
                    self.prefer_upstream = False
            except Exception:
                logger.info("🎯 Smart choice: Defaulting to source extraction (cannot check upstream state)")
                self.prefer_upstream = False
        
        # Use parent's flexible data sourcing
        return super()._get_data_flexible(context)


def create_smart_load_operator(
    task_id: str,
    sink_config: SinkConfig,
    source_config: Optional[SourceConfig] = None,
    data_source_task_id: Optional[str] = None,
    dag=None,
    **kwargs
) -> SmartLoadOperator:
    """
    Factory function to create Smart Load Operator with intelligent source selection
    """
    
    return SmartLoadOperator(
        task_id=task_id,
        sink_config=sink_config,
        source_config=source_config,
        data_source_task_id=data_source_task_id,
        dag=dag,
        **kwargs
    )