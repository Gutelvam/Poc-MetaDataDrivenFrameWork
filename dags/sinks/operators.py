"""
Sink Operators Module - Airflow 3.x Compatible (without pysftp)
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
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Float, DateTime, Boolean, inspect
from pymongo import MongoClient
from azure.storage.blob import BlobServiceClient
import paramiko
import requests

from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook

from core.config import SinkConfig, SinkType, WriteMode

logger = logging.getLogger(__name__)

class BaseSinkOperator(BaseOperator):
    """Base class for all sink operators - Airflow 3.x compatible"""
    
    def __init__(self, sink_config: SinkConfig, data_source_task_id: str = None, **kwargs):
        super().__init__(**kwargs)
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
                # This method may fix duplicate keys and return modified data
                fixed_data = self.create_table_if_not_exists(data)
                if fixed_data is not None:
                    data = fixed_data  # Use the fixed data for loading
            
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
        """Create PostgreSQL table if it doesn't exist, or recreate if schema conflicts exist"""
        if not data:
            return
        
        # DEBUG: Check for duplicate columns before table creation
        if isinstance(data, list) and data:
            sample_keys = list(data[0].keys())
            logger.info(f"🔍 DEBUG: Sample data has {len(sample_keys)} keys")
            
            # Check for duplicates
            key_counts = {}
            duplicates = []
            for key in sample_keys:
                key_counts[key] = key_counts.get(key, 0) + 1
                if key_counts[key] > 1:
                    duplicates.append(key)
            
            if duplicates:
                logger.error(f"❌ DUPLICATE KEYS FOUND: {duplicates}")
                logger.error(f"❌ Key counts: {key_counts}")
                
                # Try to fix duplicates by modifying the data
                logger.info("🔧 Attempting to fix duplicates in data...")
                data = self._fix_duplicate_keys_in_data(data)
            else:
                logger.info("✅ No duplicate keys found in sample data")
        
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
            
            if table_exists:
                # Check if we need to recreate the table due to type conflicts
                if self._should_recreate_table_for_schema_evolution(cursor, full_table_name, data):
                    logger.warning(f"🔄 Recreating table {full_table_name} due to schema evolution conflicts")
                    cursor.execute(f"DROP TABLE IF EXISTS {full_table_name}")
                    conn.commit()
                    table_exists = False
            
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
                
                logger.info(f"🔍 CREATE TABLE SQL preview: {create_table_sql[:500]}...")
                logger.info(f"🔍 Full columns definition: {columns_def}")
                
                cursor.execute(create_table_sql)
                conn.commit()
                logger.info(f"Created table: {full_table_name}")
            
        finally:
            if 'conn' in locals():
                conn.close()
        
        # Return potentially fixed data
        return data
    
    def _should_recreate_table_for_schema_evolution(self, cursor, table_name: str, data: List[Dict]) -> bool:
        """
        Check if we should recreate the table due to schema evolution conflicts.
        Returns True if table has incompatible column types for the current data.
        """
        try:
            # Extract schema and table name
            if '.' in table_name:
                schema_name, actual_table_name = table_name.split('.', 1)
            else:
                schema_name = 'public'
                actual_table_name = table_name
            
            # Get current table schema
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
            """, (schema_name, actual_table_name))
            
            current_schema = {row[0]: row[1] for row in cursor.fetchall()}
            
            if not current_schema:
                return False  # Table doesn't exist
            
            # Check if any column has a type that conflicts with string data
            problematic_types = {'real', 'double precision', 'integer', 'bigint', 'numeric', 'decimal'}
            
            for col_name, col_type in current_schema.items():
                if col_type.lower() in problematic_types:
                    logger.warning(f"⚠️ Found potentially problematic column: {col_name} ({col_type})")
                    # For now, recreate if we find any numeric types since we're using TEXT for everything
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking table schema: {str(e)}")
            return False  # Don't recreate on error
    
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
    
    def _serialize_value(self, value):
        """Serialize complex values for PostgreSQL compatibility with improved type safety"""
        import json
        import uuid
        import decimal
        import math
        from datetime import datetime, date
        
        # Handle None values
        if value is None:
            return None
            
        # Handle pandas NaN values - convert to None for PostgreSQL
        if pd.isna(value):
            return None
            
        # Handle UUID objects
        if isinstance(value, uuid.UUID):
            return str(value)
            
        # Handle datetime objects
        if isinstance(value, (datetime, date)):
            return value.isoformat()
            
        # Handle decimal objects
        if isinstance(value, decimal.Decimal):
            if math.isnan(float(value)) or math.isinf(float(value)):
                return None  # Convert invalid decimals to NULL
            return float(value)
            
        # Handle float values more carefully
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                return None  # Convert NaN/Inf to NULL instead of string
            return value
            
        # Handle bytes
        if isinstance(value, bytes):
            try:
                return value.decode('utf-8')
            except UnicodeDecodeError:
                return value.hex()
                
        # Handle complex data types (dict, list) - convert to JSON strings
        if isinstance(value, (dict, list, tuple)):
            try:
                return json.dumps(value, default=str, ensure_ascii=False)
            except (TypeError, ValueError):
                return str(value)
        
        # Handle string values that might cause type issues
        if isinstance(value, str):
            # If it's obviously not meant to be a number, keep as string
            if value.lower() in ['nan', 'null', 'none', '']:
                return None
            return value
                
        # For all other types, try to convert to string if not serializable
        try:
            # Test if it's already JSON serializable
            json.dumps(value)
            return value
        except (TypeError, ValueError):
            return str(value)
    
    def _insert_data(self, cursor, conn, table_name: str, data: List[Dict]) -> int:
        """Insert data into table with proper serialization and schema alignment"""
        if not data:
            return 0
        
        logger.info(f"🔄 Preparing to insert {len(data)} records into {table_name}")
        
        # Get column names from first record and fix duplicates immediately
        original_columns = list(data[0].keys())
        logger.info(f"📋 Original columns: {len(original_columns)} columns")
        
        # Fix duplicate column names RIGHT HERE before any PostgreSQL operations
        seen_columns = set()
        unique_columns = []
        column_mapping = {}
        
        for col in original_columns:
            original_col = col
            counter = 1
            while col in seen_columns:
                col = f"{original_col}_{counter}"
                counter += 1
            
            seen_columns.add(col)
            unique_columns.append(col)
            
            if original_col != col:
                column_mapping[original_col] = col
                logger.info(f"🔄 Fixed duplicate column: '{original_col}' -> '{col}'")
        
        # CRITICAL FIX: Get actual table schema and map columns to existing table columns
        table_column_mapping = self._get_table_column_mapping(cursor, table_name, unique_columns)
        
        # Apply both duplicate fixes AND table schema alignment
        if column_mapping or table_column_mapping:
            logger.info(f"🔧 Applying column mappings: {len(column_mapping)} duplicates + {len(table_column_mapping)} schema alignments...")
            fixed_data = []
            for record in data:
                fixed_record = {}
                for old_key, value in record.items():
                    # First apply duplicate fix
                    intermediate_key = column_mapping.get(old_key, old_key)
                    # Then apply table schema alignment
                    final_key = table_column_mapping.get(intermediate_key, intermediate_key)
                    
                    # Skip columns that don't exist in table (mapped to None)
                    if final_key is not None:
                        # Handle case where multiple columns map to same final column
                        if final_key in fixed_record:
                            # Merge values - prefer non-null values, or use the last one
                            existing_value = fixed_record[final_key]
                            if existing_value is None or (existing_value == "" and value is not None):
                                fixed_record[final_key] = value
                            elif value is not None and value != "" and existing_value != value:
                                # Both have values - concatenate or choose based on type
                                if isinstance(existing_value, str) and isinstance(value, str):
                                    fixed_record[final_key] = f"{existing_value}; {value}"
                                else:
                                    fixed_record[final_key] = value  # Use the newer value
                        else:
                            fixed_record[final_key] = value
                fixed_data.append(fixed_record)
            data = fixed_data
            
            # Update columns list to match table schema (excluding None mappings)
            final_columns = []
            seen_final_columns = set()
            
            for col in unique_columns:
                final_col = table_column_mapping.get(col, col)
                if final_col is not None:  # Only include columns that exist in table
                    # Additional check to prevent duplicates in final column list
                    if final_col not in seen_final_columns:
                        final_columns.append(final_col)
                        seen_final_columns.add(final_col)
                    else:
                        logger.warning(f"⚠️ Skipping duplicate final column: '{final_col}' (mapped from '{col}')")
            
            columns = final_columns
            logger.info(f"✅ Fixed data with {len(columns)} schema-aligned columns (filtered out unmapped columns)")
        else:
            columns = unique_columns
        
        # Final check for duplicates in columns list
        if len(columns) != len(set(columns)):
            logger.error(f"❌ DUPLICATE COLUMNS DETECTED IN FINAL LIST!")
            column_counts = {}
            duplicates = []
            for col in columns:
                column_counts[col] = column_counts.get(col, 0) + 1
                if column_counts[col] > 1 and col not in duplicates:
                    duplicates.append(col)
            logger.error(f"❌ Duplicate columns: {duplicates}")
            logger.error(f"❌ Column counts: {column_counts}")
            
            # Remove duplicates by converting to list of unique items in order
            seen = set()
            unique_final_columns = []
            for col in columns:
                if col not in seen:
                    unique_final_columns.append(col)
                    seen.add(col)
            columns = unique_final_columns
            logger.info(f"✅ Removed duplicates - final column count: {len(columns)}")
        
        columns_str = ', '.join(columns)
        
        logger.info(f"📋 Columns to insert ({len(columns)}): {columns[:10]}..." + (f" and {len(columns)-10} more" if len(columns) > 10 else ""))
        
        # Prepare data for batch insert with serialization
        values = []
        for i, record in enumerate(data):
            try:
                # Serialize each value to ensure PostgreSQL compatibility
                serialized_values = []
                for col in columns:
                    raw_value = record.get(col)
                    serialized_value = self._serialize_value(raw_value)
                    serialized_values.append(serialized_value)
                
                values.append(tuple(serialized_values))
            except Exception as e:
                logger.error(f"❌ Failed to serialize record {i}: {e}")
                logger.debug(f"Record data: {record}")
                raise
        
        logger.info(f"✅ Successfully serialized {len(values)} records")
        
        # Batch insert
        batch_size = self.sink_config.batch_size or 1000
        total_inserted = 0
        
        for i in range(0, len(values), batch_size):
            batch = values[i:i + batch_size]
            
            try:
                insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES %s"
                execute_values(cursor, insert_sql, batch)
                total_inserted += len(batch)
                logger.info(f"📤 Inserted batch {i//batch_size + 1}: {len(batch)} records")
            except Exception as e:
                logger.error(f"❌ Failed to insert batch starting at record {i}: {e}")
                # Log sample data for debugging
                if batch:
                    logger.debug(f"Sample batch data: {batch[0]}")
                raise
        
        conn.commit()
        logger.info(f"✅ Successfully inserted {total_inserted} total records into {table_name}")
        return total_inserted
    
    def _get_table_column_mapping(self, cursor, table_name: str, data_columns: List[str]) -> Dict[str, str]:
        """
        Map data columns to existing table columns to handle schema evolution.
        Returns mapping from data column names to actual table column names.
        """
        try:
            # Extract schema and table name
            if '.' in table_name:
                schema_name, actual_table_name = table_name.split('.', 1)
            else:
                schema_name = 'public'
                actual_table_name = table_name
            
            # Get actual table columns
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema_name, actual_table_name))
            
            table_columns = [row[0] for row in cursor.fetchall()]
            logger.info(f"🗄️ Table {table_name} has {len(table_columns)} columns")
            
            if not table_columns:
                logger.warning(f"⚠️ No columns found for table {table_name}")
                return {}
            
            # Create mapping for mismatched columns
            column_mapping = {}
            
            for data_col in data_columns:
                if data_col not in table_columns:
                    # Try to find a matching truncated column
                    best_match = self._find_best_column_match(data_col, table_columns)
                    if best_match:
                        column_mapping[data_col] = best_match
                        logger.info(f"🔗 Mapped column: '{data_col}' -> '{best_match}'")
                    else:
                        logger.warning(f"❌ No matching column found for: '{data_col}' in table {table_name}")
                        # Skip columns that don't exist in the table
                        column_mapping[data_col] = None
            
            return column_mapping
            
        except Exception as e:
            logger.error(f"Failed to get table column mapping: {str(e)}")
            return {}
    
    def _find_best_column_match(self, data_column: str, table_columns: List[str]) -> Optional[str]:
        """
        Find the best matching table column for a data column.
        Handles cases where columns were truncated during table creation.
        """
        # Direct match
        if data_column in table_columns:
            return data_column
        
        # For long xAPI columns, try to find the truncated version
        if data_column.startswith('xapi_') and len(data_column) > 50:
            # Look for columns that start with the same prefix and have similar hash/suffix pattern
            prefix_parts = data_column.split('_')[:3]  # xapi_category_subcategory
            prefix = '_'.join(prefix_parts)
            suffix = data_column.split('_')[-1]  # last part
            
            for table_col in table_columns:
                if (table_col.startswith(prefix) and 
                    table_col.endswith(suffix) and
                    len(table_col) <= 60):
                    return table_col
        
        # Try partial matching for other cases
        # Look for the longest common prefix
        best_match = None
        best_score = 0
        
        for table_col in table_columns:
            # Calculate similarity score
            if data_column.startswith(table_col[:20]) or table_col.startswith(data_column[:20]):
                # Common prefix match
                common_len = 0
                for i in range(min(len(data_column), len(table_col))):
                    if data_column[i] == table_col[i]:
                        common_len += 1
                    else:
                        break
                
                if common_len > best_score and common_len >= 10:  # At least 10 chars match
                    best_score = common_len
                    best_match = table_col
        
        return best_match
    
    def _clean_column_name_for_postgres(self, col_name: str) -> str:
        """
        Clean column names consistently for PostgreSQL compatibility.
        Uses same logic as dynamic JSON parser for consistency.
        """
        import re
        import hashlib
        
        clean_key = str(col_name)
        
        # Remove or replace invalid characters
        clean_key = re.sub(r'[^\w\s]', '_', clean_key)  # Replace special chars with underscore
        clean_key = re.sub(r'\s+', '_', clean_key)      # Replace spaces with underscore
        clean_key = re.sub(r'_+', '_', clean_key)       # Replace multiple underscores with single
        clean_key = clean_key.strip('_')                # Remove leading/trailing underscores
        
        # Ensure it starts with a letter or underscore
        if clean_key and clean_key[0].isdigit():
            clean_key = f"field_{clean_key}"
        
        # Handle PostgreSQL 63-char identifier limit while preserving meaning
        if len(clean_key) > 60:  # Leave room for duplicate suffixes
            # For xAPI columns, preserve the most meaningful parts
            if clean_key.startswith('xapi_'):
                parts = clean_key.split('_')
                
                if len(parts) >= 4:
                    # Keep first 3 parts (xapi_category_subcategory)
                    prefix = '_'.join(parts[:3])
                    # Keep last 1-2 parts (the most specific)
                    suffix = '_'.join(parts[-2:]) if len(parts) > 4 else parts[-1]
                    # Create hash of the middle part
                    middle_part = '_'.join(parts[3:-2]) if len(parts) > 5 else '_'.join(parts[3:-1])
                    hash_part = hashlib.md5(middle_part.encode()).hexdigest()[:6]
                    
                    clean_key = f"{prefix}_{hash_part}_{suffix}"
                    
                    # If still too long, shorten the suffix
                    if len(clean_key) > 60:
                        suffix = parts[-1]  # Just the last part
                        clean_key = f"{prefix}_{hash_part}_{suffix}"
                else:
                    # Fallback for shorter xAPI columns
                    hash_part = hashlib.md5(clean_key.encode()).hexdigest()[:8]
                    clean_key = f"{parts[0]}_{parts[1]}_{hash_part}_{parts[-1]}"[:60]
            else:
                # For non-xAPI columns, use simpler approach
                hash_part = hashlib.md5(clean_key.encode()).hexdigest()[:8]
                clean_key = f"col_{hash_part}_{clean_key.split('_')[-1]}"[:60]
        elif len(clean_key) > 50:
            clean_key = clean_key[:50]
        
        # Handle empty keys
        if not clean_key:
            clean_key = "unnamed_field"
        
        return clean_key
    
    def _upsert_data(self, cursor, conn, table_name: str, data: List[Dict]) -> int:
        """
        Improved upsert with better transaction management and error handling
        
        Note: This function manages its own transactions for the fallback scenario.
        The connection closing is still handled by the caller.
        """
        if not data or not self.sink_config.upsert_keys:
            raise ValueError("Upsert requires data and upsert_keys")

        columns = list(data[0].keys())
        columns_str = ', '.join(columns)

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

        # Prepare data with serialization
        values = []
        for i, record in enumerate(data):
            try:
                # Serialize each value to ensure PostgreSQL compatibility
                serialized_values = []
                for col in columns:
                    raw_value = record.get(col)
                    serialized_value = self._serialize_value(raw_value)
                    serialized_values.append(serialized_value)
                
                values.append(tuple(serialized_values))
            except Exception as e:
                logger.error(f"❌ Failed to serialize upsert record {i}: {e}")
                logger.debug(f"Record data: {record}")
                raise

        batch_size = self.sink_config.batch_size or 1000

        # First attempt: Try upsert
        try:
            logger.info(f"Attempting upsert operation for {len(values)} records")
            total_upserted = 0
            
            for i in range(0, len(values), batch_size):
                batch = values[i:i + batch_size]
                execute_values(cursor, upsert_sql, batch)
                total_upserted += len(batch)

            # Commit successful upsert
            conn.commit()
            logger.info(f"Successfully upserted {total_upserted} records")
            return total_upserted

        except Exception as e:
            # Check if it's a constraint error
            if "no unique or exclusion constraint" in str(e).lower():
                logger.warning("Upsert failed due to missing constraints, falling back to INSERT")
                
                # Rollback the failed upsert transaction
                try:
                    conn.rollback()
                except Exception as rollback_error:
                    logger.error(f"Rollback failed: {rollback_error}")
                    raise

                # Fallback to regular insert (NEW TRANSACTION)
                try:
                    insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES %s"
                    total_inserted = 0

                    logger.info(f"Executing fallback INSERT for {len(values)} records")
                    for i in range(0, len(values), batch_size):
                        batch = values[i:i + batch_size]
                        execute_values(cursor, insert_sql, batch)
                        total_inserted += len(batch)

                    # Commit successful insert
                    conn.commit()
                    logger.info(f"Successfully inserted {total_inserted} records via fallback")
                    return total_inserted

                except Exception as insert_error:
                    logger.error(f"Fallback INSERT also failed: {insert_error}")
                    try:
                        conn.rollback()
                    except:
                        pass  # Ignore rollback errors at this point
                    raise Exception(f"Both upsert and insert failed. Insert error: {insert_error}")

            else:
                # For non-constraint errors, rollback and re-raise
                logger.error(f"Upsert failed with error: {str(e)}")
                try:
                    conn.rollback()
                except Exception as rollback_error:
                    logger.error(f"Rollback failed: {rollback_error}")
                raise  # Re-raise the original error
        
    def _infer_schema_from_data(self, data: Union[List[Dict], pd.DataFrame]) -> str:
        """Infer PostgreSQL schema from data with consistent column naming"""
        if isinstance(data, pd.DataFrame):
            sample = data.head(1).to_dict('records')[0]
        else:
            sample = data[0] if data else {}
        
        # Handle duplicate column names by adding incremental suffixes
        columns = []
        seen_columns = set()
        
        logger.info(f"🔍 Schema inference for {len(sample)} sample columns")
        
        for col_name, value in sample.items():
            # Use the same column cleaning logic as dynamic JSON parser
            original_col_name = col_name
            col_name = self._clean_column_name_for_postgres(col_name)
            
            if original_col_name != col_name:
                logger.info(f"🔧 Cleaned column name: '{original_col_name}' -> '{col_name}'")
            
            # Create unique column name
            counter = 1
            base_col_name = col_name
            while col_name in seen_columns:
                col_name = f"{base_col_name}_{counter}"
                counter += 1
                logger.info(f"🔄 Schema inference renamed duplicate: '{base_col_name}' -> '{col_name}'")
            
            seen_columns.add(col_name)
            
            # Improved type inference - sample more data to make better decisions
            col_type = self._infer_column_type(col_name, value, sample)
            
            columns.append(f"{col_name} {col_type}")
        
        # Final safety check - ensure no duplicate column definitions
        final_columns = []
        seen_definitions = set()
        
        for col_def in columns:
            col_name = col_def.split()[0]  # Get column name before type
            counter = 1
            original_def = col_def
            
            while col_def in seen_definitions or col_name in [c.split()[0] for c in seen_definitions]:
                # Extract type part
                parts = original_def.split()
                col_type = ' '.join(parts[1:])
                col_name = f"{parts[0]}_{counter}"
                col_def = f"{col_name} {col_type}"
                counter += 1
                
            seen_definitions.add(col_def)
            final_columns.append(col_def)
            
        result = ', '.join(final_columns)
        logger.info(f"✅ Final schema with {len(final_columns)} unique columns")
        return result
    
    def _infer_column_type(self, col_name: str, sample_value: Any, full_sample: Dict[str, Any]) -> str:
        """
        Simplified column type inference - treating everything as TEXT for now to avoid type conflicts
        """
        # TEMPORARY FIX: Use TEXT for everything to avoid type conversion issues
        # This ensures schema evolution works without data type conflicts
        return "TEXT"
        
        # Original logic commented out for future use:
        # if sample_value is None or pd.isna(sample_value):
        #     return "TEXT"
        # 
        # if isinstance(sample_value, bool):
        #     return "BOOLEAN"
        # elif isinstance(sample_value, int):
        #     if -2147483648 <= sample_value <= 2147483647:
        #         return "INTEGER"
        #     else:
        #         return "BIGINT"
        # elif isinstance(sample_value, float):
        #     if pd.isna(sample_value) or sample_value in [float('inf'), float('-inf')]:
        #         return "TEXT"
        #     return "REAL"
        # elif isinstance(sample_value, datetime):
        #     return "TIMESTAMP"
        # elif isinstance(sample_value, (list, dict)):
        #     return "JSONB"
        # else:
        #     return "TEXT"
    
    def _is_string_that_looks_numeric_but_isnt(self, value: str) -> bool:
        """
        Detect strings that might look numeric but should be treated as text
        """
        if not isinstance(value, str):
            return False
            
        # Common patterns that look numeric but are actually identifiers/versions
        patterns_that_are_text = [
            '@',      # version strings like "package@1.2.3"
            '-',      # identifiers like "event-routing-backends" 
            '.',      # version numbers like "9.3.5"
            ':',      # time-like strings or ratios
            '/',      # paths or fractions
            '#',      # hex colors or IDs
            '%'       # percentages as strings
        ]
        
        # If it contains any of these patterns, treat as text
        for pattern in patterns_that_are_text:
            if pattern in value:
                return True
                
        # If it's longer than typical numeric strings, probably text
        if len(value) > 20:
            return True
            
        # Check if it looks like a UUID, ID, or other identifier
        if len(value) > 10 and any(c.isalpha() for c in value):
            return True
            
        return False
    
    def _build_columns_definition(self, schema: Dict[str, str]) -> str:
        """Build columns definition from provided schema"""
        columns = []
        for col_name, col_type in schema.items():
            columns.append(f"{col_name} {col_type}")
        return ', '.join(columns)
    
    def _fix_duplicate_keys_in_data(self, data: List[Dict]) -> List[Dict]:
        """Fix duplicate keys in data by renaming them"""
        if not data:
            return data
        
        # Get all unique keys from first record
        sample_keys = list(data[0].keys())
        seen_keys = set()
        key_mapping = {}
        
        # Create mapping for duplicate keys
        for key in sample_keys:
            original_key = key
            counter = 1
            while key in seen_keys:
                key = f"{original_key}_{counter}"
                counter += 1
            
            seen_keys.add(key)
            if original_key != key:
                key_mapping[original_key] = key
                logger.info(f"🔄 Mapped duplicate key: '{original_key}' -> '{key}'")
        
        # Apply mapping to all records
        if key_mapping:
            fixed_data = []
            for record in data:
                fixed_record = {}
                for original_key, value in record.items():
                    new_key = key_mapping.get(original_key, original_key)
                    fixed_record[new_key] = value
                fixed_data.append(fixed_record)
            
            logger.info(f"✅ Fixed duplicate keys in {len(fixed_data)} records")
            return fixed_data
        
        return data

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
    """Load data into ClickHouse using clickhouse-connect for better external connection support"""
    
    def create_table_if_not_exists(self, data: Union[List[Dict], pd.DataFrame]):
        """Create ClickHouse table if it doesn't exist"""
        if not data:
            return
        
        if isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = data
            
        try:
            import clickhouse_connect
        except ImportError:
            raise ImportError("clickhouse-connect is required for ClickHouse sink operations. Install with: pip install clickhouse-connect")
            
        connection = BaseHook.get_connection(self.sink_config.connection_id)
        
        try:
            # Extract connection parameters from Airflow connection
            host = connection.host
            port = connection.port or 8123  # Default HTTP port
            username = connection.login or 'default'
            password = connection.password or ''
            database = connection.schema or 'default'
            
            # Parse extra connection parameters if provided
            extra_params = {}
            if hasattr(connection, 'extra_dejson') and connection.extra_dejson:
                extra_params = connection.extra_dejson
            
            # Create ClickHouse client
            client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=username,
                password=password,
                database=database,
                secure=extra_params.get('secure', False),
                verify=extra_params.get('verify', True),
                connect_timeout=extra_params.get('connect_timeout', 10),
                send_receive_timeout=extra_params.get('send_receive_timeout', 300),
                compress=extra_params.get('compress', True)
            )
            
            # Check if table exists
            full_table_name = f"{self.sink_config.schema_name}.{self.sink_config.table_name}" if self.sink_config.schema_name else self.sink_config.table_name
            
            check_query = f"EXISTS TABLE {full_table_name}"
            result = client.query(check_query)
            table_exists = result.first_row[0] if result.first_row else False
            
            if not table_exists:
                # Build CREATE TABLE statement
                if self.sink_config.table_schema:
                    columns_def = self._build_clickhouse_columns(self.sink_config.table_schema)
                else:
                    columns_def = self._infer_clickhouse_schema(df)
                
                # Determine engine based on whether upserts will be used
                # Pass the DataFrame to validate ORDER BY columns
                engine_config = self._get_table_engine_config(df)
                
                create_table_sql = f"""
                    CREATE TABLE {full_table_name} (
                        {columns_def}
                    ) {engine_config}
                """
                
                logger.info(f"🔨 Creating ClickHouse table with SQL: {create_table_sql[:500]}...")
                client.command(create_table_sql)
                logger.info(f"✅ Created ClickHouse table: {full_table_name} with engine: {engine_config}")
            else:
                # Table exists - check if it needs to be recreated with nullable columns
                if self._needs_nullable_recreation(client, full_table_name, df):
                    logger.warning(f"⚠️ Table {full_table_name} exists with non-nullable columns, recreating with nullable schema")
                    self._recreate_table_with_nullable_schema(client, full_table_name, df)
                else:
                    # Standard schema evolution for missing columns
                    self._handle_clickhouse_schema_evolution(client, full_table_name, df)
        finally:
            try:
                if 'client' in locals():
                    client.close()
            except Exception as close_error:
                logger.warning(f"Error closing ClickHouse connection: {close_error}")
                
    
    def load_data(self, data: Union[List[Dict], pd.DataFrame], context) -> int:
        """Load data into ClickHouse"""
        if isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = data
        
        if df.empty:
            return 0
            
        try:
            import clickhouse_connect
        except ImportError:
            raise ImportError("clickhouse-connect is required for ClickHouse sink operations. Install with: pip install clickhouse-connect")
            
        connection = BaseHook.get_connection(self.sink_config.connection_id)
        
        try:
            # Extract connection parameters from Airflow connection
            host = connection.host
            port = connection.port or 8123  # Default HTTP port
            username = connection.login or 'default'
            password = connection.password or ''
            database = connection.schema or 'default'
            
            # Parse extra connection parameters if provided
            extra_params = {}
            if hasattr(connection, 'extra_dejson') and connection.extra_dejson:
                extra_params = connection.extra_dejson
            
            # Create ClickHouse client
            client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=username,
                password=password,
                database=database,
                secure=extra_params.get('secure', False),
                verify=extra_params.get('verify', True),
                connect_timeout=extra_params.get('connect_timeout', 10),
                send_receive_timeout=extra_params.get('send_receive_timeout', 300),
                compress=extra_params.get('compress', True)
            )
            
            logger.info(f"Connected to ClickHouse at {host}:{port}, database: {database}")
            
            full_table_name = f"{self.sink_config.schema_name}.{self.sink_config.table_name}" if self.sink_config.schema_name else self.sink_config.table_name
            
            if self.sink_config.write_mode == WriteMode.OVERWRITE:
                # Truncate table first
                truncate_sql = f"TRUNCATE TABLE {full_table_name}"
                client.command(truncate_sql)
                logger.info(f"Truncated table {full_table_name}")
                
                # Standard insert for overwrite
                return self._insert_data(client, full_table_name, df)
                
            elif self.sink_config.write_mode == WriteMode.APPEND:
                # Standard insert for append
                return self._insert_data(client, full_table_name, df)
                
            elif self.sink_config.write_mode == WriteMode.UPSERT:
                # ClickHouse upsert using MERGE operation
                return self._upsert_data(client, full_table_name, df)
                
            else:
                raise ValueError(f"Unsupported write mode: {self.sink_config.write_mode}")
                
        except Exception as e:
            logger.error(f"ClickHouse data loading failed: {str(e)}")
            raise
        finally:
            try:
                if 'client' in locals():
                    client.close()
            except Exception as close_error:
                logger.warning(f"Error closing ClickHouse connection: {close_error}")
                
    
    def _insert_data(self, client, full_table_name: str, df: pd.DataFrame) -> int:
        """Standard data insertion for ClickHouse with schema alignment"""
        # Get existing table columns to ensure we only insert matching columns
        try:
            describe_query = f"DESCRIBE TABLE {full_table_name}"
            result = client.query(describe_query)
            existing_columns = {row[0] for row in result.result_rows}
            
            # Filter DataFrame to only include columns that exist in the table
            columns_to_insert = [col for col in df.columns if col in existing_columns]
            missing_columns = [col for col in df.columns if col not in existing_columns]
            
            if missing_columns:
                logger.warning(f"⚠️ Skipping {len(missing_columns)} columns not in table: {missing_columns[:5]}...")
                logger.info(f"📝 You may want to run schema evolution first to add these columns")
            
            # Filter the DataFrame to only include existing columns
            df_filtered = df[columns_to_insert]
            
            logger.info(f"📊 Inserting {len(df_filtered)} rows with {len(columns_to_insert)} columns into ClickHouse")
            
            # Check if deduplication is enabled (this could prevent duplicate inserts)
            try:
                dedup_query = "SELECT value FROM system.settings WHERE name = 'insert_deduplicate'"
                result = client.query(dedup_query)
                dedup_setting = result.first_row[0] if result.first_row else "unknown"
                logger.info(f"🔍 ClickHouse insert_deduplicate setting: {dedup_setting}")
                
                if dedup_setting == '1':
                    logger.warning("⚠️ ClickHouse deduplication is ENABLED - duplicate data may not be inserted")
                    logger.info("💡 To disable: SET insert_deduplicate = 0")
            except Exception as dedup_e:
                logger.debug(f"Could not check deduplication setting: {str(dedup_e)}")
            
        except Exception as e:
            logger.warning(f"Could not filter columns, using all: {str(e)}")
            df_filtered = df
        
        # Convert DataFrame to list of lists for ClickHouse insertion
        data_to_insert = df_filtered.values.tolist()
        column_names = df_filtered.columns.tolist()
        
        # Insert data in batches
        batch_size = self.sink_config.batch_size or 10000
        total_inserted = 0
        
        for i in range(0, len(data_to_insert), batch_size):
            batch = data_to_insert[i:i + batch_size]
            
            try:
                logger.debug(f"🔄 Inserting batch {i//batch_size + 1} with {len(batch)} rows, columns: {column_names[:5]}...")
                
                client.insert(
                    table=full_table_name,
                    data=batch,
                    column_names=column_names
                )
                total_inserted += len(batch)
                logger.debug(f"✅ Successfully inserted batch {i//batch_size + 1} with {len(batch)} rows")
                
            except Exception as e:
                logger.error(f"❌ Failed to insert batch {i//batch_size + 1}: {str(e)}")
                logger.error(f"🔍 Batch data sample: {batch[0] if batch else 'empty'}")
                logger.error(f"🔍 Column names: {column_names}")
                # Try to continue with other batches
                continue
            
            if len(data_to_insert) > batch_size:
                logger.info(f"Inserted batch {i//batch_size + 1}: {len(batch)} rows")
        
        # Verify the actual count in the table after insertion
        try:
            count_query = f"SELECT COUNT(*) FROM {full_table_name}"
            result = client.query(count_query)
            actual_count = result.first_row[0] if result.first_row else 0
            logger.info(f"✅ Successfully inserted {total_inserted} rows into ClickHouse table {full_table_name}")
            logger.info(f"📊 Table now contains {actual_count} total rows")
            
            if total_inserted > 0 and actual_count == 0:
                logger.error("❌ Data was not actually inserted - check ClickHouse settings or constraints")
            
        except Exception as e:
            logger.warning(f"Could not verify row count: {str(e)}")
            
        return total_inserted
    
    def _upsert_data(self, client, full_table_name: str, df: pd.DataFrame) -> int:
        """ClickHouse upsert using MERGE operations or ReplacingMergeTree approach"""
        if not self.sink_config.upsert_keys:
            raise ValueError("ClickHouse upsert requires upsert_keys to be specified")
        
        # Check if table uses ReplacingMergeTree engine
        engine_query = f"""
        SELECT engine 
        FROM system.tables 
        WHERE database = splitByChar('.', '{full_table_name}')[1] 
        AND name = splitByChar('.', '{full_table_name}')[2]
        """
        
        try:
            result = client.query(engine_query)
            engine = result.first_row[0] if result.first_row else None
            
            if engine and 'ReplacingMergeTree' in engine:
                logger.info("Table uses ReplacingMergeTree engine, using direct insert for upsert")
                return self._insert_data(client, full_table_name, df)
            else:
                logger.info("Table uses standard engine, using MERGE-based upsert")
                return self._merge_upsert_data(client, full_table_name, df)
                
        except Exception as e:
            logger.warning(f"Could not determine table engine, falling back to MERGE-based upsert: {e}")
            return self._merge_upsert_data(client, full_table_name, df)
    
    def _merge_upsert_data(self, client, full_table_name: str, df: pd.DataFrame) -> int:
        """ClickHouse MERGE-based upsert implementation"""
        temp_table_name = f"{full_table_name}_temp_{int(pd.Timestamp.now().timestamp())}"
        
        try:
            # Create temporary table with same structure
            create_temp_query = f"""
            CREATE TABLE {temp_table_name} AS {full_table_name}
            """
            client.command(create_temp_query)
            logger.info(f"Created temporary table: {temp_table_name}")
            
            # Insert new data into temporary table
            temp_inserted = self._insert_data(client, temp_table_name, df)
            
            # Get all columns for MERGE operation
            columns = df.columns.tolist()
            upsert_keys = self.sink_config.upsert_keys
            update_columns = [col for col in columns if col not in upsert_keys]
            
            # Build MERGE statement
            # ClickHouse MERGE syntax (available in newer versions)
            merge_conditions = " AND ".join([f"target.{key} = source.{key}" for key in upsert_keys])
            
            if update_columns:
                update_assignments = ", ".join([f"{col} = source.{col}" for col in update_columns])
                merge_query = f"""
                ALTER TABLE {full_table_name} 
                UPDATE {update_assignments}
                WHERE ({", ".join(upsert_keys)}) IN (
                    SELECT {", ".join(upsert_keys)} FROM {temp_table_name}
                )
                """
                
                # Execute update for existing records
                client.command(merge_query)
                logger.info("Updated existing records")
            
            # Insert new records that don't exist
            insert_new_query = f"""
            INSERT INTO {full_table_name}
            SELECT * FROM {temp_table_name}
            WHERE ({", ".join(upsert_keys)}) NOT IN (
                SELECT {", ".join(upsert_keys)} FROM {full_table_name}
            )
            """
            
            client.command(insert_new_query)
            logger.info("Inserted new records")
            
            return temp_inserted
            
        except Exception as e:
            logger.error(f"MERGE-based upsert failed: {e}")
            # Fallback to simple insert if MERGE fails
            logger.info("Falling back to simple insert")
            return self._insert_data(client, full_table_name, df)
            
        finally:
            # Clean up temporary table
            try:
                client.command(f"DROP TABLE IF EXISTS {temp_table_name}")
                logger.info(f"Dropped temporary table: {temp_table_name}")
            except Exception as cleanup_error:
                logger.warning(f"Failed to cleanup temporary table: {cleanup_error}")
    
    def _validate_order_by_columns(self, order_by: str, df: pd.DataFrame) -> str:
        """
        Validate that ORDER BY columns exist in the DataFrame.
        Falls back to tuple() if columns don't exist.
        """
        if not order_by or order_by == 'tuple()':
            return 'tuple()'
        
        # Extract column names from order_by string
        # Remove parentheses if present
        clean_order_by = order_by.strip('()')
        
        # Split by comma and clean column names
        requested_columns = [col.strip() for col in clean_order_by.split(',')]
        
        # Check which columns actually exist in the DataFrame
        existing_columns = []
        for col in requested_columns:
            if col in df.columns:
                existing_columns.append(col)
            else:
                logger.warning(f"⚠️ ORDER BY column '{col}' not found in data, skipping")
        
        # Build the ORDER BY clause
        if existing_columns:
            if len(existing_columns) == 1:
                return f"({existing_columns[0]})"
            else:
                return f"({', '.join(existing_columns)})"
        else:
            logger.warning("⚠️ No ORDER BY columns found in data, using tuple()")
            return 'tuple()'
    
    def _infer_clickhouse_schema(self, df: pd.DataFrame) -> str:
        """Infer ClickHouse schema from pandas DataFrame with nullable types for safety"""
        columns = []
        for col_name, col_type in zip(df.columns, df.dtypes):
            # Check if column has any NULL values
            has_nulls = df[col_name].isna().any()
            
            # Determine base type
            if pd.api.types.is_bool_dtype(col_type):
                ch_type = "UInt8"  # ClickHouse doesn't have native boolean
            elif pd.api.types.is_integer_dtype(col_type):
                ch_type = "Int64"
            elif pd.api.types.is_float_dtype(col_type):
                ch_type = "Float64"
            elif pd.api.types.is_datetime64_any_dtype(col_type):
                ch_type = "DateTime"
            else:
                ch_type = "String"
            
            # For dynamic schemas with potential NULLs, use Nullable types
            # This is safer for evolving schemas where NULLs might appear later
            # Always use Nullable for dynamic JSON schemas to prevent NULL insertion errors
            ch_type = f"Nullable({ch_type})"
            
            columns.append(f"`{col_name}` {ch_type}")
        
        return ', '.join(columns)
    
    def _build_clickhouse_columns(self, schema: Dict[str, str]) -> str:
        """Build ClickHouse columns definition from provided schema"""
        columns = []
        for col_name, col_type in schema.items():
            columns.append(f"`{col_name}` {col_type}")
        return ', '.join(columns)
    
    def _handle_clickhouse_schema_evolution(self, client, table_name: str, df: pd.DataFrame):
        """
        Handle schema evolution for ClickHouse tables.
        Adds missing columns to the existing table.
        """
        try:
            # Get existing table columns
            describe_query = f"DESCRIBE TABLE {table_name}"
            result = client.query(describe_query)
            existing_columns = {row[0]: row[1] for row in result.result_rows}
            
            logger.info(f"🔍 Existing ClickHouse table has {len(existing_columns)} columns")
            
            # Get columns from the DataFrame
            data_columns = df.columns.tolist()
            
            # Find missing columns
            missing_columns = []
            for col in data_columns:
                if col not in existing_columns:
                    missing_columns.append(col)
            
            if missing_columns:
                logger.warning(f"⚠️ Found {len(missing_columns)} new columns that need to be added to ClickHouse table")
                logger.info(f"📝 Missing columns: {missing_columns[:10]}..." + (f" and {len(missing_columns)-10} more" if len(missing_columns) > 10 else ""))
                
                # Add each missing column
                for col_name in missing_columns:
                    # Check if column has any NULL values
                    has_nulls = df[col_name].isna().any() if col_name in df.columns else True
                    
                    # Infer data type from the DataFrame
                    sample_value = df[col_name].dropna().iloc[0] if not df[col_name].dropna().empty else None
                    
                    # Determine ClickHouse data type - ALWAYS use Nullable for safety with dynamic schemas
                    if sample_value is None or pd.isna(sample_value):
                        col_type = "Nullable(String)"  # Default to nullable string for unknown types
                    elif isinstance(sample_value, bool):
                        col_type = "Nullable(UInt8)"  # Always nullable for dynamic schemas
                    elif isinstance(sample_value, int):
                        col_type = "Nullable(Int64)"  # Always nullable for dynamic schemas
                    elif isinstance(sample_value, float):
                        col_type = "Nullable(Float64)"  # Always nullable for dynamic schemas
                    elif isinstance(sample_value, (list, dict)):
                        col_type = "Nullable(String)"  # JSON stored as nullable string
                    else:
                        # Default to Nullable(String) for safety with dynamic schemas
                        col_type = "Nullable(String)"
                    
                    # ALTER TABLE ADD COLUMN
                    alter_query = f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS `{col_name}` {col_type}"
                    
                    try:
                        client.command(alter_query)
                        logger.info(f"✅ Added column '{col_name}' ({col_type}) to ClickHouse table")
                    except Exception as e:
                        if "already exists" in str(e).lower():
                            logger.info(f"ℹ️ Column '{col_name}' already exists (race condition)")
                        else:
                            logger.error(f"❌ Failed to add column '{col_name}': {str(e)}")
                            # Try with Nullable(String) type as fallback
                            try:
                                alter_query_fallback = f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS `{col_name}` Nullable(String)"
                                client.command(alter_query_fallback)
                                logger.info(f"✅ Added column '{col_name}' as Nullable(String) (fallback)")
                            except Exception as fallback_error:
                                logger.error(f"❌ Fallback also failed: {str(fallback_error)}")
                
                logger.info(f"✅ Schema evolution complete - added {len(missing_columns)} new columns")
            else:
                logger.info("✅ No schema changes needed - all columns exist in ClickHouse table")
                
        except Exception as e:
            logger.error(f"❌ Failed to handle schema evolution: {str(e)}")
            # Don't fail the entire operation - try to proceed with insert
            logger.warning("⚠️ Proceeding with insert despite schema evolution issues")
    
    def _needs_nullable_recreation(self, client, table_name: str, df: pd.DataFrame) -> bool:
        """
        Check if table needs to be recreated because it has non-nullable String columns
        that would fail with NULL values from dynamic JSON parsing
        """
        try:
            # Get existing table columns and their types
            describe_query = f"DESCRIBE TABLE {table_name}"
            result = client.query(describe_query)
            existing_columns = {row[0]: row[1] for row in result.result_rows}
            
            # Check if any String columns are non-nullable and we have NULL values for them
            for col_name in df.columns:
                if col_name in existing_columns:
                    col_type = existing_columns[col_name]
                    has_nulls = df[col_name].isna().any()
                    
                    # If we have a String column that's not Nullable and we have NULL values
                    if col_type == "String" and has_nulls:
                        logger.warning(f"⚠️ Column '{col_name}' is non-nullable String but has NULL values")
                        return True
                    
                    # Also check for other non-nullable types with NULL values
                    if not col_type.startswith("Nullable(") and has_nulls:
                        logger.warning(f"⚠️ Column '{col_name}' is non-nullable {col_type} but has NULL values")
                        return True
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Failed to check nullable recreation need: {str(e)}")
            # If we can't check, assume we don't need recreation
            return False
    
    def _recreate_table_with_nullable_schema(self, client, table_name: str, df: pd.DataFrame):
        """
        Recreate table with nullable schema, preserving existing data
        """
        try:
            backup_table = f"{table_name}_backup_{int(pd.Timestamp.now().timestamp())}"
            
            logger.info(f"🔄 Recreating table {table_name} with nullable schema")
            
            # Step 1: Create backup table with existing data
            backup_query = f"CREATE TABLE {backup_table} AS {table_name}"
            client.command(backup_query)
            logger.info(f"✅ Created backup table: {backup_table}")
            
            # Step 2: Drop original table
            drop_query = f"DROP TABLE {table_name}"
            client.command(drop_query)
            logger.info(f"🗑️ Dropped original table: {table_name}")
            
            # Step 3: Create new table with nullable schema
            columns_def = self._infer_clickhouse_schema(df)
            engine_config = self._get_table_engine_config(df)
            
            create_table_sql = f"""
                CREATE TABLE {table_name} (
                    {columns_def}
                ) {engine_config}
            """
            
            client.command(create_table_sql)
            logger.info(f"✅ Created new table with nullable schema: {table_name}")
            
            # Step 4: Insert data from backup (only common columns)
            # Get columns that exist in both backup and new table
            describe_backup = f"DESCRIBE TABLE {backup_table}"
            backup_result = client.query(describe_backup)
            backup_columns = [row[0] for row in backup_result.result_rows]
            
            new_columns = df.columns.tolist()
            common_columns = [col for col in backup_columns if col in new_columns]
            
            if common_columns:
                columns_list = ", ".join([f"`{col}`" for col in common_columns])
                insert_query = f"""
                    INSERT INTO {table_name} ({columns_list})
                    SELECT {columns_list} FROM {backup_table}
                """
                client.command(insert_query)
                logger.info(f"✅ Restored {len(common_columns)} columns from backup")
            
            # Step 5: Drop backup table
            drop_backup_query = f"DROP TABLE {backup_table}"
            client.command(drop_backup_query)
            logger.info(f"🗑️ Cleaned up backup table: {backup_table}")
            
        except Exception as e:
            logger.error(f"❌ Failed to recreate table with nullable schema: {str(e)}")
            # Try to restore from backup if it exists
            try:
                if 'backup_table' in locals():
                    restore_query = f"RENAME TABLE {backup_table} TO {table_name}"
                    client.command(restore_query)
                    logger.info(f"🔄 Restored original table from backup")
            except Exception as restore_error:
                logger.error(f"❌ Failed to restore from backup: {str(restore_error)}")
            raise e
    
    def _get_table_engine_config(self, df: pd.DataFrame = None) -> str:
        """Get appropriate ClickHouse table engine configuration"""
        # Check if upserts will be used and if upsert_keys are provided
        use_replacing = (
            self.sink_config.write_mode == WriteMode.UPSERT and 
            self.sink_config.upsert_keys and 
            len(self.sink_config.upsert_keys) > 0
        )
        
        # Check custom config for engine preference
        custom_config = self.sink_config.custom_config or {}
        engine_type = custom_config.get('engine', 'auto')
        
        if engine_type == 'ReplacingMergeTree' or (engine_type == 'auto' and use_replacing):
            # Use ReplacingMergeTree for upsert scenarios
            if self.sink_config.upsert_keys:
                # Use the first upsert key as the version column if it's a timestamp/numeric
                version_col = custom_config.get('version_column')
                if version_col:
                    engine = f"ENGINE = ReplacingMergeTree({version_col})"
                else:
                    engine = "ENGINE = ReplacingMergeTree()"
                
                # Order by upsert keys for optimal performance
                order_by = f"ORDER BY ({', '.join(self.sink_config.upsert_keys)})"
            else:
                engine = "ENGINE = ReplacingMergeTree()"
                order_by = "ORDER BY tuple()"
                
            return f"{engine} {order_by}"
        
        else:
            # Standard MergeTree engine
            partition_by = custom_config.get('partition_by', '')
            order_by = custom_config.get('order_by', 'tuple()')
            
            # Validate ORDER BY columns exist in the data if DataFrame is provided
            if df is not None and order_by and order_by != 'tuple()':
                order_by = self._validate_order_by_columns(order_by, df)
            elif order_by and order_by != 'tuple()':
                # Fix order_by syntax if it doesn't have parentheses
                if not order_by.startswith('(') and ',' in order_by:
                    order_by = f"({order_by})"
                elif not order_by.startswith('(') and not order_by.startswith('tuple'):
                    order_by = f"({order_by})"
            
            if partition_by:
                return f"ENGINE = MergeTree() PARTITION BY {partition_by} ORDER BY {order_by}"
            else:
                return f"ENGINE = MergeTree() ORDER BY {order_by}"

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
    """Load data to SFTP server using paramiko directly"""
    
    def load_data(self, data: Union[List[Dict], pd.DataFrame], context) -> int:
        """Load data to SFTP server"""
        if isinstance(data, pd.DataFrame):
            df = data
        else:
            df = pd.DataFrame(data) if data else pd.DataFrame()
        
        if df.empty:
            return 0
        
        connection = BaseHook.get_connection(self.sink_config.connection_id)
        
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
            try:
                sftp.makedirs(directory)
            except:
                pass  # Directory might already exist
            
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
            with sftp.open(actual_file_path, 'w') as remote_file:
                remote_file.write(file_buffer.getvalue())
            
            return len(df)
            
        except Exception as e:
            logger.error(f"SFTP upload failed: {str(e)}")
            raise
        finally:
            if 'sftp' in locals():
                sftp.close()
            if 'ssh' in locals():
                ssh.close()

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