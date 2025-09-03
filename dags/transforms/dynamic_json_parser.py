"""
Dynamic JSON Parser Transform Operator

This operator dynamically parses any JSON structure into separate columns,
making it perfect for xAPI events or any evolving JSON schema.

Features:
- 100% Dynamic - adapts to any JSON structure automatically
- No hardcoded field mappings - discovers schema on-the-fly
- Handles nested objects, arrays, and primitive types
- Configurable flattening depth and naming conventions
- Supports schema evolution - new fields are automatically included
"""

import json
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Any, Union, Optional, Set
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
import re

logger = logging.getLogger(__name__)

class DynamicJSONParserOperator(BaseOperator):
    """
    Fully dynamic JSON parser that adapts to any JSON structure
    
    Perfect for:
    - xAPI events with evolving schemas
    - API responses with unknown structures
    - Any JSON data that changes over time
    """
    
    def __init__(
        self,
        source_task_id: str,
        json_column: str = "event",
        max_depth: int = 5,
        include_original: bool = False,
        column_prefix: str = "",
        separator: str = "_",
        array_handling: str = "json_string",  # "json_string", "first_item", "count", "extract_objects"
        null_handling: str = "keep",  # "keep", "drop", "empty_string"
        type_inference: bool = True,
        clean_column_names: bool = True,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.source_task_id = source_task_id
        self.json_column = json_column
        self.max_depth = max_depth
        self.include_original = include_original
        self.column_prefix = column_prefix
        self.separator = separator
        self.array_handling = array_handling
        self.null_handling = null_handling
        self.type_inference = type_inference
        self.clean_column_names = clean_column_names
        
    def execute(self, context):
        """Execute dynamic JSON parsing"""
        
        # Get data from upstream task
        data = context['task_instance'].xcom_pull(task_ids=self.source_task_id)
        
        if not data:
            logger.warning("No data received from upstream task")
            return []
        
        # Convert to DataFrame
        if isinstance(data, pd.DataFrame):
            df = data
        elif isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            raise AirflowException(f"Unsupported data type: {type(data)}")
        
        logger.info(f"Processing {len(df)} records for dynamic JSON parsing")
        
        # Validate JSON column exists
        if self.json_column not in df.columns:
            raise AirflowException(f"JSON column '{self.json_column}' not found in data. Available columns: {list(df.columns)}")
        
        # Discover complete schema first
        logger.info("🔍 Discovering JSON schema dynamically...")
        all_fields = self._discover_schema(df[self.json_column])
        logger.info(f"📊 Discovered {len(all_fields)} unique fields in JSON structure")
        
        # Parse all records with discovered schema
        logger.info("🔧 Parsing JSON records...")
        parsed_df = self._parse_json_records(df, all_fields)
        
        logger.info(f"✅ Successfully parsed JSON into {len(parsed_df.columns)} columns")
        
        # Convert back to list of dictionaries with safe column handling
        logger.info(f"📤 Converting DataFrame with {len(parsed_df.columns)} columns to records")
        
        # Log a few column names for debugging
        sample_columns = list(parsed_df.columns)[:10]
        logger.info(f"🔍 Sample columns: {sample_columns}")
        if len(parsed_df.columns) > 10:
            logger.info(f"... and {len(parsed_df.columns) - 10} more columns")
        
        return parsed_df.to_dict('records')
    
    def _discover_schema(self, json_series: pd.Series) -> Set[str]:
        """Dynamically discover all possible fields in the JSON data using bullet-proof flattening"""
        all_fields = set()
        
        logger.info(f"🔍 Starting universal schema discovery on {len(json_series)} records")
        
        for idx, json_text in json_series.items():
            try:
                json_obj = self._parse_json_safely(json_text)
                if json_obj is not None:
                    # Use the new bullet-proof flattening to discover all fields
                    flattened_records = self._flatten_json_to_records(json_obj)
                    
                    # Extract all field names from all flattened records
                    for record in flattened_records:
                        all_fields.update(record.keys())
                        
            except Exception as e:
                logger.debug(f"Skipping record {idx} during schema discovery: {str(e)}")
        
        logger.info(f"🎯 Universal schema discovery complete: {len(all_fields)} total fields discovered")
        logger.info(f"📝 Sample fields: {list(all_fields)[:10]}{'...' if len(all_fields) > 10 else ''}")
        
        return all_fields
    
    def _parse_json_safely(self, json_text: Any) -> Optional[Union[Dict, List]]:
        """Safely parse JSON from various input types"""
        if json_text is None or pd.isna(json_text):
            return None
            
        try:
            if isinstance(json_text, str):
                # Handle empty strings
                if not json_text.strip():
                    return None
                return json.loads(json_text)
            elif isinstance(json_text, (dict, list)):
                return json_text
            else:
                # Try to convert to string and parse
                return json.loads(str(json_text))
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            logger.debug(f"Invalid JSON: {str(e)[:100]}...")
            return None
    
    def _flatten_json_to_records(self, data: Any) -> List[Dict[str, Any]]:
        """
        Main function to flatten a JSON object into a list of dictionaries (table format).
        Handles nested objects and arrays with proper unnesting for data analysis.
        Based on the bullet-proof approach but optimized for our use case.
        """
        if isinstance(data, list):
            flattened_list = []
            for item in data:
                flattened_list.extend(self._flatten_json_to_records(item))
            return flattened_list

        if isinstance(data, dict):
            flattened = self._flatten_json_object(data)

            # Unnest any lists of objects found during the initial flatten pass
            final_list = [{}]
            for key, value in flattened.items():
                if isinstance(value, list):
                    if all(isinstance(item, dict) for item in value):
                        # This is a list of objects - extract fields from each item as separate columns
                        for obj in final_list:
                            # First, extract fields from the first item (most common case)
                            if value:
                                first_item = self._flatten_json_object(value[0])
                                for sub_key, sub_value in first_item.items():
                                    if sub_key:
                                        array_key = f"{key}_first_{sub_key}"
                                    else:
                                        array_key = f"{key}_first"
                                    obj[array_key] = sub_value
                            
                            # Also extract fields from first few items with index
                            for i, item in enumerate(value[:3]):  # Limit to first 3 items
                                flattened_item = self._flatten_json_object(item)
                                for sub_key, sub_value in flattened_item.items():
                                    if sub_key:
                                        array_key = f"{key}_{i}_{sub_key}"
                                    else:
                                        array_key = f"{key}_{i}"
                                    obj[array_key] = sub_value
                            
                            # Keep the full array as JSON string for reference
                            obj[key] = json.dumps(value, default=str)
                            # Add count
                            obj[f"{key}_count"] = len(value)
                    elif value:  # Non-empty list of primitives
                        # Handle simple arrays based on configuration
                        if self.array_handling == "count":
                            for obj in final_list:
                                obj[f"{key}_count"] = len(value)
                        elif self.array_handling == "first_item":
                            for obj in final_list:
                                obj[key] = value[0]
                        else:
                            # Convert to JSON string
                            for obj in final_list:
                                obj[key] = json.dumps(value, default=str)
                    else:
                        # Empty list
                        for obj in final_list:
                            obj[key] = None
                else:
                    # Simple value - add to all existing records
                    for obj in final_list:
                        obj[key] = value

            return final_list

        # If it's a primitive value, return it as a single record
        return [{"value": data}] if data is not None else [{}]
    
    def _flatten_json_object(self, data: Dict, separator: str = None) -> Dict[str, Any]:
        """
        A recursive helper function to flatten a JSON object.
        Returns a flattened dictionary with dot notation keys.
        """
        if separator is None:
            separator = self.separator
            
        out = {}

        def flatten(x, name=''):
            """The recursive part of the function."""
            if isinstance(x, dict):
                for a in x:
                    clean_key = self._clean_key_name(a)
                    new_name = f"{name}{clean_key}{separator}" if name else f"{clean_key}{separator}"
                    flatten(x[a], new_name)
            elif isinstance(x, list):
                # Always keep lists for later unnesting in the main function
                # The main function will handle whether to unnest or convert to string
                out[name[:-len(separator)]] = x
            else:
                # Primitive value
                out[name[:-len(separator)]] = x

        flatten(data)
        return out
    
    def _extract_all_paths(self, obj: Any, current_path: str = "", depth: int = 0) -> Set[str]:
        """Recursively extract all possible paths from a JSON object"""
        paths = set()
        
        if depth >= self.max_depth:
            # At max depth, just add the current path
            if current_path:
                paths.add(current_path)
            return paths
        
        if isinstance(obj, dict):
            if not obj:  # Empty dict
                paths.add(current_path or "empty_object")
            else:
                for key, value in obj.items():
                    clean_key = self._clean_key_name(key)
                    new_path = f"{current_path}{self.separator}{clean_key}" if current_path else clean_key
                    
                    # Add the path for this key
                    paths.add(new_path)
                    
                    # Recursively get paths from the value
                    nested_paths = self._extract_all_paths(value, new_path, depth + 1)
                    paths.update(nested_paths)
        
        elif isinstance(obj, list):
            if not obj:  # Empty list
                paths.add(current_path or "empty_array")
            else:
                # Handle arrays based on configuration
                if self.array_handling == "json_string":
                    # Just add the current path (will be converted to JSON string)
                    paths.add(current_path or "array")
                elif self.array_handling == "first_item":
                    # Explore structure of first item
                    if obj:
                        nested_paths = self._extract_all_paths(obj[0], current_path, depth)
                        paths.update(nested_paths)
                elif self.array_handling == "count":
                    # Just count items
                    paths.add(f"{current_path}_count" if current_path else "array_count")
                elif self.array_handling == "extract_objects":
                    # Extract meaningful fields from array of objects
                    paths.add(current_path)  # Keep the array path
                    paths.add(f"{current_path}_count")  # Add count
                    
                    # For contextactivities specifically, extract key fields from objects
                    is_contextactivities = "contextactivities" in current_path.lower() or "context_activities" in current_path.lower()
                    logger.debug(f"🔍 Array path: '{current_path}' - is contextactivities: {is_contextactivities}")
                    
                    if is_contextactivities:
                        logger.info(f"🎯 Found contextActivities array at path: {current_path}")
                        self._extract_contextactivities_paths(obj, current_path, paths, depth)
                    else:
                        # For other arrays, explore first few items
                        for i, item in enumerate(obj[:3]):  # Limit to first 3 items
                            if isinstance(item, dict):
                                nested_paths = self._extract_all_paths(item, current_path, depth)
                                paths.update(nested_paths)
                            elif i == 0:  # Only add indexed path for first item if not dict
                                item_path = f"{current_path}_0"
                                nested_paths = self._extract_all_paths(item, item_path, depth)
                                paths.update(nested_paths)
                else:
                    # For each item in array (can be expensive!)
                    for i, item in enumerate(obj[:5]):  # Limit to first 5 items
                        item_path = f"{current_path}_{i}" if current_path else f"item_{i}"
                        nested_paths = self._extract_all_paths(item, item_path, depth)
                        paths.update(nested_paths)
        
        else:
            # Primitive value - the path is already added above
            pass
        
        return paths
    
    def _extract_contextactivities_paths(self, contextactivities_array: List, base_path: str, paths: Set[str], depth: int):
        """Extract meaningful paths from xAPI contextActivities array"""
        try:
            logger.info(f"🔍 Extracting contextActivities paths from {len(contextactivities_array)} activities at path: {base_path}")
            
            # Common xAPI contextActivities fields to extract
            activity_fields = ['id', 'objectType']
            definition_fields = ['name', 'type', 'description']
            
            for i, activity in enumerate(contextactivities_array[:3]):  # Process first 3 activities
                if isinstance(activity, dict):
                    logger.debug(f"📋 Processing activity {i}: {list(activity.keys())}")
                    
                    # Extract direct activity fields
                    for field in activity_fields:
                        if field in activity:
                            field_path = f"{base_path}_{i}_{field}"
                            paths.add(field_path)
                            logger.debug(f"✅ Added indexed path: {field_path}")
                    
                    # Extract definition fields
                    if 'definition' in activity and isinstance(activity['definition'], dict):
                        definition = activity['definition']
                        logger.debug(f"📖 Processing definition with keys: {list(definition.keys())}")
                        
                        for field in definition_fields:
                            if field in definition:
                                field_path = f"{base_path}_{i}_definition_{field}"
                                paths.add(field_path)
                                logger.debug(f"✅ Added definition path: {field_path}")
                                
                                # Handle name/description objects (language maps)
                                if isinstance(definition[field], dict):
                                    for lang_key in definition[field].keys():
                                        lang_path = f"{field_path}_{self._clean_key_name(lang_key)}"
                                        paths.add(lang_path)
                                        logger.debug(f"✅ Added language path: {lang_path}")
            
            # Also extract first activity's main fields without index for easy access
            if contextactivities_array and isinstance(contextactivities_array[0], dict):
                first_activity = contextactivities_array[0]
                logger.debug(f"🥇 Processing first activity with keys: {list(first_activity.keys())}")
                
                # Main activity fields
                for field in ['id', 'objectType']:
                    if field in first_activity:
                        first_path = f"{base_path}_first_{field}"
                        paths.add(first_path)
                        logger.debug(f"✅ Added first activity path: {first_path}")
                
                # Definition fields
                if 'definition' in first_activity and isinstance(first_activity['definition'], dict):
                    definition = first_activity['definition']
                    logger.debug(f"📖 First activity definition keys: {list(definition.keys())}")
                    
                    # Type field
                    if 'type' in definition:
                        type_path = f"{base_path}_first_type"
                        paths.add(type_path)
                        logger.debug(f"✅ Added first type path: {type_path}")
                    
                    # Name field (extract first language)
                    if 'name' in definition and isinstance(definition['name'], dict):
                        name_path = f"{base_path}_first_name"
                        paths.add(name_path)
                        logger.debug(f"✅ Added first name path: {name_path}")
                        
                        # Add specific language variants
                        for lang in definition['name'].keys():
                            lang_path = f"{base_path}_first_name_{self._clean_key_name(lang)}"
                            paths.add(lang_path)
                            logger.debug(f"✅ Added first name language path: {lang_path}")
            
            logger.info(f"🎯 Total contextActivities paths discovered: {len([p for p in paths if base_path in p])}")
                            
        except Exception as e:
            logger.error(f"❌ Failed to extract contextactivities paths: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _flatten_contextactivities(self, contextactivities_array: List, base_path: str, flattened: Dict[str, Any], depth: int):
        """Flatten xAPI contextActivities array into meaningful fields"""
        try:
            logger.debug(f"🔄 Flattening contextActivities array with {len(contextactivities_array)} activities at path: {base_path}")
            # Extract fields from first few activities
            for i, activity in enumerate(contextactivities_array[:3]):
                if isinstance(activity, dict):
                    # Extract direct activity fields
                    for field in ['id', 'objectType']:
                        if field in activity:
                            flattened[f"{base_path}_{i}_{field}"] = activity[field]
                    
                    # Extract definition fields
                    if 'definition' in activity and isinstance(activity['definition'], dict):
                        definition = activity['definition']
                        
                        # Extract type
                        if 'type' in definition:
                            flattened[f"{base_path}_{i}_definition_type"] = definition['type']
                        
                        # Extract name and description (handle language maps)
                        for field in ['name', 'description']:
                            if field in definition:
                                field_value = definition[field]
                                if isinstance(field_value, dict):
                                    # Language map - extract all language variants
                                    for lang_key, lang_value in field_value.items():
                                        clean_lang = self._clean_key_name(lang_key)
                                        flattened[f"{base_path}_{i}_definition_{field}_{clean_lang}"] = lang_value
                                    
                                    # Also store the first available language as default
                                    if field_value:
                                        first_lang_value = next(iter(field_value.values()))
                                        flattened[f"{base_path}_{i}_definition_{field}"] = first_lang_value
                                else:
                                    # Simple string value
                                    flattened[f"{base_path}_{i}_definition_{field}"] = field_value
            
            # Also extract first activity's fields without index for easy access
            if contextactivities_array and isinstance(contextactivities_array[0], dict):
                first_activity = contextactivities_array[0]
                
                # Main activity fields
                for field in ['id', 'objectType']:
                    if field in first_activity:
                        flattened[f"{base_path}_first_{field}"] = first_activity[field]
                
                # Definition fields from first activity
                if 'definition' in first_activity and isinstance(first_activity['definition'], dict):
                    definition = first_activity['definition']
                    
                    # Type
                    if 'type' in definition:
                        flattened[f"{base_path}_first_type"] = definition['type']
                    
                    # Name
                    if 'name' in definition:
                        if isinstance(definition['name'], dict):
                            # Extract first language as default
                            if definition['name']:
                                first_name = next(iter(definition['name'].values()))
                                flattened[f"{base_path}_first_name"] = first_name
                                
                                # Also extract specific language variants
                                for lang, name_value in definition['name'].items():
                                    clean_lang = self._clean_key_name(lang)
                                    flattened[f"{base_path}_first_name_{clean_lang}"] = name_value
                        else:
                            flattened[f"{base_path}_first_name"] = definition['name']
                            
        except Exception as e:
            logger.debug(f"Failed to flatten contextactivities: {str(e)}")
    
    def _parse_json_records(self, df: pd.DataFrame, all_fields: Set[str]) -> pd.DataFrame:
        """Parse all JSON records using bullet-proof flattening approach"""
        
        all_parsed_rows = []
        
        logger.info(f"🔧 Starting bullet-proof JSON parsing for {len(df)} records")
        
        for idx, row in df.iterrows():
            try:
                # Parse the JSON
                json_obj = self._parse_json_safely(row[self.json_column])
                
                if json_obj is not None:
                    # Use bullet-proof flattening to get all possible records
                    flattened_records = self._flatten_json_to_records(json_obj)
                    
                    # Each JSON object might produce multiple records (due to arrays)
                    for flattened_record in flattened_records:
                        # Create new row starting with original columns
                        new_row = {}
                        
                        # Add original columns (except JSON column if not including original)
                        for col in df.columns:
                            if col != self.json_column or self.include_original:
                                new_row[col] = row[col]
                        
                        # Add all discovered fields with proper prefixing
                        for field in all_fields:
                            column_name = f"{self.column_prefix}{field}" if self.column_prefix else field
                            
                            # Handle potential duplicate column names
                            original_column_name = column_name
                            counter = 1
                            while column_name in new_row:
                                column_name = f"{original_column_name}_{counter}"
                                counter += 1
                            
                            # Get value from flattened record
                            value = flattened_record.get(field)
                            
                            # Handle null values based on configuration
                            if value is None:
                                if self.null_handling == "drop":
                                    continue
                                elif self.null_handling == "empty_string":
                                    value = ""
                            
                            new_row[column_name] = value
                        
                        all_parsed_rows.append(new_row)
                else:
                    # JSON is None or empty - create empty record with original columns
                    new_row = {}
                    for col in df.columns:
                        if col != self.json_column or self.include_original:
                            new_row[col] = row[col]
                    
                    # Add all fields as None/empty
                    for field in all_fields:
                        column_name = f"{self.column_prefix}{field}" if self.column_prefix else field
                        new_row[column_name] = "" if self.null_handling == "empty_string" else None
                    
                    all_parsed_rows.append(new_row)
                
            except Exception as e:
                logger.error(f"❌ Failed to parse row {idx}: {str(e)}")
                # Create row with original data and None for all JSON fields
                new_row = dict(row)
                for field in all_fields:
                    column_name = f"{self.column_prefix}{field}" if self.column_prefix else field
                    new_row[column_name] = None
                all_parsed_rows.append(new_row)
        
        logger.info(f"🎯 Bullet-proof parsing complete: {len(df)} input records → {len(all_parsed_rows)} output records")
        
        # Create final DataFrame
        result_df = pd.DataFrame(all_parsed_rows)
        
        # Handle duplicate column names by adding incremental suffixes
        logger.info(f"🔍 Checking for duplicate columns in {len(result_df.columns)} columns")
        result_df = self._handle_duplicate_columns(result_df)
        logger.info(f"✅ Final DataFrame has {len(result_df.columns)} unique columns")
        
        # Apply type inference if requested
        if self.type_inference:
            result_df = self._infer_types(result_df)
        
        return result_df
    
    def _flatten_object(self, obj: Any, current_path: str = "", depth: int = 0) -> Dict[str, Any]:
        """Flatten a JSON object into a dictionary with dot-notation keys"""
        flattened = {}
        
        if depth >= self.max_depth:
            # Convert complex objects to JSON strings at max depth
            if isinstance(obj, (dict, list)):
                flattened[current_path] = json.dumps(obj, default=str)
            else:
                flattened[current_path] = obj
            return flattened
        
        if isinstance(obj, dict):
            if not obj:  # Empty dict
                flattened[current_path or "empty_object"] = None
            else:
                for key, value in obj.items():
                    clean_key = self._clean_key_name(key)
                    new_path = f"{current_path}{self.separator}{clean_key}" if current_path else clean_key
                    
                    nested = self._flatten_object(value, new_path, depth + 1)
                    flattened.update(nested)
        
        elif isinstance(obj, list):
            if not obj:  # Empty list
                flattened[current_path or "empty_array"] = None
            else:
                if self.array_handling == "json_string":
                    flattened[current_path] = json.dumps(obj, default=str)
                elif self.array_handling == "first_item":
                    if obj:
                        nested = self._flatten_object(obj[0], current_path, depth)
                        flattened.update(nested)
                elif self.array_handling == "count":
                    flattened[f"{current_path}_count" if current_path else "array_count"] = len(obj)
                elif self.array_handling == "extract_objects":
                    # Store both the full array and extracted fields
                    flattened[current_path] = json.dumps(obj, default=str)
                    flattened[f"{current_path}_count"] = len(obj)
                    
                    # For contextactivities, extract specific fields
                    is_contextactivities = "contextactivities" in current_path.lower() or "context_activities" in current_path.lower()
                    logger.debug(f"🔄 Flattening array path: '{current_path}' - is contextactivities: {is_contextactivities}")
                    
                    if is_contextactivities:
                        logger.info(f"🎯 Flattening contextActivities array at path: {current_path}")
                        self._flatten_contextactivities(obj, current_path, flattened, depth)
                    else:
                        # For other arrays, extract from first few items
                        for i, item in enumerate(obj[:3]):
                            if isinstance(item, dict):
                                nested = self._flatten_object(item, current_path, depth)
                                flattened.update(nested)
                            elif i == 0:
                                item_path = f"{current_path}_0"
                                nested = self._flatten_object(item, item_path, depth)
                                flattened.update(nested)
                else:
                    # Individual items (limited to prevent explosion)
                    for i, item in enumerate(obj[:5]):
                        item_path = f"{current_path}_{i}" if current_path else f"item_{i}"
                        nested = self._flatten_object(item, item_path, depth)
                        flattened.update(nested)
        
        else:
            # Primitive value
            flattened[current_path] = obj
        
        return flattened
    
    def _clean_key_name(self, key: str) -> str:
        """Clean key names to create valid column names"""
        if not self.clean_column_names:
            return str(key)
        
        # Convert to string and handle common issues
        clean_key = str(key)
        
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
            import hashlib
            
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
                        
                    logger.info(f"🔧 Shortened xAPI column: ...{middle_part}... -> {hash_part}")
                else:
                    # Fallback for shorter xAPI columns
                    hash_part = hashlib.md5(clean_key.encode()).hexdigest()[:8]
                    clean_key = f"{parts[0]}_{parts[1]}_{hash_part}_{parts[-1]}"[:60]
            else:
                # For non-xAPI columns, use simpler approach
                hash_part = hashlib.md5(clean_key.encode()).hexdigest()[:8]
                clean_key = f"col_{hash_part}_{clean_key.split('_')[-1]}"[:60]
            
            logger.info(f"✂️ Column shortened to: '{clean_key}' (length: {len(clean_key)})")
        elif len(clean_key) > 50:
            clean_key = clean_key[:50]
        
        # Handle empty keys
        if not clean_key:
            clean_key = "unnamed_field"
        
        return clean_key
    
    def _handle_duplicate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle duplicate column names by adding incremental suffixes (_1, _2, etc.)"""
        if df.empty:
            return df
            
        columns = df.columns.tolist()
        seen_columns = {}
        new_columns = []
        
        for col in columns:
            if col not in seen_columns:
                seen_columns[col] = 0
                new_columns.append(col)
            else:
                seen_columns[col] += 1
                new_col_name = f"{col}_{seen_columns[col]}"
                new_columns.append(new_col_name)
                logger.info(f"🔄 Renamed duplicate column '{col}' to '{new_col_name}'")
        
        # Create a new DataFrame with unique column names
        df_with_unique_cols = df.copy()
        df_with_unique_cols.columns = new_columns
        
        return df_with_unique_cols
    
    def _infer_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Intelligently infer data types for parsed columns"""
        
        for col in df.columns:
            if col.startswith(self.column_prefix) or (not self.column_prefix and col not in [self.json_column]):
                try:
                    # Skip if all values are null
                    if df[col].isna().all():
                        continue
                    
                    # Get non-null values for analysis
                    non_null_values = df[col].dropna()
                    
                    if len(non_null_values) == 0:
                        continue
                    
                    # Try to convert to numeric
                    numeric_converted = pd.to_numeric(non_null_values, errors='coerce')
                    if not numeric_converted.isna().all():
                        # If most values can be converted to numeric, do it
                        if numeric_converted.notna().sum() / len(non_null_values) > 0.8:
                            df[col] = pd.to_numeric(df[col], errors='ignore')
                            continue
                    
                    # Try to convert boolean-like strings
                    if non_null_values.dtype == 'object':
                        unique_vals = non_null_values.unique()
                        bool_vals = {'true', 'false', '1', '0', 'yes', 'no', 'y', 'n'}
                        if len(unique_vals) <= 10 and all(str(v).lower() in bool_vals for v in unique_vals):
                            bool_map = {
                                'true': True, 'false': False, '1': True, '0': False,
                                'yes': True, 'no': False, 'y': True, 'n': False
                            }
                            df[col] = df[col].astype(str).str.lower().map(bool_map).fillna(df[col])
                            continue
                    
                    # Try to parse dates
                    if non_null_values.dtype == 'object' and 'time' in col.lower():
                        try:
                            parsed_dates = pd.to_datetime(non_null_values, errors='coerce')
                            if parsed_dates.notna().sum() / len(non_null_values) > 0.5:
                                df[col] = pd.to_datetime(df[col], errors='ignore')
                                continue
                        except:
                            pass
                
                except Exception as e:
                    logger.debug(f"Type inference failed for column {col}: {str(e)}")
        
        return df

# Factory function for framework integration
def create_dynamic_json_parser_operator(task_id: str, source_task_id: str, dag, **kwargs) -> DynamicJSONParserOperator:
    """Factory function to create dynamic JSON parser operator"""
    
    return DynamicJSONParserOperator(
        task_id=task_id,
        source_task_id=source_task_id,
        dag=dag,
        **kwargs
    )