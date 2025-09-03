# Sink-Based Data Sharing Migration Guide

## Overview

This framework now supports a production-ready sink-based data sharing approach that replaces XCom with configurable data sinks. This provides better scalability, easier debugging, and supports complex SQL queries between tasks.

## Key Benefits

- **Production Ready**: No more XCom size limitations
- **Better Performance**: Direct SQL queries instead of serialization/deserialization
- **Easier Debugging**: Data persisted in queryable sinks
- **SQL Support**: Write complex joins and aggregations across task outputs
- **Automatic Management**: Configurable retention and cleanup

## Migration Path

### Before (XCom-based)
```python
# Old transform operator
data = context['task_instance'].xcom_pull(task_ids='source_task')
```

### After (Sink-based)
```python
# New sink-based approach
from core.data_registry import query_task_data

data = query_task_data('source_task', dag_id, execution_date)
# or with custom SQL:
data = query_task_data('source_task', dag_id, execution_date, 
                      "SELECT DISTINCT column1 FROM source_task WHERE column2 > 100")
```

## Configuration Changes

### New Task Configuration Fields
```yaml
# In your task configuration:
tasks:
  - task_id: "my_task"
    operator_type: "transform"
    
    # NEW: Automatic sink configuration
    auto_sink: true                    # Create automatic sink (default: true)
    auto_sink_type: "temp_database"    # temp_database, temp_postgres, temp_file, none
    source_task_ids: ["upstream_task"] # Replace XCom pulls with explicit dependencies
    
    sql_transform: |
      SELECT DISTINCT(column1) as unique_values
      FROM upstream_task  -- Query directly from sink table
      WHERE column2 > 100
```

## Your Example: Birds Pipeline

### Before (XCom)
```python
# Task 1: Extract data, push to XCom
# Task 2: Pull from XCom, transform, push result to XCom
# Task 3: Pull from XCom, query data
```

### After (Sink-based)
```yaml
tasks:
  - task_id: "birds"
    operator_type: "extract"
    source:
      query: "SELECT 1 as col1, 2 as col2, 3 as col3 FROM orders"
    auto_sink: true  # Creates table 'birds' in temp database

  - task_id: "analyze_birds" 
    operator_type: "transform"
    depends_on: ["birds"]
    source_task_ids: ["birds"]
    sql_transform: |
      SELECT DISTINCT(col1) as distinct_col1,
             COUNT(*) as count_records
      FROM birds  -- Query directly from birds table
      GROUP BY col1
    auto_sink: true  # Results available for next task
```

## Sink Types

### 1. temp_database (Default)
- Uses SQLite for temporary storage
- Best for: Development, small to medium datasets
- Automatic cleanup after configurable retention period

### 2. temp_postgres  
- Uses temporary schema in PostgreSQL
- Best for: Production environments, large datasets
- Better performance for complex queries

### 3. temp_file
- Stores data as JSON/CSV files
- Best for: File-based workflows, external tool integration

### 4. temp_datalake
- Stores data as Parquet blobs in Azure Data Lake Gen2
- Best for: Cloud-native environments, large datasets, cost-effective storage
- Automatic partitioning by execution date
- Container: `airflow-temp-data` (configurable)

### 5. none
- No automatic sink created
- Best for: Procedure tasks, API calls, notifications

### 6. custom
- Use existing sink configuration
- Best for: Data that needs to persist longer

## Usage Examples

### Simple Data Flow
```yaml
tasks:
  - task_id: "extract_orders"
    operator_type: "extract" 
    source:
      query: "SELECT * FROM orders WHERE date = current_date"
    auto_sink: true  # Data available as 'extract_orders' table

  - task_id: "filter_orders"
    source_task_ids: ["extract_orders"]
    sql_transform: "SELECT * FROM extract_orders WHERE amount > 100"
    auto_sink: true

  - task_id: "load_results"
    source_task_ids: ["filter_orders"] 
    sink:
      table_name: "processed_orders"
      write_mode: "overwrite"
```

### Multi-Source Joins
```yaml
tasks:
  - task_id: "get_sales"
    auto_sink: true
    # ... extracts to 'get_sales' table
    
  - task_id: "get_customers" 
    auto_sink: true
    # ... extracts to 'get_customers' table
    
  - task_id: "join_data"
    source_task_ids: ["get_sales", "get_customers"]
    sql_transform: |
      SELECT s.*, c.customer_name
      FROM get_sales s
      LEFT JOIN get_customers c ON s.customer_id = c.customer_id
```

### Python Transformations
```yaml
tasks:
  - task_id: "python_transform"
    source_task_ids: ["upstream_task"]
    python_transform: |
      def process_data(datasets, **context):
          # datasets contains data from all source_task_ids
          data = datasets['upstream_task']
          
          # Process data
          result = []
          for record in data:
              record['processed_at'] = context['execution_date']
              result.append(record)
          
          return result

### Azure Data Lake Gen2 Large Dataset Processing
```yaml
tasks:
  - task_id: "extract_large_dataset"
    operator_type: "extract"
    source:
      query: "SELECT * FROM large_table WHERE date = current_date"
    auto_sink: true
    auto_sink_type: "temp_datalake"  # Efficient for large datasets

  - task_id: "process_large_data"
    source_task_ids: ["extract_large_dataset"]
    python_transform: |
      def process_large_data(datasets, **context):
          # Data automatically loaded from Data Lake Parquet
          data = datasets['extract_large_dataset']
          
          # Process in chunks for large datasets
          processed = []
          for record in data:
              if record.get('amount', 0) > 1000:
                  record['high_value'] = True
                  processed.append(record)
          
          return processed
    auto_sink: true
    auto_sink_type: "temp_datalake"  # Chain Data Lake storage
```

## Migration Checklist

- [ ] Update task configurations to use `source_task_ids` instead of XCom pulls
- [ ] Add `auto_sink: true` to tasks that produce data for other tasks
- [ ] Set `auto_sink: false` for procedure/notification tasks  
- [ ] Replace XCom pulls in custom Python code with `query_task_data()`
- [ ] Update SQL transforms to query from sink tables instead of XCom
- [ ] Test data flow between tasks
- [ ] Configure cleanup retention periods

## Troubleshooting

### Data Not Found
```python
# Check if data exists for a task
from core.data_registry import get_data_registry
registry = get_data_registry()
location = registry.get_task_data_location('task_id', 'dag_id', 'execution_date')
if location:
    print(f"Data stored in: {location.sink_type}")
else:
    print("No data found - check if auto_sink is enabled")
```

### Query Issues
```python
# Test queries against sink data
data = query_task_data('source_task', dag_id, execution_date, "SELECT COUNT(*) FROM source_task")
print(f"Record count: {data}")
```

## Performance Notes

- Temp database (SQLite): Good for < 1M records
- Temp PostgreSQL: Good for larger datasets and complex queries  
- Automatic cleanup runs daily by default
- Configure retention based on your pipeline needs

## Next Steps

1. Start with `auto_sink_type: "temp_database"` for development
2. Move to `temp_postgres` for production workloads
3. Use custom sinks for data that needs to persist
4. Monitor storage usage and configure appropriate cleanup