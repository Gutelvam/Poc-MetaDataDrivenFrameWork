# Enhanced LOAD Operator - Powerful Data Loading

The Enhanced LOAD Operator makes your data loading operations much more powerful and flexible by supporting multiple data sourcing strategies.

## 🚀 **Key Capabilities**

### **1. Traditional Load (Upstream → Sink)**
```yaml
- task_id: "load_from_upstream"
  operator_type: "load"
  depends_on: ["extract_task"]
  sink:
    type: "postgresql"
    table_name: "destination_table"
```
*Gets data from upstream task via XCom → Loads to sink*

### **2. Direct Load (Source → Sink)**
```yaml  
- task_id: "load_direct"
  operator_type: "load"
  source:
    type: "postgresql"
    query: "SELECT * FROM source_table"
  sink:
    type: "postgresql"
    table_name: "destination_table"
```
*Extracts data from source → Loads to sink (no upstream dependency needed)*

### **3. Smart Load (Intelligent Choice)**
```yaml
- task_id: "smart_load"
  operator_type: "load"
  depends_on: ["extract_task"]  # Has upstream
  source:                       # AND has source
    type: "postgresql"
    query: "SELECT * FROM backup_table"
  sink:
    type: "postgresql"
    table_name: "destination_table"
```
*Intelligently chooses: Upstream data if available, otherwise source extraction*

## ⚡ **Smart Decision Logic**

The Smart Load operator automatically:

1. **Checks upstream task status**
   - ✅ If upstream task succeeded → Use upstream data
   - ❌ If upstream task failed → Use source extraction
   - ⏳ If upstream task pending → Use source extraction

2. **Provides fallback options**
   - Primary: Preferred data source
   - Fallback: Alternative data source
   - Ensures data is always available

3. **Logs decision reasoning**
   ```
   🎯 Smart choice: Using upstream data (task completed successfully)
   📊 Data sourced from: upstream_task:extract_orders
   ✅ Enhanced Load completed: 1,250 records loaded
   ```

## 🛠 **Configuration Options**

### **Schema Support**
```yaml
sink:
  type: "postgresql"
  connection_id: "postgres_dev"
  schema_name: "public"          # ✅ Now required for PostgreSQL
  table_name: "my_table"
  write_mode: "overwrite"
  auto_create_table: true
```

### **Write Modes**
- `overwrite`: Replace all data in table
- `append`: Add new records to existing data  
- `upsert`: Update existing, insert new (requires upsert_keys)

### **Data Source Preferences**
```python
# In advanced configurations
prefer_upstream: true   # Prefer upstream data over source
prefer_upstream: false  # Prefer source extraction over upstream
```

## 📊 **Monitoring & Logging**

Enhanced Load operators provide comprehensive logging:

```
🚀 Starting Enhanced Load: load_results
📥 Attempting to get data from upstream task: extract_orders
✅ Got 1,250 records from upstream task
📊 Data sourced from: upstream_task:extract_orders
✅ Enhanced Load completed: 1,250 records loaded to results_sink
```

### **Monitoring Metrics**
- Data source selection (upstream vs source)
- Record counts and processing time
- Fallback usage statistics
- Success/failure rates by data source type

## 🎯 **Your Use Case - Now Supported!**

Your original configuration now works perfectly:

```yaml
tasks:
  - task_id: "load_results" 
    operator_type: "load"
    depends_on: ["inicio"]
    source:
      name: "orders_source"
      type: "postgresql"  
      connection_id: "postgres_dev"
      query: "SELECT * FROM public.orders"
    sink:
      name: "results_sink"
      type: "postgresql"
      connection_id: "postgres_dev"
      schema_name: "public"        # ✅ Add this line
      table_name: "teste_results"
      write_mode: "overwrite"
      auto_create_table: true
```

**What happens:**
1. ✅ Checks upstream task `inicio` (dummy task - always succeeds)
2. 🎯 Smart choice: Uses source extraction (dummy tasks don't produce data)
3. 📤 Extracts: `SELECT * FROM public.orders`
4. 📥 Loads: Data into `public.teste_results`
5. ✅ Creates table if needed, overwrites existing data

## 🚀 **Benefits**

1. **More Powerful**: One operator handles multiple scenarios
2. **More Reliable**: Automatic fallback options
3. **More Flexible**: Works with or without upstream dependencies
4. **More Intelligent**: Makes smart decisions automatically
5. **Better Monitoring**: Comprehensive logging and metrics

The Enhanced LOAD Operator makes your data pipelines more robust and eliminates the need to choose between different task types!