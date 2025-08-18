# NiFi Multi-Table Loading from Snowflake External Stage

## Overview
This guide covers how to load data from multiple folders in a Snowflake external stage into multiple target tables using NiFi. Each folder corresponds to a different table.

## Scenario
```
External Stage Structure:
├── customers/          → CUSTOMERS table
├── orders/            → ORDERS table  
├── products/          → PRODUCTS table
├── customer_data/     → CUSTOMER_DETAILS table
└── product_data/      → PRODUCT_CATALOG table
```

---

## Solution 1: Static Multi-Table Approach (Simplest)

**Best for**: Fixed set of tables that don't change frequently

### Flow Architecture
```
GenerateFlowFile → UpdateAttribute → ExecuteSQLStatement
```

### Configuration

#### Step 1: GenerateFlowFile
```
Processor Name: GenerateFlowFile_MultiTableTrigger
File Size: 0B
Custom Text: multi-table-load-trigger
Scheduling: Timer driven (set desired frequency)
Run Schedule: 0 */15 * * * ? (every 15 minutes)
```

#### Step 2: UpdateAttribute
```
Processor Name: UpdateAttribute_MultiTableSQL

Custom Properties:
sql.statement: 
COPY INTO CUSTOMERS FROM @YOUR_STAGE_NAME/customers/ FILE_FORMAT = (TYPE = 'JSON');
COPY INTO ORDERS FROM @YOUR_STAGE_NAME/orders/ FILE_FORMAT = (TYPE = 'JSON');  
COPY INTO PRODUCTS FROM @YOUR_STAGE_NAME/products/ FILE_FORMAT = (TYPE = 'JSON');
COPY INTO CUSTOMER_DETAILS FROM @YOUR_STAGE_NAME/customer_data/ FILE_FORMAT = (TYPE = 'JSON');
COPY INTO PRODUCT_CATALOG FROM @YOUR_STAGE_NAME/product_data/ FILE_FORMAT = (TYPE = 'JSON');
```

#### Step 3: ExecuteSQLStatement
```
Processor Name: ExecuteSQL_MultiTable
Database Connection Pooling Service: SnowflakeConnectionService
SQL select query: ${sql.statement}
Query Timeout: 300 seconds
```

### Pros:
- ✅ Simple configuration
- ✅ Single processor execution
- ✅ All tables loaded in one transaction

### Cons:
- ❌ Hard to troubleshoot individual table issues
- ❌ If one table fails, all fail

---

## Solution 2: Dynamic Routing Approach (Recommended)

**Best for**: Need individual control over each table load and error handling

### Flow Architecture
```
GenerateFlowFile → UpdateAttribute → SplitText → UpdateAttribute → RouteOnAttribute → Multiple ExecuteSQLStatement
```

### Configuration

#### Step 1: GenerateFlowFile
```
Processor Name: GenerateFlowFile_TableList
File Size: 0B
Custom Text: customers,orders,products,customer_data,product_data
Scheduling: Timer driven
```

#### Step 2: UpdateAttribute (Folder Mappings)
```
Processor Name: UpdateAttribute_FolderMappings

Custom Properties:
table.customers: CUSTOMERS
table.orders: ORDERS
table.products: PRODUCTS  
table.customer_data: CUSTOMER_DETAILS
table.product_data: PRODUCT_CATALOG

stage.customers: customers
stage.orders: orders
stage.products: products
stage.customer_data: customer_data
stage.product_data: product_data
```

#### Step 3: SplitText
```
Processor Name: SplitText_Folders
Line Split Count: 1
Header Line Count: 0
Remove Trailing Newlines: true
```

#### Step 4: UpdateAttribute (Set Current Table Info)
```
Processor Name: UpdateAttribute_SetTableInfo

Custom Properties:
current.folder: ${allLines:trim()}
target.table: ${table.${current.folder}}
stage.folder: ${stage.${current.folder}}
copy.command: COPY INTO ${target.table} FROM @YOUR_STAGE_NAME/${stage.folder}/ FILE_FORMAT = (TYPE = 'JSON')
```

#### Step 5: RouteOnAttribute
```
Processor Name: RouteOnAttribute_ByTable

Routing Strategy: Route to Property name

Custom Properties:
customers: ${current.folder:equals('customers')}
orders: ${current.folder:equals('orders')}
products: ${current.folder:equals('products')}
customer_data: ${current.folder:equals('customer_data')}
product_data: ${current.folder:equals('product_data')}
```

#### Step 6: Multiple ExecuteSQLStatement Processors
Create one for each table:

```
Processor Name: ExecuteSQL_CUSTOMERS
Database Connection Pooling Service: SnowflakeConnectionService
SQL select query: ${copy.command}
Query Timeout: 300 seconds

Processor Name: ExecuteSQL_ORDERS
Database Connection Pooling Service: SnowflakeConnectionService
SQL select query: ${copy.command}
Query Timeout: 300 seconds

... (repeat for each table)
```

### Pros:
- ✅ Individual table control
- ✅ Better error handling
- ✅ Can monitor each table separately
- ✅ Easy to add/remove tables

### Cons:
- ❌ More complex setup
- ❌ More processors to manage

---

## Solution 3: Configuration-Driven Approach (Most Flexible)

**Best for**: Frequently changing table mappings or large number of tables

### Flow Architecture
```
ListFile → EvaluateJsonPath → SplitJson → EvaluateJsonPath → UpdateAttribute → ExecuteSQLStatement
```

### Step 1: Create Configuration File
Create `table_mappings.json`:
```json
{
  "mappings": [
    {
      "folder": "customers",
      "table": "CUSTOMERS",
      "file_format": "JSON",
      "additional_options": ""
    },
    {
      "folder": "orders", 
      "table": "ORDERS",
      "file_format": "JSON",
      "additional_options": ""
    },
    {
      "folder": "products",
      "table": "PRODUCTS", 
      "file_format": "CSV",
      "additional_options": "SKIP_HEADER = 1"
    },
    {
      "folder": "customer_data",
      "table": "CUSTOMER_DETAILS",
      "file_format": "JSON",
      "additional_options": ""
    }
  ]
}
```

### Configuration

#### Step 1: ListFile
```
Processor Name: ListFile_ConfigReader
Input Directory: /path/to/config/
File Filter: table_mappings.json
Listing Strategy: Timestamps
```

#### Step 2: FetchFile
```
Processor Name: FetchFile_Config
File to Fetch: ${absolute.path}
```

#### Step 3: SplitJson
```
Processor Name: SplitJson_Mappings
JsonPath Expression: $.mappings[*]
```

#### Step 4: EvaluateJsonPath
```
Processor Name: EvaluateJsonPath_ExtractMapping
Destination: flowfile-attribute

Custom Properties:
folder.name: $.folder
table.name: $.table
file.format: $.file_format
additional.options: $.additional_options
```

#### Step 5: UpdateAttribute
```
Processor Name: UpdateAttribute_CreateCopyCommand

Custom Properties:
copy.command: COPY INTO ${table.name} FROM @YOUR_STAGE_NAME/${folder.name}/ FILE_FORMAT = (TYPE = '${file.format}' ${additional.options})
```

#### Step 6: ExecuteSQLStatement
```
Processor Name: ExecuteSQL_DynamicCopy
Database Connection Pooling Service: SnowflakeConnectionService
SQL select query: ${copy.command}
```

### Pros:
- ✅ Highly flexible
- ✅ Easy to modify without changing NiFi
- ✅ Supports different file formats per table
- ✅ Configuration versioning

### Cons:
- ❌ Most complex setup
- ❌ Requires external configuration management

---

## Error Handling and Monitoring

### Add Error Handling to Any Solution

#### LogAttribute for Debugging
```
Processor Name: LogAttribute_Success
Log Level: INFO
Attributes to Log Regex: .*
```

#### PutFile for Failed Records
```
Processor Name: PutFile_Failures
Directory: /var/log/nifi/failures/multi-table/
Conflict Resolution Strategy: replace
```

#### UpdateAttribute for Error Tracking
```
Processor Name: UpdateAttribute_ErrorInfo
Custom Properties:
error.timestamp: ${now()}
error.table: ${target.table}
error.folder: ${current.folder}
```

---

## Performance Optimization

### 1. Parallel Processing
- Use multiple ExecuteSQLStatement processors
- Set different run schedules if tables have different update frequencies

### 2. Conditional Loading
Add RouteOnContent to check if files exist:
```sql
SELECT COUNT(*) as file_count 
FROM @YOUR_STAGE_NAME/${stage.folder}/
```

### 3. Incremental Loading
Modify COPY commands for incremental loads:
```sql
COPY INTO ${target.table} 
FROM @YOUR_STAGE_NAME/${stage.folder}/ 
FILE_FORMAT = (TYPE = 'JSON')
PATTERN = '.*${yesterday}.*'
```

---

## Monitoring and Alerting

### Key Metrics to Monitor
- Processing time per table
- Number of records loaded per table  
- Failed table loads
- Stage file counts

### Sample Monitoring Query
```sql
-- Check last load status for all tables
SELECT 
    table_name,
    last_load_time,
    rows_loaded,
    status
FROM INFORMATION_SCHEMA.COPY_HISTORY 
WHERE table_name IN ('CUSTOMERS', 'ORDERS', 'PRODUCTS', 'CUSTOMER_DETAILS', 'PRODUCT_CATALOG')
ORDER BY last_load_time DESC;
```

---

## Recommendation

**For most use cases, I recommend Solution 2 (Dynamic Routing Approach)** because:

1. ✅ Good balance of flexibility and simplicity
2. ✅ Individual table error handling
3. ✅ Easy to monitor and troubleshoot
4. ✅ Can easily add/remove tables
5. ✅ Clear separation of concerns

**Use Solution 1** if you have a small, stable set of tables and want maximum simplicity.

**Use Solution 3** if you have many tables that change frequently or need different file formats per table.

---

## Quick Start Template

Here's the minimal configuration for Solution 2:

1. **GenerateFlowFile**: Custom Text = `table1,table2,table3`
2. **UpdateAttribute**: Add `table.table1: TARGET_TABLE1` properties
3. **SplitText**: Line Split Count = 1
4. **UpdateAttribute**: Add `copy.command: COPY INTO ${table.${allLines:trim()}} FROM @STAGE/${allLines:trim()}/`
5. **ExecuteSQLStatement**: SQL = `${copy.command}`

This gives you a working multi-table loader that you can expand as needed!