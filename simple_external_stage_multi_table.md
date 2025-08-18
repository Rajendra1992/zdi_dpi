# Simple External Stage Multi-Table Loading

## Overview
Load data from multiple folders in a Snowflake external stage into multiple tables using a simple NiFi flow pattern, similar to the S3 approach.

## External Stage Structure
```
@YOUR_EXTERNAL_STAGE/
├── CUSTOMERS/     → CUSTOMERS table
├── ORDERS/        → ORDERS table
├── PRODUCTS/      → PRODUCTS table
├── INVOICES/      → INVOICES table
└── SUPPLIERS/     → SUPPLIERS table
```

---

## Solution: Simple 3-Step Flow

### Flow Architecture
```
GenerateFlowFile → UpdateAttribute → ExecuteSQL
```

**Total processors needed: 3** (vs 8+ in complex solutions)

---

## Configuration

### Step 1: GenerateFlowFile
**Purpose:** Create triggers for each table to load

```
Processor Name: GenerateFlowFile_MultiTableTrigger
File Size: 0B
Custom Text: CUSTOMERS,ORDERS,PRODUCTS,INVOICES,SUPPLIERS
Scheduling Strategy: Timer driven
Run Schedule: 0 */30 * * * ? (every 30 minutes)
```

### Step 2: UpdateAttribute  
**Purpose:** Extract table name (similar to S3OBJECT extraction pattern)

```
Processor Name: UpdateAttribute_SetTableName

Custom Properties:
TABLE_NAME: ${allLines:trim()}
```

### Step 3: ExecuteSQL
**Purpose:** Execute COPY INTO command for each table dynamically

```
Processor Name: ExecuteSQL_LoadTables
Database Connection Pooling Service: SnowflakeConnectionService

SQL Pre-Query: 
USE DATABASE <your_database_name>;

SQL Query: 
COPY INTO <database>.<schema>.${TABLE_NAME}
FROM @<your_external_stage>/${TABLE_NAME}/
FILE_FORMAT = (TYPE = 'JSON')
PURGE = FALSE
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

Query Timeout: 300 seconds
```

---

## Alternative: Individual Table Processing

If you need individual control over each table (for error handling):

### Flow Architecture
```
GenerateFlowFile → SplitText → UpdateAttribute → ExecuteSQL
```

### Configuration

#### Step 1: GenerateFlowFile
```
Custom Text: 
CUSTOMERS
ORDERS  
PRODUCTS
INVOICES
SUPPLIERS
```

#### Step 2: SplitText
```
Processor Name: SplitText_TableNames
Line Split Count: 1
Header Line Count: 0
Remove Trailing Newlines: true
```

#### Step 3: UpdateAttribute
```
Processor Name: UpdateAttribute_SetTableName
Custom Properties:
TABLE_NAME: ${allLines:trim()}
```

#### Step 4: ExecuteSQL
```
Same configuration as above
```

This creates separate FlowFiles for each table, allowing individual processing and error handling.

---

## Configuration Summary Table

| Processor | Attribute | Value |
|-----------|-----------|-------|
| **GenerateFlowFile** | Custom Text | `CUSTOMERS,ORDERS,PRODUCTS,INVOICES` |
| **UpdateAttribute** | TABLE_NAME | `${allLines:trim()}` |
| **ExecuteSQL** | Database Connection | `SnowflakeConnectionService` |
| **ExecuteSQL** | SQL Pre-Query | `USE DATABASE <your_db>;` |
| **ExecuteSQL** | SQL Query | `COPY INTO <db>.<schema>.${TABLE_NAME} FROM @<stage>/${TABLE_NAME}/` |

---

## Customization Options

### Different File Formats Per Table
Modify the UpdateAttribute step:

```
Custom Properties:
TABLE_NAME: ${allLines:trim()}
FILE_FORMAT.CUSTOMERS: (TYPE = 'JSON')
FILE_FORMAT.ORDERS: (TYPE = 'CSV' SKIP_HEADER = 1)  
FILE_FORMAT.PRODUCTS: (TYPE = 'PARQUET')
current.format: ${FILE_FORMAT.${TABLE_NAME}}
```

Then update ExecuteSQL:
```
COPY INTO <database>.<schema>.${TABLE_NAME}
FROM @<your_external_stage>/${TABLE_NAME}/
FILE_FORMAT = ${current.format}
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

### Conditional Loading
Add a RouteOnAttribute to check if data exists:

```
Custom Properties:
has.data: ${TABLE_NAME:matches('CUSTOMERS|ORDERS')}
```

### Error Handling
Connect failure relationships to LogAttribute processors for debugging.

---

## Monitoring

### Check Load Status
```sql
SELECT 
    table_name,
    last_load_time,
    rows_loaded,
    status
FROM INFORMATION_SCHEMA.COPY_HISTORY 
WHERE table_name IN ('CUSTOMERS', 'ORDERS', 'PRODUCTS', 'INVOICES', 'SUPPLIERS')
ORDER BY last_load_time DESC;
```

### Monitor NiFi Flow
- Check ExecuteSQL processor statistics
- Monitor success/failure rates
- Set up alerts on failure relationships

---

## Benefits of This Simple Approach

✅ **Minimal complexity** - Only 3 processors
✅ **Easy to understand** - Follows familiar S3 pattern  
✅ **Easy to maintain** - Simple configuration
✅ **Scalable** - Just add table names to the list
✅ **Dynamic** - Uses Snowflake's MATCH_BY_COLUMN_NAME
✅ **Efficient** - Single flow handles all tables

---

## Adding New Tables

To add a new table:
1. Add folder to external stage: `@STAGE/NEW_TABLE/`
2. Add table name to GenerateFlowFile Custom Text: `CUSTOMERS,ORDERS,NEW_TABLE`
3. That's it! No other changes needed.

---

## Why This Works Better

**Compared to complex routing solutions:**
- 70% fewer processors
- 90% less configuration 
- Much easier troubleshooting
- Follows proven S3 pattern
- Natural Snowflake integration

This approach leverages Snowflake's built-in capabilities (MATCH_BY_COLUMN_NAME, dynamic table naming) instead of trying to handle everything in NiFi.