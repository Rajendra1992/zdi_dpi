# Gmail CSV to Snowflake - Direct Processing (No Format Conversion)

## 🎯 **Simplified Flow for CSV-Only Processing**
```
📧 GetEmail (Gmail) → 📎 ExtractEmailAttachments → 🔀 RouteOnAttribute → ❄️ PutSnowflake → 📝 LogAttribute
```

---

## ⚙️ **STEP 1: Configure Variables**

Go to **Controller Services** → **Variables** and add:

```
# Gmail Settings
email.host = imap.gmail.com
email.port = 993
email.username = your-gmail@gmail.com
email.password = your-app-password

# Snowflake Settings
snowflake.account = your-account.snowflakecomputing.com
snowflake.username = your-snowflake-user
snowflake.password = your-snowflake-password
snowflake.warehouse = COMPUTE_WH
snowflake.database = DEMO_DB
snowflake.schema = PUBLIC
snowflake.table = EMPLOYEE_DATA
```

---

## 📧 **PROCESSOR 1: GetEmail**

### **Add Processor:**
- Drag **GetEmail** to canvas
- Name: `GetEmail - Gmail CSV Retrieval`

### **Properties:**
| Property | Value |
|----------|-------|
| **Host** | `${email.host}` |
| **Port** | `${email.port}` |
| **Username** | `${email.username}` |
| **Password** | `${email.password}` |
| **Folder** | `INBOX` |
| **Fetch Size** | `10` |
| **Delete Messages** | `false` |
| **Mark Messages as Read** | `true` |
| **Use SSL** | `true` |
| **Protocol** | `IMAPS` |
| **Connection timeout** | `30 sec` |

### **Scheduling:**
- **Strategy**: `TIMER_DRIVEN`
- **Run Schedule**: `30 sec`
- **Concurrent Tasks**: `1`

---

## 📎 **PROCESSOR 2: ExtractEmailAttachments**

### **Add Processor:**
- Drag **ExtractEmailAttachments** to canvas
- Name: `ExtractEmailAttachments - CSV Files`

### **Properties:**
| Property | Value |
|----------|-------|
| **Attachment Filter** | `.*\.csv$` |
| **Extract Attachments Only** | `true` |

### **Settings:**
- **Auto-terminate relationships**: ☑️ **original**

---

## 🔀 **PROCESSOR 3: RouteOnAttribute**

### **Add Processor:**
- Drag **RouteOnAttribute** to canvas
- Name: `RouteOnAttribute - CSV Validation`

### **Properties:**
| Property | Value |
|----------|-------|
| **Routing Strategy** | `Route to Property name` |

**Add Custom Property:**
- Click **+** button
- **Property Name**: `valid_csv`
- **Property Value**: `${filename:endsWith('.csv')}`

### **Settings:**
- **Auto-terminate relationships**: ☑️ **unmatched**

---

## ❄️ **PROCESSOR 4: PutSnowflake**

### **Add Processor:**
- Drag **PutSnowflake** to canvas
- Name: `PutSnowflake - Direct CSV Load`

### **Properties:**
| Property | Value |
|----------|-------|
| **Account** | `${snowflake.account}` |
| **Username** | `${snowflake.username}` |
| **Password** | `${snowflake.password}` |
| **Database** | `${snowflake.database}` |
| **Schema** | `${snowflake.schema}` |
| **Table** | `${snowflake.table}` |
| **Warehouse** | `${snowflake.warehouse}` |
| **Authentication Type** | `Username/Password` |
| **File Format** | `CSV` |
| **Field Delimiter** | `,` |
| **Skip Header** | `true` |
| **Batch Size** | `1000` |
| **Connection Timeout** | `30 sec` |

### **Scheduling:**
- **Strategy**: `EVENT_DRIVEN`
- **Concurrent Tasks**: `2`

---

## 📝 **PROCESSOR 5: LogAttribute (Success)**

### **Add Processor:**
- Drag **LogAttribute** to canvas
- Name: `LogAttribute - Success`

### **Properties:**
| Property | Value |
|----------|-------|
| **Log Level** | `INFO` |
| **Log Payload** | `false` |
| **Attributes to Log** | `filename,fileSize,snowflake.table` |
| **Log prefix** | `SUCCESS: CSV loaded to Snowflake - ` |

---

## 📝 **PROCESSOR 6: LogAttribute (Errors)**

### **Add Processor:**
- Drag **LogAttribute** to canvas
- Name: `LogAttribute - Errors`

### **Properties:**
| Property | Value |
|----------|-------|
| **Log Level** | `ERROR` |
| **Log Payload** | `true` |
| **Attributes to Log** | `filename,error.message` |
| **Log prefix** | `ERROR: Failed to process - ` |

### **Settings:**
- **Auto-terminate relationships**: ☑️ **success**

---

## 🔗 **CONNECTIONS**

Make these connections:

1. **GetEmail** → **ExtractEmailAttachments**
   - Relationship: ☑️ **success**

2. **ExtractEmailAttachments** → **RouteOnAttribute**
   - Relationship: ☑️ **attachment**

3. **RouteOnAttribute** → **PutSnowflake**
   - Relationship: ☑️ **valid_csv**

4. **PutSnowflake** → **LogAttribute - Success**
   - Relationship: ☑️ **success**

5. **PutSnowflake** → **LogAttribute - Errors**
   - Relationship: ☑️ **failure**

6. **GetEmail** → **LogAttribute - Errors**
   - Relationship: ☑️ **failure**

7. **ExtractEmailAttachments** → **LogAttribute - Errors**
   - Relationship: ☑️ **failure**

---

## 🏗️ **Snowflake Table Setup**

Run this SQL in Snowflake:

```sql
-- Create database and schema
CREATE DATABASE IF NOT EXISTS DEMO_DB;
USE DATABASE DEMO_DB;
CREATE SCHEMA IF NOT EXISTS PUBLIC;

-- Create table matching your CSV structure
CREATE OR REPLACE TABLE EMPLOYEE_DATA (
    EMPLOYEE_ID INTEGER,
    FIRST_NAME VARCHAR(100),
    LAST_NAME VARCHAR(100),
    EMAIL VARCHAR(200),
    DEPARTMENT VARCHAR(100),
    HIRE_DATE DATE,
    SALARY INTEGER,
    STATUS VARCHAR(50),
    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH 
WITH WAREHOUSE_SIZE = 'SMALL' 
AUTO_SUSPEND = 60;
```

---

## 📧 **Gmail Setup**

### **Enable App Password:**
1. Go to Google Account settings
2. Security → 2-Step Verification → App passwords
3. Generate app password for "Mail"
4. Use this password in NiFi (not your regular Gmail password)

### **Enable IMAP:**
1. Gmail Settings → Forwarding and POP/IMAP
2. Enable IMAP access

---

## 🚀 **Start the Flow**

**Start processors in this order:**
1. LogAttribute - Errors
2. LogAttribute - Success  
3. PutSnowflake
4. RouteOnAttribute
5. ExtractEmailAttachments
6. GetEmail

---

## 🧪 **Test the Setup**

### **1. Create Test Email**
- Subject: "Employee Data Update"
- Attach the `sample_data.csv` file
- Send to your Gmail account

### **2. Monitor Flow**
Watch the processor queues:
- GetEmail should retrieve the email
- ExtractEmailAttachments should extract CSV
- RouteOnAttribute should route to valid_csv
- PutSnowflake should load directly to Snowflake

### **3. Verify in Snowflake**
```sql
SELECT * FROM DEMO_DB.PUBLIC.EMPLOYEE_DATA 
ORDER BY LOAD_TIMESTAMP DESC;
```

---

## 🔧 **Key Benefits of This Simplified Approach**

✅ **No Format Conversion** - Direct CSV to Snowflake  
✅ **Fewer Processors** - Only 6 processors needed  
✅ **Gmail Optimized** - Configured specifically for Gmail  
✅ **Faster Processing** - No intermediate transformations  
✅ **Simple Troubleshooting** - Fewer components to debug  

---

## 🚨 **Troubleshooting**

### **Gmail Connection Issues:**
```
Error: Authentication failed
Solution: Use App Password, not regular password
```

### **CSV Not Loading:**
```
Error: Schema mismatch
Solution: Ensure CSV headers match Snowflake table columns
```

### **PutSnowflake Fails:**
```
Error: Connection timeout
Solution: Check Snowflake account URL format
```

---

## ✅ **Success Checklist**

- [ ] Gmail IMAP enabled
- [ ] App password generated  
- [ ] Variables configured
- [ ] All 6 processors added
- [ ] Connections made
- [ ] Snowflake table created
- [ ] Processors started in order
- [ ] Test email sent
- [ ] Data appears in Snowflake

**Total Setup Time: ~30 minutes**  
**Processing Time: ~15-30 seconds per email**