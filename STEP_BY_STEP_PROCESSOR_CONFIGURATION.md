# Step-by-Step Processor Configuration Guide
## Snowflake OpenFlow Email-to-CSV-to-Snowflake Pipeline

This guide provides **exact configurations** for each processor in the correct order. Follow these steps exactly as written.

---

## 🔧 **STEP 1: Configure Variables First**

**Before adding processors, set up variables:**

1. Go to **Controller Services** (hamburger menu ☰ → Controller Services)
2. Click **Variables** tab
3. Add these variables:

```
email.host = imap.gmail.com
email.port = 993
email.username = your-email@domain.com
email.password = your-app-password
snowflake.account = your-account.snowflakecomputing.com
snowflake.username = your-snowflake-user
snowflake.password = your-snowflake-password
snowflake.warehouse = COMPUTE_WH
snowflake.role = SYSADMIN
snowflake_database = DEMO_DB
snowflake_schema = PUBLIC
snowflake_table = EMPLOYEE_DATA
```

---

## 📧 **STEP 2: Add GetEmail Processor**

### **2.1 Add Processor**
1. Drag **GetEmail** processor from toolbar to canvas
2. Double-click to configure

### **2.2 Properties Tab**
Set these **exact** properties:

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
| **Should Mark Read** | `true` |
| **Use SSL** | `true` |
| **Connection timeout** | `30 sec` |
| **Protocol** | `IMAPS` |

### **2.3 Scheduling Tab**
| Setting | Value |
|---------|-------|
| **Scheduling Strategy** | `TIMER_DRIVEN` |
| **Run Schedule** | `30 sec` |
| **Concurrent Tasks** | `1` |

### **2.4 Settings Tab**
- **Name**: `GetEmail - Retrieve CSV Files`
- **Auto-terminate relationships**: Leave empty (don't check any)

---

## 📎 **STEP 3: Add ExtractEmailAttachments Processor**

### **3.1 Add Processor**
1. Drag **ExtractEmailAttachments** processor to canvas
2. Double-click to configure

### **3.2 Properties Tab**
| Property | Value |
|----------|-------|
| **Attachment Filter** | `.*\.(csv\|CSV)$` |
| **Extract Attachments Only** | `true` |

### **3.3 Settings Tab**
- **Name**: `ExtractEmailAttachments - CSV Only`
- **Auto-terminate relationships**: Check ☑️ **original**

---

## 🔀 **STEP 4: Add RouteOnAttribute Processor**

### **4.1 Add Processor**
1. Drag **RouteOnAttribute** processor to canvas
2. Double-click to configure

### **4.2 Properties Tab**
| Property | Value |
|----------|-------|
| **Routing Strategy** | `Route to Property name` |

**Add Custom Property:**
- Click **+** button
- **Property Name**: `csv_files`
- **Property Value**: `${filename:endsWith('.csv'):or(${filename:endsWith('.CSV')})}`

### **4.3 Settings Tab**
- **Name**: `RouteOnAttribute - Validate CSV Files`
- **Auto-terminate relationships**: Check ☑️ **unmatched**

---

## 🏷️ **STEP 5: Add UpdateAttribute Processor**

### **5.1 Add Processor**
1. Drag **UpdateAttribute** processor to canvas
2. Double-click to configure

### **5.2 Properties Tab**
**Add these Custom Properties** (click **+** for each):

| Property Name | Property Value |
|---------------|----------------|
| **processed_timestamp** | `${now():format('yyyy-MM-dd HH:mm:ss')}` |
| **source_system** | `email_processor` |
| **file_size** | `${fileSize}` |
| **original_filename** | `${filename}` |
| **snowflake_table** | `${snowflake_table}` |
| **snowflake_schema** | `${snowflake_schema}` |
| **snowflake_database** | `${snowflake_database}` |

### **5.3 Settings Tab**
- **Name**: `UpdateAttribute - Add Metadata`

---

## 🔄 **STEP 6: Create Controller Services**

**Before adding ConvertRecord processor, create controller services:**

### **6.1 Create CSVReader Service**
1. Go to **Controller Services** (hamburger menu ☰)
2. Click **+** button
3. Search for **CSVReader**
4. Select **CSVReader** and click **Add**
5. Click **⚙️** (gear icon) to configure:

| Property | Value |
|----------|-------|
| **Schema Access Strategy** | `Infer Schema` |
| **CSV Format** | `RFC 4180` |
| **Value Separator** | `,` |
| **Skip Header Line** | `true` |
| **Quote Character** | `"` |
| **Escape Character** | `\` |
| **Comment Marker** | `#` |
| **Null String** | *(leave empty)* |
| **Trim Fields** | `true` |

6. Click **Apply**
7. Click **⚡** (lightning bolt) to **Enable**

### **6.2 Create JsonRecordSetWriter Service**
1. Click **+** button again
2. Search for **JsonRecordSetWriter**
3. Select and click **Add**
4. Click **⚙️** to configure:

| Property | Value |
|----------|-------|
| **Schema Write Strategy** | `no-schema` |
| **Schema Access Strategy** | `Inherit Record Schema` |
| **Pretty Print JSON** | `false` |
| **Suppress Null Values** | `Never Suppress` |

5. Click **Apply**
6. Click **⚡** to **Enable**

---

## 🔄 **STEP 7: Add ConvertRecord Processor**

### **7.1 Add Processor**
1. Drag **ConvertRecord** processor to canvas
2. Double-click to configure

### **7.2 Properties Tab**
| Property | Value |
|----------|-------|
| **Record Reader** | Select the **CSVReader** service you created |
| **Record Writer** | Select the **JsonRecordSetWriter** service you created |
| **Include Zero Record FlowFiles** | `false` |

### **7.3 Settings Tab**
- **Name**: `ConvertRecord - CSV to JSON`

---

## ✅ **STEP 8: Add ValidateRecord Processor**

### **8.1 Add Processor**
1. Drag **ValidateRecord** processor to canvas
2. Double-click to configure

### **8.2 Properties Tab**
| Property | Value |
|----------|-------|
| **Record Reader** | Select the **JsonTreeReader** service |
| **Record Writer** | Select the **JsonRecordSetWriter** service |
| **Schema Access Strategy** | `Use String Property` |
| **Schema Text** | `{"type":"record","name":"employee","fields":[{"name":"employee_id","type":"int"},{"name":"first_name","type":"string"},{"name":"last_name","type":"string"},{"name":"email","type":"string"},{"name":"department","type":"string"},{"name":"hire_date","type":"string"},{"name":"salary","type":"int"},{"name":"status","type":"string"}]}` |
| **Allow Extra Fields** | `true` |
| **Strict Type Checking** | `false` |

### **8.3 Settings Tab**
- **Name**: `ValidateRecord - Data Quality Check`

---

## ❄️ **STEP 9: Add PutSnowflake Processor**

### **9.1 Add Processor**
1. Drag **PutSnowflake** processor to canvas
2. Double-click to configure

### **9.2 Properties Tab**
| Property | Value |
|----------|-------|
| **Account** | `${snowflake.account}` |
| **Username** | `${snowflake.username}` |
| **Password** | `${snowflake.password}` |
| **Database** | `${snowflake_database}` |
| **Schema** | `${snowflake_schema}` |
| **Table** | `${snowflake_table}` |
| **Warehouse** | `${snowflake.warehouse}` |
| **Role** | `${snowflake.role}` |
| **Authentication Type** | `Username/Password` |
| **Connection Timeout** | `30 sec` |
| **Max Wait Time** | `300 sec` |
| **Batch Size** | `1000` |
| **Create Table** | `false` |
| **Table Structure** | `INFER_FROM_RECORDS` |

### **9.3 Scheduling Tab**
| Setting | Value |
|---------|-------|
| **Scheduling Strategy** | `EVENT_DRIVEN` |
| **Concurrent Tasks** | `2` |

### **9.4 Settings Tab**
- **Name**: `PutSnowflake - Load Data`

---

## 📝 **STEP 10: Add LogAttribute Processors (2 processors)**

### **10.1 Add Success Logger**
1. Drag **LogAttribute** processor to canvas
2. Double-click to configure

**Properties:**
| Property | Value |
|----------|-------|
| **Log Level** | `INFO` |
| **Log Payload** | `false` |
| **Attributes to Log** | `record.count,snowflake_table,processed_timestamp,file_size,original_filename` |
| **Log prefix** | `SNOWFLAKE_LOAD_SUCCESS: ` |

**Settings:**
- **Name**: `LogAttribute - Success`

### **10.2 Add Error Logger**
1. Drag another **LogAttribute** processor to canvas
2. Double-click to configure

**Properties:**
| Property | Value |
|----------|-------|
| **Log Level** | `ERROR` |
| **Log Payload** | `true` |
| **Attributes to Log** | `filename,email.from,email.subject,error.message,snowflake.error` |
| **Log prefix** | `PROCESSING_ERROR: ` |

**Settings:**
- **Name**: `LogAttribute - Errors`
- **Auto-terminate relationships**: Check ☑️ **success**

---

## 🔗 **STEP 11: Connect All Processors**

**Make these connections in order:**

### **Connection 1:**
- **From**: GetEmail
- **To**: ExtractEmailAttachments
- **Relationship**: Check ☑️ **success**

### **Connection 2:**
- **From**: ExtractEmailAttachments
- **To**: RouteOnAttribute
- **Relationship**: Check ☑️ **attachment**

### **Connection 3:**
- **From**: RouteOnAttribute
- **To**: UpdateAttribute
- **Relationship**: Check ☑️ **csv_files**

### **Connection 4:**
- **From**: UpdateAttribute
- **To**: ConvertRecord
- **Relationship**: Check ☑️ **success**

### **Connection 5:**
- **From**: ConvertRecord
- **To**: ValidateRecord
- **Relationship**: Check ☑️ **success**

### **Connection 6:**
- **From**: ValidateRecord
- **To**: PutSnowflake
- **Relationship**: Check ☑️ **valid**

### **Connection 7:**
- **From**: PutSnowflake
- **To**: LogAttribute - Success
- **Relationship**: Check ☑️ **success**

### **Connection 8:**
- **From**: PutSnowflake
- **To**: LogAttribute - Errors
- **Relationship**: Check ☑️ **failure**

### **Connection 9:**
- **From**: ValidateRecord
- **To**: LogAttribute - Errors
- **Relationship**: Check ☑️ **invalid**

### **Connection 10: Error Handling**
Connect these error relationships to **LogAttribute - Errors**:
- **From**: GetEmail → **Relationship**: failure
- **From**: ExtractEmailAttachments → **Relationship**: failure
- **From**: ConvertRecord → **Relationship**: failure

---

## 🚀 **STEP 12: Start the Flow**

**Start processors in this exact order:**

1. **LogAttribute - Errors** *(start first)*
2. **LogAttribute - Success**
3. **PutSnowflake**
4. **ValidateRecord**
5. **ConvertRecord**
6. **UpdateAttribute**
7. **RouteOnAttribute**
8. **ExtractEmailAttachments**
9. **GetEmail** *(start last)*

**To start each processor:**
- Right-click processor → **Start**

---

## 🧪 **STEP 13: Test the Flow**

### **13.1 Send Test Email**
1. Create email with CSV attachment
2. Send to your configured email address
3. Subject: "Test Employee Data"

### **13.2 Monitor Processing**
Watch these processor queues fill up:
1. **GetEmail** → **ExtractEmailAttachments**
2. **ExtractEmailAttachments** → **RouteOnAttribute**
3. **RouteOnAttribute** → **UpdateAttribute**
4. **UpdateAttribute** → **ConvertRecord**
5. **ConvertRecord** → **ValidateRecord**
6. **ValidateRecord** → **PutSnowflake**
7. **PutSnowflake** → **LogAttribute - Success**

### **13.3 Verify in Snowflake**
```sql
SELECT * FROM DEMO_DB.PUBLIC.EMPLOYEE_DATA 
ORDER BY PROCESSED_TIMESTAMP DESC;
```

---

## 🚨 **Troubleshooting Quick Fixes**

### **If GetEmail fails:**
- Check email credentials in variables
- Enable "Less secure app access" or use app password
- Verify IMAP is enabled

### **If PutSnowflake fails:**
- Verify Snowflake account URL format
- Check user permissions
- Ensure warehouse is running

### **If ConvertRecord fails:**
- Check CSV format (commas, quotes)
- Verify header row exists
- Check for special characters

---

## ✅ **Success Indicators**

You'll know it's working when:
- ✅ No red error indicators on processors
- ✅ Queue counters show data flowing
- ✅ Success logs appear in NiFi logs
- ✅ Data appears in Snowflake table
- ✅ Processed timestamp is recent

---

**🎉 That's it! Your email-to-Snowflake pipeline is ready!**

**Total Setup Time: ~45 minutes**
**Processing Time per Email: ~30-60 seconds**