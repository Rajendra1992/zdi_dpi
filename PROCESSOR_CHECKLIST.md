# 📋 Processor Setup Checklist

## Quick Visual Flow
```
📧 GetEmail → 📎 ExtractEmailAttachments → 🔀 RouteOnAttribute → 🏷️ UpdateAttribute 
    ↓
📝 LogAttribute(Error) ← 🔄 ConvertRecord → ✅ ValidateRecord → ❄️ PutSnowflake → 📝 LogAttribute(Success)
```

## ✅ Step-by-Step Checklist

### **Pre-Setup (5 minutes)**
- [ ] **Step 1**: Set up Variables in Controller Services
  - [ ] Email settings (host, port, username, password)
  - [ ] Snowflake settings (account, username, password, warehouse)
  - [ ] Database settings (database, schema, table)

### **Add Processors (20 minutes)**
- [ ] **Step 2**: Add **GetEmail** processor
  - [ ] Configure email connection properties
  - [ ] Set scheduling to 30 seconds
  - [ ] Name: "GetEmail - Retrieve CSV Files"

- [ ] **Step 3**: Add **ExtractEmailAttachments** processor  
  - [ ] Set attachment filter: `.*\.(csv|CSV)$`
  - [ ] Enable "Extract Attachments Only"
  - [ ] Auto-terminate "original" relationship

- [ ] **Step 4**: Add **RouteOnAttribute** processor
  - [ ] Add custom property: `csv_files` = `${filename:endsWith('.csv'):or(${filename:endsWith('.CSV')})}`
  - [ ] Auto-terminate "unmatched" relationship

- [ ] **Step 5**: Add **UpdateAttribute** processor
  - [ ] Add 7 custom properties (timestamp, source_system, file_size, etc.)

### **Controller Services (10 minutes)**
- [ ] **Step 6**: Create **CSVReader** service
  - [ ] Set "Infer Schema", skip header, comma separator
  - [ ] Enable the service

- [ ] **Step 6**: Create **JsonRecordSetWriter** service  
  - [ ] Set "Inherit Record Schema", no pretty print
  - [ ] Enable the service

### **More Processors (10 minutes)**
- [ ] **Step 7**: Add **ConvertRecord** processor
  - [ ] Link to CSVReader and JsonRecordSetWriter services

- [ ] **Step 8**: Add **ValidateRecord** processor
  - [ ] Set schema for employee data validation

- [ ] **Step 9**: Add **PutSnowflake** processor
  - [ ] Configure all Snowflake connection properties
  - [ ] Set batch size to 1000

- [ ] **Step 10**: Add 2 **LogAttribute** processors
  - [ ] One for success (INFO level)
  - [ ] One for errors (ERROR level)

### **Connect Everything (5 minutes)**
- [ ] **Step 11**: Make 10 connections between processors
  - [ ] GetEmail → ExtractEmailAttachments (success)
  - [ ] ExtractEmailAttachments → RouteOnAttribute (attachment)
  - [ ] RouteOnAttribute → UpdateAttribute (csv_files)
  - [ ] UpdateAttribute → ConvertRecord (success)
  - [ ] ConvertRecord → ValidateRecord (success)
  - [ ] ValidateRecord → PutSnowflake (valid)
  - [ ] PutSnowflake → LogAttribute-Success (success)
  - [ ] PutSnowflake → LogAttribute-Errors (failure)
  - [ ] ValidateRecord → LogAttribute-Errors (invalid)
  - [ ] Error connections from other processors

### **Start & Test (5 minutes)**
- [ ] **Step 12**: Start processors in reverse order (errors first, GetEmail last)
- [ ] **Step 13**: Send test email with CSV attachment
- [ ] **Step 13**: Monitor queues and check Snowflake table

---

## 🔧 **Quick Reference: Key Properties**

### **Variables to Set:**
```
email.host = imap.gmail.com
email.port = 993
email.username = your-email@domain.com
email.password = your-app-password
snowflake.account = your-account.snowflakecomputing.com
snowflake.username = your-snowflake-user
snowflake.password = your-snowflake-password
snowflake.warehouse = COMPUTE_WH
snowflake_database = DEMO_DB
snowflake_schema = PUBLIC
snowflake_table = EMPLOYEE_DATA
```

### **Critical Settings:**
- **GetEmail**: IMAPS protocol, SSL enabled, 30-sec schedule
- **ExtractEmailAttachments**: Filter `.*\.(csv|CSV)$`
- **RouteOnAttribute**: Custom property `csv_files`
- **PutSnowflake**: Batch size 1000, EVENT_DRIVEN

### **Controller Services:**
- **CSVReader**: Infer Schema, Skip Header = true
- **JsonRecordSetWriter**: Inherit Schema, Pretty Print = false

---

## 🚨 **Common Mistakes to Avoid**

❌ **Don't do this:**
- Starting GetEmail processor first
- Forgetting to enable controller services
- Using wrong relationship names in connections
- Not setting auto-terminate relationships

✅ **Do this:**
- Start processors in reverse order (downstream first)
- Enable all controller services before starting
- Double-check connection relationships
- Set auto-terminate on unused relationships

---

## 📊 **Success Checklist**

After setup, you should see:
- [ ] All processors show green "play" button (not red stop)
- [ ] No warning triangles on processors
- [ ] Controller services show "enabled" status
- [ ] Variables are properly referenced (${variable.name})

After sending test email:
- [ ] Queue counters increment between processors
- [ ] Success logs appear in NiFi logs
- [ ] Data appears in Snowflake table
- [ ] No error logs in LogAttribute-Errors

---

**🎯 Total Time: ~55 minutes**
**Result: Production-ready email-to-Snowflake pipeline**