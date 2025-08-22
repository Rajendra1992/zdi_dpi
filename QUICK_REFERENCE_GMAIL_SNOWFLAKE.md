# 📋 Quick Reference: Gmail CSV → Snowflake

## 🎯 **What This Does**
Automatically processes CSV files from Gmail attachments and loads them directly into Snowflake tables. **No format conversion needed.**

---

## ⚡ **Quick Setup (30 minutes)**

### **1. Gmail Setup (5 min)**
```
1. Enable IMAP: Gmail Settings → Forwarding and POP/IMAP → Enable IMAP
2. Enable 2-Step Verification: Google Account → Security
3. Generate App Password: Security → App passwords → Mail
4. Note down the 16-character app password
```

### **2. Snowflake Setup (5 min)**
```sql
CREATE DATABASE DEMO_DB;
CREATE SCHEMA DEMO_DB.PUBLIC;
CREATE TABLE DEMO_DB.PUBLIC.EMPLOYEE_DATA (
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
CREATE WAREHOUSE COMPUTE_WH WITH WAREHOUSE_SIZE = 'SMALL';
```

### **3. OpenFlow Variables (2 min)**
Go to Controller Services → Variables:
```
email.host = imap.gmail.com
email.port = 993
email.username = your-gmail@gmail.com
email.password = your-16-char-app-password
snowflake.account = your-account.snowflakecomputing.com
snowflake.username = your-snowflake-user
snowflake.password = your-snowflake-password
snowflake.warehouse = COMPUTE_WH
snowflake.database = DEMO_DB
snowflake.schema = PUBLIC
snowflake.table = EMPLOYEE_DATA
```

### **4. Add 6 Processors (15 min)**

| Order | Processor | Key Settings |
|-------|-----------|--------------|
| 1 | **GetEmail** | Host: `${email.host}`, Protocol: IMAPS, SSL: true |
| 2 | **ExtractEmailAttachments** | Filter: `.*\.csv$`, Extract Only: true |
| 3 | **RouteOnAttribute** | Custom property: `valid_csv` = `${filename:endsWith('.csv')}` |
| 4 | **PutSnowflake** | Direct CSV load, Skip Header: true, Batch: 1000 |
| 5 | **LogAttribute (Success)** | Level: INFO, Attributes: filename,fileSize |
| 6 | **LogAttribute (Errors)** | Level: ERROR, Log Payload: true |

### **5. Make 7 Connections (3 min)**
```
GetEmail → ExtractEmailAttachments (success)
ExtractEmailAttachments → RouteOnAttribute (attachment)
RouteOnAttribute → PutSnowflake (valid_csv)
PutSnowflake → LogAttribute-Success (success)
PutSnowflake → LogAttribute-Errors (failure)
GetEmail → LogAttribute-Errors (failure)
ExtractEmailAttachments → LogAttribute-Errors (failure)
```

---

## 🧪 **Test the Pipeline**

### **Send Test Email:**
1. Update `gmail_test_email.py` with your Gmail credentials
2. Run: `python3 gmail_test_email.py`
3. Or manually: Send email with `sample_data.csv` attached

### **Monitor Processing:**
```
1. GetEmail (every 30 sec) → retrieves email
2. ExtractEmailAttachments → extracts CSV
3. RouteOnAttribute → validates CSV
4. PutSnowflake → loads to Snowflake
5. LogAttribute → logs success/errors
```

### **Verify Results:**
```sql
SELECT * FROM DEMO_DB.PUBLIC.EMPLOYEE_DATA 
ORDER BY LOAD_TIMESTAMP DESC;
```

---

## 🚨 **Troubleshooting**

| Problem | Solution |
|---------|----------|
| **Gmail auth fails** | Use App Password (not regular password) |
| **No emails retrieved** | Check IMAP enabled, credentials correct |
| **CSV not extracted** | Verify attachment filter: `.*\.csv$` |
| **Snowflake connection fails** | Check account URL format, credentials |
| **No data in Snowflake** | Verify CSV headers match table columns |

---

## ✅ **Success Indicators**

**You'll know it's working when:**
- ✅ All processors show green play button
- ✅ Queue counters increment between processors
- ✅ Gmail emails marked as read
- ✅ Success logs in NiFi logs
- ✅ New records in Snowflake table

---

## 📊 **Performance**

| Metric | Expected Value |
|--------|----------------|
| **Setup Time** | ~30 minutes |
| **Processing Time** | 15-30 seconds per email |
| **Throughput** | 1000+ records per batch |
| **File Size Limit** | Up to 25MB (Gmail limit) |

---

## 🔄 **Processing Flow**
```
📧 Gmail INBOX
    ↓ (every 30 sec)
📎 Extract CSV attachment
    ↓
🔀 Validate CSV file
    ↓
❄️ Load directly to Snowflake
    ↓
📝 Log success/errors
```

---

## 🛠️ **Key Files**

| File | Purpose |
|------|---------|
| `GMAIL_CSV_TO_SNOWFLAKE_PROCESSORS.md` | **Complete processor guide** |
| `sample_data.csv` | Test CSV data (10 employee records) |
| `gmail_test_email.py` | Send test emails with CSV attachments |

---

## 🎯 **Next Steps After Setup**

1. **Test with your own CSV files**
2. **Scale up batch sizes for production**
3. **Add more error handling if needed**
4. **Set up monitoring dashboards**
5. **Configure email notifications for failures**

---

**🚀 Result: Production-ready Gmail → Snowflake CSV pipeline in 30 minutes!**