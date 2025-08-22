# Snowflake OpenFlow Email-to-CSV-to-Snowflake Implementation Steps

## Quick Start Guide

This document provides the essential steps to implement the email-to-Snowflake pipeline using Snowflake OpenFlow.

## 📋 Prerequisites Checklist

- [ ] Snowflake OpenFlow instance deployed
- [ ] Snowflake account with SYSADMIN privileges
- [ ] Email account with IMAP access
- [ ] SMTP server for notifications (optional)
- [ ] AWS S3 bucket for staging (optional)

## 🚀 Implementation Steps

### Step 1: Snowflake Setup (5 minutes)
```sql
-- 1. Create database and schema
CREATE DATABASE DEMO_DB;
CREATE SCHEMA DEMO_DB.PUBLIC;

-- 2. Create target table
CREATE TABLE DEMO_DB.PUBLIC.EMPLOYEE_DATA (
    EMPLOYEE_ID INTEGER,
    FIRST_NAME VARCHAR(100),
    LAST_NAME VARCHAR(100),
    EMAIL VARCHAR(200),
    DEPARTMENT VARCHAR(100),
    HIRE_DATE DATE,
    SALARY INTEGER,
    STATUS VARCHAR(50),
    PROCESSED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_SYSTEM VARCHAR(100),
    FILE_SIZE INTEGER,
    ORIGINAL_FILENAME VARCHAR(500)
);

-- 3. Create warehouse
CREATE WAREHOUSE COMPUTE_WH WITH WAREHOUSE_SIZE = 'SMALL';
```

### Step 2: OpenFlow Configuration (10 minutes)

1. **Access OpenFlow UI**
   - Navigate to your OpenFlow instance
   - Login and access the NiFi canvas

2. **Configure Variables** (Controller Services → Variables)
   ```
   email.host = imap.gmail.com
   email.port = 993
   email.username = your-email@domain.com
   email.password = your-app-password
   snowflake.account = your-account.snowflakecomputing.com
   snowflake.username = your-username
   snowflake.password = your-password
   snowflake.warehouse = COMPUTE_WH
   ```

### Step 3: Create Data Flow (15 minutes)

**Processors to Add (in order):**

1. **GetEmail** → Configure email server connection
2. **ExtractEmailAttachments** → Filter for CSV files only
3. **RouteOnAttribute** → Route CSV files
4. **UpdateAttribute** → Add metadata
5. **ConvertRecord** → CSV to JSON conversion
6. **ValidateRecord** → Data validation
7. **PutSnowflake** → Load to Snowflake
8. **LogAttribute** → Success/error logging

**Connections:**
```
GetEmail → ExtractEmailAttachments (success)
ExtractEmailAttachments → RouteOnAttribute (attachment)  
RouteOnAttribute → UpdateAttribute (csv_files)
UpdateAttribute → ConvertRecord (success)
ConvertRecord → ValidateRecord (success)
ValidateRecord → PutSnowflake (valid)
PutSnowflake → LogAttribute (success/failure)
```

### Step 4: Controller Services (5 minutes)

1. **CSVReader**
   - Schema Access: "Infer Schema"
   - Skip Header Line: true
   - Value Separator: ","

2. **JsonRecordSetWriter**
   - Schema Access: "Inherit Record Schema"
   - Pretty Print: false

### Step 5: Testing (10 minutes)

1. **Send Test Email**
   ```bash
   python3 test_email_simulation.py
   ```

2. **Monitor Processing**
   - Check NiFi processor queues
   - Review processor statistics
   - Monitor error logs

3. **Verify in Snowflake**
   ```sql
   SELECT * FROM DEMO_DB.PUBLIC.EMPLOYEE_DATA 
   ORDER BY PROCESSED_TIMESTAMP DESC;
   ```

## 🔧 Key Configuration Files

| File | Purpose |
|------|---------|
| `sample_data.csv` | Test CSV data |
| `nifi_email_processor_configuration.json` | Email processor configs |
| `snowflake_processor_configuration.json` | Snowflake processor configs |
| `test_email_simulation.py` | Email testing script |

## ✅ Validation Checklist

After implementation, verify:

- [ ] Email connection successful
- [ ] CSV attachments extracted
- [ ] Data converted to JSON
- [ ] Records loaded to Snowflake
- [ ] Success logs generated
- [ ] Error handling works
- [ ] Notifications configured

## 🚨 Common Issues & Solutions

| Issue | Solution |
|-------|----------|
| Email connection fails | Check credentials, enable app passwords |
| CSV parsing errors | Verify CSV format, check delimiters |
| Snowflake connection fails | Validate account URL and credentials |
| Large file timeouts | Increase processor timeouts, use S3 staging |

## 📊 Performance Tuning

**For Production:**
- Set concurrent tasks: 2-4 per processor
- Batch size: 1000-5000 records
- Connection timeout: 30 seconds
- Enable auto-suspend for warehouse

## 🔐 Security Recommendations

- Use encrypted property values for passwords
- Implement key rotation policies
- Enable SSL/TLS for all connections
- Use service accounts with minimal privileges

## 📈 Monitoring

**Key Metrics:**
- Processing throughput (records/minute)
- Error rates and types
- Queue depths
- Resource utilization

**Alerts:**
- Processing failures
- Unusual data patterns
- Capacity issues

## 🔄 Next Steps

1. **Scale Testing**: Test with larger datasets
2. **Error Scenarios**: Test various failure conditions
3. **Performance**: Optimize for production volumes
4. **Security**: Implement production security measures
5. **Monitoring**: Set up comprehensive monitoring

## 📞 Support

For issues:
1. Check NiFi logs: `/opt/nifi/logs/nifi-app.log`
2. Review Snowflake query history
3. Verify email server connectivity
4. Check processor configurations

## 📚 Additional Resources

- [Snowflake OpenFlow Documentation](https://docs.snowflake.com/en/user-guide/data-integration/openflow/)
- [Apache NiFi User Guide](https://nifi.apache.org/docs.html)
- [Snowflake JDBC Driver](https://docs.snowflake.com/en/user-guide/jdbc.html)

---

**Total Implementation Time: ~45 minutes**

This POC demonstrates a production-ready solution that can be extended for additional file formats, data sources, and processing requirements.