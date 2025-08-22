# Snowflake OpenFlow Email-to-CSV-to-Snowflake POC

## Executive Summary

This Proof of Concept (POC) demonstrates how to create a Snowflake OpenFlow (Apache NiFi-based) solution that automatically processes CSV files received via email attachments and loads them into Snowflake tables. The solution provides a complete end-to-end data pipeline with error handling, monitoring, and scalability features.

## Prerequisites

### 1. Infrastructure Requirements
- **Snowflake OpenFlow Instance**: Deployed in AWS environment
- **Snowflake Account**: With appropriate database, schema, and warehouse
- **Email Server Access**: IMAP/POP3 enabled mailbox
- **AWS S3 Bucket**: For staging large files (optional but recommended)
- **SMTP Server**: For error notifications

### 2. Software Dependencies
- Apache NiFi 1.20+ (included in Snowflake OpenFlow)
- Snowflake JDBC Driver
- AWS SDK for Java (for S3 integration)

### 3. Access Requirements
- Snowflake user with SYSADMIN or ACCOUNTADMIN role
- Email account credentials
- AWS credentials (if using S3 staging)

## Step-by-Step Implementation Guide

### Step 1: Snowflake Environment Setup

#### 1.1 Create Database and Schema
```sql
-- Create database
CREATE DATABASE IF NOT EXISTS DEMO_DB;
USE DATABASE DEMO_DB;

-- Create schema
CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

-- Create target table
CREATE OR REPLACE TABLE EMPLOYEE_DATA (
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

-- Create warehouse if needed
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH 
    WITH WAREHOUSE_SIZE = 'SMALL' 
    AUTO_SUSPEND = 60 
    AUTO_RESUME = TRUE;
```

#### 1.2 Create Snowflake User for NiFi
```sql
-- Create user for NiFi integration
CREATE USER nifi_user 
    PASSWORD = 'YourSecurePassword123!' 
    DEFAULT_ROLE = 'SYSADMIN'
    DEFAULT_WAREHOUSE = 'COMPUTE_WH'
    DEFAULT_NAMESPACE = 'DEMO_DB.PUBLIC';

-- Grant necessary privileges
GRANT ROLE SYSADMIN TO USER nifi_user;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE SYSADMIN;
GRANT USAGE ON DATABASE DEMO_DB TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA DEMO_DB.PUBLIC TO ROLE SYSADMIN;
GRANT INSERT, SELECT, UPDATE ON TABLE DEMO_DB.PUBLIC.EMPLOYEE_DATA TO ROLE SYSADMIN;
```

### Step 2: OpenFlow Environment Configuration

#### 2.1 Access OpenFlow UI
1. Navigate to your Snowflake OpenFlow instance
2. Login with your credentials
3. Access the NiFi canvas interface

#### 2.2 Configure Controller Services

##### Email Configuration Variables
Navigate to **Controller Services** → **Variables**:
```
email.host = imap.gmail.com (or your email provider)
email.port = 993
email.username = your-email@domain.com
email.password = your-app-password
```

##### Snowflake Configuration Variables
```
snowflake.account = your-account.snowflakecomputing.com
snowflake.username = nifi_user
snowflake.password = YourSecurePassword123!
snowflake.warehouse = COMPUTE_WH
snowflake.role = SYSADMIN
snowflake_database = DEMO_DB
snowflake_schema = PUBLIC
snowflake_table = EMPLOYEE_DATA
```

##### SMTP Configuration (for notifications)
```
smtp.host = smtp.gmail.com
smtp.port = 587
smtp.username = notifications@yourdomain.com
smtp.password = your-smtp-password
notification.from.email = nifi-alerts@yourdomain.com
notification.to.email = admin@yourdomain.com
```

### Step 3: Create the Data Flow

#### 3.1 Email Processing Processors

1. **Add GetEmail Processor**
   - Drag `GetEmail` processor to canvas
   - Configure properties from `nifi_email_processor_configuration.json`
   - Set scheduling to run every 30 seconds

2. **Add ExtractEmailAttachments Processor**
   - Drag `ExtractEmailAttachments` processor to canvas
   - Configure attachment filter: `.*\.(csv|CSV)$`
   - Set "Extract Attachments Only" to true

3. **Add RouteOnAttribute Processor**
   - Drag `RouteOnAttribute` processor to canvas
   - Add routing rule: `csv_files = ${filename:endsWith('.csv'):or(${filename:endsWith('.CSV')})}`

#### 3.2 Data Processing Processors

4. **Add UpdateAttribute Processor**
   - Drag `UpdateAttribute` processor to canvas
   - Add metadata attributes as defined in configuration

5. **Add ConvertRecord Processor**
   - Drag `ConvertRecord` processor to canvas
   - Configure CSV Reader and JSON Writer controller services
   - Set to infer schema from CSV headers

6. **Add ValidateRecord Processor**
   - Drag `ValidateRecord` processor to canvas
   - Configure Avro schema for data validation

#### 3.3 Snowflake Integration Processors

7. **Add PutSnowflake Processor**
   - Drag `PutSnowflake` processor to canvas
   - Configure Snowflake connection properties
   - Set batch size to 1000 records

8. **Add LogAttribute Processors**
   - Add success logging processor
   - Add error logging processor
   - Configure appropriate log levels and attributes

### Step 4: Configure Controller Services

#### 4.1 CSV Reader Service
```json
{
  "type": "org.apache.nifi.csv.CSVReader",
  "properties": {
    "Schema Access Strategy": "Infer Schema",
    "CSV Format": "RFC 4180",
    "Skip Header Line": "true",
    "Value Separator": ",",
    "Trim Fields": "true"
  }
}
```

#### 4.2 JSON Writer Service
```json
{
  "type": "org.apache.nifi.json.JsonRecordSetWriter",
  "properties": {
    "Schema Access Strategy": "Inherit Record Schema",
    "Pretty Print JSON": "false"
  }
}
```

### Step 5: Connect Processors

Create connections between processors as defined in the configuration files:

1. **GetEmail** → **ExtractEmailAttachments** (success)
2. **ExtractEmailAttachments** → **RouteOnAttribute** (attachment)
3. **RouteOnAttribute** → **UpdateAttribute** (csv_files)
4. **UpdateAttribute** → **ConvertRecord** (success)
5. **ConvertRecord** → **ValidateRecord** (success)
6. **ValidateRecord** → **PutSnowflake** (valid)
7. **PutSnowflake** → **LogAttribute_Success** (success)
8. **PutSnowflake** → **LogAttribute_Error** (failure)

### Step 6: Error Handling Configuration

#### 6.1 Configure Error Relationships
- Connect all failure relationships to error logging processors
- Set up email notifications for critical errors
- Configure retry logic for transient failures

#### 6.2 Dead Letter Queue
- Create a separate process group for failed records
- Implement manual review and reprocessing capabilities

### Step 7: Testing the POC

#### 7.1 Prepare Test Email
1. Create an email with the sample CSV file (`sample_data.csv`) as attachment
2. Send to the configured email address
3. Use subject line: "Employee Data Update - Test"

#### 7.2 Monitor the Flow
1. Start all processors in the correct order
2. Monitor processor queues and statistics
3. Check NiFi logs for any errors
4. Verify data in Snowflake table

#### 7.3 Validation Queries
```sql
-- Check loaded data
SELECT * FROM DEMO_DB.PUBLIC.EMPLOYEE_DATA 
ORDER BY PROCESSED_TIMESTAMP DESC;

-- Verify record counts
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT ORIGINAL_FILENAME) as unique_files,
    MAX(PROCESSED_TIMESTAMP) as last_processed
FROM DEMO_DB.PUBLIC.EMPLOYEE_DATA;

-- Check for any data quality issues
SELECT 
    DEPARTMENT,
    COUNT(*) as record_count,
    AVG(SALARY) as avg_salary
FROM DEMO_DB.PUBLIC.EMPLOYEE_DATA
GROUP BY DEPARTMENT;
```

## Performance Considerations

### 1. Batch Processing
- Configure appropriate batch sizes (1000-5000 records)
- Use bulk loading for large files (>10MB)
- Consider S3 staging for very large files

### 2. Concurrency Settings
- Set concurrent tasks based on system resources
- Monitor CPU and memory usage
- Adjust processor scheduling as needed

### 3. Connection Pooling
- Configure database connection pools
- Set appropriate connection timeouts
- Monitor connection usage

## Security Best Practices

### 1. Credential Management
- Use encrypted property values in NiFi
- Implement key rotation policies
- Use service accounts with minimal privileges

### 2. Data Protection
- Enable SSL/TLS for all connections
- Implement data masking for sensitive fields
- Use Snowflake's built-in encryption features

### 3. Network Security
- Configure VPC endpoints where possible
- Implement proper firewall rules
- Monitor network traffic

## Monitoring and Alerting

### 1. Key Metrics to Monitor
- Processing throughput (records/minute)
- Error rates and types
- Queue depths and processing times
- Resource utilization (CPU, memory, disk)

### 2. Alert Configuration
- Set up alerts for processing failures
- Monitor for unusual data patterns
- Configure capacity alerts

### 3. Dashboard Creation
- Create NiFi monitoring dashboards
- Set up Snowflake usage monitoring
- Implement end-to-end pipeline visibility

## Troubleshooting Guide

### Common Issues and Solutions

#### 1. Email Connection Issues
- **Problem**: Cannot connect to email server
- **Solution**: Verify credentials, enable app passwords, check firewall rules

#### 2. CSV Parsing Errors
- **Problem**: CSV format not recognized
- **Solution**: Adjust CSV reader settings, handle different delimiters, validate file encoding

#### 3. Snowflake Connection Failures
- **Problem**: Cannot connect to Snowflake
- **Solution**: Verify account URL, check user privileges, validate network connectivity

#### 4. Large File Processing
- **Problem**: Out of memory errors with large files
- **Solution**: Increase JVM heap size, implement file splitting, use S3 staging

## Scaling Considerations

### 1. Horizontal Scaling
- Deploy multiple NiFi instances
- Implement load balancing
- Use clustering for high availability

### 2. Vertical Scaling
- Increase processor concurrent tasks
- Optimize JVM settings
- Add more CPU/memory resources

### 3. Data Volume Handling
- Implement data archiving strategies
- Use Snowflake clustering keys
- Consider data partitioning

## Cost Optimization

### 1. Snowflake Costs
- Use auto-suspend for warehouses
- Implement appropriate warehouse sizing
- Monitor credit usage

### 2. Infrastructure Costs
- Right-size OpenFlow instances
- Use spot instances where appropriate
- Implement resource scheduling

## Next Steps and Enhancements

### 1. Advanced Features
- Implement data deduplication
- Add data lineage tracking
- Create automated data quality checks

### 2. Integration Enhancements
- Add support for multiple file formats
- Implement real-time streaming
- Create API endpoints for external systems

### 3. Operational Improvements
- Automate deployment processes
- Implement CI/CD pipelines
- Create automated testing frameworks

## Conclusion

This POC demonstrates a robust, scalable solution for processing CSV files from email attachments and loading them into Snowflake. The architecture provides:

- **Reliability**: Comprehensive error handling and retry logic
- **Scalability**: Horizontal and vertical scaling capabilities
- **Security**: End-to-end encryption and secure credential management
- **Monitoring**: Full observability and alerting capabilities
- **Maintainability**: Modular design and comprehensive documentation

The solution is production-ready and can be extended to handle additional file formats, data sources, and processing requirements.

## Appendix

### A. Sample Files
- `sample_data.csv`: Test CSV file with employee data
- `nifi_email_processor_configuration.json`: Email processor configurations
- `snowflake_processor_configuration.json`: Snowflake processor configurations

### B. SQL Scripts
- Database and table creation scripts
- User and role setup scripts
- Data validation queries

### C. Configuration Templates
- NiFi flow templates
- Controller service configurations
- Environment variable templates

### D. Monitoring Queries
- Performance monitoring queries
- Error analysis scripts
- Capacity planning queries