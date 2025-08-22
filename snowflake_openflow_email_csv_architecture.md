# Snowflake OpenFlow: Email CSV to Snowflake Pipeline Architecture

## Overview
This document outlines the architecture and implementation for a Snowflake OpenFlow (Apache NiFi-based) solution that processes CSV files received via email attachments and loads them into Snowflake tables.

## Architecture Components

### 1. Email Processing Layer
- **GetEmail Processor**: Connects to email server (IMAP/POP3)
- **ExtractEmailAttachments Processor**: Extracts CSV attachments from emails
- **ValidateAttachment Processor**: Validates file format and content

### 2. Data Processing Layer
- **ConvertRecord Processor**: Converts CSV to structured format (Avro/JSON)
- **UpdateAttribute Processor**: Adds metadata and routing attributes
- **SplitRecord Processor**: Handles large files by splitting into chunks

### 3. Snowflake Integration Layer
- **PutSnowflake Processor**: Loads data directly to Snowflake
- **Alternative**: PutS3Object + Snowflake COPY command for large files

### 4. Monitoring and Error Handling
- **LogAttribute Processor**: Comprehensive logging
- **PutEmail Processor**: Error notifications
- **RetryFlowFile Processor**: Automated retry logic

## Data Flow Architecture

```
Email Server → GetEmail → ExtractEmailAttachments → ValidateAttachment
                                    ↓
UpdateAttribute ← ConvertRecord ← RouteOnContent
      ↓
PutSnowflake → LogAttribute (Success)
      ↓
Error Handling → PutEmail (Notifications)
```

## Key Features

### 1. Multi-Format Support
- CSV files with various delimiters
- Excel files (.xlsx, .xls) - with additional processors
- Compressed files (.zip, .gz)

### 2. Data Validation
- Schema validation against target Snowflake table
- Data quality checks (null values, data types)
- File integrity verification

### 3. Error Handling
- Comprehensive error logging
- Automatic retry mechanisms
- Email notifications for failures
- Dead letter queue for failed records

### 4. Scalability
- Parallel processing capabilities
- Large file handling with chunking
- Load balancing across multiple flow instances

## Security Considerations

### 1. Email Authentication
- OAuth2 for modern email providers
- SSL/TLS encryption for email connections
- Secure credential storage in NiFi

### 2. Snowflake Security
- Key-pair authentication recommended
- Role-based access control (RBAC)
- Network policies and IP whitelisting

### 3. Data Protection
- Encryption in transit and at rest
- PII data masking capabilities
- Audit logging for compliance

## Performance Optimization

### 1. Batch Processing
- Accumulate multiple files before processing
- Bulk insert operations to Snowflake
- Optimal batch sizes based on data volume

### 2. Resource Management
- CPU and memory allocation tuning
- Connection pooling for database connections
- Queue management and backpressure handling

### 3. Monitoring Metrics
- Processing throughput (records/minute)
- Error rates and retry statistics
- Resource utilization metrics
- End-to-end latency tracking