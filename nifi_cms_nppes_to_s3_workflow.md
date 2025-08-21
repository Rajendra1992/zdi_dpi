# NiFi Workflow: Download CMS NPPES NPI Files to AWS S3

## Overview
This document provides complete steps to create a NiFi flow that downloads monthly or weekly NPI files from the CMS NPPES website (https://download.cms.gov/nppes/NPI_Files.html) and uploads them to AWS S3.

## Prerequisites
- Apache NiFi installed and running
- AWS S3 bucket created
- AWS credentials with S3 write permissions
- Network access to both CMS website and AWS S3

## Step-by-Step Implementation

### Step 1: Configure AWS Credentials in NiFi

1. **Access Controller Services:**
   - Open NiFi UI (typically http://localhost:8080/nifi)
   - Click on the hamburger menu (☰) in the top-right corner
   - Select "Controller Services"

2. **Add AWS Credentials Provider:**
   - Click the "+" button to add a new service
   - Search for "AWSCredentialsProviderControllerService"
   - Click "Add"

3. **Configure AWS Credentials:**
   - Click the gear icon (⚙️) to configure the service
   - Set the following properties:
     - **Access Key**: Your AWS Access Key ID
     - **Secret Key**: Your AWS Secret Access Key
     - **Region**: Your AWS region (e.g., us-east-1)
   - Click "Apply"
   - Click the lightning bolt (⚡) to enable the service
   - Click "Enable"

### Step 2: Create the NiFi Flow

#### 2.1 Add InvokeHTTP Processor

1. **Drag InvokeHTTP processor** onto the canvas from the processor toolbar
2. **Configure InvokeHTTP processor:**
   - Double-click the processor to open configuration
   - Go to "Properties" tab:
     - **HTTP Method**: GET
     - **Remote URL**: Use one of these URL patterns:
       - Monthly file: `https://download.cms.gov/nppes/NPPES_Data_Dissemination_${now():format('MMMM_dd_yyyy')}.zip`
       - Or use specific URL: `https://download.cms.gov/nppes/NPPES_Data_Dissemination_January_2025.zip`
       - Weekly file: `https://download.cms.gov/nppes/NPPES_Data_Dissemination_Weekly_${now():format('MMMM_dd_yyyy')}.zip`
     - **Follow Redirects**: true
     - **Use Chunked Encoding**: false
     - **Connection Timeout**: 30 sec
     - **Read Timeout**: 300 sec (5 minutes for large files)
     - **Include Date Header**: true
   - Go to "Scheduling" tab:
     - **Run Schedule**: Set based on your needs:
       - For monthly: `0 0 1 * * ?` (1st day of each month)
       - For weekly: `0 0 * * 1 ?` (Every Monday)
   - Click "Apply"

#### 2.2 Add UpdateAttribute Processor (Optional - for dynamic naming)

1. **Drag UpdateAttribute processor** onto the canvas
2. **Configure UpdateAttribute processor:**
   - Double-click to configure
   - Go to "Properties" tab and add custom properties:
     - **filename**: `NPPES_${now():format('yyyy-MM-dd')}.zip`
     - **s3.object.key**: `npi-files/${now():format('yyyy/MM')}/NPPES_${now():format('yyyy-MM-dd')}.zip`
   - Click "Apply"

#### 2.3 Add PutS3Object Processor

1. **Drag PutS3Object processor** onto the canvas
2. **Configure PutS3Object processor:**
   - Double-click to configure
   - Go to "Properties" tab:
     - **Bucket**: Your S3 bucket name (e.g., `my-npi-data-bucket`)
     - **Object Key**: `${s3.object.key}` (if using UpdateAttribute) or `npi-files/${filename}`
     - **AWS Credentials Provider Service**: Select the AWS credentials service created in Step 1
     - **Region**: Your AWS region (e.g., us-east-1)
     - **Storage Class**: STANDARD (or choose based on your needs)
     - **Server Side Encryption**: Choose if needed (AES256 or aws:kms)
   - Click "Apply"

#### 2.4 Add LogAttribute Processor (for monitoring)

1. **Drag LogAttribute processor** onto the canvas
2. **Configure LogAttribute processor:**
   - Double-click to configure
   - Go to "Properties" tab:
     - **Log Level**: INFO
     - **Log Payload**: false (files are large)
     - **Attributes to Log**: filename,s3.object.key,s3.bucket
   - Click "Apply"

### Step 3: Connect Processors

1. **Connect InvokeHTTP to UpdateAttribute:**
   - Drag from InvokeHTTP to UpdateAttribute
   - Select "Response" relationship
   - Click "Add"

2. **Connect UpdateAttribute to PutS3Object:**
   - Drag from UpdateAttribute to PutS3Object
   - Select "success" relationship
   - Click "Add"

3. **Connect PutS3Object to LogAttribute:**
   - Drag from PutS3Object to LogAttribute
   - Select "success" relationship
   - Click "Add"

### Step 4: Error Handling

#### 4.1 Add LogAttribute for Errors

1. **Add another LogAttribute processor** for error handling
2. **Configure for error logging:**
   - **Log Level**: ERROR
   - **Log Payload**: true (to see error details)

#### 4.2 Connect Error Relationships

1. **Connect InvokeHTTP error relationships:**
   - Connect "No Retry", "Retry", "Failure" to error LogAttribute

2. **Connect PutS3Object error relationships:**
   - Connect "failure" to error LogAttribute

### Step 5: Advanced Configuration Options

#### 5.1 File Validation (Optional)

Add **EvaluateJsonPath** or **RouteOnContent** processor to validate downloaded files:
```
- Check file size > 0
- Verify file extension is .zip
- Check for expected content patterns
```

#### 5.2 Notification Setup (Optional)

Add **PutEmail** or **PutSlack** processor for success/failure notifications:
- Connect to success/failure relationships
- Configure SMTP settings or Slack webhook

#### 5.3 Retry Logic

Configure retry settings in InvokeHTTP processor:
- **Retry Count**: 3
- **Penalty Duration**: 30 sec

### Step 6: Testing and Deployment

#### 6.1 Test the Flow

1. **Start processors in order:**
   - Start LogAttribute processors first
   - Start PutS3Object
   - Start UpdateAttribute
   - Start InvokeHTTP last

2. **Manual trigger for testing:**
   - Right-click InvokeHTTP processor
   - Select "Run Once"
   - Monitor the flow in NiFi UI

3. **Verify in AWS S3:**
   - Check your S3 bucket for uploaded files
   - Verify file integrity and size

#### 6.2 Monitor the Flow

1. **Check processor statistics:**
   - Monitor success/failure rates
   - Check processing times
   - Monitor queue sizes

2. **Review logs:**
   - Check NiFi logs for any errors
   - Monitor LogAttribute processor outputs

## Alternative URLs for Different File Types

### Monthly Files
```
https://download.cms.gov/nppes/NPPES_Data_Dissemination_January_2025.zip
https://download.cms.gov/nppes/NPPES_Data_Dissemination_February_2025.zip
```

### Weekly Files
```
https://download.cms.gov/nppes/NPPES_Data_Dissemination_Weekly_January_06_2025.zip
```

### Deactivated Files
```
https://download.cms.gov/nppes/NPPES_Deactivated_NPI_Report_January_2025.zip
```

## Scheduling Options

### Cron Expressions for Scheduling

- **Monthly (1st of month at midnight)**: `0 0 1 * * ?`
- **Weekly (Every Monday at 2 AM)**: `0 0 2 * * 1`
- **Daily (for testing)**: `0 0 2 * * ?`
- **Every 6 hours**: `0 0 */6 * * ?`

## Troubleshooting

### Common Issues and Solutions

1. **Download Timeout:**
   - Increase Read Timeout in InvokeHTTP
   - Check network connectivity

2. **S3 Upload Failures:**
   - Verify AWS credentials
   - Check S3 bucket permissions
   - Ensure correct region configuration

3. **File Not Found (404 Error):**
   - Verify URL format matches CMS naming convention
   - Check if files are published on expected dates

4. **Large File Handling:**
   - Increase JVM heap size for NiFi
   - Configure appropriate timeouts
   - Consider using multipart upload for very large files

## Security Best Practices

1. **AWS Credentials:**
   - Use IAM roles instead of access keys when possible
   - Implement least privilege access
   - Rotate credentials regularly

2. **NiFi Security:**
   - Enable HTTPS for NiFi UI
   - Configure user authentication
   - Encrypt sensitive processor properties

3. **Network Security:**
   - Use VPC endpoints for S3 access
   - Implement proper firewall rules
   - Monitor network traffic

## Performance Optimization

1. **Processor Settings:**
   - Adjust concurrent tasks based on system resources
   - Configure appropriate buffer sizes
   - Set optimal scheduling intervals

2. **S3 Configuration:**
   - Use appropriate storage class
   - Configure lifecycle policies
   - Consider compression for long-term storage

This workflow will automatically download CMS NPPES NPI files and store them in your AWS S3 bucket with proper error handling and monitoring capabilities.