# End-to-End AWS S3 to Snowflake Streaming with Snowpipe

## Architecture Overview

```
NiFi → AWS S3 → SQS → Snowpipe → Snowflake Table
```

**Data Flow:**
1. NiFi uploads NPPES files to S3
2. S3 triggers SQS notification  
3. Snowpipe receives notification and loads data
4. Data appears in Snowflake table automatically

## Step-by-Step Implementation

### Step 1: AWS S3 Configuration

#### 1.1 Create SQS Queue
```bash
# AWS CLI command
aws sqs create-queue \
    --queue-name nppes-snowpipe-queue \
    --region us-east-1 \
    --attributes '{
        "MessageRetentionPeriod": "1209600",
        "VisibilityTimeoutSeconds": "300"
    }'
```

**Or via AWS Console:**
1. Go to AWS SQS Console
2. Click "Create Queue"
3. **Queue Name:** `nppes-snowpipe-queue`
4. **Type:** Standard
5. **Message Retention:** 14 days
6. **Visibility Timeout:** 5 minutes
7. Click "Create Queue"

#### 1.2 Configure S3 Event Notifications
```bash
# Create event notification configuration
aws s3api put-bucket-notification-configuration \
    --bucket your-npi-bucket \
    --notification-configuration '{
        "QueueConfigurations": [
            {
                "Id": "NPPESFileUpload",
                "QueueArn": "arn:aws:sqs:us-east-1:123456789012:nppes-snowpipe-queue",
                "Events": ["s3:ObjectCreated:*"],
                "Filter": {
                    "Key": {
                        "FilterRules": [
                            {
                                "Name": "prefix",
                                "Value": "npi-files/"
                            },
                            {
                                "Name": "suffix", 
                                "Value": ".zip"
                            }
                        ]
                    }
                }
            }
        ]
    }'
```

**Or via AWS Console:**
1. Go to your S3 bucket → Properties
2. Scroll to "Event notifications"
3. Click "Create event notification"
4. **Name:** `NPPESFileUpload`
5. **Event types:** All object create events
6. **Prefix:** `npi-files/`
7. **Suffix:** `.zip`
8. **Destination:** SQS queue
9. **SQS queue:** Select `nppes-snowpipe-queue`

### Step 2: Create IAM Role for Snowflake

#### 2.1 Create IAM Role
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::YOUR_SNOWFLAKE_ACCOUNT_ID:root"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "YOUR_SNOWFLAKE_EXTERNAL_ID"
                }
            }
        }
    ]
}
```

#### 2.2 Create IAM Policy
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-npi-bucket",
                "arn:aws:s3:::your-npi-bucket/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "sqs:ReceiveMessage",
                "sqs:DeleteMessage",
                "sqs:GetQueueAttributes"
            ],
            "Resource": "arn:aws:sqs:us-east-1:123456789012:nppes-snowpipe-queue"
        }
    ]
}
```

### Step 3: Snowflake Configuration

#### 3.1 Create Database and Schema
```sql
-- Create database for NPPES data
CREATE DATABASE IF NOT EXISTS NPPES_DB;
USE DATABASE NPPES_DB;

-- Create schema
CREATE SCHEMA IF NOT EXISTS RAW_DATA;
USE SCHEMA RAW_DATA;
```

#### 3.2 Create NPPES Table Structure
```sql
-- Create table for NPPES NPI data
CREATE OR REPLACE TABLE NPPES_NPI_DATA (
    NPI VARCHAR(10),
    ENTITY_TYPE_CODE VARCHAR(1),
    REPLACEMENT_NPI VARCHAR(10),
    EIN VARCHAR(9),
    PROVIDER_ORGANIZATION_NAME VARCHAR(70),
    PROVIDER_LAST_NAME VARCHAR(35),
    PROVIDER_FIRST_NAME VARCHAR(20),
    PROVIDER_MIDDLE_NAME VARCHAR(20),
    PROVIDER_NAME_PREFIX VARCHAR(5),
    PROVIDER_NAME_SUFFIX VARCHAR(5),
    PROVIDER_CREDENTIAL_TEXT VARCHAR(20),
    PROVIDER_OTHER_ORGANIZATION_NAME VARCHAR(70),
    PROVIDER_OTHER_ORGANIZATION_NAME_TYPE_CODE VARCHAR(1),
    PROVIDER_OTHER_LAST_NAME VARCHAR(35),
    PROVIDER_OTHER_FIRST_NAME VARCHAR(20),
    PROVIDER_OTHER_MIDDLE_NAME VARCHAR(20),
    PROVIDER_OTHER_NAME_PREFIX VARCHAR(5),
    PROVIDER_OTHER_NAME_SUFFIX VARCHAR(5),
    PROVIDER_OTHER_CREDENTIAL_TEXT VARCHAR(20),
    PROVIDER_OTHER_LAST_NAME_TYPE_CODE VARCHAR(1),
    PROVIDER_FIRST_LINE_BUSINESS_MAILING_ADDRESS VARCHAR(55),
    PROVIDER_SECOND_LINE_BUSINESS_MAILING_ADDRESS VARCHAR(55),
    PROVIDER_BUSINESS_MAILING_ADDRESS_CITY_NAME VARCHAR(40),
    PROVIDER_BUSINESS_MAILING_ADDRESS_STATE_NAME VARCHAR(40),
    PROVIDER_BUSINESS_MAILING_ADDRESS_POSTAL_CODE VARCHAR(20),
    PROVIDER_BUSINESS_MAILING_ADDRESS_COUNTRY_CODE VARCHAR(2),
    PROVIDER_BUSINESS_MAILING_ADDRESS_TELEPHONE_NUMBER VARCHAR(20),
    PROVIDER_BUSINESS_MAILING_ADDRESS_FAX_NUMBER VARCHAR(20),
    PROVIDER_FIRST_LINE_BUSINESS_PRACTICE_LOCATION_ADDRESS VARCHAR(55),
    PROVIDER_SECOND_LINE_BUSINESS_PRACTICE_LOCATION_ADDRESS VARCHAR(55),
    PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_CITY_NAME VARCHAR(40),
    PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_STATE_NAME VARCHAR(40),
    PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_POSTAL_CODE VARCHAR(20),
    PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_COUNTRY_CODE VARCHAR(2),
    PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_TELEPHONE_NUMBER VARCHAR(20),
    PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_FAX_NUMBER VARCHAR(20),
    PROVIDER_ENUMERATION_DATE DATE,
    LAST_UPDATE_DATE DATE,
    NPI_DEACTIVATION_REASON_CODE VARCHAR(2),
    NPI_DEACTIVATION_DATE DATE,
    NPI_REACTIVATION_DATE DATE,
    PROVIDER_GENDER_CODE VARCHAR(1),
    AUTHORIZED_OFFICIAL_LAST_NAME VARCHAR(35),
    AUTHORIZED_OFFICIAL_FIRST_NAME VARCHAR(20),
    AUTHORIZED_OFFICIAL_MIDDLE_NAME VARCHAR(20),
    AUTHORIZED_OFFICIAL_TITLE_OR_POSITION VARCHAR(35),
    AUTHORIZED_OFFICIAL_TELEPHONE_NUMBER VARCHAR(20),
    HEALTHCARE_PROVIDER_TAXONOMY_CODE_1 VARCHAR(10),
    PROVIDER_LICENSE_NUMBER_1 VARCHAR(20),
    PROVIDER_LICENSE_NUMBER_STATE_CODE_1 VARCHAR(2),
    HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_1 VARCHAR(1),
    -- Add more taxonomy fields as needed (up to 15)
    IS_SOLE_PROPRIETOR VARCHAR(1),
    IS_ORGANIZATION_SUBPART VARCHAR(1),
    PARENT_ORGANIZATION_LBN VARCHAR(70),
    PARENT_ORGANIZATION_TIN VARCHAR(9),
    AUTHORIZED_OFFICIAL_NAME_PREFIX VARCHAR(5),
    AUTHORIZED_OFFICIAL_NAME_SUFFIX VARCHAR(5),
    AUTHORIZED_OFFICIAL_CREDENTIAL_TEXT VARCHAR(20),
    HEALTHCARE_PROVIDER_TAXONOMY_GROUP_1 VARCHAR(70),
    -- Metadata fields
    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_FILE VARCHAR(255),
    BATCH_ID VARCHAR(50)
);
```

#### 3.3 Create Storage Integration
```sql
-- Create storage integration for S3
CREATE OR REPLACE STORAGE INTEGRATION s3_nppes_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/SnowflakeS3Role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://your-npi-bucket/npi-files/');

-- Grant usage on integration
GRANT USAGE ON INTEGRATION s3_nppes_integration TO ROLE SYSADMIN;

-- Describe integration to get external ID and IAM user
DESC STORAGE INTEGRATION s3_nppes_integration;
```

#### 3.4 Create File Format
```sql
-- Create file format for CSV files
CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    REPLACE_INVALID_CHARACTERS = TRUE
    DATE_FORMAT = 'MM/DD/YYYY'
    TIMESTAMP_FORMAT = 'MM/DD/YYYY HH24:MI:SS'
    NULL_IF = ('', 'NULL', 'null');
```

#### 3.5 Create External Stage
```sql
-- Create external stage pointing to S3
CREATE OR REPLACE STAGE s3_nppes_stage
    STORAGE_INTEGRATION = s3_nppes_integration
    URL = 's3://your-npi-bucket/npi-files/'
    FILE_FORMAT = csv_format;

-- Test the stage
LIST @s3_nppes_stage;
```

#### 3.6 Create Snowpipe
```sql
-- Create Snowpipe for auto-ingestion
CREATE OR REPLACE PIPE nppes_pipe
    AUTO_INGEST = TRUE
    AS
    COPY INTO NPPES_NPI_DATA
    FROM (
        SELECT 
            $1::VARCHAR(10) as NPI,
            $2::VARCHAR(1) as ENTITY_TYPE_CODE,
            $3::VARCHAR(10) as REPLACEMENT_NPI,
            $4::VARCHAR(9) as EIN,
            $5::VARCHAR(70) as PROVIDER_ORGANIZATION_NAME,
            $6::VARCHAR(35) as PROVIDER_LAST_NAME,
            $7::VARCHAR(20) as PROVIDER_FIRST_NAME,
            $8::VARCHAR(20) as PROVIDER_MIDDLE_NAME,
            $9::VARCHAR(5) as PROVIDER_NAME_PREFIX,
            $10::VARCHAR(5) as PROVIDER_NAME_SUFFIX,
            $11::VARCHAR(20) as PROVIDER_CREDENTIAL_TEXT,
            $12::VARCHAR(70) as PROVIDER_OTHER_ORGANIZATION_NAME,
            $13::VARCHAR(1) as PROVIDER_OTHER_ORGANIZATION_NAME_TYPE_CODE,
            $14::VARCHAR(35) as PROVIDER_OTHER_LAST_NAME,
            $15::VARCHAR(20) as PROVIDER_OTHER_FIRST_NAME,
            $16::VARCHAR(20) as PROVIDER_OTHER_MIDDLE_NAME,
            $17::VARCHAR(5) as PROVIDER_OTHER_NAME_PREFIX,
            $18::VARCHAR(5) as PROVIDER_OTHER_NAME_SUFFIX,
            $19::VARCHAR(20) as PROVIDER_OTHER_CREDENTIAL_TEXT,
            $20::VARCHAR(1) as PROVIDER_OTHER_LAST_NAME_TYPE_CODE,
            $21::VARCHAR(55) as PROVIDER_FIRST_LINE_BUSINESS_MAILING_ADDRESS,
            $22::VARCHAR(55) as PROVIDER_SECOND_LINE_BUSINESS_MAILING_ADDRESS,
            $23::VARCHAR(40) as PROVIDER_BUSINESS_MAILING_ADDRESS_CITY_NAME,
            $24::VARCHAR(40) as PROVIDER_BUSINESS_MAILING_ADDRESS_STATE_NAME,
            $25::VARCHAR(20) as PROVIDER_BUSINESS_MAILING_ADDRESS_POSTAL_CODE,
            $26::VARCHAR(2) as PROVIDER_BUSINESS_MAILING_ADDRESS_COUNTRY_CODE,
            $27::VARCHAR(20) as PROVIDER_BUSINESS_MAILING_ADDRESS_TELEPHONE_NUMBER,
            $28::VARCHAR(20) as PROVIDER_BUSINESS_MAILING_ADDRESS_FAX_NUMBER,
            $29::VARCHAR(55) as PROVIDER_FIRST_LINE_BUSINESS_PRACTICE_LOCATION_ADDRESS,
            $30::VARCHAR(55) as PROVIDER_SECOND_LINE_BUSINESS_PRACTICE_LOCATION_ADDRESS,
            $31::VARCHAR(40) as PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_CITY_NAME,
            $32::VARCHAR(40) as PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_STATE_NAME,
            $33::VARCHAR(20) as PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_POSTAL_CODE,
            $34::VARCHAR(2) as PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_COUNTRY_CODE,
            $35::VARCHAR(20) as PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_TELEPHONE_NUMBER,
            $36::VARCHAR(20) as PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_FAX_NUMBER,
            TO_DATE($37, 'MM/DD/YYYY') as PROVIDER_ENUMERATION_DATE,
            TO_DATE($38, 'MM/DD/YYYY') as LAST_UPDATE_DATE,
            $39::VARCHAR(2) as NPI_DEACTIVATION_REASON_CODE,
            TO_DATE($40, 'MM/DD/YYYY') as NPI_DEACTIVATION_DATE,
            TO_DATE($41, 'MM/DD/YYYY') as NPI_REACTIVATION_DATE,
            $42::VARCHAR(1) as PROVIDER_GENDER_CODE,
            $43::VARCHAR(35) as AUTHORIZED_OFFICIAL_LAST_NAME,
            $44::VARCHAR(20) as AUTHORIZED_OFFICIAL_FIRST_NAME,
            $45::VARCHAR(20) as AUTHORIZED_OFFICIAL_MIDDLE_NAME,
            $46::VARCHAR(35) as AUTHORIZED_OFFICIAL_TITLE_OR_POSITION,
            $47::VARCHAR(20) as AUTHORIZED_OFFICIAL_TELEPHONE_NUMBER,
            $48::VARCHAR(10) as HEALTHCARE_PROVIDER_TAXONOMY_CODE_1,
            $49::VARCHAR(20) as PROVIDER_LICENSE_NUMBER_1,
            $50::VARCHAR(2) as PROVIDER_LICENSE_NUMBER_STATE_CODE_1,
            $51::VARCHAR(1) as HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_1,
            $52::VARCHAR(1) as IS_SOLE_PROPRIETOR,
            $53::VARCHAR(1) as IS_ORGANIZATION_SUBPART,
            $54::VARCHAR(70) as PARENT_ORGANIZATION_LBN,
            $55::VARCHAR(9) as PARENT_ORGANIZATION_TIN,
            $56::VARCHAR(5) as AUTHORIZED_OFFICIAL_NAME_PREFIX,
            $57::VARCHAR(5) as AUTHORIZED_OFFICIAL_NAME_SUFFIX,
            $58::VARCHAR(20) as AUTHORIZED_OFFICIAL_CREDENTIAL_TEXT,
            $59::VARCHAR(70) as HEALTHCARE_PROVIDER_TAXONOMY_GROUP_1,
            CURRENT_TIMESTAMP() as LOAD_TIMESTAMP,
            METADATA$FILENAME as SOURCE_FILE,
            METADATA$FILE_LAST_MODIFIED as BATCH_ID
        FROM @s3_nppes_stage
    )
    ON_ERROR = 'CONTINUE';

-- Get the Snowpipe notification channel (SQS ARN)
SHOW PIPES;
```

### Step 4: Update AWS SQS Queue Policy

After creating the Snowpipe, update your SQS queue policy to allow Snowflake to access it:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::YOUR_SNOWFLAKE_ACCOUNT_ID:root"
            },
            "Action": [
                "sqs:ReceiveMessage",
                "sqs:DeleteMessage",
                "sqs:GetQueueAttributes"
            ],
            "Resource": "arn:aws:sqs:us-east-1:123456789012:nppes-snowpipe-queue"
        }
    ]
}
```

### Step 5: Handle ZIP Files (Optional)

If you need to extract ZIP files before loading:

#### 5.1 Add Lambda Function for ZIP Extraction
```python
import boto3
import zipfile
import io

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        
        if key.endswith('.zip'):
            # Download ZIP file
            obj = s3.get_object(Bucket=bucket, Key=key)
            zip_content = obj['Body'].read()
            
            # Extract ZIP file
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
                for file_name in zip_file.namelist():
                    if file_name.endswith('.csv'):
                        # Extract CSV content
                        csv_content = zip_file.read(file_name)
                        
                        # Upload extracted CSV to S3
                        csv_key = key.replace('.zip', '.csv')
                        s3.put_object(
                            Bucket=bucket,
                            Key=csv_key,
                            Body=csv_content
                        )
    
    return {'statusCode': 200}
```

### Step 6: Testing the Pipeline

#### 6.1 Test File Upload
```bash
# Upload a test file to trigger the pipeline
aws s3 cp test_nppes_file.csv s3://your-npi-bucket/npi-files/
```

#### 6.2 Monitor Snowpipe Status
```sql
-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('nppes_pipe');

-- Check pipe history
SELECT * FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
    DATE_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
    DATE_RANGE_END => CURRENT_TIMESTAMP(),
    PIPE_NAME => 'nppes_pipe'
));

-- Check copy history
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'NPPES_NPI_DATA',
    START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP())
));
```

#### 6.3 Verify Data Loading
```sql
-- Check loaded data
SELECT COUNT(*) FROM NPPES_NPI_DATA;

-- Check recent loads
SELECT 
    SOURCE_FILE,
    COUNT(*) as RECORD_COUNT,
    MIN(LOAD_TIMESTAMP) as FIRST_LOAD,
    MAX(LOAD_TIMESTAMP) as LAST_LOAD
FROM NPPES_NPI_DATA
GROUP BY SOURCE_FILE
ORDER BY LAST_LOAD DESC;
```

### Step 7: Monitoring and Alerting

#### 7.1 Create Monitoring Views
```sql
-- Create view for monitoring
CREATE OR REPLACE VIEW PIPE_MONITORING AS
SELECT 
    PIPE_NAME,
    IS_PAUSED,
    LAST_RECEIVED_MESSAGE_TIMESTAMP,
    LAST_FORWARDED_MESSAGE_TIMESTAMP,
    NOTIFICATION_CHANNEL_NAME
FROM TABLE(INFORMATION_SCHEMA.PIPES());

-- Create view for load statistics
CREATE OR REPLACE VIEW LOAD_STATISTICS AS
SELECT 
    DATE(LOAD_TIMESTAMP) as LOAD_DATE,
    SOURCE_FILE,
    COUNT(*) as RECORDS_LOADED,
    MIN(LOAD_TIMESTAMP) as FIRST_RECORD_TIME,
    MAX(LOAD_TIMESTAMP) as LAST_RECORD_TIME
FROM NPPES_NPI_DATA
GROUP BY DATE(LOAD_TIMESTAMP), SOURCE_FILE
ORDER BY LOAD_DATE DESC;
```

#### 7.2 Set Up Alerts (Optional)
```sql
-- Create task to check for pipe failures
CREATE OR REPLACE TASK PIPE_HEALTH_CHECK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 */4 * * * UTC'  -- Every 4 hours
AS
    INSERT INTO PIPE_ALERTS (
        SELECT 
            CURRENT_TIMESTAMP() as ALERT_TIME,
            'PIPE_FAILURE' as ALERT_TYPE,
            'Snowpipe has not received messages in 4+ hours' as MESSAGE
        FROM TABLE(INFORMATION_SCHEMA.PIPES())
        WHERE PIPE_NAME = 'NPPES_PIPE'
        AND LAST_RECEIVED_MESSAGE_TIMESTAMP < DATEADD('hour', -4, CURRENT_TIMESTAMP())
    );

-- Start the task
ALTER TASK PIPE_HEALTH_CHECK RESUME;
```

## Troubleshooting

### Common Issues and Solutions

1. **Pipe Not Receiving Messages:**
   - Check SQS queue permissions
   - Verify S3 event notifications are configured
   - Ensure IAM role has correct permissions

2. **Data Not Loading:**
   - Check file format matches table structure
   - Verify stage can list files: `LIST @s3_nppes_stage;`
   - Check copy errors: `SELECT * FROM TABLE(VALIDATE(NPPES_NPI_DATA, JOB_ID => '_last'));`

3. **Permission Errors:**
   - Update IAM role trust policy
   - Grant necessary privileges in Snowflake
   - Check storage integration status

### Monitoring Commands
```sql
-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('nppes_pipe');

-- Refresh pipe (if needed)
ALTER PIPE nppes_pipe REFRESH;

-- Pause/Resume pipe
ALTER PIPE nppes_pipe SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE nppes_pipe SET PIPE_EXECUTION_PAUSED = FALSE;
```

This end-to-end setup will automatically stream your NPPES data from S3 to Snowflake as soon as files are uploaded by your NiFi flow!