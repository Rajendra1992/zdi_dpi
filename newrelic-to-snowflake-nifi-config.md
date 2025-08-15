# New Relic to Snowflake Data Pipeline - NiFi Configuration Guide

## Prerequisites

1. **New Relic Account & API Key**
   - User API Key or Insights Query Key
   - Proper permissions to access desired data

2. **Snowflake Account**
   - Database, schema, and table created
   - User with appropriate permissions
   - Snowpipe configured (optional but recommended)

3. **Apache NiFi**
   - Version 1.15+ recommended for Snowflake processors
   - Snowflake JDBC driver installed

## NiFi Flow Configuration

### 1. InvokeHTTP Processor Configuration

#### Option A: Standard REST API (GET - No Body)
```yaml
Processor: InvokeHTTP
Properties:
  - HTTP Method: GET
  - Remote URL: https://api.newrelic.com/v2/applications.json
  - Send Message Body: false
  - Request Content-Type: application/json
  - Accept Content-Type: application/json
  - Connection Timeout: 30 sec
  - Read Timeout: 60 sec
  
Dynamic Properties:
  - X-Api-Key: ${newrelic.api.key}
  - User-Agent: NiFi-NewRelic-Connector

Scheduling:
  - Run Schedule: 0 */15 * * * ?  # Every 15 minutes
  - Concurrent Tasks: 1
```

#### Option B: GraphQL NerdGraph API (POST - With Body)
```yaml
Processor: InvokeHTTP
Properties:
  - HTTP Method: POST
  - Remote URL: https://api.newrelic.com/graphql
  - Send Message Body: true
  - Content-Type: application/json
  - Connection Timeout: 30 sec
  - Read Timeout: 60 sec
  
Dynamic Properties:
  - API-Key: ${newrelic.api.key}
  - Content-Type: application/json
  - User-Agent: NiFi-NewRelic-Connector

Scheduling:
  - Run Schedule: 0 */15 * * * ?  # Every 15 minutes
  - Concurrent Tasks: 1

Body Configuration:
  - Add ReplaceText processor before InvokeHTTP to generate GraphQL query body
```

#### Option C: NRQL Insights API (POST - With Body)
```yaml
Processor: InvokeHTTP
Properties:
  - HTTP Method: POST
  - Remote URL: https://insights-api.newrelic.com/v1/accounts/${newrelic.account.id}/query
  - Send Message Body: true
  - Content-Type: application/json
  - Connection Timeout: 30 sec
  - Read Timeout: 60 sec
  
Dynamic Properties:
  - X-Query-Key: ${newrelic.query.key}
  - Content-Type: application/json
  - User-Agent: NiFi-NewRelic-Connector

Scheduling:
  - Run Schedule: 0 */15 * * * ?  # Every 15 minutes
  - Concurrent Tasks: 1

Body Example:
{
  "nrql": "SELECT average(duration), count(*) FROM Transaction WHERE appName = 'MyApp' SINCE 1 hour ago TIMESERIES"
}
```

### 2. EvaluateJsonPath Processor (Optional - for metadata extraction)

```yaml
Processor: EvaluateJsonPath
Properties:
  - Destination: flowfile-attribute
  
Dynamic Properties:
  - record.count: $.applications.length()
  - api.response.time: $.meta.response_time
```

### 3. SplitJson Processor

```yaml
Processor: SplitJson
Properties:
  - JsonPath Expression: $.applications
  - Null Value Representation: empty_string
```

### 4. JoltTransformJSON Processor (Optional - for data transformation)

```yaml
Processor: JoltTransformJSON
Properties:
  - Jolt Specification:
    [
      {
        "operation": "shift",
        "spec": {
          "id": "application_id",
          "name": "application_name", 
          "language": "language",
          "health_status": "health_status",
          "reporting": "reporting",
          "last_reported_at": "last_reported_at",
          "application_summary": {
            "response_time": "avg_response_time",
            "throughput": "throughput",
            "error_rate": "error_rate",
            "apdex_target": "apdex_target",
            "apdex_score": "apdex_score"
          }
        }
      }
    ]
```

### 5. ConvertRecord Processor

```yaml
Processor: ConvertRecord
Properties:
  - Record Reader: JsonTreeReader
  - Record Writer: CSVRecordSetWriter
  
JsonTreeReader Properties:
  - Schema Access Strategy: Use 'Schema Text' Property
  - Schema Text: |
    {
      "type": "record",
      "name": "NewRelicApplication",
      "fields": [
        {"name": "application_id", "type": ["null", "long"]},
        {"name": "application_name", "type": ["null", "string"]},
        {"name": "language", "type": ["null", "string"]},
        {"name": "health_status", "type": ["null", "string"]},
        {"name": "reporting", "type": ["null", "boolean"]},
        {"name": "last_reported_at", "type": ["null", "string"]},
        {"name": "avg_response_time", "type": ["null", "double"]},
        {"name": "throughput", "type": ["null", "double"]},
        {"name": "error_rate", "type": ["null", "double"]},
        {"name": "apdex_target", "type": ["null", "double"]},
        {"name": "apdex_score", "type": ["null", "double"]}
      ]
    }

CSVRecordSetWriter Properties:
  - Schema Write Strategy: Do Not Write Schema
  - Include Header Line: false
  - Quote Mode: ALL_NON_NULL
```

### 6. PutSnowflakeInternalStage Processor

```yaml
Processor: PutSnowflakeInternalStage
Properties:
  - Snowflake Connection Service: SnowflakeConnectionService
  - Database: ANALYTICS_DB
  - Schema: NEWRELIC
  - Stage Name: NEWRELIC_STAGE
  - Upload File: true
  - File Format: CSV
```

### 7. StartSnowflakeIngest Processor

```yaml
Processor: StartSnowflakeIngest
Properties:
  - Snowflake Connection Service: SnowflakeConnectionService
  - Database: ANALYTICS_DB
  - Schema: NEWRELIC
  - Target Table: APPLICATIONS
  - Snowpipe Name: NEWRELIC_APPLICATIONS_PIPE
```

## Snowflake Setup

### 1. Create Database and Schema

```sql
-- Create database
CREATE DATABASE IF NOT EXISTS ANALYTICS_DB;

-- Create schema
CREATE SCHEMA IF NOT EXISTS ANALYTICS_DB.NEWRELIC;

-- Use the schema
USE SCHEMA ANALYTICS_DB.NEWRELIC;
```

### 2. Create Target Table

```sql
CREATE TABLE IF NOT EXISTS APPLICATIONS (
    application_id BIGINT,
    application_name STRING,
    language STRING,
    health_status STRING,
    reporting BOOLEAN,
    last_reported_at TIMESTAMP_NTZ,
    avg_response_time FLOAT,
    throughput FLOAT,
    error_rate FLOAT,
    apdex_target FLOAT,
    apdex_score FLOAT,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### 3. Create Internal Stage

```sql
CREATE STAGE IF NOT EXISTS NEWRELIC_STAGE
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        SKIP_HEADER = 0
        NULL_IF = ('NULL', 'null', '')
        EMPTY_FIELD_AS_NULL = TRUE
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    );
```

### 4. Create Snowpipe (Recommended)

```sql
CREATE PIPE IF NOT EXISTS NEWRELIC_APPLICATIONS_PIPE
    AUTO_INGEST = FALSE
    AS
    COPY INTO APPLICATIONS (
        application_id,
        application_name,
        language,
        health_status,
        reporting,
        last_reported_at,
        avg_response_time,
        throughput,
        error_rate,
        apdex_target,
        apdex_score
    )
    FROM @NEWRELIC_STAGE
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        SKIP_HEADER = 0
        NULL_IF = ('NULL', 'null', '')
        EMPTY_FIELD_AS_NULL = TRUE
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    );
```

## NiFi Controller Services

### Snowflake Connection Service

```yaml
Service: SnowflakeConnectionService
Properties:
  - Account: your_account.snowflakecomputing.com
  - User: your_username
  - Password: ${snowflake.password}  # Use NiFi sensitive property
  - Database: ANALYTICS_DB
  - Schema: NEWRELIC
  - Warehouse: COMPUTE_WH
  - Role: DATA_ENGINEER_ROLE
```

## Error Handling and Monitoring

### 1. Configure Failure Relationships

- Route InvokeHTTP failures to LogMessage processor
- Route transformation failures to PutFile processor for later analysis
- Route Snowflake ingestion failures to retry queue

### 2. Add Monitoring Processors

```yaml
# Add LogMessage processors for tracking
Processor: LogMessage
Properties:
  - Log Level: INFO
  - Log Message: "Processed ${fragment.count} records from New Relic API"
```

## Environment Variables

Set up the following variables in NiFi:

```bash
# New Relic API Configuration
newrelic.api.key=YOUR_API_KEY_HERE
newrelic.api.url=https://api.newrelic.com/v2

# Snowflake Configuration  
snowflake.password=YOUR_PASSWORD_HERE
snowflake.account=YOUR_ACCOUNT.snowflakecomputing.com
```

## Performance Tuning

1. **Batch Processing**: Use MergeContent processor to batch records before sending to Snowflake
2. **Concurrent Tasks**: Adjust based on API rate limits and Snowflake capacity
3. **Caching**: Consider using DistributedMapCacheClient for deduplication
4. **Scheduling**: Align with New Relic data freshness and business requirements

## Testing and Validation

1. **Start Small**: Begin with a single application or metric endpoint
2. **Validate Data**: Compare record counts between New Relic and Snowflake
3. **Monitor Performance**: Track processing times and error rates
4. **Data Quality**: Implement validation rules for critical fields