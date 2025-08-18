# NiFi Aviation API to Snowflake - Complete Configuration Guide

## Overview
This document provides step-by-step instructions to fix your NiFi flow that loads aviation flight data from the AviationStack API into a Snowflake table. The current issue is that only `flightDate` and `flightStatus` fields are being populated, while nested fields remain null.

## Problem Analysis

### Current Issue
- **Working Fields**: `flightDate`, `flightStatus` (top-level JSON fields)
- **Missing Fields**: `departureAirport`, `departureScheduled`, `arrivalAirport`, `arrivalScheduled`, `airlineName`, `flightNumber` (nested JSON fields)

### Root Cause
The JSON response contains nested objects, but your database table expects flat fields. The current flow doesn't flatten the nested structure before database insertion.

## Target Table Structure
```sql
CREATE OR REPLACE TABLE FLIGHTS (
  flightDate STRING,
  flightStatus STRING,
  departureAirport STRING,
  departureScheduled STRING,
  arrivalAirport STRING,
  arrivalScheduled STRING,
  airlineName STRING,
  flightNumber STRING
);
```

## API Response Structure
```json
{
  "pagination": { ... },
  "data": [
    {
      "flight_date": "2025-08-19",
      "flight_status": "landed",
      "departure": {
        "airport": "Jandakot",
        "scheduled": "2025-08-19T07:50:00+00:00"
      },
      "arrival": {
        "airport": "Perth International",
        "scheduled": "2025-08-18T07:54:00+00:00"
      },
      "airline": {
        "name": "empty"
      },
      "flight": {
        "number": null
      }
    }
  ]
}
```

---

## Step-by-Step Configuration

### Step 1: Configure InvokeHTTP Processor

**Purpose**: Retrieve flight data from AviationStack API

**Configuration**:
```
Processor Name: InvokeHTTP_AviationAPI
HTTP Method: GET
HTTP URL: http://api.aviationstack.com/v1/flights?access_key=d73f9b7093fdf157fc7022a94e1c6caa
HTTP/2 Disabled: False
Connection Timeout: 5 secs
Socket Read Timeout: 15 secs
Socket Write Timeout: 15 secs
Socket Idle Timeout: 5 mins
Socket Idle Connections: 5
Request Date Header Enabled: True
Response Body Ignored: false
Response Cache Enabled: false
Response Cookie Strategy: DISABLED
Response Redirects Enabled: True
Response FlowFile Naming Strategy: RANDOM
```

**Output**: Single FlowFile containing complete API response

---

### Step 2: Configure SplitJson Processor

**Purpose**: Split the API response into individual flight records

**Current Configuration** (NEEDS CHANGE):
```
JsonPath Expression: $.*  ❌ (This is wrong)
```

**Fixed Configuration**:
```
Processor Name: SplitJson_FlightRecords
JsonPath Expression: $.data[*]  ✅ (Use this instead)
Null Value Representation: empty string
Max String Length: 20 MB
```

**Why the change?**
- `$.*` extracts ALL top-level elements (pagination + each flight record)
- `$.data[*]` extracts ONLY the flight records from the data array
- This prevents pagination metadata from being processed as flight data

**Input**: 1 FlowFile with complete API response
**Output**: Multiple FlowFiles, each containing one flight record

---

### Step 3: Add JoltTransformJSON Processor

**Purpose**: Flatten nested JSON structure to match database table columns

**Configuration**:
```
Processor Name: JoltTransform_FlattenFlights
Jolt Transformation DSL: Chain
```

**Jolt Specification** (copy this exactly):
```json
[
  {
    "operation": "shift",
    "spec": {
      "flight_date": "flightDate",
      "flight_status": "flightStatus",
      "departure": {
        "airport": "departureAirport",
        "scheduled": "departureScheduled"
      },
      "arrival": {
        "airport": "arrivalAirport",
        "scheduled": "arrivalScheduled"
      },
      "airline": {
        "name": "airlineName"
      },
      "flight": {
        "number": "flightNumber"
      }
    }
  }
]
```

**What this transformation does**:
- Maps `flight_date` → `flightDate`
- Maps `departure.airport` → `departureAirport`
- Maps `departure.scheduled` → `departureScheduled`
- Maps `arrival.airport` → `arrivalAirport`
- Maps `arrival.scheduled` → `arrivalScheduled`
- Maps `airline.name` → `airlineName`
- Maps `flight.number` → `flightNumber`

**Input**: Nested JSON from SplitJson
**Output**: Flattened JSON matching your table structure

---

### Step 4: Configure PutDatabaseRecord Processor

**Purpose**: Insert flattened records into Snowflake table

**Configuration** (Keep your current settings):
```
Processor Name: PutDatabaseRecord_Flights
Record Reader: JsonTreeReader
Database Type: Generic
Statement Type: INSERT
Database Connection Pooling Service: SnowflakeConnectionService
Table Name: FLIGHTS
Binary String Format: UTF-8
Translate Field Names: true
Column Name Translation Strategy: Remove Underscore
Unmatched Field Behavior: Ignore Unmatched Fields
Unmatched Column Behavior: Fail on Unmatched Columns
Quote Column Identifiers: false
Quote Table Identifiers: false
Max Wait Time: 0 seconds
Rollback On Failure: false
Table Schema Cache Size: 100
Maximum Batch Size: 1000
Database Session AutoCommit: false
```

**Input**: Flattened JSON records
**Output**: Records inserted into FLIGHTS table

---

## Flow Connections

**Updated Flow Diagram**:
```
[InvokeHTTP] 
     ↓ (Response relationship)
[SplitJson] 
     ↓ (Split relationship)
[JoltTransformJSON] 
     ↓ (Success relationship)
[PutDatabaseRecord]
     ↓ (Success relationship)
[Success Endpoint]
```

**Connection Details**:
1. **InvokeHTTP → SplitJson**: Connect "Response" relationship
2. **SplitJson → JoltTransformJSON**: Connect "Split" relationship  
3. **JoltTransformJSON → PutDatabaseRecord**: Connect "Success" relationship
4. **PutDatabaseRecord → [Terminate/LogAttribute]**: Connect "Success" relationship

---

## Verification Steps

### Step 1: Test SplitJson Output
1. Add a LogAttribute processor after SplitJson
2. Run the flow and check logs
3. Verify you see individual flight records (not pagination data)

### Step 2: Test Jolt Transformation
1. Add a LogAttribute processor after JoltTransformJSON
2. Run the flow and check logs
3. Verify the JSON structure is flattened with correct field names

### Step 3: Test Database Insertion
1. Run the complete flow
2. Query your Snowflake table:
```sql
SELECT * FROM FLIGHTS LIMIT 5;
```
3. Verify all columns are populated (not just flightDate and flightStatus)

---

## Expected Results

**Before Fix**:
```sql
SELECT * FROM FLIGHTS LIMIT 1;

flightDate    | flightStatus | departureAirport | departureScheduled | arrivalAirport | arrivalScheduled | airlineName | flightNumber
2025-08-19    | landed       | NULL             | NULL               | NULL           | NULL             | NULL        | NULL
```

**After Fix**:
```sql
SELECT * FROM FLIGHTS LIMIT 1;

flightDate    | flightStatus | departureAirport | departureScheduled        | arrivalAirport      | arrivalScheduled          | airlineName | flightNumber
2025-08-19    | landed       | Jandakot         | 2025-08-19T07:50:00+00:00 | Perth International | 2025-08-18T07:54:00+00:00 | empty       | NULL
```

---

## Troubleshooting

### Issue 1: Still getting NULL values
**Solution**: Check Jolt specification syntax - ensure JSON is valid and field mappings are correct

### Issue 2: Getting pagination data in database
**Solution**: Verify SplitJson uses `$.data[*]` not `$.*`

### Issue 3: Field name mismatches
**Solution**: Check that Jolt output field names match your table column names exactly

### Issue 4: Connection timeouts
**Solution**: Increase timeout values in InvokeHTTP processor

### Issue 5: Database connection errors
**Solution**: Verify SnowflakeConnectionService is properly configured and active

---

## Performance Considerations

1. **Batch Size**: Current setting of 1000 records per batch is optimal for most use cases
2. **API Rate Limiting**: AviationStack API may have rate limits - consider adding scheduling intervals
3. **Error Handling**: Add failure relationships to handle and log errors appropriately
4. **Monitoring**: Set up monitoring on each processor to track throughput and errors

---

## Additional Enhancements (Optional)

### Add Error Handling
1. Connect failure relationships to LogAttribute processors
2. Route failed records to separate tables for analysis

### Add Data Validation
1. Add ValidateRecord processor after Jolt transformation
2. Define schema validation rules

### Add Scheduling
1. Configure InvokeHTTP with Timer-driven scheduling
2. Set appropriate run schedule (e.g., every 15 minutes)

---

## Summary

The key changes needed:
1. ✅ **Fix SplitJson**: Change JsonPath from `$.*` to `$.data[*]`
2. ✅ **Add JoltTransformJSON**: Flatten nested JSON structure
3. ✅ **Keep PutDatabaseRecord**: Current configuration is correct

After implementing these changes, all fields from the Aviation API will properly populate in your Snowflake FLIGHTS table.