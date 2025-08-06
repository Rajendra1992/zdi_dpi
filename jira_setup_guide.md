# Snowflake Jira Integration Setup Guide

This guide provides step-by-step instructions for setting up automated Jira ticket creation from Snowflake when tasks fail.

## Prerequisites

Before starting, ensure you have:

1. **Snowflake Account** with appropriate privileges:
   - `ACCOUNTADMIN` role access
   - `ENTERPRISE_ADMIN_INTERNAL_ONSHORE` role access
   - `SYSADMIN` role access

2. **Jira Instance** with:
   - Admin access to create API tokens
   - Project where tickets will be created
   - User account for API authentication

3. **Jira API Token**: Generate from Jira → Account Settings → Security → API Tokens

## Setup Steps

### Step 1: Prepare Jira Credentials

1. **Generate Jira API Token**:
   - Log into your Jira instance
   - Go to Account Settings → Security → Create and manage API tokens
   - Create a new token and save it securely

2. **Identify Required Information**:
   - Jira Base URL (e.g., `https://your-company.atlassian.net`)
   - Username (email address)
   - API Token (generated above)
   - Project Key (where tickets will be created)

### Step 2: Execute Snowflake Setup

Run the provided SQL script (`snowflake_jira_integration.sql`) in the following order:

#### 2.1 Create External Access Integration
```sql
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION JIRA_API_EXTERNAL_ACCESS_INTEGRATION
ALLOWED_NETWORK_RULES = ()
ALLOWED_AUTHENTICATION_SECRETS = ()
ENABLED = false;
```

#### 2.2 Create Secrets (Update with your credentials)
```sql
USE ROLE ENTERPRISE_ADMIN_INTERNAL_ONSHORE;

-- Update these values with your actual Jira credentials
CREATE OR REPLACE SECRET PROD_DB_MANAGER.SECRETS.JIRA_API_INTEGRATION_SECRET
TYPE = GENERIC_STRING
SECRET_STRING = '{
    "username": "your-actual-username@company.com",
    "api_token": "your-actual-api-token",
    "base_url": "https://your-company.atlassian.net",
    "project_key": "PROD"
}';
```

#### 2.3 Create Network Rules (Update with your Jira domain)
```sql
CREATE OR REPLACE NETWORK RULE PROD_DB_MANAGER.NETWORK.JIRA_API_INTEGRATION_NETWORK_RULE
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST = ('your-company.atlassian.net');  -- Replace with your Jira domain
```

#### 2.4 Complete the remaining steps from the SQL script

### Step 3: Test the Integration

#### 3.1 Basic Test
```sql
-- Test creating a Jira ticket
CALL PROD_ADS.MONITORING.CREATE_JIRA_TICKET(
    'TEST_TASK',
    'This is a test ticket created from Snowflake',
    'TEST001',
    'PROD',
    'MEDIUM',
    NULL,
    PARSE_JSON('{"test": true, "created_by": "setup_guide"}')
);
```

#### 3.2 Verify Results
1. Check the return value for success/error status
2. Look for the created ticket in your Jira project
3. Verify the ticket contains all expected information

### Step 4: Integration with Existing Tasks

#### Option A: Modify Existing Tasks
```sql
CREATE OR REPLACE TASK your_existing_task
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = 'USING CRON 0 2 * * * UTC'
AS
BEGIN
    BEGIN
        -- Your existing task logic
        CALL your_existing_procedure();
    EXCEPTION
        WHEN OTHER THEN
            -- Add Jira ticket creation on failure
            CALL PROD_ADS.MONITORING.HANDLE_TASK_FAILURE(
                'your_existing_task',
                SQLERRM,
                SQLCODE::STRING
            );
            RAISE;  -- Re-raise the error to maintain existing behavior
    END;
END;
```

#### Option B: Modify Existing Procedures
```sql
CREATE OR REPLACE PROCEDURE your_existing_procedure()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    result STRING;
BEGIN
    BEGIN
        -- Your existing procedure logic
        INSERT INTO target_table SELECT * FROM source_table;
        result := 'Success';
    EXCEPTION
        WHEN OTHER THEN
            -- Create Jira ticket with context
            CALL PROD_ADS.MONITORING.NOTIFY_TASK_FAILURE_WITH_CONTEXT(
                'your_existing_procedure',
                'your_existing_procedure',
                CURRENT_DATABASE(),
                CURRENT_SCHEMA(),
                SQLERRM,
                SQLCODE::STRING,
                OBJECT_CONSTRUCT('source_table', 'source_table', 'target_table', 'target_table')
            );
            RAISE;
    END;
    RETURN result;
END;
$$;
```

### Step 5: Monitor and Maintain

#### 5.1 Check Failure Logs
```sql
-- View recent failures and their Jira tickets
SELECT 
    FAILURE_ID,
    TASK_NAME,
    ERROR_MESSAGE,
    JIRA_TICKET_KEY,
    FAILURE_TIMESTAMP,
    ENVIRONMENT,
    SEVERITY
FROM PROD_ADS.MONITORING.TASK_FAILURE_LOG
WHERE FAILURE_TIMESTAMP >= CURRENT_DATE - 7
ORDER BY FAILURE_TIMESTAMP DESC;
```

#### 5.2 Update Jira Credentials (if needed)
```sql
USE ROLE ENTERPRISE_ADMIN_INTERNAL_ONSHORE;

ALTER SECRET PROD_DB_MANAGER.SECRETS.JIRA_API_INTEGRATION_SECRET
SET SECRET_STRING = '{
    "username": "new-username@company.com",
    "api_token": "new-api-token",
    "base_url": "https://your-company.atlassian.net",
    "project_key": "PROD"
}';
```

## Customization Options

### Custom Ticket Fields
Modify the `create_jira_ticket` function in the stored procedure to add custom fields:

```python
# Add custom fields to ticket_payload
ticket_payload["fields"]["customfield_10001"] = "Custom Value"
ticket_payload["fields"]["components"] = [{"name": "Data Pipeline"}]
```

### Environment-Specific Configuration
Create different secrets for each environment with appropriate project keys:

```sql
-- DEV Environment
SECRET_STRING = '{"username": "...", "project_key": "DEV", ...}'

-- UAT Environment  
SECRET_STRING = '{"username": "...", "project_key": "UAT", ...}'

-- PROD Environment
SECRET_STRING = '{"username": "...", "project_key": "PROD", ...}'
```

### Custom Severity Mapping
Modify the `HANDLE_TASK_FAILURE` procedure to customize severity detection:

```sql
-- Add more sophisticated severity detection
IF (CONTAINS(UPPER(ERROR_MESSAGE), 'TIMEOUT') OR CONTAINS(UPPER(ERROR_MESSAGE), 'NETWORK')) THEN
    severity := 'MEDIUM';
ELSEIF (CONTAINS(UPPER(ERROR_MESSAGE), 'DATA_QUALITY') OR CONTAINS(UPPER(ERROR_MESSAGE), 'VALIDATION')) THEN
    severity := 'HIGH';
END IF;
```

## Troubleshooting

### Common Issues

1. **Network Access Denied**
   - Verify network rules include correct Jira domain
   - Check external access integration is enabled

2. **Authentication Failed**
   - Verify API token is correct and not expired
   - Ensure username matches the token owner

3. **Permission Denied**
   - Check role has access to secrets and network rules
   - Verify procedure execution permissions

4. **Ticket Creation Failed**
   - Verify project key exists and is accessible
   - Check Jira field requirements (some fields may be mandatory)

### Debug Steps

1. **Test Network Connectivity**:
   ```sql
   -- This will show network-related errors
   CALL PROD_ADS.MONITORING.CREATE_JIRA_TICKET('DEBUG_TEST', 'Network test', NULL, 'PROD', 'LOW', NULL, NULL);
   ```

2. **Check Secret Access**:
   ```sql
   -- Create a simple procedure to test secret retrieval
   CREATE OR REPLACE PROCEDURE TEST_SECRET_ACCESS()
   RETURNS STRING
   LANGUAGE PYTHON
   RUNTIME_VERSION = '3.8'
   PACKAGES = ('simplejson')
   HANDLER = 'main'
   SECRETS = ('test_cred' = PROD_DB_MANAGER.SECRETS.JIRA_API_INTEGRATION_SECRET)
   AS
   $$
   import _snowflake
   import simplejson as json
   
   def main(session):
       try:
           credentials = json.loads(_snowflake.get_generic_secret_string("test_cred"))
           return f"Success: Retrieved credentials for {credentials.get('username', 'unknown')}"
       except Exception as e:
           return f"Error: {str(e)}"
   $$;
   ```

## Best Practices

1. **Use Environment-Specific Secrets**: Different credentials for DEV/UAT/PROD
2. **Implement Proper Error Handling**: Don't let Jira failures break your main processes
3. **Monitor Ticket Creation**: Regularly check the failure log table
4. **Rotate API Tokens**: Periodically update Jira API tokens for security
5. **Test Regularly**: Periodically test the integration to ensure it's working
6. **Use Appropriate Severity**: Map error types to appropriate Jira priorities
7. **Include Context**: Always provide relevant context in additional_context parameter

## Security Considerations

1. **Secret Management**: 
   - Use Snowflake's secret management (never hardcode credentials)
   - Rotate API tokens regularly
   - Limit secret access to necessary roles only

2. **Network Security**:
   - Restrict network rules to specific Jira domains
   - Use HTTPS for all Jira API calls

3. **Access Control**:
   - Grant minimum necessary permissions
   - Use role-based access control
   - Audit procedure execution regularly

4. **Data Privacy**:
   - Be careful about sensitive data in error messages
   - Consider sanitizing error messages before sending to Jira
   - Review what context information is included in tickets