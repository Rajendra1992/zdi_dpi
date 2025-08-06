-- ====================================================================
-- JIRA INTEGRATION SETUP FOR SNOWFLAKE
-- This script sets up the complete infrastructure for creating Jira tickets
-- from Snowflake when tasks fail
-- ====================================================================

-- ====================================================================
-- Step 1: Create External Access Integration (ACCOUNTADMIN role required)
-- ====================================================================
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION JIRA_API_EXTERNAL_ACCESS_INTEGRATION
ALLOWED_NETWORK_RULES = ()  -- Will be populated later
ALLOWED_AUTHENTICATION_SECRETS = ()  -- Will be populated later
ENABLED = false;  -- Will be enabled after setup

-- ====================================================================
-- Step 2: Create Secrets for Each Environment
-- ====================================================================

-- DEV Environment Secret
USE ROLE ENTERPRISE_ADMIN_INTERNAL_ONSHORE;

CREATE OR REPLACE SECRET DEV_DB_MANAGER.SECRETS.JIRA_API_INTEGRATION_SECRET
TYPE = GENERIC_STRING
SECRET_STRING = '{
    "username": "your-jira-username@company.com",
    "api_token": "your-jira-api-token",
    "base_url": "https://your-company.atlassian.net",
    "project_key": "DEV"
}';

-- UAT Environment Secret
CREATE OR REPLACE SECRET UAT_DB_MANAGER.SECRETS.JIRA_API_INTEGRATION_SECRET
TYPE = GENERIC_STRING
SECRET_STRING = '{
    "username": "your-jira-username@company.com",
    "api_token": "your-jira-api-token",
    "base_url": "https://your-company.atlassian.net",
    "project_key": "UAT"
}';

-- PROD Environment Secret
CREATE OR REPLACE SECRET PROD_DB_MANAGER.SECRETS.JIRA_API_INTEGRATION_SECRET
TYPE = GENERIC_STRING
SECRET_STRING = '{
    "username": "your-jira-username@company.com",
    "api_token": "your-jira-api-token",
    "base_url": "https://your-company.atlassian.net",
    "project_key": "PROD"
}';

-- ====================================================================
-- Step 3: Create Network Rules for Each Environment
-- ====================================================================

-- DEV Environment Network Rule
CREATE OR REPLACE NETWORK RULE DEV_DB_MANAGER.NETWORK.JIRA_API_INTEGRATION_NETWORK_RULE
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST = ('your-company.atlassian.net');

-- UAT Environment Network Rule
CREATE OR REPLACE NETWORK RULE UAT_DB_MANAGER.NETWORK.JIRA_API_INTEGRATION_NETWORK_RULE
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST = ('your-company.atlassian.net');

-- PROD Environment Network Rule
CREATE OR REPLACE NETWORK RULE PROD_DB_MANAGER.NETWORK.JIRA_API_INTEGRATION_NETWORK_RULE
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST = ('your-company.atlassian.net');

-- ====================================================================
-- Step 4: Update External Access Integration (ACCOUNTADMIN role required)
-- ====================================================================
USE ROLE ACCOUNTADMIN;

ALTER EXTERNAL ACCESS INTEGRATION JIRA_API_EXTERNAL_ACCESS_INTEGRATION SET
ALLOWED_NETWORK_RULES = (
    DEV_DB_MANAGER.NETWORK.JIRA_API_INTEGRATION_NETWORK_RULE,
    UAT_DB_MANAGER.NETWORK.JIRA_API_INTEGRATION_NETWORK_RULE,
    PROD_DB_MANAGER.NETWORK.JIRA_API_INTEGRATION_NETWORK_RULE
)
ALLOWED_AUTHENTICATION_SECRETS = (
    DEV_DB_MANAGER.SECRETS.JIRA_API_INTEGRATION_SECRET,
    UAT_DB_MANAGER.SECRETS.JIRA_API_INTEGRATION_SECRET,
    PROD_DB_MANAGER.SECRETS.JIRA_API_INTEGRATION_SECRET
)
ENABLED = true;

-- ====================================================================
-- Step 5: Create Error Logging Table (Optional but Recommended)
-- ====================================================================
USE ROLE SYSADMIN;

CREATE OR REPLACE TABLE PROD_ADS.MONITORING.TASK_FAILURE_LOG (
    FAILURE_ID STRING DEFAULT CONCAT('FAIL_', TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS'), '_', RANDSTR(6, RANDOM())),
    TASK_NAME STRING,
    DATABASE_NAME STRING,
    SCHEMA_NAME STRING,
    PROCEDURE_NAME STRING,
    ERROR_MESSAGE STRING,
    ERROR_CODE STRING,
    FAILURE_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ENVIRONMENT STRING,
    SEVERITY STRING DEFAULT 'HIGH',
    JIRA_TICKET_KEY STRING,
    JIRA_TICKET_STATUS STRING,
    ADDITIONAL_CONTEXT VARIANT,
    CREATED_BY STRING DEFAULT CURRENT_USER(),
    PRIMARY KEY (FAILURE_ID)
);

-- ====================================================================
-- Step 6: Create Jira Ticket Creation Stored Procedure
-- ====================================================================

CREATE OR REPLACE PROCEDURE PROD_ADS.MONITORING.CREATE_JIRA_TICKET(
    TASK_NAME STRING,
    ERROR_MESSAGE STRING,
    ERROR_CODE STRING DEFAULT NULL,
    ENVIRONMENT STRING DEFAULT 'PROD',
    SEVERITY STRING DEFAULT 'HIGH',
    ASSIGNEE STRING DEFAULT NULL,
    ADDITIONAL_CONTEXT VARIANT DEFAULT NULL
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('requests', 'simplejson', 'snowflake-snowpark-python', 'base64')
HANDLER = 'main'
EXTERNAL_ACCESS_INTEGRATIONS = (JIRA_API_EXTERNAL_ACCESS_INTEGRATION)
SECRETS = ('jira_cred' = PROD_DB_MANAGER.SECRETS.JIRA_API_INTEGRATION_SECRET)
EXECUTE AS OWNER
AS
$$
import _snowflake
import simplejson as json
import requests
import base64
import snowflake.snowpark as snowpark
from datetime import datetime

def get_jira_credentials():
    """Get Jira credentials from Snowflake secret"""
    try:
        credentials = json.loads(_snowflake.get_generic_secret_string("jira_cred"), strict=False)
        return credentials
    except Exception as e:
        raise ValueError(f"Failed to retrieve Jira credentials: {str(e)}")

def create_jira_ticket(credentials, task_name, error_message, error_code, environment, severity, assignee, additional_context):
    """Create a Jira ticket with the provided details"""
    
    base_url = credentials["base_url"]
    username = credentials["username"]
    api_token = credentials["api_token"]
    project_key = credentials.get("project_key", "SNOWFLAKE")
    
    # Create authentication header
    auth_string = f"{username}:{api_token}"
    auth_bytes = auth_string.encode('ascii')
    auth_b64 = base64.b64encode(auth_bytes).decode('ascii')
    
    headers = {
        "Authorization": f"Basic {auth_b64}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    # Determine priority based on severity
    priority_map = {
        "CRITICAL": "Highest",
        "HIGH": "High", 
        "MEDIUM": "Medium",
        "LOW": "Low"
    }
    priority = priority_map.get(severity.upper(), "High")
    
    # Create ticket summary and description
    summary = f"[{environment}] Snowflake Task Failure: {task_name}"
    
    description = f"""
*Snowflake Task Failure Report*

*Environment:* {environment}
*Task Name:* {task_name}
*Failure Time:* {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC
*Severity:* {severity}

*Error Details:*
{error_message}
"""
    
    if error_code:
        description += f"\n*Error Code:* {error_code}"
    
    if additional_context:
        description += f"\n\n*Additional Context:*\n{json.dumps(additional_context, indent=2)}"
    
    description += """

*Next Steps:*
1. Investigate the root cause of the failure
2. Check data dependencies and connections
3. Verify system resources and permissions
4. Implement fix and test in lower environment
5. Deploy fix to production

*Please update this ticket with investigation findings and resolution steps.*
"""
    
    # Prepare ticket payload
    ticket_payload = {
        "fields": {
            "project": {"key": project_key},
            "summary": summary,
            "description": description,
            "issuetype": {"name": "Bug"},
            "priority": {"name": priority},
            "labels": [
                "snowflake",
                "data-pipeline",
                "automated-ticket",
                f"env-{environment.lower()}",
                f"severity-{severity.lower()}"
            ]
        }
    }
    
    # Add assignee if provided
    if assignee:
        ticket_payload["fields"]["assignee"] = {"name": assignee}
    
    # Create the ticket
    create_url = f"{base_url}/rest/api/3/issue"
    response = requests.post(create_url, headers=headers, json=ticket_payload)
    
    if response.status_code == 201:
        ticket_data = response.json()
        return ticket_data["key"], ticket_data["id"]
    else:
        raise Exception(f"Failed to create Jira ticket. Status: {response.status_code}, Response: {response.text}")

def log_failure(session, task_name, error_message, error_code, environment, severity, jira_ticket_key, additional_context):
    """Log the failure to the monitoring table"""
    try:
        log_query = f"""
        INSERT INTO PROD_ADS.MONITORING.TASK_FAILURE_LOG 
        (TASK_NAME, ERROR_MESSAGE, ERROR_CODE, ENVIRONMENT, SEVERITY, JIRA_TICKET_KEY, JIRA_TICKET_STATUS, ADDITIONAL_CONTEXT)
        VALUES ('{task_name}', '{error_message.replace("'", "''")}', '{error_code or ""}', '{environment}', '{severity}', '{jira_ticket_key}', 'CREATED', PARSE_JSON('{json.dumps(additional_context or {})}'))
        """
        session.sql(log_query).collect()
    except Exception as e:
        # Don't fail the main process if logging fails
        print(f"Warning: Failed to log to monitoring table: {str(e)}")

def main(session: snowpark.Session, task_name, error_message, error_code, environment, severity, assignee, additional_context):
    try:
        # Get Jira credentials
        credentials = get_jira_credentials()
        
        # Create Jira ticket
        jira_ticket_key, jira_ticket_id = create_jira_ticket(
            credentials, task_name, error_message, error_code, 
            environment, severity, assignee, additional_context
        )
        
        # Log the failure
        log_failure(session, task_name, error_message, error_code, environment, severity, jira_ticket_key, additional_context)
        
        result = {
            "status": "SUCCESS",
            "jira_ticket_key": jira_ticket_key,
            "jira_ticket_id": jira_ticket_id,
            "jira_url": f"{credentials['base_url']}/browse/{jira_ticket_key}",
            "message": f"Jira ticket {jira_ticket_key} created successfully"
        }
        
        return json.dumps(result)
        
    except Exception as e:
        error_result = {
            "status": "ERROR",
            "error": str(e),
            "message": "Failed to create Jira ticket"
        }
        return json.dumps(error_result)
$$;

-- ====================================================================
-- Step 7: Create Wrapper Procedure for Easy Task Integration
-- ====================================================================

CREATE OR REPLACE PROCEDURE PROD_ADS.MONITORING.HANDLE_TASK_FAILURE(
    TASK_NAME STRING,
    ERROR_MESSAGE STRING,
    ERROR_CODE STRING DEFAULT NULL
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    result STRING;
    environment STRING DEFAULT 'PROD';
    severity STRING DEFAULT 'HIGH';
BEGIN
    -- Determine environment based on database/task name patterns
    IF (CONTAINS(TASK_NAME, 'DEV_') OR CONTAINS(TASK_NAME, '_DEV')) THEN
        environment := 'DEV';
    ELSEIF (CONTAINS(TASK_NAME, 'UAT_') OR CONTAINS(TASK_NAME, '_UAT')) THEN
        environment := 'UAT';
    ELSE
        environment := 'PROD';
    END IF;
    
    -- Determine severity based on error patterns
    IF (CONTAINS(UPPER(ERROR_MESSAGE), 'CRITICAL') OR CONTAINS(UPPER(ERROR_MESSAGE), 'FATAL')) THEN
        severity := 'CRITICAL';
    ELSEIF (CONTAINS(UPPER(ERROR_MESSAGE), 'WARNING') OR CONTAINS(UPPER(ERROR_MESSAGE), 'WARN')) THEN
        severity := 'MEDIUM';
    ELSE
        severity := 'HIGH';
    END IF;
    
    -- Call the main Jira ticket creation procedure
    CALL PROD_ADS.MONITORING.CREATE_JIRA_TICKET(
        :TASK_NAME,
        :ERROR_MESSAGE, 
        :ERROR_CODE,
        :environment,
        :severity,
        NULL,  -- assignee
        NULL   -- additional_context
    ) INTO :result;
    
    RETURN result;
END;
$$;

-- ====================================================================
-- Step 8: Create Task Failure Notification Procedure (Enhanced)
-- ====================================================================

CREATE OR REPLACE PROCEDURE PROD_ADS.MONITORING.NOTIFY_TASK_FAILURE_WITH_CONTEXT(
    TASK_NAME STRING,
    PROCEDURE_NAME STRING DEFAULT NULL,
    DATABASE_NAME STRING DEFAULT NULL,
    SCHEMA_NAME STRING DEFAULT NULL,
    ERROR_MESSAGE STRING DEFAULT NULL,
    ERROR_CODE STRING DEFAULT NULL,
    CUSTOM_CONTEXT VARIANT DEFAULT NULL
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    result STRING;
    context_json VARIANT;
    full_error_message STRING;
BEGIN
    -- Build comprehensive context
    context_json := OBJECT_CONSTRUCT(
        'database_name', COALESCE(:DATABASE_NAME, CURRENT_DATABASE()),
        'schema_name', COALESCE(:SCHEMA_NAME, CURRENT_SCHEMA()),
        'procedure_name', :PROCEDURE_NAME,
        'user', CURRENT_USER(),
        'role', CURRENT_ROLE(),
        'warehouse', CURRENT_WAREHOUSE(),
        'timestamp', CURRENT_TIMESTAMP(),
        'custom_context', :CUSTOM_CONTEXT
    );
    
    -- Build comprehensive error message
    full_error_message := COALESCE(:ERROR_MESSAGE, 'Task execution failed');
    
    IF (:PROCEDURE_NAME IS NOT NULL) THEN
        full_error_message := full_error_message || ' in procedure: ' || :PROCEDURE_NAME;
    END IF;
    
    IF (:DATABASE_NAME IS NOT NULL AND :SCHEMA_NAME IS NOT NULL) THEN
        full_error_message := full_error_message || ' (Location: ' || :DATABASE_NAME || '.' || :SCHEMA_NAME || ')';
    END IF;
    
    -- Call the main Jira ticket creation procedure with context
    CALL PROD_ADS.MONITORING.CREATE_JIRA_TICKET(
        :TASK_NAME,
        :full_error_message,
        :ERROR_CODE,
        'PROD',  -- Default to PROD, can be enhanced
        'HIGH',  -- Default severity
        NULL,    -- assignee
        :context_json
    ) INTO :result;
    
    RETURN result;
END;
$$;

-- ====================================================================
-- Step 9: Grant Necessary Permissions
-- ====================================================================

-- Grant permissions to execute the procedures
GRANT USAGE ON SCHEMA PROD_ADS.MONITORING TO ROLE SYSADMIN;
GRANT EXECUTE ON PROCEDURE PROD_ADS.MONITORING.CREATE_JIRA_TICKET TO ROLE SYSADMIN;
GRANT EXECUTE ON PROCEDURE PROD_ADS.MONITORING.HANDLE_TASK_FAILURE TO ROLE SYSADMIN;
GRANT EXECUTE ON PROCEDURE PROD_ADS.MONITORING.NOTIFY_TASK_FAILURE_WITH_CONTEXT TO ROLE SYSADMIN;

-- Grant permissions on the logging table
GRANT SELECT, INSERT, UPDATE ON TABLE PROD_ADS.MONITORING.TASK_FAILURE_LOG TO ROLE SYSADMIN;

-- ====================================================================
-- Step 10: Example Usage in Tasks and Procedures
-- ====================================================================

/*
-- Example 1: Basic usage in a task's error handling
CREATE OR REPLACE TASK my_data_task
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = 'USING CRON 0 2 * * * UTC'
ERROR_INTEGRATION = 'MY_EMAIL_INTEGRATION'
AS
BEGIN
    BEGIN
        -- Your main task logic here
        CALL my_data_procedure();
    EXCEPTION
        WHEN OTHER THEN
            CALL PROD_ADS.MONITORING.HANDLE_TASK_FAILURE(
                'my_data_task',
                SQLERRM,
                SQLCODE::STRING
            );
            RAISE;
    END;
END;

-- Example 2: Enhanced usage with context
CREATE OR REPLACE PROCEDURE my_enhanced_procedure()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    result STRING;
BEGIN
    BEGIN
        -- Your procedure logic here
        INSERT INTO my_table SELECT * FROM source_table;
        result := 'Success';
    EXCEPTION
        WHEN OTHER THEN
            CALL PROD_ADS.MONITORING.NOTIFY_TASK_FAILURE_WITH_CONTEXT(
                'my_enhanced_procedure',
                'my_enhanced_procedure',
                CURRENT_DATABASE(),
                CURRENT_SCHEMA(),
                SQLERRM,
                SQLCODE::STRING,
                OBJECT_CONSTRUCT('source_table', 'source_table', 'target_table', 'my_table')
            );
            RAISE;
    END;
    RETURN result;
END;
$$;

-- Example 3: Manual ticket creation for specific scenarios
CALL PROD_ADS.MONITORING.CREATE_JIRA_TICKET(
    'Data_Quality_Check_Failed',
    'Data quality validation failed: Found 150 duplicate records in customer table',
    'DQ001',
    'PROD',
    'HIGH',
    'john.doe',  -- assignee
    PARSE_JSON('{"table": "customers", "duplicate_count": 150, "validation_rule": "unique_email"}')
);
*/