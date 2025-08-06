-- ====================================================================
-- PRACTICAL EXAMPLES FOR JIRA INTEGRATION
-- This file contains real-world examples of how to integrate Jira ticket
-- creation with various Snowflake tasks and procedures
-- ====================================================================

-- ====================================================================
-- Example 1: Data Loading Task with Jira Integration
-- Similar to your ADP example but with error handling
-- ====================================================================

CREATE OR REPLACE TASK PROD_ADS.TASKS.DAILY_DATA_LOAD_TASK
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = 'USING CRON 0 2 * * * UTC'
ERROR_INTEGRATION = 'EMAIL_NOTIFICATION_INTEGRATION'  -- Keep existing email notifications
AS
BEGIN
    DECLARE
        task_result STRING;
        error_occurred BOOLEAN DEFAULT FALSE;
    BEGIN
        -- Call your main data loading procedure
        CALL PROD_ADS.ADP.ADP_LOAD(
            'PROD_ADS',
            'ADP', 
            'EMPLOYEE_DATA',
            0,    -- current_month_offset
            -9    -- previous_month_offset (9 months back)
        ) INTO task_result;
        
    EXCEPTION
        WHEN OTHER THEN
            -- Create Jira ticket for the failure
            CALL PROD_ADS.MONITORING.HANDLE_TASK_FAILURE(
                'DAILY_DATA_LOAD_TASK',
                SQLERRM,
                SQLCODE::STRING
            );
            
            -- Re-raise the error to trigger existing error handling
            RAISE;
    END;
END;

-- ====================================================================
-- Example 2: Enhanced ADP Procedure with Jira Integration
-- Modified version of your ADP_LOAD procedure with built-in error handling
-- ====================================================================

CREATE OR REPLACE PROCEDURE PROD_ADS.ADP.ADP_LOAD_WITH_JIRA_INTEGRATION(
    DBNAME STRING,
    SCHEMA_NAME STRING,
    TARGET_TABLE_NAME STRING,
    CURRENT_MONTH_OFFSET INT,
    PREVIOUS_MONTH_OFFSET INT
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('requests', 'simplejson', 'snowflake-snowpark-python', 'pandas')
HANDLER = 'main'
EXTERNAL_ACCESS_INTEGRATIONS = (ADP_API_EXTERNAL_ACCESS_INTEGRATION)
SECRETS = ('cred' = PROD_DB_MANAGER.SECRETS.ADP_API_INTEGRATION_SECRET)
EXECUTE AS OWNER
AS
$$
import _snowflake
import simplejson as json
import requests
import pandas as pd
import snowflake.snowpark as snowpark
from datetime import datetime, timedelta
import calendar

def create_jira_ticket_for_failure(session, task_name, error_message, additional_context=None):
    """Helper function to create Jira ticket from within the procedure"""
    try:
        jira_call = f"""
        CALL PROD_ADS.MONITORING.NOTIFY_TASK_FAILURE_WITH_CONTEXT(
            '{task_name}',
            'ADP_LOAD_WITH_JIRA_INTEGRATION',
            '{session.get_current_database()}',
            '{session.get_current_schema()}',
            '{error_message.replace("'", "''")}',
            NULL,
            PARSE_JSON('{json.dumps(additional_context or {})}')
        )
        """
        session.sql(jira_call).collect()
    except Exception as jira_error:
        # Don't fail the main process if Jira ticket creation fails
        print(f"Warning: Failed to create Jira ticket: {str(jira_error)}")

def calculate_full_date_range(current_month_offset, previous_month_offset):
    today = datetime.utcnow()
    first_day_of_current_month = today.replace(day=1)
    
    if previous_month_offset < 0:
        start_month = first_day_of_current_month.month + previous_month_offset
        start_year = first_day_of_current_month.year
        if start_month < 1:
            start_month += 12
            start_year -= 1
        leave_from_date = datetime(start_year, start_month, 1)
    else:
        leave_from_date = first_day_of_current_month

    days_in_current_month = calendar.monthrange(today.year, today.month)[1]
    leave_to_date = first_day_of_current_month + timedelta(days=days_in_current_month - 1, hours=23, minutes=59, seconds=59)
    
    return leave_from_date.strftime('%Y-%m-%d %H:%M:%S'), leave_to_date.strftime('%Y-%m-%d %H:%M:%S')

def get_api_url(session: snowpark.Session, dbname, schema_name, target_table_name):
    try:
        query = f"""
            SELECT URL 
            FROM {dbname}.{schema_name}.API_ENDPOINTS_PARAM 
            WHERE TARGET_TABLE_NAME = '{target_table_name}'
        """
        
        url_df = session.sql(query).collect()
        
        if len(url_df) == 0:
            raise ValueError(f"No URL found for {target_table_name} in {dbname}.{schema_name}.API_ENDPOINTS_PARAM")
        
        return url_df[0]['URL']
    except Exception as e:
        # Create Jira ticket for configuration issues
        create_jira_ticket_for_failure(
            session,
            f"ADP_LOAD_CONFIG_ERROR_{target_table_name}",
            f"Failed to retrieve API URL configuration: {str(e)}",
            {"target_table": target_table_name, "error_type": "configuration"}
        )
        raise

def process(session: snowpark.Session, url, leave_from, leave_to, dbname, schema_name, target_table_name):
    try:
        credentials = json.loads(_snowflake.get_generic_secret_string("cred"), strict=False)
        
        headers = {
            "x-apiKey": credentials["apiKey"],
            "x-secret": credentials["secret"],
            "Accept": "application/json"
        }
        
        # Replace the placeholders in the URL with actual date values
        final_url = url.replace("start_date", leave_from).replace("end_date", leave_to)
        
        # Log the final URL for debugging purposes
        log_table = f"{dbname}.{schema_name}.LOG_TABLE"
        session.sql(f"INSERT INTO {log_table} (LOG_MESSAGE) VALUES ('Final API URL: {final_url}')").collect()
        
        # Make the request to the API
        response = requests.get(final_url, headers=headers)
        response.raise_for_status()
        
        data = response.json()

        # Handle dynamic JSON structure - find the first key that holds list data
        for key, value in data.items():
            if isinstance(value, list):
                return value
        
        raise ValueError("No list-type data found in the response")
        
    except requests.exceptions.RequestException as e:
        # Create Jira ticket for API issues
        create_jira_ticket_for_failure(
            session,
            f"ADP_API_ERROR_{target_table_name}",
            f"API request failed: {str(e)}",
            {
                "target_table": target_table_name,
                "api_url": final_url,
                "error_type": "api_request",
                "date_range": f"{leave_from} to {leave_to}"
            }
        )
        raise
    except Exception as e:
        # Create Jira ticket for data processing issues
        create_jira_ticket_for_failure(
            session,
            f"ADP_DATA_PROCESSING_ERROR_{target_table_name}",
            f"Data processing failed: {str(e)}",
            {
                "target_table": target_table_name,
                "error_type": "data_processing",
                "date_range": f"{leave_from} to {leave_to}"
            }
        )
        raise

def main(session: snowpark.Session, dbname, schema_name, target_table_name, current_month_offset, previous_month_offset):
    try:
        # Get the URL from the parameter table
        api_url = get_api_url(session, dbname, schema_name, target_table_name)
        
        # Calculate the full date range
        leave_from, leave_to = calculate_full_date_range(current_month_offset, previous_month_offset)
        
        # Fetch the data
        leave_data = process(session, api_url, leave_from, leave_to, dbname, schema_name, target_table_name)
        
        # Convert JSON data to pandas DataFrame
        df = pd.DataFrame(leave_data)
        
        if df.empty:
            # Create Jira ticket for empty data scenarios
            create_jira_ticket_for_failure(
                session,
                f"ADP_EMPTY_DATA_{target_table_name}",
                f"No data returned from API for date range {leave_from} to {leave_to}",
                {
                    "target_table": target_table_name,
                    "date_range": f"{leave_from} to {leave_to}",
                    "error_type": "empty_data"
                }
            )
            return f"Warning: No data found for {target_table_name}"
        
        # Convert all column names to uppercase
        df.columns = [col.upper() for col in df.columns]
        
        table_name = f"{dbname}.{schema_name}.RAW_{target_table_name}"
        raw_row_cre_dt = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        
        # Add RAW_ROW_CRE_DT column to the DataFrame
        df["RAW_ROW_CRE_DT"] = raw_row_cre_dt
        
        # Create Snowpark DataFrame from pandas DataFrame
        df_raw = session.create_dataframe(df)
        
        # Write the DataFrame to the Snowflake table
        df_raw.write.mode("overwrite").save_as_table(table_name)
        
        return f"{table_name} created successfully with {len(df)} records"
        
    except Exception as e:
        # Create Jira ticket for any unhandled errors
        create_jira_ticket_for_failure(
            session,
            f"ADP_LOAD_FAILURE_{target_table_name}",
            f"ADP data load failed: {str(e)}",
            {
                "target_table": target_table_name,
                "parameters": {
                    "dbname": dbname,
                    "schema_name": schema_name,
                    "current_month_offset": current_month_offset,
                    "previous_month_offset": previous_month_offset
                },
                "error_type": "general_failure"
            }
        )
        raise
$$;

-- ====================================================================
-- Example 3: Data Quality Check Task with Jira Integration
-- ====================================================================

CREATE OR REPLACE TASK PROD_ADS.TASKS.DAILY_DATA_QUALITY_CHECK
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = 'USING CRON 0 4 * * * UTC'  -- Run after data load
AS
BEGIN
    DECLARE
        duplicate_count INT;
        null_count INT;
        total_records INT;
        quality_issues STRING DEFAULT '';
    BEGIN
        -- Check for data quality issues
        SELECT COUNT(*) INTO total_records FROM PROD_ADS.ADP.RAW_EMPLOYEE_DATA;
        
        -- Check for duplicates
        SELECT COUNT(*) - COUNT(DISTINCT EMPLOYEE_ID) INTO duplicate_count 
        FROM PROD_ADS.ADP.RAW_EMPLOYEE_DATA;
        
        -- Check for null critical fields
        SELECT COUNT(*) INTO null_count 
        FROM PROD_ADS.ADP.RAW_EMPLOYEE_DATA 
        WHERE EMPLOYEE_ID IS NULL OR EMPLOYEE_EMAIL IS NULL;
        
        -- Build quality issues summary
        IF (duplicate_count > 0) THEN
            quality_issues := quality_issues || duplicate_count || ' duplicate records found. ';
        END IF;
        
        IF (null_count > 0) THEN
            quality_issues := quality_issues || null_count || ' records with missing critical fields. ';
        END IF;
        
        -- Create Jira ticket if quality issues found
        IF (LENGTH(quality_issues) > 0) THEN
            CALL PROD_ADS.MONITORING.CREATE_JIRA_TICKET(
                'DATA_QUALITY_ISSUES_EMPLOYEE_DATA',
                'Data quality validation failed: ' || quality_issues,
                'DQ001',
                'PROD',
                CASE 
                    WHEN duplicate_count > 100 OR null_count > 50 THEN 'CRITICAL'
                    WHEN duplicate_count > 10 OR null_count > 10 THEN 'HIGH'
                    ELSE 'MEDIUM'
                END,
                'data-team-lead',  -- Assign to data team lead
                PARSE_JSON(OBJECT_CONSTRUCT(
                    'total_records', total_records,
                    'duplicate_count', duplicate_count,
                    'null_count', null_count,
                    'table_name', 'PROD_ADS.ADP.RAW_EMPLOYEE_DATA',
                    'check_timestamp', CURRENT_TIMESTAMP()
                )::STRING)
            );
        END IF;
        
    EXCEPTION
        WHEN OTHER THEN
            CALL PROD_ADS.MONITORING.HANDLE_TASK_FAILURE(
                'DAILY_DATA_QUALITY_CHECK',
                SQLERRM,
                SQLCODE::STRING
            );
            RAISE;
    END;
END;

-- ====================================================================
-- Example 4: ETL Pipeline with Multiple Steps and Jira Integration
-- ====================================================================

CREATE OR REPLACE PROCEDURE PROD_ADS.ETL.FULL_ETL_PIPELINE_WITH_MONITORING()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    step_result STRING;
    pipeline_status STRING DEFAULT 'SUCCESS';
    failed_steps STRING DEFAULT '';
    total_steps INT DEFAULT 5;
    completed_steps INT DEFAULT 0;
BEGIN
    -- Step 1: Extract data
    BEGIN
        CALL PROD_ADS.EXTRACT.EXTRACT_SOURCE_DATA() INTO step_result;
        completed_steps := completed_steps + 1;
    EXCEPTION
        WHEN OTHER THEN
            failed_steps := failed_steps || '1-EXTRACT ';
            pipeline_status := 'PARTIAL_FAILURE';
            CALL PROD_ADS.MONITORING.NOTIFY_TASK_FAILURE_WITH_CONTEXT(
                'ETL_PIPELINE_STEP_1_EXTRACT',
                'FULL_ETL_PIPELINE_WITH_MONITORING',
                CURRENT_DATABASE(),
                CURRENT_SCHEMA(),
                'Extract step failed: ' || SQLERRM,
                SQLCODE::STRING,
                PARSE_JSON('{"step": 1, "step_name": "EXTRACT", "pipeline_id": "FULL_ETL_PIPELINE"}')
            );
    END;
    
    -- Step 2: Transform data
    BEGIN
        CALL PROD_ADS.TRANSFORM.TRANSFORM_DATA() INTO step_result;
        completed_steps := completed_steps + 1;
    EXCEPTION
        WHEN OTHER THEN
            failed_steps := failed_steps || '2-TRANSFORM ';
            pipeline_status := 'PARTIAL_FAILURE';
            CALL PROD_ADS.MONITORING.NOTIFY_TASK_FAILURE_WITH_CONTEXT(
                'ETL_PIPELINE_STEP_2_TRANSFORM',
                'FULL_ETL_PIPELINE_WITH_MONITORING',
                CURRENT_DATABASE(),
                CURRENT_SCHEMA(),
                'Transform step failed: ' || SQLERRM,
                SQLCODE::STRING,
                PARSE_JSON('{"step": 2, "step_name": "TRANSFORM", "pipeline_id": "FULL_ETL_PIPELINE"}')
            );
    END;
    
    -- Step 3: Load data
    BEGIN
        CALL PROD_ADS.LOAD.LOAD_TARGET_DATA() INTO step_result;
        completed_steps := completed_steps + 1;
    EXCEPTION
        WHEN OTHER THEN
            failed_steps := failed_steps || '3-LOAD ';
            pipeline_status := 'PARTIAL_FAILURE';
            CALL PROD_ADS.MONITORING.NOTIFY_TASK_FAILURE_WITH_CONTEXT(
                'ETL_PIPELINE_STEP_3_LOAD',
                'FULL_ETL_PIPELINE_WITH_MONITORING',
                CURRENT_DATABASE(),
                CURRENT_SCHEMA(),
                'Load step failed: ' || SQLERRM,
                SQLCODE::STRING,
                PARSE_JSON('{"step": 3, "step_name": "LOAD", "pipeline_id": "FULL_ETL_PIPELINE"}')
            );
    END;
    
    -- Step 4: Validate data
    BEGIN
        CALL PROD_ADS.VALIDATE.VALIDATE_DATA_QUALITY() INTO step_result;
        completed_steps := completed_steps + 1;
    EXCEPTION
        WHEN OTHER THEN
            failed_steps := failed_steps || '4-VALIDATE ';
            pipeline_status := 'PARTIAL_FAILURE';
            CALL PROD_ADS.MONITORING.NOTIFY_TASK_FAILURE_WITH_CONTEXT(
                'ETL_PIPELINE_STEP_4_VALIDATE',
                'FULL_ETL_PIPELINE_WITH_MONITORING',
                CURRENT_DATABASE(),
                CURRENT_SCHEMA(),
                'Validation step failed: ' || SQLERRM,
                SQLCODE::STRING,
                PARSE_JSON('{"step": 4, "step_name": "VALIDATE", "pipeline_id": "FULL_ETL_PIPELINE"}')
            );
    END;
    
    -- Step 5: Generate reports
    BEGIN
        CALL PROD_ADS.REPORTS.GENERATE_SUMMARY_REPORTS() INTO step_result;
        completed_steps := completed_steps + 1;
    EXCEPTION
        WHEN OTHER THEN
            failed_steps := failed_steps || '5-REPORTS ';
            pipeline_status := 'PARTIAL_FAILURE';
            CALL PROD_ADS.MONITORING.NOTIFY_TASK_FAILURE_WITH_CONTEXT(
                'ETL_PIPELINE_STEP_5_REPORTS',
                'FULL_ETL_PIPELINE_WITH_MONITORING',
                CURRENT_DATABASE(),
                CURRENT_SCHEMA(),
                'Reports step failed: ' || SQLERRM,
                SQLCODE::STRING,
                PARSE_JSON('{"step": 5, "step_name": "REPORTS", "pipeline_id": "FULL_ETL_PIPELINE"}')
            );
    END;
    
    -- Create summary ticket if any steps failed
    IF (pipeline_status = 'PARTIAL_FAILURE') THEN
        CALL PROD_ADS.MONITORING.CREATE_JIRA_TICKET(
            'ETL_PIPELINE_PARTIAL_FAILURE',
            'ETL Pipeline completed with failures. Failed steps: ' || failed_steps || 
            '. Completed ' || completed_steps || ' out of ' || total_steps || ' steps successfully.',
            'ETL001',
            'PROD',
            CASE 
                WHEN completed_steps < 2 THEN 'CRITICAL'
                WHEN completed_steps < 4 THEN 'HIGH'
                ELSE 'MEDIUM'
            END,
            'etl-team-lead',
            PARSE_JSON(OBJECT_CONSTRUCT(
                'pipeline_id', 'FULL_ETL_PIPELINE',
                'total_steps', total_steps,
                'completed_steps', completed_steps,
                'failed_steps', failed_steps,
                'pipeline_status', pipeline_status,
                'execution_timestamp', CURRENT_TIMESTAMP()
            )::STRING)
        );
    END IF;
    
    RETURN 'Pipeline completed with status: ' || pipeline_status || 
           '. Completed steps: ' || completed_steps || '/' || total_steps;
END;
$$;

-- ====================================================================
-- Example 5: Scheduled Task with Custom Error Categorization
-- ====================================================================

CREATE OR REPLACE TASK PROD_ADS.TASKS.WEEKLY_REPORT_GENERATION
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = 'USING CRON 0 6 * * 1 UTC'  -- Every Monday at 6 AM
AS
BEGIN
    DECLARE
        report_count INT DEFAULT 0;
        error_category STRING;
        severity STRING;
    BEGIN
        -- Generate weekly reports
        CALL PROD_ADS.REPORTS.GENERATE_WEEKLY_REPORTS() INTO report_count;
        
        -- Validate report generation
        IF (report_count = 0) THEN
            CALL PROD_ADS.MONITORING.CREATE_JIRA_TICKET(
                'WEEKLY_REPORTS_NO_DATA',
                'Weekly report generation completed but no reports were created. This may indicate a data availability issue.',
                'RPT001',
                'PROD',
                'MEDIUM',
                'reports-team',
                PARSE_JSON('{"expected_reports": "weekly_summary", "actual_count": 0, "issue_type": "no_data"}')
            );
        END IF;
        
    EXCEPTION
        WHEN OTHER THEN
            -- Categorize the error based on error message
            IF (CONTAINS(UPPER(SQLERRM), 'TIMEOUT') OR CONTAINS(UPPER(SQLERRM), 'WAREHOUSE')) THEN
                error_category := 'INFRASTRUCTURE';
                severity := 'HIGH';
            ELSEIF (CONTAINS(UPPER(SQLERRM), 'PERMISSION') OR CONTAINS(UPPER(SQLERRM), 'ACCESS')) THEN
                error_category := 'SECURITY';
                severity := 'HIGH';
            ELSEIF (CONTAINS(UPPER(SQLERRM), 'DATA') OR CONTAINS(UPPER(SQLERRM), 'TABLE')) THEN
                error_category := 'DATA_ISSUE';
                severity := 'MEDIUM';
            ELSE
                error_category := 'UNKNOWN';
                severity := 'HIGH';
            END IF;
            
            CALL PROD_ADS.MONITORING.CREATE_JIRA_TICKET(
                'WEEKLY_REPORTS_FAILURE',
                'Weekly report generation failed: ' || SQLERRM,
                SQLCODE::STRING,
                'PROD',
                severity,
                CASE error_category
                    WHEN 'INFRASTRUCTURE' THEN 'infrastructure-team'
                    WHEN 'SECURITY' THEN 'security-team'
                    WHEN 'DATA_ISSUE' THEN 'data-team'
                    ELSE 'reports-team'
                END,
                PARSE_JSON(OBJECT_CONSTRUCT(
                    'error_category', error_category,
                    'task_name', 'WEEKLY_REPORT_GENERATION',
                    'scheduled_time', 'Monday 6AM UTC',
                    'failure_timestamp', CURRENT_TIMESTAMP()
                )::STRING)
            );
            
            RAISE;
    END;
END;

-- ====================================================================
-- Example 6: Manual Jira Ticket Creation for Specific Scenarios
-- ====================================================================

-- Create a procedure for manual ticket creation with predefined templates
CREATE OR REPLACE PROCEDURE PROD_ADS.MONITORING.CREATE_DATA_ISSUE_TICKET(
    ISSUE_TYPE STRING,  -- 'DATA_QUALITY', 'DATA_MISSING', 'DATA_CORRUPTION'
    TABLE_NAME STRING,
    DESCRIPTION STRING,
    SEVERITY STRING DEFAULT 'HIGH',
    ASSIGNEE STRING DEFAULT NULL
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    ticket_summary STRING;
    ticket_description STRING;
    result STRING;
BEGIN
    -- Create issue-specific summary and description
    CASE ISSUE_TYPE
        WHEN 'DATA_QUALITY' THEN
            ticket_summary := 'Data Quality Issue: ' || TABLE_NAME;
            ticket_description := 'Data quality validation failed for table ' || TABLE_NAME || '. ' || DESCRIPTION;
        WHEN 'DATA_MISSING' THEN
            ticket_summary := 'Missing Data: ' || TABLE_NAME;
            ticket_description := 'Expected data is missing from table ' || TABLE_NAME || '. ' || DESCRIPTION;
        WHEN 'DATA_CORRUPTION' THEN
            ticket_summary := 'Data Corruption Detected: ' || TABLE_NAME;
            ticket_description := 'Data corruption has been detected in table ' || TABLE_NAME || '. ' || DESCRIPTION;
        ELSE
            ticket_summary := 'Data Issue: ' || TABLE_NAME;
            ticket_description := 'Data issue reported for table ' || TABLE_NAME || '. ' || DESCRIPTION;
    END CASE;
    
    CALL PROD_ADS.MONITORING.CREATE_JIRA_TICKET(
        ticket_summary,
        ticket_description,
        ISSUE_TYPE,
        'PROD',
        SEVERITY,
        ASSIGNEE,
        PARSE_JSON(OBJECT_CONSTRUCT(
            'issue_type', ISSUE_TYPE,
            'table_name', TABLE_NAME,
            'reported_by', CURRENT_USER(),
            'report_timestamp', CURRENT_TIMESTAMP()
        )::STRING)
    ) INTO result;
    
    RETURN result;
END;
$$;

-- Usage examples:
/*
-- Report data quality issue
CALL PROD_ADS.MONITORING.CREATE_DATA_ISSUE_TICKET(
    'DATA_QUALITY',
    'PROD_ADS.ADP.RAW_EMPLOYEE_DATA',
    'Found 50 duplicate employee records with same email address',
    'HIGH',
    'data-quality-team'
);

-- Report missing data
CALL PROD_ADS.MONITORING.CREATE_DATA_ISSUE_TICKET(
    'DATA_MISSING',
    'PROD_ADS.SALES.DAILY_TRANSACTIONS',
    'No transaction data received for the last 3 days',
    'CRITICAL',
    'data-ops-team'
);
*/