import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import time

# Custom CSS for styling
st.markdown(
    """
    <style>
    body {
        background-color: #F0F4F8; /* Soft Light Blue background */
    }

    .font {
        font-size: 34px;
        font-family: 'Helvetica Neue', sans-serif;
        color: #34495E; /* Dark Slate Gray */
        text-transform: uppercase; /* Make text uppercase */
    }

    .sidebar .sidebar-content {
        background-color: #BDC3C7; /* Light Gray */
        color: #2C3E50; /* Dark Gray */
    }

    /* Styling for the buttons */
    .stButton button {
        background-color: #2980B9; /* Ocean Blue */
        color: white;
        padding: 10px 20px;
        border-radius: 5px;
        font-size: 16px;
        margin: 5px 0; /* Add some space between the buttons */
        border: none; /* Remove button border */
    }

    .stButton button:hover {
        background-color: #1A6695; /* Darker Blue on hover */
    }

    /* Styling for text and headers */
    .stMarkdown h1, .stMarkdown h2, .stMarkdown h3 {
        color: #2C3E50; /* Dark text color for headers */
    }

    /* Additional styles if necessary */
    .stMarkdown p {
        color: #34495E; /* Text color for paragraph */
    }

    /* Full screen data editor styles */
    .stDataFrame {
        width: 100% !important;
        height: 70vh !important;
    }
    
    .stDataFrame > div {
        width: 100% !important;
        height: 70vh !important;
    }
    
    /* Auto-save indicator styles */
    .auto-save-status {
        position: fixed;
        top: 80px;
        right: 20px;
        background-color: #28a745;
        color: white;
        padding: 8px 12px;
        border-radius: 4px;
        font-size: 14px;
        z-index: 1000;
        opacity: 0.9;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# Function to log actions to the specified audit table
def log_audit(action, status, audit_type):
    """Log an action to the specified audit table."""
    try:
        session = get_active_session()
        current_user = session.get_current_user().replace('"', '')
        current_role = session.get_current_role().replace('"', '')

        if audit_type == "masking":
            audit_sql = f"""
            INSERT INTO PROD_DB_MANAGER.PUBLIC.MASKING_AUDIT (
                ACTIVITY, 
                ACTIVITY_STATUS, 
                ROLE, 
                "USER_NAME", 
                ROW_CREATE_DATE, 
                ROW_MOD_DATE
            )
            VALUES (
                '{action}',
                '{status}', 
                '{current_role}',
                '{current_user}',
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            );
            """
        elif audit_type == "synthetic":
            audit_sql = f"""
            INSERT INTO PROD_DB_MANAGER.PUBLIC.SYNTHETIC_AUDIT (
                ACTIVITY, 
                ACTIVITY_STATUS, 
                ROLE, 
                "USER_NAME", 
                ROW_CREATE_DATE, 
                ROW_MOD_DATE
            )
            VALUES (
                '{action}',
                '{status}', 
                '{current_role}',
                '{current_user}',
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            );
            """
        elif audit_type == "encryption":
            audit_sql = f"""
            INSERT INTO PROD_DB_MANAGER.PUBLIC.ENCRYPTION_AUDIT (
                ACTIVITY, 
                ACTIVITY_STATUS, 
                ROLE, 
                "USER_NAME", 
                ROW_CREATE_DATE, 
                ROW_MOD_DATE
            )
            VALUES (
                '{action}',
                '{status}', 
                '{current_role}',
                '{current_user}',
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            );
            """

        session.sql(audit_sql).collect()
    except Exception as e:
        st.error(f"❌ Error logging to audit: {str(e)}", icon="🚨")

# Main app title
st.sidebar.title("ZDC APP")
app_mode = st.sidebar.radio("Select a function:", 
                             ["Home", 
                              "Synthetic Data Generation", 
                              "Snowflake Masking",
                              "Snowflake Encryption",                              
                              "Classifications"])

# Home Page for the Data Governance App
if app_mode == "Home":
    st.markdown('<h1 class="font">WELCOME TO THE ZDC APP</h1>', unsafe_allow_html=True)
    st.markdown("""
    <p>This application enables users to perform data masking on Snowflake schemas, generate synthetic data for specified schemas, encryption and validate the applied masking.</p>
    <p>Please select a process from the sidebar to generate synthetic data, perform data masking, encryption or validate masking.</p>
    """, unsafe_allow_html=True)

# Synthetic Data Generation App
elif app_mode == "Synthetic Data Generation":
    st.sidebar.subheader("Synthetic Data Generation Process")
    data_gen_mode = st.sidebar.radio("Select a process:", ["Home", "Data Generation"])

    if data_gen_mode == "Home":
        st.markdown('<h1 class="font">Synthetic Data Generation Process</h1>', unsafe_allow_html=True)
        st.markdown("""
        <p>This application is designed to generate synthetic data based on the selected source and target schemas, as well as the selected tables within your Snowflake environment.</p>
        <p>Users can choose a source database and schema from which to extract data and specify a target database and schema where the synthetic data will be stored. If users want to generate synthetic data for specific tables within a schema, they can select only those tables, and the system will generate data accordingly. You need to select either schema-level or table-level synthetic data generation and then proceed by clicking the appropriate button to start the process.</p>
        <p>The generated synthetic data maintains the structure of the source data without compromising any sensitive information.</p>
        <p>To generate synthetic data, each input table or view must meet specific requirements and the following guidelines apply:</p>
        <ul>
            <li>Each input table or view must have a minimum of 20 distinct rows.</li>
            <li>Each input table or view is limited to a maximum of 100 columns.</li>
            <li>The maximum row limit for each input table or view is 14 million rows.</li>
        </ul>
        <p>The following input table types are supported:</p>
        <ul>
            <li>Regular, temporary, dynamic, and transient tables.</li>
            <li>Regular, materialized, secure, and secure materialized views.</li>
        </ul>
        <p>However, the following input table types are not supported:</p>
        <ul>
            <li>External, Apache Iceberg™, and hybrid tables.</li>
            <li>Streams.</li>
        </ul>
        """, unsafe_allow_html=True)

    elif data_gen_mode == "Data Generation":
        st.markdown('<h1 class="font">Synthetic Data Generation</h1>', unsafe_allow_html=True)
        session = get_active_session()

        # Functions to fetch databases, schemas, and tables
        def get_databases(env_prefix=None):
            if env_prefix:
                db_query = f"""
                SELECT DATABASE_NAME 
                FROM INFORMATION_SCHEMA.DATABASES 
                WHERE DATABASE_NAME LIKE '{env_prefix}%' AND DATABASE_NAME NOT LIKE '%_MASKED%' AND DATABASE_NAME NOT LIKE '%_ENCRYPT%'
                """
            else:
                db_query = """
                SELECT DATABASE_NAME 
                FROM INFORMATION_SCHEMA.DATABASES
                """
            rows = session.sql(db_query).collect()
            return [row[0] for row in rows]

        def get_schemas(database):
            schema_query = f"SELECT SCHEMA_NAME FROM {database}.INFORMATION_SCHEMA.SCHEMATA"
            rows = session.sql(schema_query).collect()
            return [row[0] for row in rows]

        def get_tables_for_schema(database, schema):
            table_query = f"""
            SELECT TABLE_NAME 
            FROM {database}.INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_TYPE = 'BASE TABLE'
            """
            rows = session.sql(table_query).collect()
            return [row[0] for row in rows]

        def get_columns_for_table(database, schema, table):
            columns_query = f"""
            SELECT COLUMN_NAME 
            FROM {database}.INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
            """
            rows = session.sql(columns_query).collect()
            return [row[0] for row in rows]

        # Function to check if a table has sufficient non-null values
        def has_valid_data(database, schema, table):
            try:
                column_query = f"""
                SELECT COLUMN_NAME 
                FROM {database}.INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}' 
                ORDER BY ORDINAL_POSITION LIMIT 1
                """
                columns = session.sql(column_query).collect()

                if not columns:
                    return False
                
                first_column = columns[0][0]

                check_query = f"""
                SELECT COUNT(*) 
                FROM {database}.{schema}.{table} 
                WHERE {first_column} IS NOT NULL
                """
                result = session.sql(check_query).collect()
                return result[0][0] > 1  

            except Exception as e:
                st.error(f"Error checking valid data for {database}.{schema}.{table}: {e}")
                return False

        # Environment dropdown selection
        env = st.selectbox("Environment", ["DEV", "QA", "UAT", "PROD"])

        # Get list of source databases based on selected environment
        database_list_source = get_databases(env)

        # Get all databases for target
        database_list_target = (
            get_databases("DEV") +
            get_databases("QA") +
            get_databases("UAT") +
            get_databases("PROD")
        )

        # Organize inputs for source selection in columns
        col1, col2, col3 = st.columns(3)

        with col1:
            selected_source_database = st.selectbox("Source Database", database_list_source, key="source_database")

        with col2:
            if selected_source_database:
                source_schema_list = get_schemas(selected_source_database)
                selected_source_schema = st.selectbox("Source Schema", source_schema_list, key="source_schema")

        with col3:
            if selected_source_schema:
                source_table_list = get_tables_for_schema(selected_source_database, selected_source_schema)

                # Store selected tables and join keys in session state
                if "selected_tables" not in st.session_state:
                    st.session_state.selected_tables = []
                if "join_keys" not in st.session_state:
                    st.session_state.join_keys = {}

                selected_tables = st.multiselect("Select Source Tables", options=source_table_list, key="source_tables", 
                                                  default=st.session_state.selected_tables)

                # Update session state for selected tables
                st.session_state.selected_tables = selected_tables

                for table in selected_tables:
                    # If this table's join key is not stored in the session state, initialize it to empty list
                    if table not in st.session_state.join_keys:
                        st.session_state.join_keys[table] = []

                    # Get column options
                    columns = get_columns_for_table(selected_source_database, selected_source_schema, table)

                    # Check to ensure default join keys are part of available column options
                    default_join_keys = [
                        key for key in st.session_state.join_keys[table] if key in columns
                    ]

                    # Utilize multiselect for Join Key selection
                    join_keys = st.multiselect(
                        f"Join Keys for {table}", 
                        options=columns, 
                        default=default_join_keys,  # Only use valid default join keys
                        key=f"join_keys_{table}"
                    )

                    # Update the join keys in session state
                    st.session_state.join_keys[table] = join_keys

        # Organize inputs for target selection in columns
        col1, col2 = st.columns(2)

        with col1:
            selected_target_database = st.selectbox("Target Database", database_list_target, key="target_database")

        with col2:
            if selected_target_database:
                target_schema_list = get_schemas(selected_target_database)
                selected_target_schema = st.selectbox("Target Schema", target_schema_list, key="target_schema")

        # Text input for custom target table name
        target_table_name = st.text_input("Target Table Name", placeholder="Type target table name or leave blank")

        # Automatically set to the first selected table name if left blank
        default_output_table_names = {table: table for table in selected_tables}  # Store default names for each selected table

        # Buttons for generating synthetic data
        col1, col2 = st.columns(2)

        with col1:
            if st.button("Generate Synthetic Data for Schema"):
                # Generate synthetic data for the entire schema
                try:
                    tables_with_invalid_data = []
                    for table in source_table_list:
                        if not has_valid_data(selected_source_database, selected_source_schema, table):
                            tables_with_invalid_data.append(table)

                    if tables_with_invalid_data:
                        st.warning(
                            f"⚠️ The following tables do not contain sufficient valid data: {', '.join(tables_with_invalid_data)}"
                        )
                        log_audit("Synthetic Data Generation failed due to insufficient data.", "FAILED", "synthetic")
                    else:
                        # Generate synthetic data for each selected table
                        for table in selected_tables:
                            join_keys = st.session_state.join_keys[table]
                            
                            if join_keys:  # Check Join Keys list is not empty
                                for join_key in join_keys:
                                    sql_command = f"""
                                    CALL SNOWFLAKE.DATA_PRIVACY.GENERATE_SYNTHETIC_DATA(
                                        {{
                                            'datasets': [
                                                {{
                                                    'input_table': '{selected_source_database}.{selected_source_schema}.{table}',
                                                    'output_table': '{selected_target_database}.{selected_target_schema}.{table}',
                                                    'columns': {{ '{join_key}':{{'join_key': True}} }}
                                                }}
                                            ],
                                            'replace_output_tables': true
                                        }}
                                    );
                                    """
                                    session.sql(sql_command).collect()
                            else:
                                sql_command = f"""
                                CALL SNOWFLAKE.DATA_PRIVACY.GENERATE_SYNTHETIC_DATA(
                                    {{
                                        'datasets': [
                                            {{
                                                'input_table': '{selected_source_database}.{selected_source_schema}.{table}',
                                                'output_table': '{selected_target_database}.{selected_target_schema}.{table}'
                                            }}
                                        ],
                                        'replace_output_tables': true
                                    }}
                                );
                                """
                                session.sql(sql_command).collect()
                        
                        st.success("✅ Synthetic data has been successfully generated for selected tables!", icon="✅")
                        log_audit("Synthetic Data Generation for schema completed successfully.", "SUCCESS", "synthetic")

                except Exception as e:
                    st.error(f"❌ Error executing SQL command: {e}", icon="🚨")
                    log_audit("Synthetic Data Generation for schema encountered an error.", "FAILED", "synthetic")

        with col2:
            if st.button("Generate Synthetic Data for Tables"):
                # Generate synthetic data for the selected tables
                if selected_tables and selected_target_schema:
                    try:
                        for table in selected_tables:
                            output_table_name = default_output_table_names.get(table, table)
                            join_keys = st.session_state.join_keys[table]

                            if join_keys:  # Check Join Keys list is not empty
                                for join_key in join_keys:
                                    sql_command = f"""
                                    CALL SNOWFLAKE.DATA_PRIVACY.GENERATE_SYNTHETIC_DATA(
                                        {{
                                            'datasets': [
                                                {{
                                                    'input_table': '{selected_source_database}.{selected_source_schema}.{table}',
                                                    'output_table': '{selected_target_database}.{selected_target_schema}.{output_table_name}',
                                                    'columns': {{ '{join_key}':{{'join_key': True}} }}
                                                }}
                                            ],
                                            'replace_output_tables': true
                                        }}
                                    );
                                    """
                                    session.sql(sql_command).collect()
                            else:
                                sql_command = f"""
                                CALL SNOWFLAKE.DATA_PRIVACY.GENERATE_SYNTHETIC_DATA(
                                    {{
                                        'datasets': [
                                            {{
                                                'input_table': '{selected_source_database}.{selected_source_schema}.{table}',
                                                'output_table': '{selected_target_database}.{selected_target_schema}.{output_table_name}'
                                            }}
                                        ],
                                        'replace_output_tables': true
                                    }}
                                );
                                """
                                session.sql(sql_command).collect()
                        
                        st.success("✅ Synthetic data has been successfully generated for the selected tables!", icon="✅")
                        log_audit("Synthetic Data Generation for selected tables completed successfully.", "SUCCESS", "synthetic")
                    except Exception as e:
                        st.error(f"❌ Error executing SQL command: {e}", icon="🚨")
                        log_audit("Synthetic Data Generation for selected tables encountered an error.", "FAILED", "synthetic")
                else:
                    st.error("❌ Please select at least one source table and a target schema.", icon="🚨")

import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

if app_mode == "Snowflake Masking":
    session = get_active_session()

    # Navigation buttons for the masking
    app_mode_masking = st.sidebar.radio("Select Process", [
        "Home",
        "MASKING",
        "MASKING VALIDATION"  # New classification edit option
    ], index=0)

    # Home page for Snowflake Masking app
    if app_mode_masking == "Home":
        st.markdown('<h2 class="font">Snowflake Masking App</h2>', unsafe_allow_html=True)
        st.markdown('<p>This application is designed to assist you with data masking in Snowflake and classfication edit and submission. Please follow each step to mask Snowflake schemas.</p>', unsafe_allow_html=True)

        # Overview of processes
        st.subheader('Overview of Processes:')
        st.markdown("""
        **ALTR Mapper:**
        In this step, we are inserting classification results into the `ALTR_DSAAS_DB.PUBLIC.CLASSIFICATION_DETAILS` table from the ALTR portal.

        **ALTR_CLASSIFICATION_DETAILS:**
        In this step, we are transforming the classification results from `ALTR_DSAAS_DB.PUBLIC.CLASSIFICATION_DETAILS` into the `DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS` table.

        **TRANSFER_CLASSIFICATION_DETAILS:**
        In this step, we transform the latest version of classification data from `DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS` into the `DEV_DB_MANAGER.MASKING.CLASSIFICATION_DETAILS` table for the respective schema.

        **METADATA_REFRESH:**
        This step fetches the respective databases, schemas, tables, and columns from `INFORMATION_SCHEMA` and inserts them into the metadata tables.

        **COLUMN_TAG_MAPPING:**
        In this step, we map columns and tags based on the classification details.

        **INSERT_DATA_OUTPUT_FINAL:**
        In this step, we populate the `DATA_OUTPUT_ID` for each schema and insert respective schema information into the `DATA_SET` table.

        **CREATE_VIEWS:**
        In this step, we create views based on source schemas and tables in target schemas.

        **CLASSIFICATION_GENERATION:**
        In this step, we execute the `CLASSIFICATION_REPORT_V1` procedure to generate classification reports.
        """, unsafe_allow_html=True)

    # Perform selections for Masking
    elif app_mode_masking == "MASKING":
        # Function to get databases based on prefix
        def get_databases(env):
            db_prefix = f"{env}_"
            db_query = f"""
            SELECT DATABASE_NAME
            FROM INFORMATION_SCHEMA.DATABASES
            WHERE DATABASE_NAME LIKE '{db_prefix}%'
            AND DATABASE_NAME NOT LIKE '%_MASKED%' AND DATABASE_NAME NOT LIKE '%_ENCRYPT%'
            """
            rows = session.sql(db_query).collect()
            return [row[0] for row in rows]

        # Function to fetch schemas for a specific database
        def get_schemas(database_name):
            if not database_name:
                return []
            schema_query = f"SELECT SCHEMA_NAME FROM {database_name}.INFORMATION_SCHEMA.SCHEMATA"
            rows = session.sql(schema_query).collect()
            return [row[0] for row in rows]

        # Function to fetch distinct BU names based on environment
        def get_bu_names(env):
            bu_query = f"SELECT DISTINCT BU_NAME FROM {env}_DB_MANAGER.MASKING.CONSUMER"
            try:
                rows = session.sql(bu_query).collect()
                return [row[0] for row in rows]
            except Exception as e:
                st.warning(f"Could not fetch BU names for environment {env}: {e}")
                return []

        # Input selections for masking environment
        masking_environment = st.selectbox("Masking Environment", ["DEV", "QA", "UAT", "PROD"])

        # Get databases based on the selected environment
        masking_database_list = get_databases(masking_environment)
        selected_masking_database = st.selectbox("Database", masking_database_list)

        masking_schema_list = []
        selected_masking_schema = None
        if selected_masking_database:
            masking_schema_list = get_schemas(selected_masking_database)
            selected_masking_schema = st.selectbox("Schema", masking_schema_list)

        # Determine selected_classification_database based on selected_masking_database
        selected_classification_database = None
        if selected_masking_database:
            # Split the database name by '_' and take the part after the environment prefix
            db_suffix = selected_masking_database.split('_', 1)[-1]
            selected_classification_database = f"PROD_{db_suffix}" # Always point to PROD

        # Keep selected_classification_schema the same as selected_masking_schema
        selected_classification_schema = selected_masking_schema

        # Get BU names based on the selected environment
        bu_name_list = get_bu_names(masking_environment)
        selected_bu_name = st.selectbox("BU Name", bu_name_list)

        # Get classification owner based on the new query criteria
        classification_owner_list = []
        if selected_classification_database and selected_classification_schema:
            owner_query = f"""
            WITH latest_import AS (
              SELECT MAX(import_id) AS max_id
              FROM DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS
              WHERE database_name = '{selected_classification_database}'
              AND schema_name = '{selected_classification_schema}'
            )
            SELECT DISTINCT classification_owner
            FROM DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS
            WHERE database_name = '{selected_classification_database}'
              AND schema_name = '{selected_classification_schema}'
              AND import_id = (SELECT max_id FROM latest_import);
            """
            try:
                rows = session.sql(owner_query).collect()
                classification_owner_list = [row[0] for row in rows]
            except Exception as e:
                st.warning(f"Could not fetch classification owner: {e}")
                classification_owner_list = []

        # Use classification owner from query results or fallback to "ALTR"
        selected_classification_owner = classification_owner_list[0] if classification_owner_list else "ALTR"

        # Button to execute all the masking processes
        if st.button("Run Masking"):
            if (selected_masking_database and selected_masking_schema and
                selected_bu_name and selected_classification_database and selected_classification_schema):

                # Track success of all operations
                success = True

                # Execute each process in sequence
                if selected_classification_owner == "ALTR":
                    try:
                        # Execute ALTR MAPPER
                        sql_command = f"""
                        CALL ALTR_DSAAS_DB.PUBLIC.ALTR_TAG_MAPPER(
                            MAPPING_FILE_PATH => BUILD_SCOPED_FILE_URL(@ALTR_DSAAS_DB.PUBLIC.ALTR_TAG_MAPPER_STAGE, 'gdlp-to-hipaa-map.json'),
                            TAG_DB => '{masking_environment}_DB_MANAGER',
                            TAG_SCHEMA => 'MASKING',
                            RUN_COMMENT => '{selected_classification_database} DATABASE CLASSIFICATION',
                            USE_DATABASES => '{selected_classification_database}',
                            EXECUTE_SQL => FALSE,
                            LOG_TABLE => 'CLASSIFICATION_DETAILS'
                        );
                        """
                        session.sql(sql_command).collect()
                        st.success("✅ ALTR MAPPER executed successfully!")
                    except Exception as e:
                        st.error(f"❌ Error executing ALTR MAPPER: {str(e)}")
                        success = False

                    if success:
                        try:
                            # Execute ALTR CLASSIFICATION DETAILS
                            sql_command = f"CALL DEV_DB_MANAGER.MASKING.ALTR_CLASSIFICATION_DETAILS('{selected_classification_database}', '{selected_classification_schema}')"
                            session.sql(sql_command).collect()
                            st.success("✅ ALTR CLASSIFICATION DETAILS executed successfully!")
                        except Exception as e:
                            st.error(f"❌ Error executing ALTR CLASSIFICATION DETAILS: {str(e)}")
                            success = False

                # Section for handling transfers when classification owner is NOT ALTR
                if selected_classification_owner != "ALTR":
                    try:
                        # Execute TRANSFER CLASSIFICATION DETAILS
                        sql_command = f"CALL DEV_DB_MANAGER.MASKING.TRANSFER_CLASSIFICATION_DETAILS('{selected_classification_database}', '{selected_classification_schema}', '{selected_classification_owner}')"
                        session.sql(sql_command).collect()
                        st.success("✅ TRANSFER CLASSIFICATION DETAILS executed successfully!")
                    except Exception as e:
                        st.error(f"❌ Error executing TRANSFER CLASSIFICATION DETAILS: {str(e)}")
                        success = False

                # Metadata Refresh
                if success:
                    try:
                        # Execute Metadata Refresh
                        db_manager = f"{masking_environment}_DB_MANAGER"
                        # Pass the selected masking database to the procedure
                        sql_command = f"CALL {db_manager}.MASKING.UPDATE_METADATA_REFRESH_DATABASE('{selected_masking_database}')"
                        session.sql(sql_command).collect()
                        st.success("✅ Metadata Refresh executed successfully!")
                    except Exception as e:
                        st.error(f"❌ Error executing Metadata Refresh: {str(e)}")
                        success = False

                # Column Tag Mapping
                if success:
                    try:
                        # Execute COLUMN TAG MAPPING
                        sql_command = f"""
                        CALL {masking_environment}_DB_MANAGER.MASKING.COLUMN_TAG_MAPPING(
                            '{selected_classification_database}',
                            '{selected_classification_schema}',
                            '{selected_masking_database}',  -- Use selected masking database
                            '{selected_masking_schema}',    -- Use selected masking schema
                            '{selected_classification_owner}'
                        )
                        """
                        session.sql(sql_command).collect()
                        st.success("✅ COLUMN TAG MAPPING executed successfully!")
                    except Exception as e:
                        st.error(f"❌ Error executing COLUMN TAG MAPPING: {str(e)}")
                        success = False

                # Insert Data Output Final
                if success:
                    try:
                        # Execute INSERT DATA OUTPUT FINAL
                        sql_command = f"""
                        CALL {masking_environment}_DB_MANAGER.MASKING.INSERT_DATA_OUTPUT_FINAL(
                            '{selected_masking_database}',  -- Use selected masking database
                            '{selected_masking_schema}',    -- Use selected masking schema
                            '{selected_bu_name}',
                            '{selected_classification_owner}'
                        )
                        """
                        session.sql(sql_command).collect()
                        st.success("✅ INSERT DATA OUTPUT FINAL executed successfully!")
                    except Exception as e:
                        st.error(f"❌ Error executing INSERT DATA OUTPUT FINAL: {str(e)}")
                        success = False

                 # Classification Generation
                if success:
                    try:
                        # Execute CLASSIFICATION_GENERATION
                        sql_command = f"CALL DEV_DB_MANAGER.MASKING.CLASSIFICATION_REPORT_V1('{selected_classification_database}', '{selected_classification_schema}', '{selected_classification_owner}');"
                        session.sql(sql_command).collect()
                        st.success("✅ CLASSIFICATION_GENERATION executed successfully!")
                    except Exception as e:
                        st.error(f"❌ Error executing CLASSIFICATION_GENERATION: {str(e)}")
                        success = False

                # Create Views
                if success:
                    try:
                        # Execute CREATE VIEWS
                        sql_command = f"""
                        CALL {masking_environment}_DB_MANAGER.MASKING.CREATE_VIEWS(
                            '{selected_masking_database}',  -- Use selected masking database
                            '{selected_masking_schema}',    -- Use selected masking schema
                            '{selected_masking_database}_MASKED',
                            '{selected_masking_schema}'
                        )
                        """
                        session.sql(sql_command).collect()
                        st.success("✅ CREATE VIEWS executed successfully!")
                    except Exception as e:
                        st.error(f"❌ Error executing CREATE VIEWS: {str(e)}")
                        success = False

                try:
                    if success:
                        audit_message = f"MASKING for {selected_masking_database}_MASKED.{selected_masking_schema}"
                        log_audit(audit_message, "Success", "masking")
                    else:
                        audit_message = f"MASKING for {selected_masking_database}_MASKED.{selected_masking_schema}"
                        log_audit(audit_message, "Failure", "masking")
                except Exception as e:
                    st.error(f"❌ Error logging audit: {str(e)}")

                if success:
                    st.success("✅ Completed all processes successfully!")
                else:
                    st.warning("Some steps failed. Please review the errors.")
            else:
                st.warning("Please ensure all selections are made before running the masking process.")
  
           
    elif app_mode_masking == "MASKING VALIDATION":
        # Define all functions inside this block

        def get_databases(env_prefix):
            db_prefix = f"{env_prefix}_"
            db_query = f"""
                SELECT DATABASE_NAME 
                FROM INFORMATION_SCHEMA.DATABASES 
                WHERE DATABASE_NAME LIKE '{db_prefix}%'
            """
            rows = session.sql(db_query).collect()
            return [row[0] for row in rows]

        def get_schemas(database):
            schema_query = f"SELECT SCHEMA_NAME FROM {database}.INFORMATION_SCHEMA.SCHEMATA"
            rows = session.sql(schema_query).collect()
            return [row[0] for row in rows]

        def get_classification_owners(env):
            owner_query = f"""
                SELECT DISTINCT CLASSIFICATION_OWNER
                FROM DEV_DB_MANAGER.MASKING.CLASSIFICATION_DETAILS
            """
            rows = session.sql(owner_query).collect()
            return [row[0] for row in rows]

        def execute_validation_queries_tags(env, selected_database, selected_schema, classification_owner):
            try:
                # Derive production database name
                production_database = selected_database.replace("DEV_", "PROD_").replace("QA_", "PROD_").replace("UAT_", "PROD_")
                # Query source tags
                source_tags_query = f"""
                SELECT COUNT(*) AS total_records
                FROM DEV_DB_MANAGER.MASKING.CLASSIFICATION_DETAILS
                WHERE "DATABASE" = '{production_database}'
                  AND "SCHEMA" = '{selected_schema}'
                  AND CLASSIFICATION_OWNER = '{classification_owner}'
                """
                # Query target tags
                target_tags_query = f"""
                SELECT COUNT(*) AS TAG_COUNT
                FROM DEV_DB_MANAGER.ACCOUNT_USAGE.TAG_REFERENCES
                WHERE OBJECT_DATABASE = '{selected_database}_MASKED'
                  AND OBJECT_SCHEMA = '{selected_schema}'
                """
                source_count = session.sql(source_tags_query).collect()[0][0]
                target_count = session.sql(target_tags_query).collect()[0][0]
                return source_count, target_count
            except Exception as e:
                return None, str(e)

        def execute_validation_queries_tables(env, selected_database, selected_schema):
            try:
                db_manager = f"{env}_DB_MANAGER"
                count_tables_query = f"""
                SELECT COUNT(TABLE_NAME) 
                FROM {selected_database}.INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_CATALOG = '{selected_database}'
                  AND TABLE_SCHEMA = '{selected_schema}'
                  AND TABLE_TYPE = 'BASE TABLE'
                  AND TABLE_NAME NOT LIKE 'RAW_%'
                  AND TABLE_NAME NOT LIKE 'VW_%'
                """
                validation_query = f"""
                SELECT COUNT(*) AS TABLE_COUNT
                FROM {db_manager}.MASKING.MD_TABLE t
                JOIN {db_manager}.MASKING.MD_SCHEMA s ON t.SCHEMA_ID = s.SCHEMA_ID
                JOIN {db_manager}.MASKING.MD_DATABASE d ON s.DATABASE_ID = d.DATABASE_ID
                WHERE d.DATABASE_NAME = '{selected_database}'
                  AND s.SCHEMA_NAME = '{selected_schema}'
                """
                table_count = session.sql(count_tables_query).collect()[0][0]
                validation_count = session.sql(validation_query).collect()[0][0]
                return table_count, validation_count
            except Exception as e:
                return None, str(e)

        def execute_validation_queries_columns(env, selected_database, selected_schema):
            try:
                db_manager = f"{env}_DB_MANAGER"
                count_columns_query = f"""
                SELECT COUNT(c.COLUMN_NAME) AS COLUMN_COUNT
                FROM {selected_database}.INFORMATION_SCHEMA.COLUMNS c
                JOIN {selected_database}.INFORMATION_SCHEMA.TABLES t
                  ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
                WHERE c.TABLE_SCHEMA = '{selected_schema}'
                  AND t.TABLE_TYPE = 'BASE TABLE'
                  AND c.TABLE_NAME NOT LIKE 'RAW_%'
                  AND c.TABLE_NAME NOT LIKE 'VW_%'
                """
                validation_query = f"""
                SELECT COUNT(col.COLUMN_ID) AS COLUMN_COUNT
                FROM {db_manager}.MASKING.MD_DATABASE db
                JOIN {db_manager}.MASKING.MD_SCHEMA sc ON db.DATABASE_ID = sc.DATABASE_ID
                JOIN {db_manager}.MASKING.MD_TABLE tb ON sc.SCHEMA_ID = tb.SCHEMA_ID
                JOIN {db_manager}.MASKING.MD_COLUMN col ON tb.TABLE_ID = col.TABLE_ID
                WHERE db.database_name='{selected_database}'
                  AND sc.schema_name='{selected_schema}'
                  AND db.IS_ACTIVE = TRUE
                  AND sc.IS_ACTIVE = TRUE
                  AND tb.IS_ACTIVE = TRUE
                  AND col.IS_ACTIVE = TRUE
                """
                column_count = session.sql(count_columns_query).collect()[0][0]
                validation_count = session.sql(validation_query).collect()[0][0]
                return column_count, validation_count
            except Exception as e:
                return None, str(e)

        def execute_validation_queries_views(env, selected_database, selected_schema):
            try:
                db_manager = f"{env}_DB_MANAGER"
                count_tables_query = f"""
                SELECT COUNT(TABLE_NAME) 
                FROM {selected_database}.INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_CATALOG = '{selected_database}'
                  AND TABLE_SCHEMA = '{selected_schema}'
                  AND TABLE_TYPE = 'BASE TABLE'
                  AND TABLE_NAME NOT LIKE 'RAW_%'
                  AND TABLE_NAME NOT LIKE 'VW_%'
                """
                count_target_query = f"""
                SELECT COUNT(TABLE_NAME) 
                FROM {selected_database}_MASKED.INFORMATION_SCHEMA.VIEWS
                WHERE TABLE_SCHEMA = '{selected_schema}'
                """
                table_count = session.sql(count_tables_query).collect()[0][0]
                validation_count = session.sql(count_target_query).collect()[0][0]
                return table_count, validation_count
            except Exception as e:
                return None, str(e)

        def execute_validation_queries_data_set(env, selected_database, selected_schema):
            try:
                db_manager = f"{env}_DB_MANAGER"
                count_columns_query = f"""
                SELECT COUNT(col.COLUMN_ID) AS COLUMN_COUNT
                FROM {db_manager}.MASKING.MD_DATABASE db
                JOIN {db_manager}.MASKING.MD_SCHEMA sc ON db.DATABASE_ID = sc.DATABASE_ID
                JOIN {db_manager}.MASKING.MD_TABLE tb ON sc.SCHEMA_ID = tb.SCHEMA_ID
                JOIN {db_manager}.MASKING.MD_COLUMN col ON tb.TABLE_ID = col.TABLE_ID
                WHERE db.database_name='{selected_database}'
                  AND sc.schema_name='{selected_schema}'
                  AND db.IS_ACTIVE = TRUE
                  AND sc.IS_ACTIVE = TRUE
                  AND tb.IS_ACTIVE = TRUE
                  AND col.IS_ACTIVE = TRUE
                """
                validation_query = f"""
                SELECT COUNT(*) AS total_records
                FROM (
                    SELECT DISTINCT
                        ds.data_output_id,
                        d.database_name,
                        s.schema_name,
                        t.table_name,
                        c.column_name
                    FROM {db_manager}.MASKING.DATA_SET ds
                    INNER JOIN {db_manager}.MASKING.MD_DATABASE d ON ds.database_id = d.database_id
                    INNER JOIN {db_manager}.MASKING.MD_SCHEMA s ON ds.schema_id = s.schema_id
                    INNER JOIN {db_manager}.MASKING.MD_TABLE t ON ds.TABLE_ID = t.TABLE_ID
                    INNER JOIN {db_manager}.MASKING.MD_COLUMN c ON ds.COLUMN_ID = c.COLUMN_ID
                    WHERE d.database_name = '{selected_database}'
                      AND s.schema_name = '{selected_schema}'
                      AND ds.data_output_id = (
                          SELECT MAX(ds1.data_output_id) 
                          FROM {db_manager}.MASKING.DATA_SET ds1
                          INNER JOIN {db_manager}.MASKING.MD_DATABASE d1 ON ds1.database_id = d1.database_id
                          INNER JOIN {db_manager}.MASKING.MD_SCHEMA s1 ON ds1.schema_id = s1.schema_id
                          WHERE d1.database_name = '{selected_database}'
                            AND s1.schema_name = '{selected_schema}'
                      )
                ) AS subquery
                """
                column_count = session.sql(count_columns_query).collect()[0][0]
                data_count = session.sql(validation_query).collect()[0][0]
                return column_count, data_count
            except Exception as e:
                return None, str(e)

        # User input selections
        env = st.selectbox("Select Environment", ["DEV", "QA", "UAT", "PROD"])
        database_list = get_databases(env)
        selected_database = st.selectbox("Select Database", database_list, key="db_select")
        schema_list = get_schemas(selected_database)
        selected_schema = st.selectbox("Select Schema", schema_list, key="schema_select")
        classification_owners = get_classification_owners(env)
        classification_owner = st.selectbox("Select Classification Owner", classification_owners)

        if st.button("Run All Validations"):
            results = {}

            # Run all validations
            table_count, table_validation_count = execute_validation_queries_tables(env, selected_database, selected_schema)
            results['MD Tables'] = {
                "Source Count": table_count,
                "Target Count": table_validation_count,
            }
            column_count, column_validation_count = execute_validation_queries_columns(env, selected_database, selected_schema)
            results['MD Columns'] = {
                "Source Count": column_count,
                "Target Count": column_validation_count,
            }
            dataset_count, dataset_data_count = execute_validation_queries_data_set(env, selected_database, selected_schema)
            results['Data Set'] = {
                "Source Count": dataset_count,
                "Target Count": dataset_data_count,
            }
            view_table_count, validation_count = execute_validation_queries_views(env, selected_database, selected_schema)
            results['Views'] = {
                "Source Count": view_table_count,
                "Target Count": validation_count,
            }
            tags_source_count, tags_target_count = execute_validation_queries_tags(env, selected_database, selected_schema, classification_owner)
            results['Tags'] = {
                "Source Count": tags_source_count,
                "Target Count": tags_target_count,
            }

            # Display results
            for validation_type, counts in results.items():
                st.markdown(f"### {validation_type} Validation Results")
                if None in counts.values():
                    st.error(f"Error during {validation_type} validation.")
                else:
                    st.success(f"Source Count: {counts['Source Count']}, Target Count: {counts['Target Count']}")

        
                    
elif app_mode == "Snowflake Encryption":
    session = get_active_session()

    # Navigation buttons for the encryption process
    app_mode_encryption = st.sidebar.radio("Select Process", [
        "Home",
        "ENCRYPTION"
    ], index=0)

    # Home page for Snowflake Encryption app
    if app_mode_encryption == "Home":
        st.markdown('<h2 class="font">Snowflake Encryption App</h2>', unsafe_allow_html=True)
        st.markdown(
            '<p>This application is designed to assist you with data encryption in Snowflake. '
            'Please follow each step to encrypt Snowflake schemas.</p>', unsafe_allow_html=True
        )

       # Overview of processes
        st.subheader('Overview of Processes:')
        st.markdown("""
        **ENCRYPTION:**  
        This application enables you to encrypt Snowflake schema tables. You need to select the database and schema you wish to encrypt. Additionally, you must choose the target environment where the encrypted tables will be deployed. 
        All encrypted databases will have a `_ENCRYPT` suffix, e.g., `DEV_DATALAKE_ENCRYPT`""", unsafe_allow_html=True)

         # Limitations & Workarounds
        st.subheader('Limitations & Workarounds:')
        st.markdown("""
        **JOINS USING ENCRYPTED COLUMNS:**  
        Tables can be joined using encrypted columns.Join columns must be encrypted using the same **KEY**, **TWEAK**, and **ALPHABET**.

 **SINGLE/MULTIPOINT SEARCHES:**  
        Search values should be encrypted using the same **KEY**, **TWEAK**, and **ALPHABET**.

**AGGREGATION (COUNT, SUM):**  
        Values encrypted with the same **NUMERIC ALPHABET** can be aggregated.

   **LEXICOGRAPHIC COMPARISON:**  
        Ciphertext cannot be compared lexicographically without decryption.

**STRING SEARCHES & PATTERN MATCHING (STRATWITH, SUBSTR, LIKE, REGEXP):**  
        Cannot be performed on encrypted data since ciphertext does not resemble plaintext patterns.
		The workaround is to use DECRYPT VIEW.""", unsafe_allow_html=True)

    # Encryption process
    elif app_mode_encryption == "ENCRYPTION":
        session = get_active_session()

        import re
    # Then select Source Database
        def get_databases(env):
            db_prefix = f"{env}_"
            db_query = f"""
            SELECT DATABASE_NAME
            FROM INFORMATION_SCHEMA.DATABASES
            WHERE DATABASE_NAME LIKE '{db_prefix}%'
            AND DATABASE_NAME NOT LIKE '%_MASKED%' AND DATABASE_NAME NOT LIKE '%_ENCRYPT%'
            """
            rows = session.sql(db_query).collect()
            return [row[0] for row in rows]

        # Function to fetch schemas for a specific database
        def get_schemas(database_name):
            if not database_name:
                return []
            schema_query = f"SELECT SCHEMA_NAME FROM {database_name}.INFORMATION_SCHEMA.SCHEMATA"
            rows = session.sql(schema_query).collect()
            return [row[0] for row in rows]

        # Function to fetch distinct BU names based on environment
        def get_bu_names(env):
            bu_query = f"SELECT DISTINCT BU_NAME FROM {env}_DB_MANAGER.MASKING.CONSUMER"
            try:
                rows = session.sql(bu_query).collect()
                return [row[0] for row in rows]
            except Exception as e:
                st.warning(f"Could not fetch BU names for environment {env}: {e}")
                return []

        # Input selections for masking environment
        encryption_environment = st.selectbox("Encryption Environment", ["DEV", "QA", "UAT", "PROD"])

        # Get databases based on the selected environment
        masking_database_list = get_databases(encryption_environment)
        selected_masking_database = st.selectbox("Database", masking_database_list)

        masking_schema_list = []
        selected_masking_schema = None
        if selected_masking_database:
            masking_schema_list = get_schemas(selected_masking_database)
            selected_masking_schema = st.selectbox("Schema", masking_schema_list)

        # Determine selected_classification_database based on selected_masking_database
        selected_classification_database = None
        if selected_masking_database:
            # Split the database name by '_' and take the part after the environment prefix
            db_suffix = selected_masking_database.split('_', 1)[-1]
            selected_classification_database = f"PROD_{db_suffix}" # Always point to PROD

        # Keep selected_classification_schema the same as selected_masking_schema
        selected_classification_schema = selected_masking_schema

        # Get BU names based on the selected environment
        bu_name_list = get_bu_names(encryption_environment)
        selected_bu_name = st.selectbox("BU Name", bu_name_list)

        # Get classification owner based on the new query criteria
        classification_owner_list = []
        if selected_classification_database and selected_classification_schema:
            owner_query = f"""
            WITH latest_import AS (
              SELECT MAX(import_id) AS max_id
              FROM DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS
              WHERE database_name = '{selected_classification_database}'
              AND schema_name = '{selected_classification_schema}'
            )
            SELECT DISTINCT classification_owner
            FROM DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS
            WHERE database_name = '{selected_classification_database}'
              AND schema_name = '{selected_classification_schema}'
              AND import_id = (SELECT max_id FROM latest_import);
            """
            try:
                rows = session.sql(owner_query).collect()
                classification_owner_list = [row[0] for row in rows]
            except Exception as e:
                st.warning(f"Could not fetch classification owner: {e}")
                classification_owner_list = []

        # Use classification owner from query results or fallback to "ALTR"
        selected_classification_owner = classification_owner_list[0] if classification_owner_list else "ALTR"

        # Button to execute all the masking processes
        if st.button("Run Encryption"):
            if (selected_masking_database and selected_masking_schema and
                selected_bu_name and selected_classification_database and selected_classification_schema):

                # Track success of all operations
                success = True

                # Execute each process in sequence
                if selected_classification_owner == "ALTR":
                    try:
                        # Execute ALTR MAPPER
                        sql_command = f"""
                        CALL ALTR_DSAAS_DB.PUBLIC.ALTR_TAG_MAPPER(
                            MAPPING_FILE_PATH => BUILD_SCOPED_FILE_URL(@ALTR_DSAAS_DB.PUBLIC.ALTR_TAG_MAPPER_STAGE, 'gdlp-to-hipaa-map.json'),
                            TAG_DB => '{encryption_environment}_DB_MANAGER',
                            TAG_SCHEMA => 'MASKING',
                            RUN_COMMENT => '{selected_classification_database} DATABASE CLASSIFICATION',
                            USE_DATABASES => '{selected_classification_database}',
                            EXECUTE_SQL => FALSE,
                            LOG_TABLE => 'CLASSIFICATION_DETAILS'
                        );
                        """
                        session.sql(sql_command).collect()
                        st.success("✅ ALTR MAPPER executed successfully!")
                    except Exception as e:
                        st.error(f"❌ Error executing ALTR MAPPER: {str(e)}")
                        success = False

                    if success:
                        try:
                            # Execute ALTR CLASSIFICATION DETAILS
                            sql_command = f"CALL DEV_DB_MANAGER.MASKING.ALTR_CLASSIFICATION_DETAILS('{selected_classification_database}', '{selected_classification_schema}')"
                            session.sql(sql_command).collect()
                            st.success("✅ ALTR CLASSIFICATION DETAILS executed successfully!")
                        except Exception as e:
                            st.error(f"❌ Error executing ALTR CLASSIFICATION DETAILS: {str(e)}")
                            success = False

                # Section for handling transfers when classification owner is NOT ALTR
                if selected_classification_owner != "ALTR":
                    try:
                        # Execute TRANSFER CLASSIFICATION DETAILS
                        sql_command = f"CALL DEV_DB_MANAGER.MASKING.TRANSFER_CLASSIFICATION_DETAILS('{selected_classification_database}', '{selected_classification_schema}', '{selected_classification_owner}')"
                        session.sql(sql_command).collect()
                        st.success("✅ TRANSFER CLASSIFICATION DETAILS executed successfully!")
                    except Exception as e:
                        st.error(f"❌ Error executing TRANSFER CLASSIFICATION DETAILS: {str(e)}")
                        success = False

                # Insert Data Output Final
                if success:
                    try:
                        # Execute INSERT DATA OUTPUT FINAL
                        sql_command = f"""
                        CALL {encryption_environment}_DB_MANAGER.MASKING.INSERT_DATA_OUTPUT_FINAL_ENCRYPTION(
                            '{selected_masking_database}',  -- Use selected masking database
                            '{selected_masking_schema}',    -- Use selected masking schema
                            '{selected_bu_name}',
                            '{selected_classification_owner}'
                        )
                        """
                        session.sql(sql_command).collect()
                        st.success("✅ INSERT DATA OUTPUT FINAL executed successfully!")
                    except Exception as e:
                        st.error(f"❌ Error executing INSERT DATA OUTPUT FINAL: {str(e)}")
                        success = False

                # Classification Generation
                if success:
                    try:
                        # Execute CLASSIFICATION_GENERATION
                        sql_command = f"CALL DEV_DB_MANAGER.MASKING.CLASSIFICATION_REPORT_V1('{selected_classification_database}', '{selected_classification_schema}', '{selected_classification_owner}');"
                        session.sql(sql_command).collect()
                        st.success("✅ CLASSIFICATION_GENERATION executed successfully!")
                    except Exception as e:
                        st.error(f"❌ Error executing CLASSIFICATION_GENERATION: {str(e)}")
                        success = False


                # Create Tables
                if success:
                    try:
                        # Execute CREATE TABLES
                        sql_command = f"""
                        CALL {encryption_environment}_DB_MANAGER.ENCRYPTION.ENCRYPT_TABLES(
                            '{selected_masking_database}',  -- Use selected masking database
                            '{selected_masking_schema}',    -- Use selected masking schema
                            '{selected_masking_database}_ENCRYPT',
                            '{selected_masking_schema}'
                        )
                        """
                        session.sql(sql_command).collect()
                        st.success("✅ CREATE TABLES executed successfully!")
                    except Exception as e:
                        st.error(f"❌ Error executing CREATE TABLES: {str(e)}")
                        success = False
                # Insert a single audit record for the entire process
                try:
                    if success:
                        audit_message = f"ENCRYPTION for {selected_masking_database}_ENCRYPT.{selected_masking_schema}"
                        log_audit(audit_message, "Success", "encryption")
                    else:
                        audit_message = f"ENCRYPTION for {selected_masking_database}_ENCRYPT.{selected_masking_schema}"
                        log_audit(audit_message, "Failure", "encryption")
                except Exception as e:
                    st.error(f"❌ Error logging audit: {str(e)}")

                if success:
                    st.success("✅ Completed all processes successfully!")
                else:
                    st.warning("Some steps failed. Please review the errors.")
            else:
                st.warning("Please ensure all selections are made before running the masking process.")

# Classifications App with Auto-Save
if app_mode == "Classifications":
    session = get_active_session()

    # Main UI
    app_mode_classification = st.sidebar.radio("Select Process", ["Home", "Classification edit and Submission"], index=0)

    if app_mode_classification == "Home":
        st.markdown('<h2 class="font">Classifications</h2>', unsafe_allow_html=True)
        st.markdown('<p>This page is designed to assist you with classification editing and submission in Snowflake.</p>', unsafe_allow_html=True)
       
        st.subheader('Overview of Processes:')
        st.markdown('<p>To review the classification report, you need to select a specific database and schema, then click on "Get Classification Report." </p>', unsafe_allow_html=True)
        st.markdown('<p>Once the report is displayed, review the classifications based on the BU_APPROVAL_STATUS field. You can select options such as APPROVED, MASKED, or NO MASKING NEEDED.</p>', unsafe_allow_html=True)
        st.markdown('<p><strong>The classification report now features auto-save functionality - your changes are automatically saved as you edit. No need to manually save!</strong></p>', unsafe_allow_html=True)
        st.markdown('<p>After reviewing the entire classification report and making necessary edits, you can finally submit the report. Based on your review, a new classification report will be generated and stored in the DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_REPORT table.</p>', unsafe_allow_html=True)
        st.markdown('<p>Note: Your edits are automatically saved, but the report must be officially submitted only once to complete the process. </p>', unsafe_allow_html=True)
    
    elif app_mode_classification == "Classification edit and Submission":
        # Session state initialization
        for key in ["report_fetched", "edited_df", "submitted", "confirm_submission", "last_save_time", "auto_save_key", "save_status"]:
            if key not in st.session_state:
                if key == "edited_df":
                    st.session_state[key] = None
                elif key == "last_save_time":
                    st.session_state[key] = 0
                elif key == "auto_save_key":
                    st.session_state[key] = 0
                elif key == "save_status":
                    st.session_state[key] = ""
                else:
                    st.session_state[key] = False

        # Helper functions
        def fetch_databases():
            session = get_active_session()
            rows = session.sql("""
                SELECT DATABASE_NAME FROM INFORMATION_SCHEMA.DATABASES 
                WHERE DATABASE_NAME LIKE 'PROD_%' AND DATABASE_NAME NOT LIKE '%_MASKED%' AND DATABASE_NAME NOT LIKE '%_ENCRYPT%'
            """).collect()
            return [row[0] for row in rows]

        def fetch_schemas(database):
            session = get_active_session()
            rows = session.sql(f"SELECT SCHEMA_NAME FROM {database}.INFORMATION_SCHEMA.SCHEMATA").collect()
            return [row[0] for row in rows]

        def fetch_classification_report(database, schema):
            session = get_active_session()
            query = f"""
                SELECT * 
                FROM DEV_DB_MANAGER.MASKING.CLASSIFICATION_REPORT_V1
                WHERE DATABASE_NAME = '{database}' 
                  AND SCHEMA_NAME = '{schema}' 
                  AND VERSION = (
                      SELECT MAX(VERSION)
                      FROM DEV_DB_MANAGER.MASKING.CLASSIFICATION_REPORT_V1
                      WHERE DATABASE_NAME = '{database}' 
                        AND SCHEMA_NAME = '{schema}'
                  )
            """
            return session.sql(query).collect()

        def save_classification_report(df, database, schema):
            session = get_active_session()
            try:
                values = []
                for _, row in df.iterrows():
                    # Handle potential None values and escape single quotes
                    def safe_str(val):
                        if val is None:
                            return ''
                        return str(val).replace("'", "''")
                    
                    values.append(f"""(
                        '{safe_str(database)}', '{safe_str(schema)}', '{safe_str(row['CLASSIFICATION_OWNER'])}', '{safe_str(row['DATE'])}',
                        '{safe_str(row['TABLE_NAME'])}', '{safe_str(row['COLUMN_NAME'])}', '{safe_str(row['CLASSIFICATION'])}',
                        '{safe_str(row['HIPAA_CLASS'])}', '{safe_str(row['MASKED'])}', '{safe_str(row['BU_APPROVAL_STATUS'])}',
                        '{safe_str(row['BU_COMMENTS'])}', '{safe_str(row['BU_ASSIGNEE'])}', '{safe_str(row['INFOSEC_APPROVAL_STATUS'])}',
                        '{safe_str(row['INFOSEC_APPROVER'])}', '{safe_str(row['INFOSEC_COMMENTS'])}',
                        {int(row['IS_ACTIVE']) if row['IS_ACTIVE'] is not None else 0},
                        {int(row['VERSION']) if row['VERSION'] is not None else 1},
                        {int(row['ID'])}
                    )""")
                values_str = ",\n".join(values)
                merge_sql = f"""
                    MERGE INTO DEV_DB_MANAGER.MASKING.CLASSIFICATION_REPORT_V1 AS target
                    USING (
                        SELECT * FROM VALUES
                        {values_str}
                        AS source (
                            DATABASE_NAME, SCHEMA_NAME, CLASSIFICATION_OWNER, DATE,
                            TABLE_NAME, COLUMN_NAME, CLASSIFICATION, HIPAA_CLASS,
                            MASKED, BU_APPROVAL_STATUS, BU_COMMENTS, BU_ASSIGNEE,
                            INFOSEC_APPROVAL_STATUS, INFOSEC_APPROVER, INFOSEC_COMMENTS,
                            IS_ACTIVE, VERSION, ID
                        )
                    ) AS source
                    ON target.ID = source.ID
                    WHEN MATCHED THEN UPDATE SET
                        DATE = source.DATE,
                        DATABASE_NAME = source.DATABASE_NAME,
                        SCHEMA_NAME = source.SCHEMA_NAME,
                        TABLE_NAME = source.TABLE_NAME,
                        COLUMN_NAME = source.COLUMN_NAME,
                        CLASSIFICATION = source.CLASSIFICATION,
                        HIPAA_CLASS = source.HIPAA_CLASS,
                        MASKED = source.MASKED,
                        BU_APPROVAL_STATUS = source.BU_APPROVAL_STATUS,
                        BU_COMMENTS = source.BU_COMMENTS,
                        BU_ASSIGNEE = source.BU_ASSIGNEE,
                        INFOSEC_APPROVAL_STATUS = source.INFOSEC_APPROVAL_STATUS,
                        INFOSEC_APPROVER = source.INFOSEC_APPROVER,
                        INFOSEC_COMMENTS = source.INFOSEC_COMMENTS,
                        IS_ACTIVE = source.IS_ACTIVE,
                        CLASSIFICATION_OWNER = source.CLASSIFICATION_OWNER,
                        VERSION = source.VERSION
                    WHEN NOT MATCHED THEN INSERT (
                        DATABASE_NAME, SCHEMA_NAME, CLASSIFICATION_OWNER, DATE,
                        TABLE_NAME, COLUMN_NAME, CLASSIFICATION, HIPAA_CLASS,
                        MASKED, BU_APPROVAL_STATUS, BU_COMMENTS, BU_ASSIGNEE,
                        INFOSEC_APPROVAL_STATUS, INFOSEC_APPROVER, INFOSEC_COMMENTS,
                        IS_ACTIVE, VERSION, ID
                    )
                    VALUES (
                        source.DATABASE_NAME, source.SCHEMA_NAME, source.CLASSIFICATION_OWNER, source.DATE,
                        source.TABLE_NAME, source.COLUMN_NAME, source.CLASSIFICATION, source.HIPAA_CLASS,
                        source.MASKED, source.BU_APPROVAL_STATUS, source.BU_COMMENTS, source.BU_ASSIGNEE,
                        source.INFOSEC_APPROVAL_STATUS, source.INFOSEC_APPROVER, source.INFOSEC_COMMENTS,
                        source.IS_ACTIVE, source.VERSION, source.ID
                    )
                """
                session.sql(merge_sql).collect()
                st.session_state.last_save_time = time.time()
                st.session_state.save_status = "success"
                return True
            except Exception as e:
                st.session_state.save_status = f"error: {str(e)}"
                return False

        def insert_raw_classification_details(database, schema, bu_name):
            session = get_active_session()

            # Define mapping for classification owner and HIPAA class
            classification_mapping = {
                "I&E Business Intelligence": ("IE_BU", "IE_PII"),
                "PRICE": ("PRICE_BU", "PRICE_PII"),
                "Marketing": ("MARKETING_BU", "MARKETING_PII"),
                "ZDI Provider Intelligence": ("PROVIDER_BU", "PROVIDER_PII"),
                "ZDI Member Intelligence": ("MEMBER_BU", "MEMBER_PII"),
                "Payments Optimization": ("PAYMENTS_BU", "PAYMENTS_PII"),
                "ZDI Data Science Engineer": ("DSE_BU", "DSE_PII"),
                "ZEDI Claims & Price Intelligence": ("CPI_BU", "CPI_PII"),
            }

            # Get classification owner and HIPAA class based on bu_name
            classification_owner, hipaa_class = classification_mapping.get(bu_name, (None, None))

            if classification_owner is None or hipaa_class is None:
                st.error("Invalid BU Name selected. Please select a valid BU.")
                return False

            # Determine the maximum version for the specific combination of database, schema, and classification owner
            max_version_row = session.sql(f"""
                SELECT MAX(VERSION) 
                FROM DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS 
                WHERE DATABASE_NAME = '{database}' 
                    AND SCHEMA_NAME = '{schema}' 
                    AND CLASSIFICATION_OWNER = '{classification_owner}'
            """).first()

            max_version = max_version_row[0] if max_version_row[0] is not None else 0
            new_version = max_version + 1  # Increment the version for the insert

            fetch_sql = f"""
                SELECT * 
                FROM DEV_DB_MANAGER.MASKING.CLASSIFICATION_REPORT_V1
                WHERE DATABASE_NAME = '{database}' 
                    AND SCHEMA_NAME = '{schema}' 
                    AND VERSION = (
                        SELECT MAX(VERSION)
                        FROM DEV_DB_MANAGER.MASKING.CLASSIFICATION_REPORT_V1
                        WHERE DATABASE_NAME = '{database}' 
                            AND SCHEMA_NAME = '{schema}'
                    )
                    AND ((BU_APPROVAL_STATUS = 'APPROVED' AND MASKED = 'YES') 
                    OR (BU_APPROVAL_STATUS = 'MASK' AND MASKED = 'NO'))
            """
            classification_details = session.sql(fetch_sql).collect()

            if not classification_details:
                st.warning("No classification details available for insertion.")
                return False

            insert_values = []
            duplicate_count = 0  # Counter for duplicate records

            for row in classification_details:
                # Check for existing records to prevent duplicates
                existing_record_check = session.sql(f"""
                    SELECT COUNT(*) 
                    FROM DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS
                    WHERE DATABASE_NAME = '{database}'
                        AND SCHEMA_NAME = '{schema}'
                        AND CLASSIFICATION_OWNER = '{classification_owner}'
                        AND TABLE_NAME = '{row['TABLE_NAME']}'
                        AND COLUMN_NAME = '{row['COLUMN_NAME']}'
                        AND HIPAA_CLASS = '{hipaa_class}'
                        AND BU_APPROVAL_STATUS = '{row['BU_APPROVAL_STATUS']}'
                        AND BU_COMMENTS = '{row['BU_COMMENTS']}'
                        AND BU_ASSIGNEE = '{row['BU_ASSIGNEE']}'
                        AND INFOSEC_APPROVAL_STATUS = '{row['INFOSEC_APPROVAL_STATUS']}'
                        AND INFOSEC_APPROVER = '{row['INFOSEC_APPROVER']}'
                        AND INFOSEC_COMMENTS = '{row['INFOSEC_COMMENTS']}'
                        AND IS_ACTIVE = TRUE
                """).first()[0]

                if existing_record_check > 0:
                    duplicate_count += 1  # Increment the duplicate counter
                    continue

                max_import_id_row = session.sql("SELECT MAX(IMPORT_ID) FROM DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS").first()
                max_import_id = max_import_id_row[0] if max_import_id_row[0] is not None else 0
                new_import_id = max_import_id + 1

                # Mark existing records as inactive
                session.sql(f"""
                    UPDATE DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS
                    SET IS_ACTIVE = false
                    WHERE DATABASE_NAME = '{database}'
                        AND SCHEMA_NAME = '{schema}'
                        AND CLASSIFICATION_OWNER = '{classification_owner}'
                """).collect()

                insert_values.append(f"""(
                    {new_import_id}, '{row['DATE']}', '{database}', '{schema}', 
                    '{row['TABLE_NAME']}', '{row['COLUMN_NAME']}', 'HIPAA', 
                    '{hipaa_class}', '{row['BU_APPROVAL_STATUS']}', '{row['BU_COMMENTS']}', 
                    '{row['BU_ASSIGNEE']}', '{row['INFOSEC_APPROVAL_STATUS']}', 
                    '{row['INFOSEC_APPROVER']}', '{row['INFOSEC_COMMENTS']}', 
                    true, '{classification_owner}', {new_version}
                )""")

            if insert_values:
                values_str = ",\n".join(insert_values)
                insert_sql = f"""
                    INSERT INTO DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS (
                        IMPORT_ID, DATE, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, 
                        CLASSIFICATION, HIPAA_CLASS, BU_APPROVAL_STATUS, BU_COMMENTS, 
                        BU_ASSIGNEE, INFOSEC_APPROVAL_STATUS, INFOSEC_APPROVER, 
                        INFOSEC_COMMENTS, IS_ACTIVE, CLASSIFICATION_OWNER, VERSION
                    ) VALUES {values_str}
                """

                try:
                    session.sql(insert_sql).collect()
                    return True
                except Exception as e:
                    st.error(f"Error inserting into RAW_CLASSIFICATION_DETAILS: {e}")
                    return False
            else:
                # Show a consolidated duplicate message
                if duplicate_count > 0:
                    st.info(f"{duplicate_count} records already exist for the specified classification criteria. Skipping these entries.")
                else:
                    st.info("No new records to insert.")
                return False

        # Function to fetch distinct BU names
        def get_bu_names():
            session = get_active_session()
            rows = session.sql("SELECT DISTINCT BU_NAME FROM DEV_DB_MANAGER.MASKING.CONSUMER").collect()
            return [row[0] for row in rows]

        # UI for classification report editing
        st.title("Classification Report Editor")

        database = st.selectbox("Select Database", fetch_databases())
        if database:
            schema = st.selectbox("Select Schema", fetch_schemas(database))
            if schema and st.button("Get Classification Report"):
                data = fetch_classification_report(database, schema)
                if data:
                    df = pd.DataFrame([row.as_dict() for row in data])
                    # Get current user
                    try:
                        current_user = get_active_session().sql("SELECT CURRENT_USER()").collect()[0][0]
                    except:
                        current_user = get_active_session().get_current_user()
                    # Replace BU_ASSIGNEE with current user
                    df['BU_ASSIGNEE'] = current_user
                    st.session_state.edited_df = df.copy()
                    st.session_state.report_fetched = True
                    st.session_state.auto_save_key += 1  # Increment key to reset data_editor
                    st.session_state.save_status = ""
                else:
                    st.warning("No data found for the selected database and schema.")

        # Editable DataFrame with auto-save
        if st.session_state.report_fetched and st.session_state.edited_df is not None:
            st.subheader("Edit Classification Report (Auto-Save Enabled)")
            
            # Display save status and last save time
            status_col1, status_col2 = st.columns([2, 1])
            with status_col1:
                if st.session_state.save_status == "success" and st.session_state.last_save_time > 0:
                    last_save_str = time.strftime("%H:%M:%S", time.localtime(st.session_state.last_save_time))
                    st.success(f"✅ Auto-saved at: {last_save_str}", icon="💾")
                elif st.session_state.save_status.startswith("error"):
                    st.error(f"❌ Save failed: {st.session_state.save_status[7:]}")
            
            with status_col2:
                if st.session_state.last_save_time > 0:
                    last_save_str = time.strftime("%H:%M:%S", time.localtime(st.session_state.last_save_time))
                    st.caption(f"Last saved: {last_save_str}")

            # Ensure the relevant columns are treated as categories with specific options
            st.session_state.edited_df['BU_APPROVAL_STATUS'] = st.session_state.edited_df['BU_APPROVAL_STATUS'].astype('category')
            st.session_state.edited_df['BU_APPROVAL_STATUS'] = st.session_state.edited_df['BU_APPROVAL_STATUS'].cat.set_categories(['MASK', 'APPROVED', 'NO MASKING NEEDED'])

            st.session_state.edited_df['INFOSEC_APPROVAL_STATUS'] = st.session_state.edited_df['INFOSEC_APPROVAL_STATUS'].astype('category')
            st.session_state.edited_df['INFOSEC_APPROVAL_STATUS'] = st.session_state.edited_df['INFOSEC_APPROVAL_STATUS'].cat.set_categories(['MASK', 'APPROVED', 'NO MASKING NEEDED'])

            # Create the data editor with full screen height
            edited_df = st.data_editor(
                st.session_state.edited_df, 
                num_rows="dynamic", 
                use_container_width=True,
                height=600,  # Fixed height for better full-screen experience
                key=f"data_editor_{st.session_state.auto_save_key}"
            )

            # Auto-save functionality - check if data has changed (without page refresh)
            if not edited_df.equals(st.session_state.edited_df):
                # Update session state with new data
                st.session_state.edited_df = edited_df.copy()
                
                # Auto-save the changes silently
                save_success = save_classification_report(edited_df, database, schema)
                
                # Update save status without causing refresh
                if save_success:
                    st.session_state.save_status = "success"
                    st.session_state.last_save_time = time.time()
                else:
                    st.session_state.save_status = "error"

            st.subheader("Submit Classifications")
            bu_name = st.selectbox("Select BU Name", get_bu_names())
            if bu_name and st.button("Submit Classifications"):
                success = insert_raw_classification_details(database, schema, bu_name)
                if success:
                    st.success("Classification details inserted successfully!")