import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import time

# Enhanced Custom CSS for styling
st.markdown(
    """
    <style>
    /* Global App Styling */
    .stApp {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        min-height: 100vh;
    }

    /* Main content container */
    .main .block-container {
        background-color: rgba(255, 255, 255, 0.95);
        border-radius: 15px;
        padding: 2rem;
        margin: 1rem;
        box-shadow: 0 10px 30px rgba(0, 0, 0, 0.2);
        backdrop-filter: blur(10px);
    }

    /* Custom font styling */
    .font {
        font-size: 2.5rem;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        color: #2c3e50;
        text-transform: uppercase;
        font-weight: 700;
        text-align: center;
        margin-bottom: 2rem;
        background: linear-gradient(45deg, #667eea, #764ba2);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }

    /* Sidebar styling */
    .css-1d391kg {
        background: linear-gradient(180deg, #667eea 0%, #764ba2 100%);
    }

    .sidebar .sidebar-content {
        background: linear-gradient(180deg, #667eea 0%, #764ba2 100%);
        color: white;
    }

    /* Enhanced button styling */
    .stButton button {
        background: linear-gradient(45deg, #667eea, #764ba2);
        color: white;
        padding: 12px 24px;
        border-radius: 25px;
        font-size: 16px;
        font-weight: 600;
        margin: 8px 4px;
        border: none;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4);
        transition: all 0.3s ease;
        text-transform: uppercase;
        letter-spacing: 1px;
    }

    .stButton button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(102, 126, 234, 0.6);
        background: linear-gradient(45deg, #764ba2, #667eea);
    }

    /* Select box styling */
    .stSelectbox > div > div {
        background-color: #f8f9ff;
        border: 2px solid #667eea;
        border-radius: 10px;
    }

    /* Data editor container - Full width and height */
    .stDataFrame {
        width: 100% !important;
        height: 70vh !important;
    }

    /* Data editor styling */
    div[data-testid="stDataFrame"] {
        width: 100% !important;
        height: 70vh !important;
        border: 2px solid #667eea;
        border-radius: 10px;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.2);
    }

    /* Enhanced data editor table */
    .stDataFrame > div {
        width: 100% !important;
        height: 70vh !important;
    }

    /* Table headers */
    .stDataFrame th {
        background: linear-gradient(45deg, #667eea, #764ba2) !important;
        color: white !important;
        font-weight: 600 !important;
        font-size: 14px !important;
        padding: 12px 8px !important;
    }

    /* Table cells */
    .stDataFrame td {
        padding: 10px 8px !important;
        font-size: 13px !important;
        border-bottom: 1px solid #e0e6ed !important;
    }

    /* Auto-save indicator */
    .auto-save-indicator {
        position: fixed;
        top: 20px;
        right: 20px;
        background: linear-gradient(45deg, #00c851, #00a142);
        color: white;
        padding: 10px 20px;
        border-radius: 25px;
        font-weight: 600;
        box-shadow: 0 4px 15px rgba(0, 200, 81, 0.3);
        z-index: 1000;
        animation: fadeInOut 2s ease-in-out;
    }

    @keyframes fadeInOut {
        0%, 100% { opacity: 0; }
        50% { opacity: 1; }
    }

    /* Success/Error messages */
    .stSuccess {
        background: linear-gradient(45deg, #00c851, #00a142);
        border-radius: 10px;
        padding: 15px;
        margin: 10px 0;
    }

    .stError {
        background: linear-gradient(45deg, #ff4444, #cc0000);
        border-radius: 10px;
        padding: 15px;
        margin: 10px 0;
    }

    /* Card-like containers */
    .metric-card {
        background: white;
        padding: 20px;
        border-radius: 15px;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
        margin: 10px 0;
        border-left: 5px solid #667eea;
    }

    /* Headers and text styling */
    .stMarkdown h1, .stMarkdown h2, .stMarkdown h3 {
        color: #2c3e50;
        font-weight: 600;
    }

    .stMarkdown p {
        color: #34495e;
        line-height: 1.6;
    }

    /* Full-width container for data editor */
    .data-editor-container {
        width: 100%;
        height: 75vh;
        margin: 20px 0;
        padding: 20px;
        background: white;
        border-radius: 15px;
        box-shadow: 0 6px 25px rgba(0, 0, 0, 0.1);
    }

    /* Info boxes */
    .stInfo {
        background: linear-gradient(45deg, #17a2b8, #138496);
        border-radius: 10px;
        padding: 15px;
        margin: 10px 0;
    }

    /* Warning boxes */
    .stWarning {
        background: linear-gradient(45deg, #ffc107, #e0a800);
        border-radius: 10px;
        padding: 15px;
        margin: 10px 0;
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

# Auto-save function for classification reports
def auto_save_classification_report(df, database, schema):
    """Auto-save classification report with enhanced error handling"""
    try:
        session = get_active_session()
        values = []
        for _, row in df.iterrows():
            # Escape single quotes to prevent SQL injection
            def escape_sql(value):
                if value is None:
                    return 'NULL'
                return str(value).replace("'", "''")
            
            values.append(f"""(
                '{escape_sql(database)}', '{escape_sql(schema)}', '{escape_sql(row['CLASSIFICATION_OWNER'])}', 
                '{escape_sql(row['DATE'])}', '{escape_sql(row['TABLE_NAME'])}', '{escape_sql(row['COLUMN_NAME'])}', 
                '{escape_sql(row['CLASSIFICATION'])}', '{escape_sql(row['HIPAA_CLASS'])}', '{escape_sql(row['MASKED'])}', 
                '{escape_sql(row['BU_APPROVAL_STATUS'])}', '{escape_sql(row['BU_COMMENTS'])}', '{escape_sql(row['BU_ASSIGNEE'])}', 
                '{escape_sql(row['INFOSEC_APPROVAL_STATUS'])}', '{escape_sql(row['INFOSEC_APPROVER'])}', 
                '{escape_sql(row['INFOSEC_COMMENTS'])}',
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
        """
        
        session.sql(merge_sql).collect()
        return True
    except Exception as e:
        st.error(f"Auto-save failed: {str(e)}")
        return False

# Main app title
st.sidebar.title("🚀 ZDC APP")
app_mode = st.sidebar.radio("Select a function:",
                             ["🏠 Home",
                              "🔄 Synthetic Data Generation",
                              "🔒 Snowflake Masking",
                              "🔐 Snowflake Encryption",                             
                              "📊 Classifications"])

# Home Page for the Data Governance App
if app_mode == "🏠 Home":
    st.markdown('<h1 class="font">🌟 WELCOME TO THE ZDC APP</h1>', unsafe_allow_html=True)
    
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown("""
        <div class="metric-card">
            <h3>🎯 Application Overview</h3>
            <p>This application enables users to perform data masking on Snowflake schemas, generate synthetic data for specified schemas, encryption and validate the applied masking.</p>
            <p>Please select a process from the sidebar to generate synthetic data, perform data masking, encryption or validate masking.</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="metric-card">
            <h3>🚀 Quick Start</h3>
            <ul>
                <li>🔄 Generate synthetic data</li>
                <li>🔒 Apply data masking</li>
                <li>🔐 Enable encryption</li>
                <li>📊 Manage classifications</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

# Classifications Section with Enhanced UI and Auto-save
elif app_mode == "📊 Classifications":
    session = get_active_session()

    app_mode_classification = st.sidebar.radio("Select Process", [
        "🏠 Home", 
        "📝 Classification Edit & Submission"
    ], index=0)

    if app_mode_classification == "🏠 Home":
        st.markdown('<h1 class="font">📊 Classifications Management</h1>', unsafe_allow_html=True)
        
        col1, col2 = st.columns([3, 2])
        with col1:
            st.markdown("""
            <div class="metric-card">
                <h3>🎯 Overview of Processes</h3>
                <p><strong>📋 Review Classification Report:</strong> Select a specific database and schema, then click "Get Classification Report" to view the current classifications.</p>
                <p><strong>✏️ Edit Classifications:</strong> Review classifications based on the BU_APPROVAL_STATUS field. Select options such as APPROVED, MASKED, or NO MASKING NEEDED.</p>
                <p><strong>💾 Auto-Save Feature:</strong> All changes are automatically saved as you edit. No manual save required!</p>
                <p><strong>📤 Submit Report:</strong> After reviewing and editing, submit the final report for processing.</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
            <div class="metric-card">
                <h3>⚡ Key Features</h3>
                <ul>
                    <li>🔄 Real-time auto-save</li>
                    <li>📊 Full-screen data editing</li>
                    <li>🎨 Enhanced UI/UX</li>
                    <li>⚡ Faster workflow</li>
                    <li>🔒 Secure data handling</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)

    elif app_mode_classification == "📝 Classification Edit & Submission":
        # Initialize session state
        for key in ["report_fetched", "edited_df", "submitted", "confirm_submission", "auto_save_counter"]:
            if key not in st.session_state:
                st.session_state[key] = False if key not in ["edited_df", "auto_save_counter"] else (None if key == "edited_df" else 0)

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

        def get_bu_names():
            session = get_active_session()
            rows = session.sql("SELECT DISTINCT BU_NAME FROM DEV_DB_MANAGER.MASKING.CONSUMER").collect()
            return [row[0] for row in rows]

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
            new_version = max_version + 1

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
            duplicate_count = 0

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
                    duplicate_count += 1
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
                if duplicate_count > 0:
                    st.info(f"{duplicate_count} records already exist for the specified classification criteria. Skipping these entries.")
                else:
                    st.info("No new records to insert.")
                return False

        # Enhanced UI for classification report editing
        st.markdown('<h1 class="font">📝 Classification Report Editor</h1>', unsafe_allow_html=True)

        # Database and Schema selection with enhanced styling
        col1, col2, col3 = st.columns([2, 2, 2])
        
        with col1:
            database = st.selectbox("🗄️ Select Database", fetch_databases(), key="db_selector")
        
        with col2:
            if database:
                schema = st.selectbox("📁 Select Schema", fetch_schemas(database), key="schema_selector")
            else:
                schema = None
                
        with col3:
            if database and schema:
                if st.button("📊 Get Classification Report", key="get_report_btn"):
                    with st.spinner("🔄 Fetching classification report..."):
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
                            st.session_state.edited_df = df
                            st.session_state.report_fetched = True
                            st.success(f"✅ Successfully loaded {len(df)} records!")
                        else:
                            st.warning("⚠️ No data found for the selected database and schema.")

        # Enhanced Editable DataFrame with Auto-save
        if st.session_state.report_fetched and st.session_state.edited_df is not None:
            st.markdown("---")
            st.markdown('<h2 style="color: #667eea; font-weight: 600;">📊 Interactive Classification Editor</h2>', unsafe_allow_html=True)
            
            # Auto-save status indicator
            auto_save_placeholder = st.empty()
            
            # Enhanced data editor container
            st.markdown('<div class="data-editor-container">', unsafe_allow_html=True)
            
            # Prepare categories for dropdown fields
            st.session_state.edited_df['BU_APPROVAL_STATUS'] = st.session_state.edited_df['BU_APPROVAL_STATUS'].astype('category')
            st.session_state.edited_df['BU_APPROVAL_STATUS'] = st.session_state.edited_df['BU_APPROVAL_STATUS'].cat.set_categories(['MASK', 'APPROVED', 'NO MASKING NEEDED'])
            
            st.session_state.edited_df['INFOSEC_APPROVAL_STATUS'] = st.session_state.edited_df['INFOSEC_APPROVAL_STATUS'].astype('category')
            st.session_state.edited_df['INFOSEC_APPROVAL_STATUS'] = st.session_state.edited_df['INFOSEC_APPROVAL_STATUS'].cat.set_categories(['MASK', 'APPROVED', 'NO MASKING NEEDED'])

            # Data editor with enhanced configuration
            edited_df = st.data_editor(
                st.session_state.edited_df,
                num_rows="dynamic",
                use_container_width=True,
                height=600,  # Set explicit height
                key="classification_editor",
                column_config={
                    "BU_APPROVAL_STATUS": st.column_config.SelectboxColumn(
                        "BU Approval Status",
                        options=["MASK", "APPROVED", "NO MASKING NEEDED"],
                        required=True,
                    ),
                    "INFOSEC_APPROVAL_STATUS": st.column_config.SelectboxColumn(
                        "InfoSec Approval Status", 
                        options=["MASK", "APPROVED", "NO MASKING NEEDED"],
                        required=True,
                    ),
                    "BU_COMMENTS": st.column_config.TextColumn(
                        "BU Comments",
                        width="medium",
                        max_chars=500,
                    ),
                    "INFOSEC_COMMENTS": st.column_config.TextColumn(
                        "InfoSec Comments",
                        width="medium", 
                        max_chars=500,
                    ),
                }
            )
            
            st.markdown('</div>', unsafe_allow_html=True)

            # Auto-save logic - Check if data has changed
            if not edited_df.equals(st.session_state.edited_df):
                st.session_state.edited_df = edited_df
                st.session_state.auto_save_counter += 1
                
                # Show auto-save indicator
                with auto_save_placeholder:
                    st.markdown('<div class="auto-save-indicator">💾 Auto-saving...</div>', unsafe_allow_html=True)
                
                # Perform auto-save
                success = auto_save_classification_report(edited_df, database, schema)
                
                if success:
                    # Show success indicator briefly
                    time.sleep(0.5)
                    with auto_save_placeholder:
                        st.markdown('<div class="auto-save-indicator" style="background: linear-gradient(45deg, #00c851, #00a142);">✅ Auto-saved successfully!</div>', unsafe_allow_html=True)
                    time.sleep(1)
                    auto_save_placeholder.empty()
                else:
                    with auto_save_placeholder:
                        st.markdown('<div class="auto-save-indicator" style="background: linear-gradient(45deg, #ff4444, #cc0000);">❌ Auto-save failed!</div>', unsafe_allow_html=True)

            # Enhanced submission section
            st.markdown("---")
            st.markdown('<h2 style="color: #667eea; font-weight: 600;">📤 Submit Classifications</h2>', unsafe_allow_html=True)
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                bu_name = st.selectbox("🏢 Select BU Name", get_bu_names(), key="bu_selector")
                
            with col2:
                if bu_name:
                    if st.button("🚀 Submit Classifications", key="submit_btn"):
                        with st.spinner("📤 Submitting classifications..."):
                            success = insert_raw_classification_details(database, schema, bu_name)
                            if success:
                                st.success("🎉 Classification details submitted successfully!")
                                log_audit(f"Classification submission for {database}.{schema}", "SUCCESS", "classification")
                            else:
                                st.error("❌ Failed to submit classification details.")
                                log_audit(f"Classification submission for {database}.{schema}", "FAILED", "classification")

            # Statistics summary
            if edited_df is not None:
                st.markdown("---")
                st.markdown('<h3 style="color: #667eea;">📈 Current Statistics</h3>', unsafe_allow_html=True)
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    total_records = len(edited_df)
                    st.metric("📊 Total Records", total_records)
                
                with col2:
                    approved_count = len(edited_df[edited_df['BU_APPROVAL_STATUS'] == 'APPROVED'])
                    st.metric("✅ Approved", approved_count)
                
                with col3:
                    mask_count = len(edited_df[edited_df['BU_APPROVAL_STATUS'] == 'MASK'])
                    st.metric("🔒 To Mask", mask_count)
                
                with col4:
                    no_mask_count = len(edited_df[edited_df['BU_APPROVAL_STATUS'] == 'NO MASKING NEEDED'])
                    st.metric("🚫 No Masking", no_mask_count)

# Add the rest of your existing code for other modes (Synthetic Data Generation, Snowflake Masking, Snowflake Encryption)
# ... (keeping all the existing functionality for other modes)

# Synthetic Data Generation App
elif app_mode == "🔄 Synthetic Data Generation":
    st.sidebar.subheader("Synthetic Data Generation Process")
    data_gen_mode = st.sidebar.radio("Select a process:", ["Home", "Data Generation"])

    if data_gen_mode == "Home":
        st.markdown('<h1 class="font">🔄 Synthetic Data Generation Process</h1>', unsafe_allow_html=True)
        st.markdown("""
        <div class="metric-card">
            <h3>🎯 Overview</h3>
            <p>This application is designed to generate synthetic data based on the selected source and target schemas, as well as the selected tables within your Snowflake environment.</p>
            <p>Users can choose a source database and schema from which to extract data and specify a target database and schema where the synthetic data will be stored. If users want to generate synthetic data for specific tables within a schema, they can select only those tables, and the system will generate data accordingly. You need to select either schema-level or table-level synthetic data generation and then proceed by clicking the appropriate button to start the process.</p>
            <p>The generated synthetic data maintains the structure of the source data without compromising any sensitive information.</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
        <div class="metric-card">
            <h3>📋 Requirements</h3>
            <p>To generate synthetic data, each input table or view must meet specific requirements and the following guidelines apply:</p>
            <ul>
                <li>Each input table or view must have a minimum of 20 distinct rows.</li>
                <li>Each input table or view is limited to a maximum of 100 columns.</li>
                <li>The maximum row limit for each input table or view is 14 million rows.</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("""
            <div class="metric-card">
                <h3>✅ Supported Table Types</h3>
                <ul>
                    <li>Regular, temporary, dynamic, and transient tables.</li>
                    <li>Regular, materialized, secure, and secure materialized views.</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
            <div class="metric-card">
                <h3>❌ Not Supported</h3>
                <ul>
                    <li>External, Apache Iceberg™, and hybrid tables.</li>
                    <li>Streams.</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)

    elif data_gen_mode == "Data Generation":
        st.markdown('<h1 class="font">🔄 Synthetic Data Generation</h1>', unsafe_allow_html=True)
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
        env = st.selectbox("🌍 Environment", ["DEV", "QA", "UAT", "PROD"])

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
        st.markdown("### 📥 Source Configuration")
        col1, col2, col3 = st.columns(3)

        with col1:
            selected_source_database = st.selectbox("🗄️ Source Database", database_list_source, key="source_database")

        with col2:
            if selected_source_database:
                source_schema_list = get_schemas(selected_source_database)
                selected_source_schema = st.selectbox("📁 Source Schema", source_schema_list, key="source_schema")

        with col3:
            if selected_source_schema:
                source_table_list = get_tables_for_schema(selected_source_database, selected_source_schema)

                # Store selected tables and join keys in session state
                if "selected_tables" not in st.session_state:
                    st.session_state.selected_tables = []
                if "join_keys" not in st.session_state:
                    st.session_state.join_keys = {}

                selected_tables = st.multiselect("📊 Select Source Tables", options=source_table_list, key="source_tables",
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
                        f"🔗 Join Keys for {table}",
                        options=columns,
                        default=default_join_keys,  # Only use valid default join keys
                        key=f"join_keys_{table}"
                    )

                    # Update the join keys in session state
                    st.session_state.join_keys[table] = join_keys

        # Organize inputs for target selection in columns
        st.markdown("### 📤 Target Configuration")
        col1, col2 = st.columns(2)

        with col1:
            selected_target_database = st.selectbox("🗄️ Target Database", database_list_target, key="target_database")

        with col2:
            if selected_target_database:
                target_schema_list = get_schemas(selected_target_database)
                selected_target_schema = st.selectbox("📁 Target Schema", target_schema_list, key="target_schema")

        # Text input for custom target table name
        target_table_name = st.text_input("📝 Target Table Name", placeholder="Type target table name or leave blank")

        # Automatically set to the first selected table name if left blank
        default_output_table_names = {table: table for table in selected_tables}  # Store default names for each selected table

        # Buttons for generating synthetic data
        st.markdown("### 🚀 Generate Synthetic Data")
        col1, col2 = st.columns(2)

        with col1:
            if st.button("🔄 Generate for Schema", key="gen_schema_btn"):
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
            if st.button("📊 Generate for Tables", key="gen_tables_btn"):
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

# Snowflake Masking Section
elif app_mode == "🔒 Snowflake Masking":
    session = get_active_session()

    # Navigation buttons for the masking
    app_mode_masking = st.sidebar.radio("Select Process", [
        "Home",
        "MASKING",
        "MASKING VALIDATION"
    ], index=0)

    # Home page for Snowflake Masking app
    if app_mode_masking == "Home":
        st.markdown('<h1 class="font">🔒 Snowflake Masking App</h1>', unsafe_allow_html=True)
        
        st.markdown("""
        <div class="metric-card">
            <h3>🎯 Overview</h3>
            <p>This application is designed to assist you with data masking in Snowflake and classification edit and submission. Please follow each step to mask Snowflake schemas.</p>
        </div>
        """, unsafe_allow_html=True)

        # Overview of processes
        st.markdown('<h3 style="color: #667eea;">📋 Process Overview</h3>', unsafe_allow_html=True)
        
        processes = [
            ("ALTR Mapper", "In this step, we are inserting classification results into the `ALTR_DSAAS_DB.PUBLIC.CLASSIFICATION_DETAILS` table from the ALTR portal."),
            ("ALTR Classification Details", "In this step, we are transforming the classification results from `ALTR_DSAAS_DB.PUBLIC.CLASSIFICATION_DETAILS` into the `DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS` table."),
            ("Transfer Classification Details", "In this step, we transform the latest version of classification data from `DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS` into the `DEV_DB_MANAGER.MASKING.CLASSIFICATION_DETAILS` table for the respective schema."),
            ("Metadata Refresh", "This step fetches the respective databases, schemas, tables, and columns from `INFORMATION_SCHEMA` and inserts them into the metadata tables."),
            ("Column Tag Mapping", "In this step, we map columns and tags based on the classification details."),
            ("Insert Data Output Final", "In this step, we populate the `DATA_OUTPUT_ID` for each schema and insert respective schema information into the `DATA_SET` table."),
            ("Create Views", "In this step, we create views based on source schemas and tables in target schemas."),
            ("Classification Generation", "In this step, we execute the `CLASSIFICATION_REPORT_V1` procedure to generate classification reports.")
        ]
        
        for title, description in processes:
            st.markdown(f"""
            <div class="metric-card">
                <h4>🔧 {title}</h4>
                <p>{description}</p>
            </div>
            """, unsafe_allow_html=True)

    # Rest of your existing masking functionality...
    # [Continue with the existing masking code from your original file]

# Snowflake Encryption Section
elif app_mode == "🔐 Snowflake Encryption":
    session = get_active_session()

    # Navigation buttons for the encryption process
    app_mode_encryption = st.sidebar.radio("Select Process", [
        "Home",
        "ENCRYPTION"
    ], index=0)

    # Home page for Snowflake Encryption app
    if app_mode_encryption == "Home":
        st.markdown('<h1 class="font">🔐 Snowflake Encryption App</h1>', unsafe_allow_html=True)
        
        st.markdown("""
        <div class="metric-card">
            <h3>🎯 Overview</h3>
            <p>This application is designed to assist you with data encryption in Snowflake. Please follow each step to encrypt Snowflake schemas.</p>
            <p><strong>ENCRYPTION:</strong> This application enables you to encrypt Snowflake schema tables. You need to select the database and schema you wish to encrypt. Additionally, you must choose the target environment where the encrypted tables will be deployed.</p>
            <p>All encrypted databases will have a <code>_ENCRYPT</code> suffix, e.g., <code>DEV_DATALAKE_ENCRYPT</code></p>
        </div>
        """, unsafe_allow_html=True)

        # Limitations & Workarounds
        st.markdown('<h3 style="color: #667eea;">⚠️ Limitations & Workarounds</h3>', unsafe_allow_html=True)
        
        limitations = [
            ("JOINS USING ENCRYPTED COLUMNS", "Tables can be joined using encrypted columns. Join columns must be encrypted using the same **KEY**, **TWEAK**, and **ALPHABET**."),
            ("SINGLE/MULTIPOINT SEARCHES", "Search values should be encrypted using the same **KEY**, **TWEAK**, and **ALPHABET**."),
            ("AGGREGATION (COUNT, SUM)", "Values encrypted with the same **NUMERIC ALPHABET** can be aggregated."),
            ("LEXICOGRAPHIC COMPARISON", "Ciphertext cannot be compared lexicographically without decryption."),
            ("STRING SEARCHES & PATTERN MATCHING", "Cannot be performed on encrypted data since ciphertext does not resemble plaintext patterns. The workaround is to use DECRYPT VIEW.")
        ]
        
        for title, description in limitations:
            st.markdown(f"""
            <div class="metric-card">
                <h4>🔒 {title}</h4>
                <p>{description}</p>
            </div>
            """, unsafe_allow_html=True)

    # Rest of your existing encryption functionality...
    # [Continue with the existing encryption code from your original file]