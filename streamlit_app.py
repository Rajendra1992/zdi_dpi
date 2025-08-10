import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time

# Page configuration
st.set_page_config(
    page_title="ZDQ - Data Quality Hub",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: 700;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
        background: linear-gradient(90deg, #1f77b4, #ff7f0e);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    
    .sub-header {
        font-size: 1.5rem;
        font-weight: 600;
        color: #2c3e50;
        margin-bottom: 1rem;
        border-left: 4px solid #3498db;
        padding-left: 1rem;
    }
    
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 15px;
        color: white;
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
        text-align: center;
        margin: 1rem 0;
    }
    
    .success-card {
        background: linear-gradient(135deg, #28a745 0%, #20c997 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
        box-shadow: 0 4px 15px rgba(40, 167, 69, 0.3);
    }
    
    .failure-card {
        background: linear-gradient(135deg, #dc3545 0%, #fd7e14 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
        box-shadow: 0 4px 15px rgba(220, 53, 69, 0.3);
    }
    
    .info-box {
        background: linear-gradient(135deg, #74b9ff 0%, #0984e3 100%);
        padding: 1.5rem;
        border-radius: 15px;
        color: white;
        margin: 1rem 0;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
    }
    
    .sidebar .sidebar-content {
        background: linear-gradient(180deg, #667eea 0%, #764ba2 100%);
    }
    
    .stSelectbox > div > div {
        background-color: #f8f9fa;
        border-radius: 10px;
    }
    
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 25px;
        padding: 0.75rem 2rem;
        font-weight: 600;
        transition: all 0.3s ease;
        box-shadow: 0 4px 15px rgba(0,0,0,0.2);
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(0,0,0,0.3);
    }
    
    .nav-card {
        background: white;
        padding: 2rem;
        border-radius: 15px;
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
        border-left: 4px solid #3498db;
        margin: 1rem 0;
        transition: transform 0.3s ease;
    }
    
    .nav-card:hover {
        transform: translateY(-5px);
    }
    
    /* Enhanced table styling with Brazilian colors */
    .dataframe {
        border-collapse: collapse;
        margin: 1rem 0;
        font-size: 0.9rem;
        border-radius: 10px;
        overflow: hidden;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
    }
    
    .dataframe th {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        font-weight: 600;
        padding: 1rem;
        text-align: center;
        border: none;
    }
    
    .dataframe td {
        padding: 0.8rem;
        text-align: center;
        border-bottom: 1px solid #e9ecef;
    }
    
    /* Brazilian light colors for alternating rows */
    .dataframe tbody tr:nth-child(odd) {
        background-color: #fff8e1; /* Light warm yellow */
    }
    
    .dataframe tbody tr:nth-child(even) {
        background-color: #e8f5e8; /* Light green */
    }
    
    .dataframe tbody tr:hover {
        background-color: #e3f2fd; /* Light blue on hover */
        transform: scale(1.02);
        transition: all 0.3s ease;
    }
    
    /* Status-specific styling */
    .status-success {
        background: linear-gradient(135deg, #28a745 0%, #20c997 100%) !important;
        color: white !important;
        font-weight: bold !important;
        border-radius: 20px !important;
        padding: 0.5rem 1rem !important;
        margin: 0.2rem !important;
        box-shadow: 0 2px 8px rgba(40, 167, 69, 0.3) !important;
    }
    
    .status-failure {
        background: linear-gradient(135deg, #dc3545 0%, #fd7e14 100%) !important;
        color: white !important;
        font-weight: bold !important;
        border-radius: 20px !important;
        padding: 0.5rem 1rem !important;
        margin: 0.2rem !important;
        box-shadow: 0 2px 8px rgba(220, 53, 69, 0.3) !important;
    }
</style>
""", unsafe_allow_html=True)

# Initialize Snowflake session
@st.cache_resource
def init_snowflake_session():
    try:
        session = get_active_session()
        return session, True
    except:
        return None, False

session, session_connected = init_snowflake_session()

if not session_connected:
    st.error("❌ Could not establish Snowflake connection. Please ensure you're properly connected.")
    st.stop()

# Environment to database mapping
ENV_DB_MAP = {
    "DEV": "dev_db_manager",
    "QA": "qa_db_manager", 
    "UAT": "uat_db_manager",
    "PROD": "prod_db_manager"
}

# Sidebar navigation with icons
st.sidebar.markdown("### ZDQ ")
page = st.sidebar.radio(
    "Choose your validation process:",
    ["🏠 Home", "📊 Data Ingestion DQ", "🎭 Masking DQ", "🔐 Encryption DQ"],
    label_visibility="collapsed"
)

# Data fetching functions with caching
@st.cache_data(ttl=300)
def fetch_list(query):
    if session:
        try:
            return [row['name'] for row in session.sql(query).collect()]
        except:
            return []
    return []

@st.cache_data(ttl=300)
def fetch_databases(environment):
    return fetch_list("SHOW DATABASES") if session else []

@st.cache_data(ttl=300)
def fetch_schemas(database_name):
    return fetch_list(f"SHOW SCHEMAS IN DATABASE {database_name}") if session else []

@st.cache_data(ttl=300)
def fetch_source_db_types(environment):
    db_name = ENV_DB_MAP.get(environment)
    if not db_name: return []
    query = f"SELECT DISTINCT db_type FROM {db_name}.public.audit_recon WHERE db_type IS NOT NULL"
    try:
        return [row['DB_TYPE'] for row in session.sql(query).collect()] if session else []
    except:
        return []

@st.cache_data(ttl=300)
def fetch_load_groups(environment):
    db_name = ENV_DB_MAP.get(environment)
    if not db_name: return []
    query = f"SELECT DISTINCT LOAD_GROUP FROM {db_name}.public.audit_recon"
    try:
        return [row['LOAD_GROUP'] for row in session.sql(query).collect()] if session else []
    except:
        return []

@st.cache_data(ttl=300)
def fetch_tables(database, schema):
    """Fetch tables from a specific database and schema"""
    if not database or not schema: return []
    query = f"""
        SELECT TABLE_NAME 
        FROM {database}.INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema}' 
        AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
    """
    try:
        return [row['TABLE_NAME'] for row in session.sql(query).collect()] if session else []
    except:
        return []

@st.cache_data(ttl=300)
def fetch_columns(database, schema, table):
    """Fetch columns from a specific table"""
    if not database or not schema or not table: return []
    query = f"""
        SELECT COLUMN_NAME 
        FROM {database}.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = '{schema}' 
        AND TABLE_NAME = '{table}'
        ORDER BY ORDINAL_POSITION
    """
    try:
        return [row['COLUMN_NAME'] for row in session.sql(query).collect()] if session else []
    except:
        return []

def get_source_target_tables(load_group, load_type, source_db_type, environment):
    db_name = ENV_DB_MAP.get(environment)
    if not db_name: return [], []

    query_template = lambda db_type: f"""
        SELECT upper(table_name) as table_name, row_count FROM {db_name}.public.audit_recon
        WHERE LOAD_GROUP IN ('{load_group}') AND LOAD_TYPE IN ('{load_type}') AND db_type = '{db_type}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY TABLE_NAME ORDER BY ROW_CRE_DT) = 1
    """

    try:
        source_result = session.sql(query_template(source_db_type)).collect() if session else []
        target_result = session.sql(query_template('SNOWFLAKE')).collect() if session else []

        source_tables = [{'table_name': r['TABLE_NAME'], 'row_count': r['ROW_COUNT']} for r in source_result]
        target_tables = [{'table_name': r['TABLE_NAME'], 'row_count': r['ROW_COUNT']} for r in target_result]

        source_tables.sort(key=lambda x: x['table_name'])
        target_tables.sort(key=lambda x: x['table_name'])

        return source_tables, target_tables
    except Exception as e:
        st.error(f"Error fetching tables: {e}")
        return [], []

def run_count_validation(selected_load_group, load_type, source_db_type, environment):
    with st.spinner("🔄 Running count validation..."):
        s_list, t_list = get_source_target_tables(selected_load_group, load_type, source_db_type, environment)
        max_len = max(len(s_list), len(t_list))
        rows = []

        for i in range(max_len):
            s = s_list[i] if i < len(s_list) else {'table_name': 'N/A', 'row_count': 0}
            t = t_list[i] if i < len(t_list) else {'table_name': 'N/A', 'row_count': 0}
            test_result = "SUCCESS" if s['row_count'] == t['row_count'] else "FAILURE"

            detail_msg = ""
            if test_result == "FAILURE":
                if s['row_count'] > t['row_count']:
                    detail_msg = "Source count is greater than target count"
                elif t['row_count'] > s['row_count']:
                    detail_msg = "Target count is greater than source count"

            rows.append({
                "Load Type": load_type,
                "Load Group": selected_load_group,
                "Environment": environment,
                "SOURCE_TABLE": s['table_name'],
                "SOURCE_ROWS": s['row_count'],
                "TARGET_TABLE": t['table_name'],
                "TARGET_ROWS": t['row_count'],
                "Test Case": test_result,
                "Details": detail_msg
            })

        df = pd.DataFrame(rows)
        return df

def run_data_validation(selected_db, selected_schema, load_type, selected_load_group, environment):
    with st.spinner("🔄 Running data validation..."):
        query_tables = f"""
            SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME
            FROM {selected_db}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{selected_schema}'
            AND TABLE_NAME IN (
                SELECT DISTINCT UPPER(TABLE_NAME)
                FROM {ENV_DB_MAP[environment]}.public.audit_recon
                WHERE LOAD_GROUP IN ('{selected_load_group}')
                AND LOAD_TYPE IN ('{load_type}')
            )
            AND COLUMN_NAME NOT LIKE 'ROW_%'
            AND COLUMN_NAME NOT LIKE 'RAW_%'
        """

        try:
            tables_df = session.sql(query_tables).to_pandas()
            if tables_df.empty:
                st.info("ℹ️ No tables found for the given criteria.")
                return pd.DataFrame([])

            results = []
            env_datalake_map = {
                "DEV": "dev_datalake",
                "QA": "qa_datalake",
                "UAT": "uat_datalake",
                "PROD": "prod_datalake"
            }

            progress_bar = st.progress(0)
            for idx, (_, row) in enumerate(tables_df.iterrows()):
                schema_name = row['TABLE_SCHEMA']
                table_name = row['TABLE_NAME']
                
                progress_bar.progress((idx + 1) / len(tables_df))

                source_db_name = env_datalake_map.get(selected_db, f"{selected_db}_RAW")

                try:
                    # Target vs View
                    target_vs_view_query = f"""
                        SELECT COUNT(*) AS DIFF_COUNT FROM (
                            SELECT * EXCLUDE (ROW_CRE_DT, ROW_MOD_DT, ROW_CRE_USR_ID, ROW_MOD_USR_ID, RAW_ROW_CRE_DT)
                            FROM {selected_db}.{schema_name}.{table_name}
                            MINUS
                            SELECT DISTINCT * EXCLUDE (RAW_ROW_CRE_DT)
                            FROM {source_db_name}.{schema_name}.VW_RAW_{table_name}
                        )
                    """
                    t2v_result = session.sql(target_vs_view_query).collect()
                    t2v_diff = t2v_result[0]['DIFF_COUNT'] if t2v_result else 0

                    # View vs Target
                    view_vs_target_query = f"""
                        SELECT COUNT(*) AS DIFF_COUNT FROM (
                            SELECT DISTINCT * EXCLUDE (RAW_ROW_CRE_DT)
                            FROM {source_db_name}.{schema_name}.VW_RAW_{table_name}
                            MINUS
                            SELECT * EXCLUDE (ROW_CRE_DT, ROW_MOD_DT, ROW_CRE_USR_ID, ROW_MOD_USR_ID, RAW_ROW_CRE_DT)
                            FROM {selected_db}.{schema_name}.{table_name}
                        )
                    """
                    v2t_result = session.sql(view_vs_target_query).collect()
                    v2t_diff = v2t_result[0]['DIFF_COUNT'] if v2t_result else 0

                except Exception as e:
                    st.warning(f"⚠️ Error comparing {schema_name}.{table_name}: {e}")
                    t2v_diff = v2t_diff = -1

                test_case_result = "SUCCESS" if t2v_diff == 0 and v2t_diff == 0 else "FAILURE"

                results.append({
                    "Load Type": load_type,
                    "Load Group": selected_load_group,
                    "Environment": environment,
                    "Database": selected_db,
                    "Schema": schema_name,
                    "Table": table_name,
                    "TARGET VS VIEW": t2v_diff,
                    "VIEW VS TARGET": v2t_diff,
                    "Test Case": test_case_result
                })

            progress_bar.empty()
            return pd.DataFrame(results)
        except Exception as e:
            st.error(f"❌ Error during data validation: {e}")
            return pd.DataFrame([])

def run_duplicate_validation(selected_db, selected_schema, load_type, selected_load_group, environment):
    with st.spinner("🔄 Running duplicate validation..."):
        query_tables = f"""
            SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME
            FROM {selected_db}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{selected_schema}'
            AND TABLE_NAME IN (
                SELECT DISTINCT UPPER(TABLE_NAME)
                FROM {ENV_DB_MAP[environment]}.public.audit_recon
                WHERE LOAD_GROUP IN ('{selected_load_group}')
                AND LOAD_TYPE IN ('{load_type}')
            )
            AND COLUMN_NAME NOT LIKE 'ROW_%'
            AND COLUMN_NAME NOT LIKE 'RAW_%'
        """

        try:
            tables_df = session.sql(query_tables).to_pandas()
            if tables_df.empty:
                st.info("ℹ️ No tables found for the given criteria.")
                return pd.DataFrame([])

            results = []
            progress_bar = st.progress(0)

            for idx, (_, row) in enumerate(tables_df.iterrows()):
                schema_name = row['TABLE_SCHEMA']
                table_name = row['TABLE_NAME']
                
                progress_bar.progress((idx + 1) / len(tables_df))

                try:
                    dup_query = f"""
                        SELECT COUNT(*) AS DUP_COUNT FROM (
                            SELECT * EXCLUDE (ROW_CRE_DT, ROW_MOD_DT, ROW_CRE_USR_ID, ROW_MOD_USR_ID, RAW_ROW_CRE_DT)
                            FROM {selected_db}.{schema_name}.{table_name}
                            GROUP BY ALL
                            HAVING COUNT(*) > 1
                        )
                    """
                    result = session.sql(dup_query).collect()
                    dup_count = result[0]['DUP_COUNT'] if result else 0
                    test_case_result = "SUCCESS" if dup_count == 0 else "FAILURE"
                except Exception as e:
                    st.warning(f"⚠️ Error checking duplicates in {schema_name}.{table_name}: {e}")
                    dup_count = -1
                    test_case_result = "FAILURE"

                results.append({
                    "Load Type": load_type,
                    "Load Group": selected_load_group,
                    "Environment": environment,
                    "Database": selected_db,
                    "Schema": schema_name,
                    "Table": table_name,
                    "DUP COUNT": dup_count,
                    "Test Case": test_case_result
                })

            progress_bar.empty()
            return pd.DataFrame(results)
        except Exception as e:
            st.error(f"❌ Error during duplicate validation: {e}")
            return pd.DataFrame([])

def run_distinct_count_validation(selected_db, selected_schema, selected_table, selected_column, environment):
    """Run distinct count validation for a specific column"""
    with st.spinner("🔄 Running distinct count validation..."):
        try:
            # Get distinct count
            distinct_query = f"""
                SELECT COUNT(DISTINCT {selected_column}) AS DISTINCT_COUNT,
                       COUNT({selected_column}) AS TOTAL_COUNT,
                       COUNT(*) AS TOTAL_ROWS
                FROM {selected_db}.{selected_schema}.{selected_table}
            """
            
            result = session.sql(distinct_query).collect()
            
            if result:
                distinct_count = result[0]['DISTINCT_COUNT']
                total_count = result[0]['TOTAL_COUNT']
                total_rows = result[0]['TOTAL_ROWS']
                null_count = total_rows - total_count
                
                # Calculate uniqueness percentage
                uniqueness_pct = (distinct_count / total_count * 100) if total_count > 0 else 0
                
                # Determine test case result based on uniqueness
                # You can adjust these thresholds based on your requirements
                if uniqueness_pct == 100:
                    test_case = "SUCCESS"
                    details = "All values are unique"
                elif uniqueness_pct >= 90:
                    test_case = "SUCCESS"
                    details = f"High uniqueness: {uniqueness_pct:.2f}%"
                elif uniqueness_pct >= 50:
                    test_case = "SUCCESS"
                    details = f"Moderate uniqueness: {uniqueness_pct:.2f}%"
                else:
                    test_case = "FAILURE"
                    details = f"Low uniqueness: {uniqueness_pct:.2f}%"
                
                result_data = {
                    "Environment": environment,
                    "Database": selected_db,
                    "Schema": selected_schema,
                    "Table": selected_table,
                    "Column": selected_column,
                    "Total Rows": total_rows,
                    "Non-Null Count": total_count,
                    "Null Count": null_count,
                    "Distinct Count": distinct_count,
                    "Uniqueness %": f"{uniqueness_pct:.2f}%",
                    "Test Case": test_case,
                    "Details": details
                }
                
                return pd.DataFrame([result_data])
            else:
                st.error("❌ No data returned from query")
                return pd.DataFrame([])
                
        except Exception as e:
            st.error(f"❌ Error during distinct count validation: {e}")
            error_data = {
                "Environment": environment,
                "Database": selected_db,
                "Schema": selected_schema,
                "Table": selected_table,
                "Column": selected_column,
                "Total Rows": 0,
                "Non-Null Count": 0,
                "Null Count": 0,
                "Distinct Count": 0,
                "Uniqueness %": "0.00%",
                "Test Case": "FAILURE",
                "Details": f"Error: {str(e)}"
            }
            return pd.DataFrame([error_data])

def style_dataframe(df):
    """Apply beautiful styling to dataframes with Brazilian colors and enhanced status styling"""
    def highlight_test_case(val):
        if val == "SUCCESS":
            return 'background: linear-gradient(135deg, #28a745 0%, #20c997 100%); color: green; font-weight: bold; border-radius: 20px; padding: 0.5rem 1rem; text-align: center; box-shadow: 0 2px 8px rgba(40, 167, 69, 0.3);'
        elif val == "FAILURE":
            return 'background: linear-gradient(135deg, #dc3545 0%, #fd7e14 100%); color: red; font-weight: bold; border-radius: 20px; padding: 0.5rem 1rem; text-align: center; box-shadow: 0 2px 8px rgba(220, 53, 69, 0.3);'
        return ''
    
    def highlight_rows(row):
        # Brazilian-inspired alternating colors
        colors = ['background-color: #fff8e1;', 'background-color: #e8f5e8;']  # Light warm yellow and light green
        return [colors[row.name % 2]] * len(row)
    
    if 'Test Case' in df.columns:
        styled = df.style.apply(highlight_rows, axis=1).applymap(highlight_test_case, subset=['Test Case'])
        return styled.set_table_styles([
            {'selector': 'th', 'props': [
                ('background', 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)'),
                ('color', 'white'),
                ('font-weight', 'bold'),
                ('text-align', 'center'),
                ('padding', '1rem'),
                ('border', 'none')
            ]},
            {'selector': 'td', 'props': [
                ('text-align', 'center'),
                ('padding', '0.8rem'),
                ('border-bottom', '1px solid #e9ecef')
            ]},
            {'selector': 'table', 'props': [
                ('border-collapse', 'collapse'),
                ('border-radius', '10px'),
                ('overflow', 'hidden'),
                ('box-shadow', '0 4px 15px rgba(0,0,0,0.1)')
            ]}
        ])
    else:
        return df.style.apply(highlight_rows, axis=1).set_table_styles([
            {'selector': 'th', 'props': [
                ('background', 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)'),
                ('color', 'white'),
                ('font-weight', 'bold'),
                ('text-align', 'center'),
                ('padding', '1rem'),
                ('border', 'none')
            ]},
            {'selector': 'td', 'props': [
                ('text-align', 'center'),
                ('padding', '0.8rem'),
                ('border-bottom', '1px solid #e9ecef')
            ]},
            {'selector': 'table', 'props': [
                ('border-collapse', 'collapse'),
                ('border-radius', '10px'),
                ('overflow', 'hidden'),
                ('box-shadow', '0 4px 15px rgba(0,0,0,0.1)')
            ]}
        ])

def display_summary_metrics(df):
    """Display summary metrics with beautiful cards"""
    if df.empty:
        return
    
    total_tests = len(df)
    success_count = len(df[df['Test Case'] == 'SUCCESS']) if 'Test Case' in df.columns else 0
    failure_count = total_tests - success_count
    success_rate = (success_count / total_tests * 100) if total_tests > 0 else 0
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <h3>📊 Total Tests</h3>
            <h2>{total_tests}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="success-card">
            <h3>✅ Passed</h3>
            <h2>{success_count}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="failure-card">
            <h3>❌ Failed</h3>
            <h2>{failure_count}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
        <div class="metric-card">
            <h3>🎯 Success Rate</h3>
            <h2>{success_rate:.1f}%</h2>
        </div>
        """, unsafe_allow_html=True)

# Initialize session state
if 'load_group' not in st.session_state:
    st.session_state['load_group'] = None

# Main application logic
if page == "🏠 Home":
    st.markdown('<h1 class="main-header">Welcome to ZDQ Hub</h1>', unsafe_allow_html=True)
    
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="nav-card">
            <h3>📊 Data Ingestion DQ</h3>
            <p>Validate data ingestion processes including count validation, data integrity checks, and duplicate detection.</p>
            <ul>
                <li>✅ Count Validation</li>
                <li>🔍 Data Validation</li>
                <li>🔄 Duplicate Detection</li>
                <li>📈 Distinct Count Validation</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="nav-card">
            <h3>🎭 Masking DQ</h3>
            <p>Ensure data masking compliance and validate masked data integrity across environments.</p>
            <ul>
                <li>🏷️ Tag Validation</li>
                <li>📋 Table Validation</li>
                <li>📊 Column Validation</li>
                <li>👁️ View Validation</li>
                <li>🔍 Classification Validation</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="nav-card">
            <h3>🔐 Encryption DQ</h3>
            <p>Validate encryption processes and ensure data security compliance.</p>
            <ul>
                <li>🔒 Table Validation</li>
                <li>🔑 Column Encryption validation</li
                <li>🔍 Tags Comparision with source and target</li
            </ul>
        </div>
        """, unsafe_allow_html=True)

elif page == "📊 Data Ingestion DQ":
    st.markdown('<h1 class="main-header">📊 Data Ingestion Quality</h1>', unsafe_allow_html=True)
    
    # Control panel    
    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        environment = st.selectbox("🌍 Environment", ["DEV", "QA", "UAT", "PROD"])
    with col2:
        rules = ["COUNT VALIDATION", "DATA VALIDATION", "DUPLICATE VALIDATION", "DISTINCT COUNT"]
        dq_rule = st.selectbox("📋 Validation Rule", rules)
    with col3:
        if dq_rule != "DISTINCT COUNT":
            source_db_types = fetch_source_db_types(environment)
            source_db_type = st.selectbox("🗄️ Source DB Type", source_db_types if source_db_types else ["No db_type found"])
        else:
            source_db_type = None

    # Database and schema selection (always shown except for COUNT VALIDATION)
    databases = fetch_databases(environment) if dq_rule != "COUNT VALIDATION" else []
    selected_db = ""
    selected_schema = ""
    selected_table = ""
    selected_column = ""

    # Row 2: Database, Schema, and conditionally Table/Column for DISTINCT COUNT
    if dq_rule == "DISTINCT COUNT":
        col4, col5, col6, col7 = st.columns([1, 1, 1, 1])
    else:
        col4, col5, col6 = st.columns([1.2, 1, 1])
        col7 = None

    with col4:
        if dq_rule != "COUNT VALIDATION":
            selected_db = st.selectbox("🏢 Database", databases if databases else ["No databases found"])
    
    with col5:
        if dq_rule != "COUNT VALIDATION" and selected_db:
            schemas = fetch_schemas(selected_db)
            selected_schema = st.selectbox("📁 Schema", schemas if schemas else ["No schemas found"])
    
    with col6:
        if dq_rule == "DISTINCT COUNT" and selected_db and selected_schema:
            tables = fetch_tables(selected_db, selected_schema)
            selected_table = st.selectbox("📋 Table", tables if tables else ["No tables found"])
        elif dq_rule != "COUNT VALIDATION":
            load_type_input = st.text_input("⚡ Load Type", "")
    
    if col7:  # Only for DISTINCT COUNT
        with col7:
            if selected_db and selected_schema and selected_table:
                columns = fetch_columns(selected_db, selected_schema, selected_table)
                selected_column = st.selectbox("📊 Column", columns if columns else ["No columns found"])

    # Load Type input for DISTINCT COUNT (separate row)
    if dq_rule == "DISTINCT COUNT":
        load_type_input = st.text_input("⚡ Load Type", "")

    # Load Group selection
    load_groups = fetch_load_groups(environment)
    if st.session_state['load_group'] is None and load_groups:
        st.session_state['load_group'] = load_groups[0]
    
    if load_groups and dq_rule != "DISTINCT COUNT":
        selected_load_group = st.selectbox("📦 Load Group", load_groups,
                                           index=load_groups.index(st.session_state['load_group']) if st.session_state['load_group'] in load_groups else 0)
        st.session_state['load_group'] = selected_load_group
    elif dq_rule != "DISTINCT COUNT":
        st.warning("⚠️ No load groups found for the selected environment.")
        selected_load_group = None
    else:
        selected_load_group = None  # Not needed for DISTINCT COUNT

    # Validation execution
    if st.button("🚀 Run Validation", type="primary"):
        if dq_rule == "DISTINCT COUNT":
            # Validation for DISTINCT COUNT
            if not all([selected_db, selected_schema, selected_table, selected_column]):
                st.error("❌ Please select Database, Schema, Table, and Column for distinct count validation")
            else:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                df = run_distinct_count_validation(selected_db, selected_schema, selected_table, selected_column, environment)
                
                if not df.empty:
                    st.markdown('<h3 class="sub-header">📈 Distinct Count Validation Results</h3>', unsafe_allow_html=True)
                    display_summary_metrics(df)
                    
                    st.markdown("### 📋 Detailed Results")
                    styled_df = style_dataframe(df)
                    st.dataframe(styled_df, use_container_width=True)
                    
                    csv_data = df.to_csv(index=False).encode('utf-8')
                    st.download_button("📥 Download Results", data=csv_data, 
                                     file_name=f"distinct_count_validation_{timestamp}.csv", mime="text/csv")
        else:
            # Existing validation logic
            if not load_type_input.strip():
                st.error("❌ Please enter a Load Type")
            elif not selected_load_group:
                st.error("❌ Please select a Load Group")
            else:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                if dq_rule == "COUNT VALIDATION":
                    df = run_count_validation(selected_load_group, load_type_input.strip(), source_db_type, environment)
                    if not df.empty:
                        st.markdown('<h3 class="sub-header">📈 Validation Results</h3>', unsafe_allow_html=True)
                        display_summary_metrics(df)
                        
                        st.markdown("### 📋 Detailed Results")
                        styled_df = style_dataframe(df)
                        st.dataframe(styled_df, use_container_width=True)
                        
                        csv_data = df.to_csv(index=False).encode('utf-8')
                        st.download_button("📥 Download Results", data=csv_data, 
                                         file_name=f"count_validation_{timestamp}.csv", mime="text/csv")
                    
                elif dq_rule == "DATA VALIDATION":
                    if not selected_schema:
                        st.error("❌ Please select a schema.")
                    else:
                        df = run_data_validation(selected_db, selected_schema, load_type_input.strip(), selected_load_group, environment)
                        if not df.empty:
                            st.markdown('<h3 class="sub-header">📈 Validation Results</h3>', unsafe_allow_html=True)
                            display_summary_metrics(df)
                            
                            st.markdown("### 📋 Detailed Results")
                            styled_df = style_dataframe(df)
                            st.dataframe(styled_df, use_container_width=True)
                            
                            csv_data = df.to_csv(index=False).encode('utf-8')
                            st.download_button("📥 Download Results", data=csv_data,
                                             file_name=f"data_validation_{timestamp}.csv", mime="text/csv")
                    
                elif dq_rule == "DUPLICATE VALIDATION":
                    if not selected_schema:
                        st.error("❌ Please select a schema.")
                    else:
                        df = run_duplicate_validation(selected_db, selected_schema, load_type_input.strip(), selected_load_group, environment)
                        if not df.empty:
                            st.markdown('<h3 class="sub-header">📈 Validation Results</h3>', unsafe_allow_html=True)
                            display_summary_metrics(df)
                            
                            st.markdown("### 📋 Detailed Results")
                            styled_df = style_dataframe(df)
                            st.dataframe(styled_df, use_container_width=True)
                            
                            csv_data = df.to_csv(index=False).encode('utf-8')
                            st.download_button("📥 Download Results", data=csv_data,
                                             file_name=f"duplicate_validation_{timestamp}.csv", mime="text/csv")

elif page == "🎭 Masking DQ":
    st.markdown('<h1 class="main-header">Data Masking Quality</h1>', unsafe_allow_html=True)
    
    # Masking DQ functions
    @st.cache_data(ttl=300)
    def get_databases(env_prefix):
        db_prefix = f"{env_prefix}_"
        db_query = f"""
            SELECT DATABASE_NAME 
            FROM INFORMATION_SCHEMA.DATABASES 
            WHERE DATABASE_NAME LIKE '{db_prefix}%'
        """
        try:
            rows = session.sql(db_query).collect()
            return [row[0] for row in rows]
        except:
            return []

    @st.cache_data(ttl=300)
    def get_schemas(database):
        schema_query = f"SELECT SCHEMA_NAME FROM {database}.INFORMATION_SCHEMA.SCHEMATA"
        try:
            rows = session.sql(schema_query).collect()
            return [row[0] for row in rows]
        except:
            return []

    @st.cache_data(ttl=300)
    def get_classification_owners(env):
        """Get classification owners for masking validation"""
        owner_query = f"""
            SELECT DISTINCT CLASSIFICATION_OWNER
            FROM DEV_DB_MANAGER.MASKING.CLASSIFICATION_DETAILS
        """
        try:
            rows = session.sql(owner_query).collect()
            return [row[0] for row in rows]
        except:
            return []

    def execute_validation_queries_tags(env, selected_database, selected_schema, classification_owner):
        """Run tag validation comparing source and target tag counts"""
        try:
            # Convert selected database to PROD for classification lookup
            production_database = selected_database.replace("DEV_", "PROD_").replace("QA_", "PROD_").replace("UAT_", "PROD_")
            
            # Get source classifications
            source_query = f"""
                SELECT COUNT(*) AS total_records
                FROM DEV_DB_MANAGER.MASKING.CLASSIFICATION_DETAILS
                WHERE "DATABASE" = '{production_database}'
                  AND "SCHEMA" = '{selected_schema}'
                  AND CLASSIFICATION_OWNER = '{classification_owner}'
            """
            
            # Get target tag references
            target_query = f"""
                SELECT COUNT(*) AS TAG_COUNT
                FROM DEV_DB_MANAGER.ACCOUNT_USAGE.TAG_REFERENCES
                WHERE OBJECT_DATABASE = '{selected_database}_MASKED'
                  AND OBJECT_SCHEMA = '{selected_schema}'
            """
            
            # Execute queries
            source_count = session.sql(source_query).collect()[0][0]
            target_count = session.sql(target_query).collect()[0][0]
            
            return source_count, target_count
        except Exception as e:
            return None, str(e)

    def execute_validation_queries_tables(env, selected_database, selected_schema):
        """Run MD Table validation"""
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
        """Run MD Column validation"""
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
        """Run View validation"""
        try:
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
        """Run Data Set validation"""
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

    # NEW CLASSIFICATION VALIDATION FUNCTION
    def run_classification_validation(env, selected_database, selected_schema, classification_owner):
        """Run classification validation comparing classification details with tag references"""
        with st.spinner("🔄 Running classification validation..."):
            try:
                # Convert selected database to PROD for classification lookup
                production_database = selected_database.replace("DEV_", "PROD_").replace("QA_", "PROD_").replace("UAT_", "PROD_")
                
                # Get source classifications
                source_query = f"""
                    SELECT "TABLE", "COLUMN", TAG
                    FROM DEV_DB_MANAGER.MASKING.CLASSIFICATION_DETAILS
                    WHERE "DATABASE" = '{production_database}'
                      AND "SCHEMA" = '{selected_schema}'
                      AND CLASSIFICATION_OWNER = '{classification_owner}'
                """
                
                # Get target tag references
                target_query = f"""
                    SELECT OBJECT_NAME AS "TABLE", COLUMN_NAME AS "COLUMN", TAG_NAME
                    FROM DEV_DB_MANAGER.ACCOUNT_USAGE.TAG_REFERENCES
                    WHERE OBJECT_DATABASE = '{selected_database}_MASKED'
                      AND OBJECT_SCHEMA = '{selected_schema}'
                """
                
                # Execute queries
                source_result = session.sql(source_query).collect()
                target_result = session.sql(target_query).collect()
                
                # Convert to sets for comparison
                source_set = set()
                for row in source_result:
                    source_set.add((row['TABLE'], row['COLUMN'], row['TAG']))
                
                target_set = set()
                for row in target_result:
                    target_set.add((row['TABLE'], row['COLUMN'], row['TAG_NAME']))
                
                # Create results list
                results = []
                
                # Check each source classification
                for table, column, tag in source_set:
                    target_match = (table, column, tag) in target_set
                    test_case = "SUCCESS" if target_match else "FAILURE"
                    
                    results.append({
                        "Environment": env,
                        "Database": selected_database,
                        "Schema": selected_schema,
                        "Classification Owner": classification_owner,
                        "Source Table": table,
                        "Source Column": column,
                        "Source Tag": tag,
                        "Target Table": table if target_match else "Not Found",
                        "Target Column": column if target_match else "Not Found",
                        "Target Tag": tag if target_match else "Not Found",
                        "Test Case": test_case
                    })
                
                # Check for extra target tags not in source
                for table, column, tag in target_set:
                    if (table, column, tag) not in source_set:
                        results.append({
                            "Environment": env,
                            "Database": selected_database,
                            "Schema": selected_schema,
                            "Classification Owner": classification_owner,
                            "Source Table": "Not in Source",
                            "Source Column": "Not in Source",
                            "Source Tag": "Not in Source",
                            "Target Table": table,
                            "Target Column": column,
                            "Target Tag": tag,
                            "Test Case": "FAILURE"
                        })
                
                return pd.DataFrame(results)
                
            except Exception as e:
                st.error(f"❌ Error during classification validation: {e}")
                return pd.DataFrame([])

    # UI Controls
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        env = st.selectbox("🌍 Environment", ["DEV", "QA", "UAT", "PROD"])
    with col2:
        database_list = get_databases(env)
        selected_database = st.selectbox("🏢 Database", database_list, key="db_select")
    with col3:
        schema_list = get_schemas(selected_database) if selected_database else []
        selected_schema = st.selectbox("📁 Schema", schema_list, key="schema_select")
    with col4:
        classification_owners = get_classification_owners(env)
        classification_owner = st.selectbox("👤 Classification Owner", classification_owners)

    # Button layout
    col_btn1, col_btn2 = st.columns(2)
    
    with col_btn1:
        if st.button("🚀 Run Masking Validations", type="primary"):
            if not all([env, selected_database, selected_schema, classification_owner]):
                st.error("❌ Please fill in all required fields")
            else:
                with st.spinner("🔄 Running comprehensive masking validations..."):
                    results_for_csv = []
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    
                    # Progress tracking
                    validation_steps = ["MD Tables", "MD Columns", "Data Set", "Views", "Tags"]
                    progress_bar = st.progress(0)
                    
                    for idx, validation_type in enumerate(validation_steps):
                        progress_bar.progress((idx + 1) / len(validation_steps))
                        
                        if validation_type == "MD Tables":
                            table_count, table_validation_count = execute_validation_queries_tables(env, selected_database, selected_schema)
                            test_case = "SUCCESS" if table_count == table_validation_count else "FAILURE"
                            results_for_csv.append({
                                "Environment": env,
                                "Database": selected_database,
                                "Schema": selected_schema,
                                "Validation": "MD Tables",
                                "Source Count": table_count,
                                "Target Count": table_validation_count,
                                "Test Case": test_case
                            })
                        
                        elif validation_type == "MD Columns":
                            column_count, column_validation_count = execute_validation_queries_columns(env, selected_database, selected_schema)
                            test_case = "SUCCESS" if column_count == column_validation_count else "FAILURE"
                            results_for_csv.append({
                                "Environment": env,
                                "Database": selected_database,
                                "Schema": selected_schema,
                                "Validation": "MD Columns",
                                "Source Count": column_count,
                                "Target Count": column_validation_count,
                                "Test Case": test_case
                            })
                        
                        elif validation_type == "Data Set":
                            dataset_count, dataset_data_count = execute_validation_queries_data_set(env, selected_database, selected_schema)
                            test_case = "SUCCESS" if dataset_count == dataset_data_count else "FAILURE"
                            results_for_csv.append({
                                "Environment": env,
                                "Database": selected_database,
                                "Schema": selected_schema,
                                "Validation": "Data Set",
                                "Source Count": dataset_count,
                                "Target Count": dataset_data_count,
                                "Test Case": test_case
                            })
                        
                        elif validation_type == "Views":
                            view_table_count, validation_count_views = execute_validation_queries_views(env, selected_database, selected_schema)
                            test_case = "SUCCESS" if view_table_count == validation_count_views else "FAILURE"
                            results_for_csv.append({
                                "Environment": env,
                                "Database": selected_database,
                                "Schema": selected_schema,
                                "Validation": "Views",
                                "Source Count": view_table_count,
                                "Target Count": validation_count_views,
                                "Test Case": test_case
                            })
                        
                        elif validation_type == "Tags":
                            tags_source_count, tags_target_count = execute_validation_queries_tags(env, selected_database, selected_schema, classification_owner)
                            test_case = "SUCCESS" if tags_source_count == tags_target_count else "FAILURE"
                            results_for_csv.append({
                                "Environment": env,
                                "Database": selected_database,
                                "Schema": selected_schema,
                                "Validation": "Tags",
                                "Source Count": tags_source_count,
                                "Target Count": tags_target_count,
                                "Test Case": test_case
                            })

                    progress_bar.empty()
                    
                    # Display results
                    results_df = pd.DataFrame(results_for_csv)
                    
                    st.markdown('<h3 class="sub-header">📈 Validation Results</h3>', unsafe_allow_html=True)
                    display_summary_metrics(results_df)
                    
                    st.markdown("### 📋 Detailed Results")
                    styled_df = style_dataframe(results_df)
                    st.dataframe(styled_df, use_container_width=True)
                    
                    # Download button
                    csv_bytes = results_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Download Validation Results",
                        data=csv_bytes,
                        file_name=f"masking_validation_results_{timestamp}.csv",
                        mime="text/csv"
                    )

    with col_btn2:
        if st.button("🔍 Run Tags Validation", type="secondary"):
            if not all([env, selected_database, selected_schema, classification_owner]):
                st.error("❌ Please fill in all required fields")
            else:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                df = run_classification_validation(env, selected_database, selected_schema, classification_owner)
                
                if not df.empty:
                    st.markdown('<h3 class="sub-header">🔍 Classification Validation Results</h3>', unsafe_allow_html=True)
                    display_summary_metrics(df)
                    
                    st.markdown("### 📋 Detailed Results")
                    styled_df = style_dataframe(df)
                    st.dataframe(styled_df, use_container_width=True)
                    
                    # Download button
                    csv_bytes = df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Download Classification Results",
                        data=csv_bytes,
                        file_name=f"classification_validation_{timestamp}.csv",
                        mime="text/csv"
                    )
                else:
                    st.info("ℹ️ No classification data found for validation.")

elif page == "🔐 Encryption DQ":
    st.markdown('<h1 class="main-header">🔐 Encryption Quality</h1>', unsafe_allow_html=True)
    
    # Encryption DQ functions
    @st.cache_data(ttl=300)
    def get_encryption_databases(env_prefix):
        """Get databases for encryption validation"""
        db_prefix = f"{env_prefix}_"
        db_query = f"""
            SELECT DATABASE_NAME 
            FROM INFORMATION_SCHEMA.DATABASES 
            WHERE DATABASE_NAME LIKE '{db_prefix}%'
            AND DATABASE_NAME NOT LIKE '%_ENCRYPT'
            AND DATABASE_NAME NOT LIKE '%_MASKED%'
        """
        try:
            rows = session.sql(db_query).collect()
            return [row[0] for row in rows]
        except:
            return []

    @st.cache_data(ttl=300)
    def get_encryption_schemas(database):
        """Get schemas for encryption validation"""
        schema_query = f"SELECT SCHEMA_NAME FROM {database}.INFORMATION_SCHEMA.SCHEMATA"
        try:
            rows = session.sql(schema_query).collect()
            return [row[0] for row in rows]
        except:
            return []

    @st.cache_data(ttl=300)
    def get_encryption_classification_owners(env):
        """Get classification owners for encryption validation"""
        owner_query = f"""
            SELECT DISTINCT CLASSIFICATION_OWNER
            FROM DEV_DB_MANAGER.MASKING.CLASSIFICATION_DETAILS
        """
        try:
            rows = session.sql(owner_query).collect()
            return [row[0] for row in rows]
        except:
            return []

    def get_tables_and_columns_from_classification(env, selected_database, selected_schema, classification_owner):
        """Get tables and columns from classification details based on environment mapping"""
        try:
            # Convert selected database to PROD for classification lookup
            prod_database = selected_database.replace("DEV_", "PROD_").replace("QA_", "PROD_").replace("UAT_", "PROD_")
            
            classification_query = f"""
                SELECT DISTINCT 
                    "TABLE" as table_name,
                    "COLUMN" as column_name
                FROM DEV_DB_MANAGER.MASKING.CLASSIFICATION_DETAILS
                WHERE "DATABASE" = '{prod_database}'
                AND "SCHEMA" = '{selected_schema}'
                AND CLASSIFICATION_OWNER = '{classification_owner}'
                ORDER BY "TABLE", "COLUMN"
            """
            
            rows = session.sql(classification_query).collect()
            return [(row['TABLE_NAME'], row['COLUMN_NAME']) for row in rows]
        except Exception as e:
            st.error(f"Error fetching classification data: {e}")
            return []

    def compare_column_data(original_db, encrypted_db, schema, table_name, column_name, sample_size=100):
        """Compare data between original and encrypted columns"""
        try:
            # Get sample data from original database
            original_query = f"""
                SELECT {column_name}
                FROM {original_db}.{schema}.{table_name}
                WHERE {column_name} IS NOT NULL
                LIMIT {sample_size}
            """
            
            # Get sample data from encrypted database
            encrypted_query = f"""
                SELECT {column_name}
                FROM {encrypted_db}.{schema}.{table_name}
                WHERE {column_name} IS NOT NULL
                LIMIT {sample_size}
            """
            
            original_result = session.sql(original_query).collect()
            encrypted_result = session.sql(encrypted_query).collect()
            
            if not original_result or not encrypted_result:
                return False, "No data found in one or both databases", 0, 0
            
            # Extract values
            original_values = [str(row[0]) for row in original_result if row[0] is not None]
            encrypted_values = [str(row[0]) for row in encrypted_result if row[0] is not None]
            
            if not original_values or not encrypted_values:
                return False, "No valid data to compare", 0, 0
            
            # Compare if data is different (encryption should make data different)
            different_count = 0
            min_length = min(len(original_values), len(encrypted_values))
            
            for i in range(min_length):
                if original_values[i] != encrypted_values[i]:
                    different_count += 1
            
            # Success if data is different (indicating encryption worked)
            is_encrypted = different_count > 0
            comparison_details = f"Compared {min_length} records, {different_count} different"
            
            return is_encrypted, comparison_details, len(original_values), len(encrypted_values)
            
        except Exception as e:
            return False, f"Error comparing data: {str(e)}", 0, 0

    def check_table_column_exists(database, schema, table_name, column_name):
        """Check if table and column exist in database"""
        try:
            check_query = f"""
                SELECT COUNT(*) as count
                FROM {database}.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema}'
                AND TABLE_NAME = '{table_name}'
                AND COLUMN_NAME = '{column_name}'
            """
            result = session.sql(check_query).collect()
            return result[0]['COUNT'] > 0 if result else False
        except:
            return False

    def run_encryption_data_validation(env, selected_database, selected_schema, classification_owner):
        """Run encryption validation by comparing actual data between original and encrypted databases"""
        with st.spinner("🔄 Running encryption data validation..."):
            # Get encrypted database name
            encrypt_database = f"{selected_database}_SETUP_ENCRYPT"
            
            # Get tables and columns from classification
            classification_data = get_tables_and_columns_from_classification(env, selected_database, selected_schema, classification_owner)
            
            if not classification_data:
                st.warning("⚠️ No classification data found for the selected criteria.")
                return pd.DataFrame([])

            results = []
            progress_bar = st.progress(0)
            
            for idx, (table_name, column_name) in enumerate(classification_data):
                progress_bar.progress((idx + 1) / len(classification_data))
                
                # Check if table and column exist in both databases
                original_exists = check_table_column_exists(selected_database, selected_schema, table_name, column_name)
                encrypted_exists = check_table_column_exists(encrypt_database, selected_schema, table_name, column_name)
                
                if not original_exists or not encrypted_exists:
                    test_case = "FAILURE"
                    details = f"Column missing - Original: {'Yes' if original_exists else 'No'}, Encrypted: {'Yes' if encrypted_exists else 'No'}"
                    original_count = encrypted_count = 0
                else:
                    # Compare actual data
                    is_encrypted, comparison_details, original_count, encrypted_count = compare_column_data(
                        selected_database, encrypt_database, selected_schema, table_name, column_name
                    )
                    
                    if is_encrypted:
                        test_case = "SUCCESS"
                        details = f"Data encrypted successfully - {comparison_details}"
                    else:
                        test_case = "FAILURE"
                        details = f"Data not encrypted or identical - {comparison_details}"
                
                results.append({
                    "Environment": env,
                    "Original Database": selected_database,
                    "Encrypted Database": encrypt_database,
                    "Schema": selected_schema,
                    "Table": table_name,
                    "Column": column_name,
                    "Classification Owner": classification_owner,
                    "Original Count": original_count,
                    "Encrypted Count": encrypted_count,
                    "Original Exists": "Yes" if original_exists else "No",
                    "Encrypted Exists": "Yes" if encrypted_exists else "No",
                    "Test Case": test_case,
                    "Details": details
                })

            progress_bar.empty()
            return pd.DataFrame(results)

    def run_non_encryption_validation(env, selected_database, selected_schema, classification_owner):
        """Run validation for columns that should NOT be encrypted"""
        with st.spinner("🔄 Running non-encryption validation..."):
            # Get encrypted database name
            encrypt_database = f"{selected_database}_SETUP_ENCRYPT"
            
            # Get all columns from the schema that are NOT in classification details
            try:
                # Get all columns in the schema
                all_columns_query = f"""
                    SELECT DISTINCT TABLE_NAME, COLUMN_NAME
                    FROM {selected_database}.INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = '{selected_schema}'
                    AND TABLE_NAME NOT LIKE 'RAW_%'
                    AND TABLE_NAME NOT LIKE 'VW_%'
                    AND COLUMN_NAME NOT LIKE 'ROW_%'
                    ORDER BY TABLE_NAME, COLUMN_NAME
                """
                
                all_columns_result = session.sql(all_columns_query).collect()
                all_columns = [(row['TABLE_NAME'], row['COLUMN_NAME']) for row in all_columns_result]
                
                # Get classified columns (these should be encrypted)
                prod_database = selected_database.replace("DEV_", "PROD_").replace("QA_", "PROD_").replace("UAT_", "PROD_")
                classified_columns_query = f"""
                    SELECT DISTINCT "TABLE" as table_name, "COLUMN" as column_name
                    FROM DEV_DB_MANAGER.MASKING.CLASSIFICATION_DETAILS
                    WHERE "DATABASE" = '{prod_database}'
                    AND "SCHEMA" = '{selected_schema}'
                    AND CLASSIFICATION_OWNER = '{classification_owner}'
                """
                
                classified_result = session.sql(classified_columns_query).collect()
                classified_columns = set((row['TABLE_NAME'], row['COLUMN_NAME']) for row in classified_result)
                
                # Get non-classified columns (these should NOT be encrypted)
                non_classified_columns = [col for col in all_columns if col not in classified_columns]
                
                if not non_classified_columns:
                    st.info("ℹ️ All columns in this schema are classified for encryption.")
                    return pd.DataFrame([])
                
                results = []
                progress_bar = st.progress(0)
                
                for idx, (table_name, column_name) in enumerate(non_classified_columns):
                    progress_bar.progress((idx + 1) / len(non_classified_columns))
                    
                    # Check if table and column exist in both databases
                    original_exists = check_table_column_exists(selected_database, selected_schema, table_name, column_name)
                    encrypted_exists = check_table_column_exists(encrypt_database, selected_schema, table_name, column_name)
                    
                    if not original_exists or not encrypted_exists:
                        test_case = "FAILURE"
                        details = f"Column missing - Original: {'Yes' if original_exists else 'No'}, Encrypted: {'Yes' if encrypted_exists else 'No'}"
                        original_count = encrypted_count = 0
                    else:
                        # Compare actual data - for non-encrypted columns, data should be identical
                        is_different, comparison_details, original_count, encrypted_count = compare_column_data(
                            selected_database, encrypt_database, selected_schema, table_name, column_name
                        )
                        
                        if not is_different:
                            test_case = "SUCCESS"
                            details = f"Data identical (not encrypted) - {comparison_details}"
                        else:
                            test_case = "FAILURE"
                            details = f"Data unexpectedly different - {comparison_details}"
                    
                    results.append({
                        "Environment": env,
                        "Original Database": selected_database,
                        "Encrypted Database": encrypt_database,
                        "Schema": selected_schema,
                        "Table": table_name,
                        "Column": column_name,
                        "Classification": "Not Required",
                        "Original Count": original_count,
                        "Encrypted Count": encrypted_count,
                        "Original Exists": "Yes" if original_exists else "No",
                        "Encrypted Exists": "Yes" if encrypted_exists else "No",
                        "Test Case": test_case,
                        "Details": details
                    })

                progress_bar.empty()
                return pd.DataFrame(results)
                
            except Exception as e:
                st.error(f"Error during non-encryption validation: {e}")
                return pd.DataFrame([])

    # UI Controls
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        encrypt_env = st.selectbox("🌍 Environment", ["DEV", "QA", "UAT", "PROD"], key="encrypt_env")
    with col2:
        encrypt_database_list = get_encryption_databases(encrypt_env)
        encrypt_selected_database = st.selectbox("🏢 Database", encrypt_database_list, key="encrypt_db_select")
    with col3:
        encrypt_schema_list = get_encryption_schemas(encrypt_selected_database) if encrypt_selected_database else []
        encrypt_selected_schema = st.selectbox("📁 Schema", encrypt_schema_list, key="encrypt_schema_select")
    with col4:
        encrypt_classification_owners = get_encryption_classification_owners(encrypt_env)
        encrypt_classification_owner = st.selectbox("👤 Classification Owner", encrypt_classification_owners, key="encrypt_owner_select")

    # Run validation automatically when all fields are selected
    if all([encrypt_env, encrypt_selected_database, encrypt_selected_schema, encrypt_classification_owner]):
        
        # Two validation buttons
        col_btn1, col_btn2 = st.columns(2)
        
        with col_btn1:
            if st.button("🔐 Validate Encrypted Columns", type="primary", key="encrypt_validate"):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                df = run_encryption_data_validation(encrypt_env, encrypt_selected_database, encrypt_selected_schema, encrypt_classification_owner)
                
                if not df.empty:
                    st.markdown('<h3 class="sub-header">🔐 Encrypted Columns Validation Results</h3>', unsafe_allow_html=True)
                    display_summary_metrics(df)
                    
                    st.markdown("### 📋 Detailed Results")
                    styled_df = style_dataframe(df)
                    st.dataframe(styled_df, use_container_width=True)
                    
                    # Download button
                    csv_bytes = df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Download Encryption Results",
                        data=csv_bytes,
                        file_name=f"encryption_validation_{timestamp}.csv",
                        mime="text/csv"
                    )
                else:
                    st.info("ℹ️ No encrypted columns found for validation.")