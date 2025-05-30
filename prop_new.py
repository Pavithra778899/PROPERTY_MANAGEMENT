import streamlit as st
import json
import re
import requests
import snowflake.connector
import pandas as pd
from snowflake.snowpark import Session
from snowflake.core import Root
from typing import Any, Dict, List, Optional
import plotly.express as px
import time
from textblob import TextBlob
import random
import os
from retrying import retry

# Configuration (use Streamlit secrets or environment variables)
HOST = st.secrets.get("SNOWFLAKE_HOST", os.getenv("SNOWFLAKE_HOST", "GBJYVCT-LSB50763.snowflakecomputing.com"))
DATABASE = "AI"
SCHEMA = "DWH_MART"
API_ENDPOINT = "/api/v2/cortex/agent:run"
API_TIMEOUT = 50000  # in milliseconds
CORTEX_SEARCH_SERVICES = "AI.DWH_MART.propertymanagement"
SEMANTIC_MODEL = '@"AI"."DWH_MART"."PROPERTY_MANAGEMENT"/property_management.yaml'
MODELS = ["mistral-large", "snowflake-arctic", "llama3-70b", "llama3-8b"]

# Streamlit Page Config
st.set_page_config(page_title="Property Management AI", layout="wide", initial_sidebar_state="auto")

# Initialize session state
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
    st.session_state.username = ""
    st.session_state.password = ""
    st.session_state.CONN = None
    st.session_state.snowpark_session = None
    st.session_state.chat_history = []
    st.session_state.messages = []
    st.session_state.debug_mode = False
    st.session_state.last_suggestions = []
    st.session_state.chart_x_axis = None
    st.session_state.chart_y_axis = None
    st.session_state.chart_type = "Bar Chart"
    st.session_state.current_query = None
    st.session_state.current_results = None
    st.session_state.current_sql = None
    st.session_state.current_summary = None
    st.session_state.service_metadata = [{"name": CORTEX_SEARCH_SERVICES, "search_column": ""}]
    st.session_state.selected_cortex_search_service = CORTEX_SEARCH_SERVICES
    st.session_state.model_name = "mistral-large"
    st.session_state.num_retrieved_chunks = 100
    st.session_state.num_chat_messages = 10
    st.session_state.use_chat_history = True
    st.session_state.clear_conversation = False
    st.session_state.show_greeting = True
    st.session_state.data_source = "Database"
    st.session_state.tenant_id = None

# CSS Styling
st.markdown("""
<style>
.stChatMessage.user {
    background-color: #29B5E8;
    color: white;
    border-radius: 10px;
    padding: 10px;
    margin: 5px;
    max-width: 70%;
    margin-left: auto;
}
.stChatMessage.assistant {
    background-color: #1e1e1e;
    color: white;
    border-radius: 10px;
    padding: 10px;
    margin: 5px;
    max-width: 70%;
    margin-right: auto;
}
.stSidebar button {
    background-color: #29B5E8 !important;
    color: white !important;
    border-radius: 5px !important;
    width: 100% !important;
    margin: 5px 0 !important;
}
</style>
""", unsafe_allow_html=True)

# Helper Functions
def stream_text(text: str, chunk_size: int = 2, delay: float = 0.02):
    for i in range(0, len(text), chunk_size):
        yield text[i:i + chunk_size]
        time.sleep(delay)

def display_chart_tab(df: pd.DataFrame, prefix: str = "chart", query: str = ""):
    try:
        if df is None or df.empty or len(df.columns) < 2:
            st.warning("No valid data for visualization.")
            if st.session_state.debug_mode:
                st.sidebar.warning(f"Chart Data Issue: df={df}, columns={df.columns if df is not None else 'None'}")
            return

        query_lower = query.lower()
        default_data = "Bar Chart"
        if re.search(r'\b(county|jurisdiction|status)\b', query_lower):
            default_data = "Pie Chart"
        elif re.search(r'\b(month|year|date|time)\b', query_lower):
            default_data = "Line Chart"
        elif re.search(r'\b(rent|payment|amount|total)\b', query_lower):
            default_data = "Bar Chart"

        all_cols = list(df.columns)
        x_default = next((col for col in all_cols if df[col].dtype in ['object', 'category']), all_cols[0])
        y_default = next((col for col in all_cols if df[col].dtype in ['int64', 'float64'] and col != x_default), all_cols[1] if len(all_cols) > 1 else all_cols[0])

        st.markdown("**Customize Your Chart**")
        col1, col2, col3 = st.columns([1, 1, 1])
        x_col = col1.selectbox("X Axis", all_cols, index=all_cols.index(x_default), key=f"{prefix}_x")
        remaining_cols = [c for c in all_cols if c != x_col]
        y_col = col2.selectbox("Y Axis", remaining_cols, index=remaining_cols.index(y_default) if y_default in remaining_cols else 0, key=f"{prefix}_y")
        chart_options = ["Line Chart", "Bar Chart", "Pie Chart", "Scatter Chart", "Histogram Chart"]
        chart_type = col3.selectbox("Chart Type", chart_options, index=chart_options.index(default_data), key=f"{prefix}_type")

        chart_title = f"{chart_type}: {query[:50]}{'...' if len(query) > 50 else ''}" if query else f"{chart_type}"
        plot_config = {
            'template': 'plotly_dark',
            'layout': {
                'title': {'text': chart_title, 'x': 0.5, 'xanchor': 'center'},
                'font': {'color': 'white'},
                'plot_bgcolor': '#1e1e1e',
                'paper_bgcolor': '#1e1e1e',
                'xaxis': {'title': x_col, 'gridcolor': '#444'},
                'yaxis': {'title': y_col, 'gridcolor': '#444'},
                'hovermode': 'closest',
                'colorway': ['#29B5E8', '#FF6F61', '#6B7280', '#10B981', '#F59E0B']
            }
        }

        if chart_type == "Line Chart":
            fig = px.line(df, x=x_col, y=y_col, title=chart_title)
        elif chart_type == "Bar Chart":
            fig = px.bar(df, x=x_col, y=y_col, title=chart_title)
        elif chart_type == "Pie Chart":
            fig = px.pie(df, names=x_col, values=y_col, title=chart_title)
        elif chart_type == "Scatter Chart":
            fig = px.scatter(df, x=x_col, y=y_col, title=chart_title)
        elif chart_type == "Histogram Chart":
            fig = px.histogram(df, x=x_col, title=chart_title)
        
        fig.update_layout(**plot_config['layout'])
        st.plotly_chart(fig, key=f"{prefix}_{chart_type.lower().replace(' ', '_')}", use_container_width=True)
        
        desc = f"This {chart_type.lower()} visualizes {y_col} by {x_col}."
        if chart_type == "Pie Chart":
            desc += f" It shows the distribution of {x_col} based on {y_col}."
        elif chart_type in ["Line Chart", "Bar Chart"]:
            desc += f" It highlights trends or comparisons across {x_col}."
        st.caption(desc)

        with st.expander("Preview Data", expanded=False):
            st.dataframe(df.head())

        fig.write_png(f"{prefix}_chart.png")
        with open(f"{prefix}_chart.png", "rb") as file:
            st.download_button(
                label="Download Chart",
                data=file,
                file_name=f"{chart_title.replace(':', '_')}.png",
                mime="image/png"
            )

    except Exception as e:
        st.error(f"❌ Failed to generate chart: {str(e)}")
        if st.session_state.debug_mode:
            with st.sidebar.expander("Chart Error"):
                st.error(f"Details: {str(e)}")

def init_config_options():
    with st.sidebar:
        st.image("https://www.snowflake.com/wp-content/themes/snowflake/assets/img/logo-blue.svg", width=250)
        st.radio("Data Source:", ["Database", "Document"], key="data_source")
        st.button("Clear Conversation", on_click=lambda: start_new_conversation())
        with st.expander("Advanced Settings"):
            st.selectbox("Model:", MODELS, key="model_name")
            st.number_input("Context Chunks", value=100, min_value=1, max_value=200, key="num_retrieved_chunks")
            st.number_input("Chat History Messages", value=10, min_value=1, max_value=50, key="num_chat_messages")
            st.toggle("Use Chat History", key="use_chat_history")
            st.toggle("Debug Mode", key="debug_mode")
        st.markdown("### About")
        st.write("This app uses Snowflake Cortex Analyst to interpret natural language questions and generate property management insights.")
        st.markdown("### Help & Documentation")
        st.write(
            "- [User Guide](https://docs.snowflake.com/en/guides-overview-ai-features)\n"
            "- [Cortex Analyst Docs](https://docs.snowflake.com/)\n"
            "- [Contact Support](https://www.snowflake.com/en/support/)"
        )

@st.cache_data
def init_service_metadata():
    try:
        session = st.session_state.snowpark_session
        services = session.sql(f"SHOW CORTEX SEARCH SERVICES LIKE '{CORTEX_SEARCH_SERVICES}';").collect()
        service_metadata = []
        if services:
            svc_name = services[0]["name"]
            svc_search_col = session.sql(f"DESC CORTEX SEARCH SERVICE {svc_name};").collect()[0]["search_column"]
            service_metadata.append({"name": svc_name, "search_column": svc_search_col})
        else:
            service_metadata = [{"name": CORTEX_SEARCH_SERVICES, "search_column": ""}]
        return service_metadata
    except Exception as e:
        st.error(f"❌ Failed to initialize Cortex Search service metadata: {str(e)}")
        return [{"name": CORTEX_SEARCH_SERVICES, "search_column": ""}]

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def snowflake_api_call(query: str, is_structured: bool = False):
    payload = {
        "model": st.session_state.model_name,
        "messages": [{"role": "user", "content": [{"type": "text", "text": query}]}],
        "tools": []
    }
    if is_structured:
        payload["tools"].append({"tool_spec": {"type": "cortex_analyst_text_to_sql", "name": "analyst1"}})
        payload["tool_resources"] = {"analyst1": {"semantic_model_file": SEMANTIC_MODEL}}
    else:
        payload["tools"].append({"tool_spec": {"type": "cortex_search", "name": "search1"}})
        payload["tool_resources"] = {"search1": {"name": st.session_state.selected_cortex_search_service, "max_results": min(st.session_state.num_retrieved_chunks, 200)}}
    
    try:
        resp = requests.post(
            url=f"https://{HOST}{API_ENDPOINT}",
            json=payload,
            headers={
                "Authorization": f'Snowflake Token="{st.session_state.CONN.rest.token}"',
                "Content-Type": "application/json",
            },
            timeout=API_TIMEOUT // 1000
        )
        if resp.status_code < 400:
            if not resp.text.strip():
                st.error("❌ API returned an empty response.")
                return None
            return parse_sse_response(resp.text)
        else:
            raise Exception(f"Failed request with status {resp.status_code}: {resp.text}")
    except Exception as e:
        st.error(f"❌ API Request Error: {str(e)}")
        return None

# Main Application Logic
if not st.session_state.authenticated:
    st.title("Property Management AI")
    st.markdown("Please log in to manage your properties.")
    st.session_state.username = st.text_input("Snowflake Username:", value=st.session_state.username)
    st.session_state.password = st.text_input("Password:", type="password")
    if st.button("Login"):
        try:
            conn = snowflake.connector.connect(
                user=st.session_state.username,
                password=st.session_state.password,
                account="GBJYVCT-LSB50763",
                host=HOST,
                port=443,
                warehouse="COMPUTE_WH",
                role="ACCOUNTADMIN",
                database=DATABASE,
                schema=SCHEMA,
            )
            st.session_state.CONN = conn
            snowpark_session = Session.builder.configs({"connection": conn}).create()
            st.session_state.snowpark_session = snowpark_session
            with conn.cursor() as cur:
                cur.execute(f"USE DATABASE {DATABASE}")
                cur.execute(f"USE SCHEMA {SCHEMA}")
                cur.execute("ALTER SESSION SET TIMEZONE = 'UTC'")
                cur.execute("ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE")
            st.session_state.authenticated = True
            st.success("Logged in successfully! 🏠")
            st.rerun()
        except Exception as e:
            st.error(f"Login failed: {str(e)}")
else:
    session = st.session_state.snowpark_session
    st.session_state.service_metadata = init_service_metadata()
    init_config_options()
    st.title("Property Management AI")
    st.markdown(f"Semantic Model: `{SEMANTIC_MODEL.split('/')[-1]}`")

    if not st.session_state.tenant_id:
        with st.form("tenant_form"):
            tenant_id = st.text_input("Enter Your Tenant ID:")
            if st.form_submit_button("Submit"):
                st.session_state.tenant_id = tenant_id
                st.success(f"Welcome, Tenant {tenant_id}! 🏠")
                st.rerun()

    if st.session_state.show_greeting and not st.session_state.chat_history:
        st.markdown(f"""
        **{random.choice(GREETINGS)}**  
        Ask about occupancy rates, lease details, rent payments, or submit a maintenance request!
        """)

    # Chat History and Query Handling
    for message in st.session_state.chat_history:
        with st.chat_message(message["role"]):
            st.markdown(message["content"], unsafe_allow_html=True)
            if message["role"] == "assistant" and "results" in message and message["results"] is not None:
                if message.get("sql"):
                    with st.expander("View SQL Query", expanded=False):
                        st.code(message["sql"], language="sql")
                st.markdown(f"**Query Results ({len(message['results'])} rows):**")
                st.dataframe(message["results"])
                if not message["results"].empty and len(message["results"].columns) >= 2:
                    st.markdown("**📈 Visualization:**")
                    display_chart_tab(message["results"], prefix=f"chart_{hash(message['content'])}", query=message.get("query", ""))

    query = st.chat_input("Ask your question...")
    if query:
        st.session_state.show_greeting = False
        st.session_state.chat_history.append({"role": "user", "content": query})
        with st.chat_message("user"):
            st.markdown(query)
        with st.chat_message("assistant"):
            with st.spinner("Generating Response..."):
                response_placeholder = st.empty()
                is_structured = is_structured_query(query) and st.session_state.data_source == "Database"
                response_content = f"**{get_tone(query)}**\nProcessing your request..."
                response_placeholder.write_stream(stream_text(response_content))
                # Further query processing logic would go here (similar to original)
