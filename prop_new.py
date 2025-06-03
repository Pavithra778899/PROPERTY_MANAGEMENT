# Cortex AI – Property Management Assistant (Full App)
import streamlit as st
import json
import re
import requests
import snowflake.connector
import pandas as pd
from snowflake.snowpark import Session
from snowflake.core import Root
from typing import Any, Dict, List, Optional, Tuple
import plotly.express as px
import time
import uuid
import retrying

# --- Configuration ---
HOST = "QNWFESR-LKB66742.snowflakecomputing.com"
DATABASE = "AI"
SCHEMA = "DWH_MART"
API_ENDPOINT = "/api/v2/cortex/agent:run"
API_TIMEOUT = 50000
CORTEX_SEARCH_SERVICES = None
SEMANTIC_MODEL = '@"AI"."DWH_MART"."PROPERTY_MANAGEMENT"/property_management (1).yaml'
MODELS = ["mistral-large", "snowflake-arctic", "llama3-70b", "llama3-8b"]

# --- Page Setup ---
st.set_page_config(page_title="Cortex AI-Property Management Assistant", layout="wide")

# --- Initialize Session State ---
def init_session_state():
    defaults = {
        "authenticated": False,
        "username": "",
        "password": "",
        "CONN": None,
        "snowpark_session": None,
        "chat_history": [],
        "messages": [],
        "debug_mode": False,
        "last_suggestions": [],
        "chart_x_axis": None,
        "chart_y_axis": None,
        "chart_type": "Bar Chart",
        "current_query": None,
        "current_results": None,
        "current_sql": None,
        "current_summary": None,
        "service_metadata": [{"name": "", "search_column": ""}],
        "selected_cortex_search_service": "",
        "model_name": "mistral-large",
        "num_retrieved_chunks": 100,
        "num_chat_messages": 10,
        "use_chat_history": True,
        "clear_conversation": False,
        "show_selector": False,
        "show_greeting": True,
        "show_about": False,
        "show_help": False,
        "show_history": False,
        "query": None,
        "previous_query": None,
        "previous_sql": None,
        "previous_results": None,
        "show_sample_questions": False
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

init_session_state()
# --- Query Type Detection ---
def is_structured_query(query: str):
    patterns = [
        r'\b(count|number|where|group by|order by|sum|avg|max|min|total|how many|which|show|list)\b',
        r'\b(property|tenant|lease|rent|occupancy|maintenance|billing|payment)\b'
    ]
    return any(re.search(pattern, query.lower()) for pattern in patterns)

def is_complete_query(query: str):
    return bool(re.search(r'\b(generate|write|create|describe|explain)\b', query.lower()))

def is_summarize_query(query: str):
    return bool(re.search(r'\b(summarize|summary|condense)\b', query.lower()))

def is_greeting_query(query: str):
    return bool(re.search(r'\b(hi|hello|hey|thank|how are you|start over|what can you do|who are you|what is this app)\b', query.lower()))

# --- Snowflake API Utility ---
def complete(model, prompt):
    prompt = prompt.replace("'", "\\'")
    try:
        result = st.session_state.snowpark_session.sql(
            f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{prompt}') AS response"
        ).collect()
        return result[0]["RESPONSE"]
    except Exception as e:
        st.error(f"❌ COMPLETE Error: {e}")
        return ""

def summarize(text):
    text = text.replace("'", "\\'")
    try:
        result = st.session_state.snowpark_session.sql(
            f"SELECT SNOWFLAKE.CORTEX.SUMMARIZE('{text}') AS summary"
        ).collect()
        return result[0]["SUMMARY"]
    except Exception as e:
        st.error(f"❌ SUMMARIZE Error: {e}")
        return ""

# --- Cortex API Call (Structured or Unstructured) ---
@retrying.retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def snowflake_api_call(query: str, is_structured: bool = False):
    payload = {
        "model": st.session_state.model_name,
        "messages": [{"role": "user", "content": [{"type": "text", "text": query}]}],
        "tools": [],
        "tool_resources": {}
    }

    if is_structured:
        payload["tools"].append({"tool_spec": {"type": "cortex_analyst_text_to_sql", "name": "analyst1"}})
        payload["tool_resources"]["analyst1"] = {"semantic_model_file": SEMANTIC_MODEL}
    else:
        payload["tools"].append({"tool_spec": {"type": "cortex_search", "name": "search1"}})
        payload["tool_resources"]["search1"] = {
            "name": st.session_state.selected_cortex_search_service,
            "max_results": st.session_state.num_retrieved_chunks
        }

    try:
        resp = requests.post(
            url=f"https://{HOST}{API_ENDPOINT}",
            json=payload,
            headers={
                "Authorization": f'Snowflake Token="{st.session_state.CONN.rest.token}"',
                "Content-Type": "application/json"
            },
            timeout=API_TIMEOUT // 1000
        )
        if resp.status_code >= 400:
            raise Exception(f"Status {resp.status_code}: {resp.text}")
        return parse_sse_response(resp.text)
    except Exception as e:
        st.error(f"❌ Cortex API Error: {e}")
        raise

# --- Parse SSE Response ---
def parse_sse_response(response_text: str) -> List[Dict]:
    events = []
    lines = response_text.strip().split("\n")
    current_event = {}
    for line in lines:
        if line.startswith("event:"):
            current_event["event"] = line.split(":", 1)[1].strip()
        elif line.startswith("data:"):
            data_str = line.split(":", 1)[1].strip()
            if data_str != "[DONE]":
                try:
                    data_json = json.loads(data_str)
                    current_event["data"] = data_json
                    events.append(current_event)
                    current_event = {}
                except json.JSONDecodeError:
                    continue
    return events

# --- Charting ---
def display_chart_tab(df: pd.DataFrame, prefix: str = "chart", query: str = ""):
    if df.empty or len(df.columns) < 2:
        st.warning("Not enough data to display chart.")
        return

    all_cols = list(df.columns)
    col1, col2, col3 = st.columns(3)
    x_col = col1.selectbox("X axis", all_cols, index=0, key=f"{prefix}_x")
    y_col = col2.selectbox("Y axis", [c for c in all_cols if c != x_col], index=0, key=f"{prefix}_y")
    chart_type = col3.selectbox("Chart Type", ["Bar Chart", "Line Chart", "Pie Chart"], key=f"{prefix}_type")

    if chart_type == "Bar Chart":
        fig = px.bar(df, x=x_col, y=y_col)
    elif chart_type == "Line Chart":
        fig = px.line(df, x=x_col, y=y_col)
    elif chart_type == "Pie Chart":
        fig = px.pie(df, names=x_col, values=y_col)
    else:
        st.warning("Unsupported chart type.")
        return

    st.plotly_chart(fig, use_container_width=True)
