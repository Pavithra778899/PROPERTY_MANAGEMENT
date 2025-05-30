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
import uuid

# Configuration
HOST = st.secrets.get("SNOWFLAKE_HOST", os.getenv("SNOWFLAKE_HOST", "GBJYVCT-LSB50763.snowflakecomputing.com"))
DATABASE = "AI"
SCHEMA = "DWH_MART"
API_ENDPOINT = "/api/v2/cortex/agent:run"
API_TIMEOUT = 50000
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
    st.session_state.debug_mode = True  # Enable debug mode
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
    st.session_state.sample_query = None  # Track sample question clicks

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

def start_new_conversation():
    st.session_state.chat_history = []
    st.session_state.messages = []
    st.session_state.current_query = None
    st.session_state.current_results = None
    st.session_state.current_sql = None
    st.session_state.current_summary = None
    st.session_state.chart_x_axis = None
    st.session_state.chart_y_axis = None
    st.session_state.chart_type = "Bar Chart"
    st.session_state.last_suggestions = []
    st.session_state.clear_conversation = False
    st.session_state.show_greeting = True
    st.session_state.tenant_id = None
    st.session_state.sample_query = None
    st.rerun()

def get_tone(user_input):
    sentiment = TextBlob(user_input).sentiment.polarity
    if "urgent" in user_input.lower() or sentiment < -0.3:
        return "Whoa, sounds urgent! 🚨 Let’s tackle this:"
    elif "thanks" in user_input.lower() or sentiment > 0.3:
        return "Sweet, loving the good vibes! 😎 Here’s the deal:"
    return "Yo, let’s dive into this! 🏠"

GREETINGS = [
    "Yo, welcome to your Property Management AI! 😎 Ready to manage some properties?",
    "Hey there! Let’s make property management smoother than a sunny day! ☀️",
    "What’s good? Your property management sidekick is here! 🚀"
]

def init_config_options():
    with st.sidebar:
        st.image("https://www.snowflake.com/wp-content/themes/snowflake/assets/img/logo-blue.svg", width=250)
        st.radio("Data Source:", ["Database", "Document"], key="data_source")
        st.button("Clear Conversation", on_click=start_new_conversation)
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
        if session is None:
            st.error("Snowpark session not initialized.")
            return [{"name": CORTEX_SEARCH_SERVICES, "search_column": ""}]
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
            'template': 'plotly_white',
            'layout': {
                'title': {'text': chart_title, 'x': 0.5, 'xanchor': 'center'},
                'font': {'color': '#000000'},
                'plot_bgcolor': '#FFFFFF',
                'paper_bgcolor': '#FFFFFF',
                'xaxis': {'title': x_col, 'gridcolor': '#CCCCCC'},
                'yaxis': {'title': y_col, 'gridcolor': '#CCCCCC'},
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

def query_cortex_search_service(query):
    try:
        session = st.session_state.snowpark_session
        if session is None:
            st.error("Snowpark session not initialized.")
            return ""
        db, schema = session.get_current_database(), session.get_current_schema()
        root = Root(session)
        cortex_search_service = (
            root.databases[db]
            .schemas[schema]
            .cortex_search_services[CORTEX_SEARCH_SERVICES]
        )
        context_documents = cortex_search_service.search(
            query, columns=[], limit=min(st.session_state.num_retrieved_chunks, 200)
        )
        results = context_documents.results
        search_col = st.session_state.service_metadata[0]["search_column"]
        context_str = ""
        for i, r in enumerate(results):
            context_str += f"Context document {i+1}: {r[search_col]} \n" + "\n"
        if st.session_state.debug_mode:
            st.sidebar.text_area("Context Documents", context_str, height=300)
        return context_str
    except Exception as e:
        st.error(f"❌ Error querying Cortex Search service: {str(e)}")
        if st.session_state.debug_mode:
            st.sidebar.error(f"Cortex Search Error: {str(e)}")
        return ""

def get_chat_history():
    start_index = max(0, len(st.session_state.chat_history) - st.session_state.num_chat_messages)
    return st.session_state.chat_history[start_index : len(st.session_state.chat_history) - 1]

def make_chat_history_summary(chat_history, question):
    chat_history_str = "\n".join([f"{msg['role']}: {msg['content']}" for msg in chat_history])
    prompt = f"""
        [INST]
        Based on the chat history below and the question, generate a query that extends the question
        with the chat history provided. The query should be in natural language.
        Answer with only the query. Do not add explanation.

        <chat_history>
        {chat_history_str}
        </chat_history>
        <question>
        {question}
        </question>
        [/INST]
    """
    summary = complete(st.session_state.model_name, prompt)
    if st.session_state.debug_mode:
        st.sidebar.text_area("Chat History Summary", summary.replace("$", "\$"), height=150)
    return summary

def create_prompt(user_question):
    try:
        chat_history_str = ""
        if st.session_state.use_chat_history:
            chat_history = get_chat_history()
            if chat_history:
                question_summary = make_chat_history_summary(chat_history, user_question)
                prompt_context = query_cortex_search_service(question_summary)
                chat_history_str = "\n".join([f"{msg['role']}: {msg['content']}" for msg in chat_history])
            else:
                prompt_context = query_cortex_search_service(user_question)
        else:
            prompt_context = query_cortex_search_service(user_question)
            chat_history = []
        
        if not prompt_context.strip():
            if st.session_state.debug_mode:
                st.sidebar.warning("No context retrieved, using direct completion.")
            return complete(st.session_state.model_name, user_question)
        
        prompt = f"""
            [INST]
            You are a helpful AI chat assistant for property management with RAG capabilities.
            Use the context between <context> and </context> tags and the user's chat history between
            <chat_history> and </chat_history> tags to provide a concise, professional, yet friendly
            answer relevant to property management (e.g., leases, tenants, rent, maintenance).
            Add a touch of personality.

            <chat_history>
            {chat_history_str}
            </chat_history>
            <context>
            {prompt_context}
            </context>
            <question>
            {user_question}
            </question>
            [/INST]
            Answer:
        """
        return complete(st.session_state.model_name, prompt)
    except Exception as e:
        if st.session_state.debug_mode:
            st.sidebar.error(f"Create Prompt Error: {str(e)}")
        return None

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def complete(model, prompt):
    try:
        session = st.session_state.snowpark_session
        if session is None:
            st.error("Snowpark session not initialized.")
            return None
        prompt = prompt.replace("'", "\\'").replace('"', '\\"')
        query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{prompt}') AS response"
        result = session.sql(query).collect()
        return result[0]["RESPONSE"]
    except Exception as e:
        st.error(f"❌ COMPLETE Function Error: {str(e)}")
        if st.session_state.debug_mode:
            st.sidebar.error(f"Complete Error: {str(e)}")
        return None

def summarize(text):
    try:
        session = st.session_state.snowpark_session
        if session is None:
            st.error("Snowpark session not initialized.")
            return None
        text = text.replace("'", "\\'").replace('"', '\\"')
        query = f"SELECT SNOWFLAKE.CORTEX.SUMMARIZE('{text}') AS summary"
        result = session.sql(query).collect()
        return result[0]["SUMMARY"]
    except Exception as e:
        st.error(f"❌ SUMMARIZE Function Error: {str(e)}")
        if st.session_state.debug_mode:
            st.sidebar.error(f"Summarize Error: {str(e)}")
        return None

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
                except json.JSONDecodeError as e:
                    st.error(f"❌ Failed to parse SSE data: {str(e)} - Data: {data_str}")
                    if st.session_state.debug_mode:
                        st.sidebar.error(f"SSE Parse Error: {str(e)}")
    return events

def process_sse_response(response, is_structured):
    sql = ""
    search_results = []
    if not response:
        return sql, search_results
    try:
        for event in response:
            if event.get("event") == "message.delta" and "data" in event:
                delta = event["data"].get("delta", {})
                content = delta.get("content", [])
                for item in content:
                    if item.get("type") == "tool_results":
                        tool_results = item.get("tool_results", {})
                        if "content" in tool_results:
                            for result in tool_results["content"]:
                                if result.get("type") == "json":
                                    result_data = result.get("json", {})
                                    if is_structured and "sql" in result_data:
                                        sql = result_data.get("sql", "")
                                    elif not is_structured and "searchResults" in result_data:
                                        search_results = [sr["text"] for sr in result_data["searchResults"]]
    except Exception as e:
        st.error(f"❌ Error Processing Response: {str(e)}")
        if st.session_state.debug_mode:
            st.sidebar.error(f"Process SSE Error: {str(e)}")
    return sql.strip(), search_results

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
        if st.session_state.CONN is None:
            st.error("Snowflake connection not initialized.")
            return None
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
                if st.session_state.debug_mode:
                    st.sidebar.error("Empty API response.")
                return None
            return parse_sse_response(resp.text)
        else:
            raise Exception(f"Failed request with status {resp.status_code}: {resp.text}")
    except Exception as e:
        st.error(f"❌ API Request Error: {str(e)}")
        if st.session_state.debug_mode:
            st.sidebar.error(f"API Call Error: {str(e)}")
        return None

def run_snowflake_query(query):
    try:
        session = st.session_state.snowpark_session
        if session is None:
            st.error("Snowpark session not initialized.")
            return None
        if not query:
            return None
        df = session.sql(query)
        data = df.collect()
        if not data:
            if st.session_state.debug_mode:
                st.sidebar.warning("Query returned no data.")
            return None
        columns = df.schema.names
        result_df = pd.DataFrame(data, columns=columns)
        if st.session_state.debug_mode:
            st.sidebar.text_area("Query Results", result_df.to_string(), height=200)
        return result_df
    except Exception as e:
        st.error(f"❌ SQL Execution Error: {str(e)}")
        if st.session_state.debug_mode:
            st.sidebar.error(f"SQL Error Details: {str(e)}")
        return None

def is_structured_query(query: str):
    structured_patterns = [
        r'\b(count|number|where|group by|order by|sum|avg|max|min|total|how many|which|show|list|names?|are there any|rejected deliveries?|least|highest|duration|approval)\b',
        r'\b(vendor|supplier|requisition|purchase order|po|organization|department|buyer|delivery|received|billed|rejected|late|on time|late deliveries?|Suppliers|payment|billing|percentage|list)\b',
        r'\b(property|tenant|lease|rent|occupancy|maintenance)\b'
    ]
    return any(re.search(pattern, query.lower()) for pattern in structured_patterns)

def is_complete_query(query: str):
    complete_patterns = [r'\b(generate|write|create|describe|explain)\b']
    return any(re.search(pattern, query.lower()) for pattern in complete_patterns)

def is_summarize_query(query: str):
    summarize_patterns = [r'\b(summarize|summary|condense)\b']
    return any(re.search(pattern, query.lower()) for pattern in summarize_patterns)

def is_question_suggestion_query(query: str):
    suggestion_patterns = [
        r'\b(what|which|how)\b.*\b(questions|type of questions|queries)\b.*\b(ask|can i ask|pose)\b',
        r'\b(give me|show me|list)\b.*\b(questions|examples|sample questions)\b'
    ]
    return any(re.search(pattern, query.lower()) for pattern in suggestion_patterns)

def is_greeting_query(query: str):
    greeting_patterns = [r'^\b(hello|hi|hey,greet)\b$', r'^\b(hello|hi|hey,greet)\b\s.*$']
    return any(re.search(pattern, query.lower()) for pattern in greeting_patterns)

def is_maintenance_query(query: str):
    return "maintenance" in query.lower()

def summarize_unstructured_answer(answer):
    sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|")\s', answer)
    return "\n".join(f"• {sent.strip()}" for sent in sentences[:6])

def suggest_sample_questions(query: str) -> List[str]:
    try:
        prompt = (
            f"The user asked: '{query}'. Generate 3–5 clear, concise sample questions related to properties, leases, tenants, rent, or occupancy metrics. "
            f"Format as a numbered list."
        )
        response = complete(st.session_state.model_name, prompt)
        if response:
            questions = []
            for line in response.split("\n"):
                line = line.strip()
                if re.match(r'^\d+\.\s*.+', line):
                    question = re.sub(r'^\d+\.\s*', '', line)
                    questions.append(question)
            return questions[:5]
        else:
            return [
                "Which properties have the highest occupancy rates?",
                "What is the average rent collected per tenant?",
                "Which leases expire in the next 30 days?",
                "What’s the total rental income by property?",
                "Which tenants have pending rent payments?"
            ]
    except Exception as e:
        st.error(f"❌ Failed to generate sample questions: {str(e)}")
        if st.session_state.debug_mode:
            st.sidebar.error(f"Suggest Questions Error: {str(e)}")
        return [
            "Which lease applications are pending?",
            "What’s the total rental income by property?",
            "Which tenants have delayed move-ins?",
            "What’s the average lease approval time?",
            "Which manager signed the most leases?"
        ]

def process_query(query: str):
    try:
        if st.session_state.debug_mode:
            st.sidebar.info(f"Processing query: {query}")

        if query.lower().startswith("no of"):
            query = query.replace("no of", "number of", 1)
        st.session_state.show_greeting = False
        st.session_state.chart_x_axis = None
        st.session_state.chart_y_axis = None
        st.session_state.chart_type = "Bar Chart"
        original_query = query
        if query.strip().isdigit() and st.session_state.last_suggestions:
            try:
                index = int(query.strip()) - 1
                if 0 <= index < len(st.session_state.last_suggestions):
                    query = st.session_state.last_suggestions[index]
                else:
                    query = original_query
            except ValueError:
                query = original_query
        
        st.session_state.chat_history.append({"role": "user", "content": original_query})
        st.session_state.messages.append({"role": "user", "content": original_query})
        with st.chat_message("user"):
            st.markdown(original_query)
        with st.chat_message("assistant"):
            with st.spinner("Generating Response..."):
                response_placeholder = st.empty()
                is_structured = is_structured_query(query)
                is_complete = is_complete_query(query)
                is_summarize = is_summarize_query(query)
                is_suggestion = is_question_suggestion_query(query)
                is_greeting = is_greeting_query(query)
                is_maintenance = is_maintenance_query(query)
                assistant_response = {"role": "assistant", "content": "", "query": query}
                response_content = ""
                failed_response = False

                if st.session_state.debug_mode:
                    st.sidebar.info(f"Query Type: Structured={is_structured}, Complete={is_complete}, Summarize={is_summarize}, Suggestion={is_suggestion}, Greeting={is_greeting}, Maintenance={is_maintenance}")

                # Check for data source mismatch
                if st.session_state.data_source == "Database" and not is_structured and not (is_greeting or is_suggestion or is_complete or is_summarize or is_maintenance):
                    response_content = f"**{get_tone(query)}**\nThis question is better suited for the Document data source. Please select 'Document' in the sidebar."
                    response_placeholder.markdown(response_content, unsafe_allow_html=True)
                    assistant_response["content"] = response_content
                    st.session_state.messages.append({"content": response_content})
                    st.session_state.chat_history.append(assistant_response)
                    st.session_state.current_query = query
                    return
                elif st.session_state.data_source == "Document" and is_structured and not (is_greeting or is_suggestion or is_complete or is_summarize or is_maintenance):
                    response_content = f"**{get_tone(query)}**\nThis question is better suited for the Database data source."
                    response_placeholder.markdown(response_content, unsafe_allow_html=True)
                    assistant_response["content"] = response_content
                    st.session_state.messages.append({"content": response_content})
                    st.session_state.chat_history.append(assistant_response)
                    st.session_state.current_query = query
                    return

                if is_greeting and original_query.lower().strip() == "hi":
                    response_content = f"""
                    {get_tone(original_query)}  
                    Property management is all about keeping your properties in tip-top shape—leasing, tenant screening, rent collection, and maintenance, with transparency and efficiency. 🏠 Ask about your rent, lease, or submit a maintenance request to get started!
                    """
                    response_placeholder.write_stream(stream_text(response_content))
                    assistant_response["content"] = response_content
                    st.session_state.messages.append({"content": response_content})
                    st.session_state.last_suggestions = suggest_sample_questions(query)

                elif is_greeting or is_suggestion:
                    greeting = original_query.lower().split()[0]
                    if greeting not in ["hi", "hello", "hey", "greet"]:
                        greeting = "hello"
                    response_content = f"{get_tone(original_query)} Here are some property management questions you can ask:\n\n"
                    selected_questions = suggest_sample_questions(query)
                    for i, q in enumerate(selected_questions, 1):
                        response_content += f"{i}. {q}\n"
                    response_content += "\nFeel free to ask any of these or your own question!"
                    response_placeholder.markdown(response_content, unsafe_allow_html=True)
                    assistant_response["content"] = response_content
                    st.session_state.last_suggestions = selected_questions
                    st.session_state.messages.append({"content": response_content})

                elif is_complete:
                    response = create_prompt(query)
                    if response:
                        response_content = f"**{get_tone(query)}**\n{response}"
                        response_placeholder.markdown(response_content, unsafe_allow_html=True)
                        assistant_response["content"] = response_content
                        st.session_state.messages.append({"content": response_content})
                    else:
                        response_content = ""
                        failed_response = True
                        assistant_response["content"] = response_content

                elif is_summerize:
                    summary = summerize(query)
                    if summary:
                        response_content = f"**{get_tone(query)}**\n**Summary:**\n{summary}"
                        response_placeholder.markdown(response_content, unsafe_allow_html=True)
                        assistant_response["content"] = response_content
                        st.session_state.messages.append({"content": response_content})
                    else:
                        response_content = ""
                        failed_response = True
                        assistant_response["content"] = response_content

                elif is_maintenance:
                    with st.form("maintenance_form"):
                        request = st.text_area("Describe your maintenance issue:")
                        if st.form_submit_button("Submit Request"):
                            try:
                                session.sql(
                                    "INSERT INTO maintenance_requests (tenant_id, request, status, timestamp) VALUES (?, ?, 'Pending', CURRENT_TIMESTAMP)",
                                    params=[st.session_state.tenant_id, request]
                                ).collect()
                                response_content = f"**{get_tone(query)}**\nRequest submitted! 🛠️ Our team will jump on it faster than you can say 'fixed'!"
                            except Exception as e:
                                response_content = f"**{get_tone(query)}**\nOops, something broke! 😅 Try again or contact support."
                                if st.session_state.debug_mode:
                                    st.sidebar.error(f"Maintenance Request Error: {str(e)}")
                            response_placeholder.markdown(response_content, unsafe_allow_html=True)
                            assistant_response["content"] = response_content
                            st.session_state.messages.append({"content": response_content})

                elif st.session_state.data_source == "Database" and is_structured:
                    response = snowflake_api_call(query, is_structured=True)
                    sql, _ = process_sse_response(response, is_structured=True)
                    if sql:
                        if st.session_state.debug_mode:
                            st.sidebar.text_area("Generated SQL", sql, height=150)
                        results = run_snowflake_query(sql)
                        if results is not None and not results.empty:
                            results_text = results.to_string(index=False)
                            prompt = f"Provide a concise natural language answer to the query '{query}' using the following data, avoiding phrases like 'Based on the query results':\n\n{results_text}"
                            summary = complete(st.session_state.model_name, prompt)
                            if not summary:
                                summary = "⚠️ Unable to generate a natural language summary."
                            response_content = f"**{get_tone(query)}**\n{summary}"
                            response_placeholder.markdown(response_content, unsafe_allow_html=True)
                            if sql:
                                with st.expander("View SQL Query", expanded=False):
                                    st.code(sql, language="sql")
                            st.markdown(f"**Query Results ({len(results)} rows):**")
                            st.dataframe(results)
                            if len(results.columns) >= 2:
                                st.markdown("**Insights Visualization:**)
                                display_chart(results, prefix=f"chart_{hash(query)}", query=query)
                            assistant_response.update({
                                "content": response_content,
                                "sql": sql,
                                "results": results,
                                "summary": summary
                            })
                            st.session_state.messages.append({
                                "content": response_content,
                                "sql": sql,
                                "results": results,
                                "summary": summary
                            })
                        else:
                            response_content = f"**{get_tone(query)}**\nNo data returned for the query."
                            failed_response = True
                            assistant_response["content"] = response_content
                    else:
                        response_content = f"**{get_tone(query)}**\nFailed to generate SQL query."
                        failed_response = True
                        assistant_response["content"] = response_content

                elif st.session_state.data_source == "Document":
                    response = snowflake_api_call(query, is_structured=False)
                    _, search_results = process_sse_response(response, is_structured=False)
                    if search_results:
                        raw_result = search_results[0]
                        summary = create_prompt(query)
                        if summary:
                            response_content = f"**{get_tone(query)}**\n**Answer:**\n{summary}"
                            response_placeholder.markdown(response_content, unsafe_allow_html=True)
                            assistant_response["content"] = response_content
                            st.session_state.messages.append({"content": response_content})
                        else:
                            response_content = f"**{get_tone(query)}**\n**🔍 Key Information (Unsummarized):**\n{summarize_unstructured_answer(raw_result)}"
                            response_placeholder.markdown(response_content, unsafe_allow_html=True)
                            assistant_response["content"] = response_content
                            st.session_state.messages.append({"content": response_content})
                    else:
                        response_content = ""
                        failed_response = True
                        assistant_response["content"] = response_content

                else:
                    response_content = f"**{get_tone(query)}**\nPlease select a data source to proceed with your query."
                    response_placeholder.markdown(response_content, unsafe_allow_html=True)
                    assistant_response["content"] = response_content
                    st.session_state.messages.append({"content": response_content})

                if failed_response:
                    suggestions = suggest_sample_questions(query)
                    response_content = f"**{get_tone(query)}**\nI’m not sure about your question. Here are some property management questions you can ask:\n\n"
                    for i, suggestion in enumerate(suggestions, 1):
                        response_content += f"{i}. {suggestion}\n"
                    response_content += "\nTry one of these or rephrase your question!"
                    response_placeholder.markdown(response_content, unsafe_allow_html=True)
                    assistant_response["content"] = response_content
                    st.session_state.last_suggestions = suggestions
                    st.session_state.messages.append({"content": response_content})

                st.session_state.chat_history.append(assistant_response)
                st.session_state.current_query = query
                st.session_state.current_results = assistant_response.get("results")
                st.session_state.current_sql = assistant_response.get("sql")
                st.session_state.current_summary = assistant_response.get("summary")
                if st.session_state.debug_mode:
                    st.sidebar.success(f"Query processed: {query}")

    except Exception as e:
        st.error(f"❌ Error processing query: {str(e)}")
        if st.session_state.debug_mode:
            st.sidebar.error(f"Process Query Error: {str(e)}")

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
    st.title("Property Management AI")
    st.markdown(f"Semantic Model: `{SEMANTIC_MODEL.split('/')[-1]}`")
    init_config_options()

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
        Property management is all about keeping your properties in tip-top shape—leasing, tenant screening, rent collection, and maintenance, with transparency and efficiency. 🏠 Ask about occupancy rates, lease details, rent payments, or submit a maintenance request!
        """)

    # Sample Questions in Sidebar
    sample_questions = [
        "What is Property Management",
        "Total number of properties currently occupied?",
        "What is the number of properties by occupancy status?",
        "What is the number of properties currently leased?",
        "What are the supplier payments compared to customer billing by month?",
        "What is the total number of suppliers?",
        "What is the average supplier payment per property?",
        "What are the details of lease execution, commencement, and termination?",
        "What are the customer billing and supplier payment details by location and purpose?",
        "What is the budget recovery by billing purpose?",
        "What are the details of customer billing?",
        "What are the details of supplier payments?",
    ]
    with st.sidebar:
        st.markdown("### Sample Questions")
        for sample in sample_questions:
            if st.button(sample, key=f"sample_{uuid.uuid4()}"):
                if st.session_state.debug_mode:
                    st.sidebar.info(f"Sample question clicked: {sample}")
                st.session_state.sample_query = sample
                st.rerun()

    # Process Sample Query
    if st.session_state.sample_query:
        with st.spinner("Processing sample question..."):
            process_query(st.session_state.sample_query)
            st.session_state.sample_query = None  # Clear after processing

    # Chat Input
    query = st.chat_input("Ask your question...")
    if query:
        process_query(query)

    # Chat History
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
