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

# Snowflake/Cortex Configuration
HOST = "GBJYVCT-LSB50763.snowflakecomputing.com"
DATABASE = "AI"
SCHEMA = "DWH_MART"
API_ENDPOINT = "/api/v2/cortex/agent:run"
API_TIMEOUT = 50000  # in milliseconds
CORTEX_SEARCH_SERVICES = "AI.DWH_MART.propertymanagement"
SEMANTIC_MODEL = '@"AI"."DWH_MART"."PROPERTY_MANAGEMENT"/property_management.yaml'

# Model options
MODELS = [
    "mistral-large",
    "snowflake-arctic",
    "llama3-70b",
    "llama3-8b",
]

# Streamlit Page Config
st.set_page_config(
    page_title="Welcome to Cortex AI Assistant",
    layout="wide",
    initial_sidebar_state="auto"
)

# Initialize session state
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
    st.session_state.username = ""
    st.session_state.password = ""
    st.session_state.CONN = None
    st.session_state.snowpark_session = None
    st.session_state.chat_history = []
    st.session_state.messages = []
if "debug_mode" not in st.session_state:
    st.session_state.debug_mode = False
if "last_suggestions" not in st.session_state:
    st.session_state.last_suggestions = []
if "chart_x_axis" not in st.session_state:
    st.session_state.chart_x_axis = None
if "chart_y_axis" not in st.session_state:
    st.session_state.chart_y_axis = None
if "chart_type" not in st.session_state:
    st.session_state.chart_type = "Bar Chart"
if "current_query" not in st.session_state:
    st.session_state.current_query = None
if "current_results" not in st.session_state:
    st.session_state.current_results = None
if "current_sql" not in st.session_state:
    st.session_state.current_sql = None
if "current_summary" not in st.session_state:
    st.session_state.current_summary = None
if "service_metadata" not in st.session_state:
    st.session_state.service_metadata = []
if "selected_cortex_search_service" not in st.session_state:
    st.session_state.selected_cortex_search_service = CORTEX_SEARCH_SERVICES
if "model_name" not in st.session_state:
    st.session_state.model_name = "mistral-large"
if "num_retrieved_chunks" not in st.session_state:
    st.session_state.num_retrieved_chunks = 100
if "num_chat_messages" not in st.session_state:
    st.session_state.num_chat_messages = 10
if "use_chat_history" not in st.session_state:
    st.session_state.use_chat_history = True
if "clear_conversation" not in st.session_state:
    st.session_state.clear_conversation = False
if "show_selector" not in st.session_state:
    st.session_state.show_selector = False
if "show_greeting" not in st.session_state:
    st.session_state.show_greeting = True
if "data_source" not in st.session_state:
    st.session_state.data_source = "Database"
if "show_about" not in st.session_state:
    st.session_state.show_about = False
if "show_help" not in st.session_state:
    st.session_state.show_help = False
if "show_history" not in st.session_state:
    st.session_state.show_history = False
if "query" not in st.session_state:
    st.session_state.query = None

# Hide Streamlit branding and prevent chat history shading
st.markdown("""
<style>
#MainMenu, header, footer {visibility: hidden;}
[data-testid="stChatMessage"] {
    opacity: 1 !important;
    background-color: transparent !important;
}
</style>
""", unsafe_allow_html=True)

def stream_text(text: str, chunk_size: int = 2, delay: float = 0.04):
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
    st.session_state.query = None
    st.session_state.show_history = False  # Reset history toggle
    st.rerun()

def init_service_metadata():
    if not st.session_state.service_metadata:
        try:
            services = session.sql("SHOW CORTEX SEARCH SERVICES;").collect()
            service_metadata = []
            if services:
                for s in services:
                    svc_name = s["name"]
                    if svc_name == CORTEX_SEARCH_SERVICES:
                        svc_search_col = session.sql(
                            f"DESC CORTEX SEARCH SERVICE {svc_name};"
                        ).collect()[0]["search_column"]
                        service_metadata.append(
                            {"name": svc_name, "search_column": svc_search_col}
                        )
            st.session_state.service_metadata = service_metadata or [{"name": CORTEX_SEARCH_SERVICES, "search_column": ""}]
        except Exception as e:
            st.error(f"❌ Failed to initialize Cortex Search service metadata: {str(e)}")
            st.session_state.service_metadata = [{"name": CORTEX_SEARCH_SERVICES, "search_column": ""}]

def init_config_options():
    pass

def query_cortex_search_service(query):
    try:
        db, schema = session.get_current_database(), session.get_current_schema()
        root = Root(session)
        cortex_search_service = (
            root.databases[db]
            .schemas[schema]
            .cortex_search_services[st.session_state.selected_cortex_search_service]
        )
        context_documents = cortex_search_service.search(
            query, columns=[], limit=st.session_state.num_retrieved_chunks
        )
        results = context_documents.results
        service_metadata = st.session_state.service_metadata
        search_col = [s["search_column"] for s in service_metadata
                      if s["name"] == st.session_state.selected_cortex_search_service][0]
        context_str = ""
        for i, r in enumerate(results):
            context_str += f"Context document {i+1}: {r[search_col]} \n" + "\n"
        if st.session_state.debug_mode:
            st.sidebar.text_area("Context documents", context_str, height=500)
        return context_str
    except Exception as e:
        st.error(f"❌ Error querying Cortex Search service: {str(e)}")
        return ""

def get_chat_history():
    start_index = max(
        0, len(st.session_state.chat_history) - st.session_state.num_chat_messages
    )
    return st.session_state.chat_history[start_index : len(st.session_state.chat_history) - 1]

def make_chat_history_summary(chat_history, question):
    chat_history_str = "\n".join([f"{msg['role']}: {msg['content']}" for msg in chat_history])
    prompt = f"""
        [INST]
        Based on the chat history below and the question, generate a query that extends the question
        with the chat history provided. The query should be in natural language.
        Answer with only the query. Do not add any explanation.

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
        st.sidebar.text_area("Chat history summary", summary.replace("$", "\$"), height=150)
    return summary

def create_prompt(user_question):
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
        return complete(st.session_state.model_name, user_question)
    
    prompt = f"""
        [INST]
        You are a helpful AI chat assistant with RAG capabilities. When a user asks you a question,
        you will also be given context provided between <context> and </context> tags. Use that context
        with the user's chat history provided in the between <chat_history> and </chat_history> tags
        to provide a summary that addresses the user's question. Ensure the answer is coherent, concise,
        and directly relevant to the user's question.

        If the user asks a generic question which cannot be answered with the given context or chat_history,
        just respond directly and concisely to the user's question using the LLM.

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

def get_user_questions(limit=10):
    """
    Extract the last 'limit' user questions from the chat history.
    Returns a list of questions in reverse chronological order (most recent first).
    """
    user_questions = [msg["content"] for msg in st.session_state.chat_history if msg["role"] == "user"]
    return user_questions[-limit:][::-1]  # Last 'limit' questions, reversed to show most recent first

if not st.session_state.authenticated:
    st.title("Welcome to Snowflake Cortex AI")
    st.markdown("Please login to interact with your data")
    st.session_state.username = st.text_input("Enter Snowflake Username:", value=st.session_state.username)
    st.session_state.password = st.text_input("Enter Password:", type="password")
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
            snowpark_session = Session.builder.configs({
                "connection": conn
            }).create()
            st.session_state.snowpark_session = snowpark_session
            with conn.cursor() as cur:
                cur.execute(f"USE DATABASE {DATABASE}")
                cur.execute(f"USE SCHEMA {SCHEMA}")
                cur.execute("ALTER SESSION SET TIMEZONE = 'UTC'")
                cur.execute("ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE")
            st.session_state.authenticated = True
            st.success("Authentication successful! Redirecting...")
            st.rerun()
        except Exception as e:
            st.error(f"Authentication failed: {e}")
else:
    session = st.session_state.snowpark_session
    root = Root(session)

    def run_snowflake_query(query):
        try:
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
            r'\b(what|which|how)\b.*\b(questions|type of questions|queries|information|data|insights)\b.*\b(ask|can i ask|pose|get|available)\b',
            r'\b(give me|show me|list)\b.*\b(questions|examples|sample questions)\b'
        ]
        return any(re.search(pattern, query.lower()) for pattern in suggestion_patterns)

    def is_greeting_query(query: str):
        greeting_patterns = [
            r'^\b(hello|hi|hey|greet)\b$',
            r'^\b(hello|hi|hey,greet)\b\s.*$'
        ]
        return any(re.search(pattern, query.lower()) for pattern in greeting_patterns)

    def complete(model, prompt):
        try:
            prompt = prompt.replace("'", "\\'")
            query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{prompt}') AS response"
            result = session.sql(query).collect()
            return result[0]["RESPONSE"]
        except Exception as e:
            st.error(f"❌ COMPLETE Function Error: {str(e)}")
            return None

    def summarize(text):
        try:
            text = text.replace("'", "\\'")
            query = f"SELECT SNOWFLAKE.CORTEX.SUMMARIZE('{text}') AS summary"
            result = session.sql(query).collect()
            return result[0]["SUMMARY"]
        except Exception as e:
            st.error(f"❌ SUMMARIZE Function Error: {str(e)}")
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
        return sql.strip(), search_results

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
            payload["tool_resources"] = {"search1": {"name": st.session_state.selected_cortex_search_service, "max_results": st.session_state.num_retrieved_chunks}}
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
            if st.session_state.debug_mode:
                st.write(f"API Response Status: {resp.status_code}")
                st.write(f"API Raw Response: {resp.text}")
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

    def summarize_unstructured_answer(answer):
        sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|")\s', answer)
        return "\n".join(f"• {sent.strip()}" for sent in sentences[:6])

    def suggest_sample_questions(query: str) -> List[str]:
        try:
            prompt = (
                f"The user asked for: '{query}'. Generate 3–5 clear, concise sample questions related to properties, leases, tenants, rent, or occupancy metrics. "
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
            return [
                "Which lease applications are pending?",
                "What’s the total rental income by property?",
                "Which tenants have delayed move-ins?",
                "What’s the average lease approval time?",
                "Which manager signed the most leases?"
            ]

    def display_chart_tab(df: pd.DataFrame, prefix: str = "chart", query: str = ""):
        try:
            if df is None or df.empty or len(df.columns) < 2:
                st.warning("No valid data available for visualization.")
                if st.session_state.debug_mode:
                    st.sidebar.warning(f"Chart Data Issue: df={df}, columns={df.columns if df is not None else 'None'}")
                return
            query_lower = query.lower()
            if re.search(r'\b(county|jurisdiction)\b', query_lower):
                default_data = "Pie Chart"
            elif re.search(r'\b(month|year|date)\b', query_lower):
                default_data = "Line Chart"
            else:
                default_data = "Bar Chart"
            all_cols = list(df.columns)
            col1, col2, col3 = st.columns(3)
            x_col = col1.selectbox("X axis", all_cols, index=0, key=f"{prefix}_x")
            remaining_cols = [c for c in all_cols if c != x_col]
            y_col = col2.selectbox("Y axis", remaining_cols, index=0, key=f"{prefix}_y")
            chart_options = ["Line Chart", "Bar Chart", "Pie Chart", "Scatter Chart", "Histogram Chart"]
            chart_type = col3.selectbox("Chart Type", chart_options, index=chart_options.index(default_data), key=f"{prefix}_type")
            if st.session_state.debug_mode:
                st.sidebar.text_area("Chart Config", f"X: {x_col}, Y: {y_col}, Type: {chart_type}", height=100)
            if chart_type == "Line Chart":
                fig = px.line(df, x=x_col, y=y_col, title=chart_type)
                st.plotly_chart(fig, key=f"{prefix}_line")
            elif chart_type == "Bar Chart":
                fig = px.bar(df, x=x_col, y=y_col, title=chart_type)
                st.plotly_chart(fig, key=f"{prefix}_bar")
            elif chart_type == "Pie Chart":
                fig = px.pie(df, names=x_col, values=y_col, title=chart_type)
                st.plotly_chart(fig, key=f"{prefix}_pie")
            elif chart_type == "Scatter Chart":
                fig = px.scatter(df, x=x_col, y=y_col, title=chart_type)
                st.plotly_chart(fig, key=f"{prefix}_scatter")
            elif chart_type == "Histogram Chart":
                fig = px.histogram(df, x=x_col, title=chart_type)
                st.plotly_chart(fig, key=f"{prefix}_hist")
        except Exception as e:
            st.error(f"❌ Error generating chart: {str(e)}")
            if st.session_state.debug_mode:
                st.sidebar.error(f"Chart Error Details: {str(e)}")

    def toggle_about():
        st.session_state.show_about = not st.session_state.show_about
        st.session_state.show_help = False
        st.session_state.show_history = False  # Close history if open

    def toggle_help():
        st.session_state.show_help = not st.session_state.show_help
        st.session_state.show_about = False
        st.session_state.show_history = False  # Close history if open

    def toggle_history():
        st.session_state.show_history = not st.session_state.show_history
        st.session_state.show_about = False  # Close about if open
        st.session_state.show_help = False  # Close help if open

    with st.sidebar:
        st.markdown("""
        <style>
        /* Default styling for sidebar buttons (suggested questions) */
        [data-testid="stSidebar"] [data-testid="stButton"] > button {
            background-color: #29B5E8 !important;
            color: white !important;
            font-weight: bold !important;
            width: 100% !important;
            border-radius: 0px !important;
            margin: 0 !important;
            border: none !important;
            padding: 0.5rem 1rem !important;
        }
        /* Custom styling for Clear conversation, About, Help & Documentation, History, and Sample Questions buttons */
        [data-testid="stSidebar"] [data-testid="stButton"][aria-label="Clear conversation"] > button,
        [data-testid="stSidebar"] [data-testid="stButton"][aria-label="About"] > button,
        [data-testid="stSidebar"] [data-testid="stButton"][aria-label="Help & Documentation"] > button,
        [data-testid="stSidebar"] [data-testid="stButton"][aria-label="History"] > button,
        [data-testid="stSidebar"] [data-testid="stButton"][aria-label="Sample Questions"] > button {
            background-color: #28A745 !important;
            color: white !important;
            font-weight: normal !important;
            border: 1px solid #28A745 !important;
        }
        </style>
        """, unsafe_allow_html=True)

        # 1. Snowflake Logo
        logo_url = "https://www.snowflake.com/wp-content/themes/snowflake/assets/img/logo-blue.svg"
        st.image(logo_url, width=250)

        # 2. Clear Conversation Button
        if st.button("Clear conversation", key="clear_conversation_button"):
            start_new_conversation()

        # 3. Select Data Source
        st.radio("Select Data Source:", ["Database", "Document"], key="data_source")

        # 4. Select Cortex Search Service
        st.selectbox(
            "Select Cortex Search Service:",
            [CORTEX_SEARCH_SERVICES],
            key="selected_cortex_search_service"
        )

        # 5. Debug
        st.toggle("Debug", key="debug_mode", value=st.session_state.debug_mode)

        # 6. Use Chat History
        st.toggle("Use chat history", key="use_chat_history", value=True)

        # Advanced Options
        with st.expander("Advanced options"):
            st.selectbox("Select model:", MODELS, key="model_name")
            st.number_input(
                "Select number of context chunks",
                value=100,
                key="num_retrieved_chunks",
                min_value=1,
                max_value=400
            )
            st.number_input(
                "Select number of messages to use in chat history",
                value=10,
                key="num_chat_messages",
                min_value=1,
                max_value=100
            )
        if st.session_state.debug_mode:
            st.expander("Session State").write(st.session_state)

        # 7. Sample Questions Button
        if st.button("Sample Questions", key="sample_questions_button"):
            st.session_state.show_sample_questions = not st.session_state.get("show_sample_questions", False)
        if st.session_state.get("show_sample_questions", False):
            st.markdown("### Sample Questions")
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
                "What are the details of supplier payments?"
            ]
            for sample in sample_questions:
                if st.button(sample, key=f"sidebar_{sample}"):
                    st.session_state.query = sample
                    st.session_state.show_greeting = False

        # Divider
        st.markdown("---")

        # History, About, Help & Documentation
        with st.container():
            if st.button("History", key="history_button"):
                toggle_history()
            if st.session_state.show_history:
                st.markdown("### Recent Questions")
                user_questions = get_user_questions(limit=10)
                if not user_questions:
                    st.write("No questions in history yet.")
                else:
                    for idx, question in enumerate(user_questions):
                        if st.button(question, key=f"history_{idx}"):
                            st.session_state.query = question
                            st.session_state.show_greeting = False

            if st.button("About", key="about_button"):
                toggle_about()
            if st.session_state.show_about:
                st.markdown("### About")
                st.write(
                    "This application uses **Snowflake Cortex Analyst** to interpret "
                    "your natural language questions and generate data insights. "
                    "Simply ask a question below to see relevant answers and visualizations."
                )

            if st.button("Help & Documentation", key="help_button"):
                toggle_help()
            if st.session_state.show_help:
                st.markdown("### Help & Documentation")
                st.write(
                    "- [User Guide](https://docs.snowflake.com/en/guides-overview-ai-features)  \n"
                    "- [Snowflake Cortex Analyst Docs](https://docs.snowflake.com/)  \n"
                    "- [Contact Support](https://www.snowflake.com/en/support/)"
                )

    st.title("Cortex AI Assistant by DiLytics")
    semantic_model_filename = SEMANTIC_MODEL.split("/")[-1]
    st.markdown(f"Semantic Model: `{semantic_model_filename}`")
    init_service_metadata()

    if st.session_state.show_greeting and not st.session_state.chat_history:
        st.markdown("Welcome! I’m the Snowflake AI Assistant, ready to assist you with Property management. Property management is all about keeping your properties in tip-top shape—leasing, tenant screening, rent collection, and maintenance, with transparency and efficiency. 🏠 Ask about your rent, lease, or submit a maintenance request to get started! — simply type your question to get started.")
    else:
        st.session_state.show_greeting = False

    for message in st.session_state.chat_history:
        with st.chat_message(message["role"]):
            st.markdown(message["content"], unsafe_allow_html=True)
            if message["role"] == "assistant" and "results" in message and message["results"] is not None:
                with st.expander("View SQL Query", expanded=False):
                    st.code(message["sql"], language="sql")
                st.markdown(f"**Query Results ({len(message['results'])} rows):**")
                st.dataframe(message["results"])
                if not message["results"].empty and len(message["results"].columns) >= 2:
                    st.markdown("**📈 Visualization:**")
                    display_chart_tab(message["results"], prefix=f"chart_{hash(message['content'])}", query=message.get("query", ""))

    chat_input_query = st.chat_input("Ask your question...")
    if chat_input_query:
        st.session_state.query = chat_input_query

    if st.session_state.query:
        query = st.session_state.query
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
                if st.session_state.data_source not in ["Database", "Document"]:
                    st.session_state.data_source = "Database"
                is_structured = is_structured_query(query) and st.session_state.data_source == "Database"
                is_complete = is_complete_query(query)
                is_summarize = is_summarize_query(query)
                is_suggestion = is_question_suggestion_query(query)
                is_greeting = is_greeting_query(query)
                if st.session_state.debug_mode:
                    st.sidebar.text_area("Debug Info", f"is_structured: {is_structured}\nData Source: {st.session_state.data_source}", height=100)
                assistant_response = {"role": "assistant", "content": "", "query": query}
                response_content = ""
                failed_response = False

                if is_greeting and original_query.lower().strip() == "hi":
                    response_content = """
                    Hello! Welcome to the Property Management AI Assistant! I'm here to help you explore and analyze property-related data, answer questions about leasing, tenant screening, rent collection, and maintenance, or provide insights from documents.

                    Here are some questions you can try:

                    Total number of properties currently occupied?
                    What is the number of properties currently leased?
                    What are the details of lease execution, commencement, and termination?
                    What is the average supplier payment per property?
                    Feel free to ask anything, or pick one of the suggested questions to get started!
                    """
                    with response_placeholder:
                        st.write_stream(stream_text(response_content))
                        st.markdown(response_content, unsafe_allow_html=True)
                    assistant_response["content"] = response_content
                    st.session_state.messages.append({"role": "assistant", "content": response_content})
                    st.session_state.last_suggestions = [
                        "Total number of properties currently occupied?",
                        "What is the number of properties currently leased?",
                        "What are the details of lease execution, commencement, and termination?",
                        "What is the average supplier payment per property?"
                    ]

                elif is_greeting or is_suggestion:
                    greeting = original_query.lower().split()[0]
                    if greeting not in ["hi", "hello", "hey", "greet"]:
                        greeting = "hello"
                    response_content = (
                        f"Hello! Welcome to the Property Management AI Assistant! I'm here to help you explore and analyze property-related data, answer questions about leasing, tenant screening, rent collection, and maintenance, or provide insights from documents.\n\n"
                        "Here are some types of information you can get from me:\n\n"
                        "1. **Property Metrics**: Information on occupancy rates, number of properties leased, or total rental income by property.\n"
                        "2. **Lease Details**: Insights into lease execution, commencement, and termination dates.\n"
                        "3. **Tenant Information**: Details on tenant screening, pending rent payments, or tenant move-ins.\n"
                        "4. **Financial Insights**: Data on supplier payments, customer billing, budget recovery, or average payments per property.\n"
                        "5. **Maintenance Requests**: Information on submitting or tracking maintenance requests.\n\n"
                        "Feel free to ask anything, or try one of these sample questions:\n"
                        "- Total number of properties currently occupied?\n"
                        "- What are the details of lease execution, commencement, and termination?\n"
                        "- What is the average supplier payment per property?\n"
                        "- Which tenants have pending rent payments?"
                    )
                    with response_placeholder:
                        st.write_stream(stream_text(response_content))
                        st.markdown(response_content, unsafe_allow_html=True)
                    assistant_response["content"] = response_content
                    st.session_state.last_suggestions = [
                        "Total number of properties currently occupied?",
                        "What are the details of lease execution, commencement, and termination?",
                        "What is the average supplier payment per property?",
                        "Which tenants have pending rent payments?"
                    ]
                    st.session_state.messages.append({"role": "assistant", "content": response_content})

                elif is_complete:
                    response = create_prompt(query)
                    if response:
                        response_content = f"**✍️ Generated Response:**\n{response}"
                        with response_placeholder:
                            st.write_stream(stream_text(response_content))
                            st.markdown(response_content, unsafe_allow_html=True)
                        assistant_response["content"] = response_content
                        st.session_state.messages.append({"role": "assistant", "content": response_content})
                    else:
                        response_content = ""
                        failed_response = True
                        assistant_response["content"] = response_content

                elif is_summarize:
                    summary = summarize(query)
                    if summary:
                        response_content = f"**Summary:**\n{summary}"
                        with response_placeholder:
                            st.write_stream(stream_text(response_content))
                            st.markdown(response_content, unsafe_allow_html=True)
                        assistant_response["content"] = response_content
                        st.session_state.messages.append({"role": "assistant", "content": response_content})
                    else:
                        response_content = ""
                        failed_response = True
                        assistant_response["content"] = response_content

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
                            response_content = f"**✍️ Generated Response:**\n{summary}"
                            with response_placeholder:
                                st.write_stream(stream_text(response_content))
                                st.markdown(response_content, unsafe_allow_html=True)
                            with st.expander("View SQL Query", expanded=False):
                                st.code(sql, language="sql")
                            st.markdown(f"**Query Results ({len(results)} rows):**")
                            st.dataframe(results)
                            if len(results.columns) >= 2:
                                st.markdown("**📈 Visualization:**")
                                display_chart_tab(results, prefix=f"chart_{hash(query)}", query=query)
                            assistant_response.update({
                                "content": response_content,
                                "sql": sql,
                                "results": results,
                                "summary": summary
                            })
                            st.session_state.messages.append({
                                "role": "assistant",
                                "content": response_content,
                                "sql": sql,
                                "results": results,
                                "summary": summary
                            })
                        else:
                            response_content = "No data returned for the query."
                            failed_response = True
                            assistant_response["content"] = response_content
                    else:
                        response_content = "Failed to generate SQL query."
                        failed_response = True
                        assistant_response["content"] = response_content

                elif st.session_state.data_source == "Document":
                    response = snowflake_api_call(query, is_structured=False)
                    _, search_results = process_sse_response(response, is_structured=False)
                    if search_results:
                        raw_result = search_results[0]
                        summary = create_prompt(query)
                        if summary:
                            response_content = f"**Here is the Answer:**\n{summary}"
                            with response_placeholder:
                                st.write_stream(stream_text(response_content))
                                st.markdown(response_content, unsafe_allow_html=True)
                            assistant_response["content"] = response_content
                            st.session_state.messages.append({"role": "assistant", "content": response_content})
                        else:
                            response_content = f"**🔍 Key Information (Unsummarized):**\n{summarize_unstructured_answer(raw_result)}"
                            with response_placeholder:
                                st.write_stream(stream_text(response_content))
                                st.markdown(response_content, unsafe_allow_html=True)
                            assistant_response["content"] = response_content
                            st.session_state.messages.append({"role": "assistant", "content": response_content})
                    else:
                        response_content = ""
                        failed_response = True
                        assistant_response["content"] = response_content

                else:
                    response_content = "Please select a data source to proceed with your query."
                    with response_placeholder:
                        st.write_stream(stream_text(response_content))
                        st.markdown(response_content, unsafe_allow_html=True)
                    assistant_response["content"] = response_content
                    st.session_state.messages.append({"role": "assistant", "content": response_content})

                if failed_response:
                    suggestions = suggest_sample_questions(query)
                    response_content = "I am not sure about your question. Here are some questions you can ask me:\n\n"
                    for i, suggestion in enumerate(suggestions, 1):
                        response_content += f"{i}. {suggestion}\n"
                    response_content += "\nThese questions might help clarify your query. Feel free to try one or rephrase your question!"
                    with response_placeholder:
                        st.write_stream(stream_text(response_content))
                        st.markdown(response_content, unsafe_allow_html=True)
                    assistant_response["content"] = response_content
                    st.session_state.last_suggestions = suggestions
                    st.session_state.messages.append({"role": "assistant", "content": response_content})

                st.session_state.chat_history.append(assistant_response)
                st.session_state.current_query = query
                st.session_state.current_results = assistant_response.get("results")
                st.session_state.current_sql = assistant_response.get("sql")
                st.session_state.current_summary = assistant_response.get("summary")
                st.session_state.query = None
