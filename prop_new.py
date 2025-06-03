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
import random

# Snowflake/Cortex Configuration
HOST = "HLGSIYM-COB42429.snowflakecomputing.com"
DATABASE = "AI"
SCHEMA = "DWH_MART"
API_ENDPOINT = "/api/v2/cortex/agent:run"
API_TIMEOUT = 50000  # in milliseconds
CORTEX_SEARCH_SERVICES = "AI.DWH_MART.PROPERTYMANAGEMENT"
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
    st.session_state.username = "CORTEX"
    st.session_state.password = "Dilytics@12345"
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
    st.session_state.service_metadata = [{"name": "AI.DWH_MART.PROPERTYMANAGEMENT", "search_column": ""}]
if "selected_cortex_search_service" not in st.session_state:
    st.session_state.selected_cortex_search_service = "AI.DWH_MART.PROPERTYMANAGEMENT"
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
if "rerun_trigger" not in st.session_state:
    st.session_state.rerun_trigger = False
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
if "previous_query" not in st.session_state:
    st.session_state.previous_query = None
if "previous_sql" not in st.session_state:
    st.session_state.previous_sql = None
if "previous_results" not in st.session_state:
    st.session_state.previous_results = None

# CSS Styling (reduced header text size)
st.markdown("""
<style>
#MainMenu, header, footer {visibility: hidden;}
[data-testid="stChatMessage"] {
    opacity: 1 !important;
    background-color: transparent !important;
    white-space: pre-wrap !important;
    word-wrap: break-word !important;
    overflow: hidden !important;
}
[data-testid="stChatMessageContent"] {
    white-space: pre-wrap !important;
    word-wrap: break-word !important;
    overflow: hidden !important;
    width: 100% !important;
    max-width: 100% !important;
    box-sizing: border-box !important;
}
.copy-button, [data-testid="copy-button"], [title="Copy to clipboard"], [data-testid="stTextArea"] {
    display: none !important;
}
.dilytics-logo {
    position: fixed;
    top: 10px;
    right: 10px;
    z-index: 1000;
    width: 150px;
    height: auto;
}
.fixed-header {
    position: fixed;
    top: 0;
    left: 20px;
    right: 0;
    z-index: 999;
    background-color: #ffffff;
    padding: 10px;
    text-align: center;
    pointer-events: none;
}
.fixed-header a {
    pointer-events: none !important;
    text-decoration: none !important;
    color: inherit !important;
    cursor: default !important;
}
.stApp {
    padding-top: 80px;
}
.follow-up-button {
    background-color: #29B5E8 !important;
    color: white !important;
    font-weight: bold !important;
    width: 100% !important;
    border-radius: 0px !important;
    margin: 0 !important;
    border: none !important;
    padding: 0.5rem 1rem !important;
}
.fixed-header h1 {
    font-size: 20px !important;
    margin-bottom: 2px !important;
    color: #29B5E8;
}
.fixed-header p {
    font-size: 12px !important;
    color: #333;
    margin: 0 !important;
}
</style>
""", unsafe_allow_html=True)

# Stream Text Function
def stream_text(text: str, chunk_size: int = 1, delay: float = 0.01):
    for i in range(0, len(text), chunk_size):
        yield text[i:i + chunk_size]
        time.sleep(delay)

# Submit Maintenance Request
def submit_maintenance_request(property_id: str, tenant_name: str, issue_description: str):
    try:
        request_id = str(uuid.uuid4())
        insert_query = """
        INSERT INTO MAINTENANCE_REQUESTS 
        (REQUEST_ID, PROPERTY_ID, TENANT_NAME, ISSUE_DESCRIPTION, SUBMITTED_AT, STATUS)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP(), 'PENDING')
        """
        session.sql(insert_query).bind((request_id, property_id, tenant_name, issue_description)).collect()
        return True, f"📝 Maintenance request submitted successfully! Request ID: {request_id}"
    except Exception as e:
        return False, f"❌ Failed to submit maintenance request: {str(e)}"

# Start New Conversation
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
    st.session_state.rerun_trigger = True
    st.session_state.show_greeting = True
    st.session_state.query = None
    st.session_state.show_history = False
    st.session_state.previous_query = None
    st.session_state.previous_sql = None
    st.session_state.previous_results = None

# Initialize Service Metadata
def init_service_metadata(session):
    st.session_state.service_metadata = [{"name": "AI.DWH_MART.PROPERTYMANAGEMENT", "search_column": ""}]
    try:
        svc_search_col = session.sql("DESC CORTEX SEARCH SERVICE AI.DWH_MART.PROPERTYMANAGEMENT;").collect()[0]["search_column"]
        st.session_state.service_metadata = [{"name": "AI.DWH_MART.PROPERTYMANAGEMENT", "search_column": svc_search_col}]
    except Exception as e:
        st.error(f"❌ Failed to verify AI.DWH_MART.PROPERTYMANAGEMENT: {str(e)}. Using default configuration.")
        if st.session_state.debug_mode:
            st.session_state.debug_logs["Service Metadata Error"] = str(e)

# Initialize Config Options
def init_config_options():
    st.sidebar.button("Clear conversation", on_click=start_new_conversation)
    st.sidebar.toggle("Use chat history", key="use_chat_history", value=True)
    with st.sidebar.expander("Advanced options"):
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

# Query Cortex Search Service
def query_cortex_search_service(query):
    try:
        db, schema = session.get_current_database(), session.get_current_schema()
        root = Root(session)
        cortex_search_service = (
            root.databases[db]
            .schemas[schema]
            .cortex_search_services["AI.DWH_MART.PROPERTYMANAGEMENT"]
        )
        context_documents = cortex_search_service.search(
            query, columns=[], limit=st.session_state.num_retrieved_chunks
        )
        results = context_documents.results
        service_metadata = st.session_state.service_metadata
        search_col = service_metadata[0]["search_column"]
        context_str = ""
        for i, r in enumerate(results):
            context_str += f"Context document {i+1}: {r[search_col]} \n" + "\n"
        if st.session_state.debug_mode:
            st.session_state.debug_logs["Context documents"] = context_str if context_str else "No context documents retrieved."
        return context_str
    except Exception as e:
        st.error(f"❌ Error querying Cortex Search service: {str(e)}")
        if st.session_state.debug_mode:
            st.session_state.debug_logs["Service Metadata Error"] = str(e)
        return ""

# Get Chat History
def get_chat_history():
    start_index = max(
        0, len(st.session_state.chat_history) - st.session_state.num_chat_messages
    )
    return st.session_state.chat_history[start_index : len(st.session_state.chat_history) - 1]

# Make Chat History Summary
def make_chat_history_summary(chat_history, question):
    chat_history_str = "\n".join([f"{msg['role']}: {msg['content']}" for msg in chat_history])
    prompt = f"""
        [INST]
        You are a conversational AI assistant. Based on the chat history below and the current question, generate a single, clear, and concise query that combines the context of the chat history with the current question. The resulting query should be in natural language and should reflect the user's intent in the conversational flow. Ensure the query is standalone and can be understood without needing to refer back to the chat history.

        For example:
        - If the chat history contains "user: Total number of properties currently occupied?" and the current question is "by state", the resulting query should be "What is the total number of properties currently occupied by state?"

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
        st.sidebar.text_area("Chat History Summary", summary.replace("$", "\$"), height=150)
    return summary

# Create Prompt
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

# Get User Questions
def get_user_questions(limit=10):
    user_questions = [msg["content"] for msg in st.session_state.chat_history if msg["role"] == "user"]
    return user_questions[-limit:][::-1]

# Detect Generic Queries
def is_generic_query(query: str):
    generic_patterns = [
        r'^\b(what|how|why|where|when)\b\s*$',
        r'^\b(what|how|why|where|when)\b\s+(should|can|do)\b.*$',
        r'^\b(help|assist|tell me)\b.*$',
        r'^(?!.*\b(property|tenant|lease|rent|occupancy|maintenance|supplier|billing)\b).*$',
    ]
    query_lower = query.lower().strip()
    if (is_structured_query(query) or 
        is_complete_query(query) or 
        is_summarize_query(query) or 
        is_question_suggestion_query(query) or 
        is_greeting_query(query)):
        return False
    return (any(re.search(pattern, query_lower) for pattern in generic_patterns) or 
            len(query_lower.split()) < 3 or
            not any(word in query_lower for word in ["property", "tenant", "lease", "rent", "occupancy", "maintenance"]))

# Suggest Follow-Up Questions
def suggest_follow_up_questions(query: str) -> List[str]:
    sample_questions = [
        "What is Property Management?",
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
    query_lower = query.lower()
    keywords = {
        "property": ["property"],
        "occupancy": ["occupied", "occupancy"],
        "lease": ["leased", "lease"],
        "rent": ["rent", "billing", "payment"],
        "supplier": ["supplier"],
        "maintenance": ["maintenance"]
    }
    relevant_categories = []
    for category, category_keywords in keywords.items():
        if any(keyword in query_lower for keyword in category_keywords):
            relevant_categories.append(category)
    
    filtered_questions = []
    for question in sample_questions:
        question_lower = question.lower()
        for category in relevant_categories:
            if any(keyword in question_lower for keyword in keywords[category]):
                filtered_questions.append(question)
                break
    
    if not filtered_questions:
        filtered_questions = sample_questions
    
    num_questions = min(len(filtered_questions), 5)
    if num_questions < 3:
        num_questions = min(len(sample_questions), 5)
        filtered_questions = sample_questions
    return random.sample(filtered_questions, num_questions)

# Authentication Logic
if not st.session_state.authenticated:
    st.title("Welcome to Snowflake Cortex AI")
    st.markdown("Please login to interact with your data")
    st.session_state.username = st.text_input("Enter Snowflake Username:", value=st.session_state.username)
    st.session_state.password = st.text_input("Enter Password:", type="password", value=st.session_state.password)
    if st.button("Login"):
        try:
            conn = snowflake.connector.connect(
                user=st.session_state.username,
                password=st.session_state.password,
                account="HLGSIYM-COB42429",
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

    # Initialize service metadata before sidebar widgets
    init_service_metadata(session)

    if st.session_state.rerun_trigger:
        st.session_state.rerun_trigger = False
        st.rerun()

    # Run Snowflake Query
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

    # Query Classification Functions
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
            r'\b(what|which|how)\b.*\b(questions|type of questions|queries|information|data|insights)\b.*\b(ask|can i ask|pose|generate|available)\b',
            r'\b(give me|show me|list)\b.*\b(questions|examples|sample questions)\b'
        ]
        return any(re.search(pattern, query.lower()) for pattern in suggestion_patterns)

    def is_greeting_query(query: str):
        greeting_patterns = [
            r'^\b(hello|hi|hey|greet)\b$',
            r'^\b(hello|hi|hey,greet)\b\s.*$'
        ]
        return any(re.search(pattern, query.lower()) for pattern in greeting_patterns)

    # Cortex Complete Function
    def complete(model, prompt):
        try:
            prompt = prompt.replace("'", "\\'")
            query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{prompt}') AS response"
            result = session.sql(query).collect()
            return result[0]["RESPONSE"]
        except Exception as e:
            st.error(f"❌ COMPLETE Function Error: {str(e)}")
            return None

    # Summarize Function
    def summarize(text):
        try:
            text = text.replace("'", "\\'")
            query = f"SELECT SNOWFLAKE.CORTEX.SUMMARIZE('{text}') AS summary"
            result = session.sql(query).collect()
            return result[0]["SUMMARY"]
        except Exception as e:
            st.error(f"❌ SUMMARIZE Function Error: {str(e)}")
            return None

    # Parse SSE Response
    def parse_sse_response(response_text: str) -> List[Dict]:
        if not response_text or not isinstance(response_text, str):
            st.error(f"❌ Invalid response_text: {response_text}")
            return []
        events = []
        lines = response_text.strip().split("\n")
        current_event = {}
        for line in lines:
            if st.session_state.debug_mode:
                st.sidebar.write(f"Parsing line: {line}")
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

    # Process SSE Response
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

    # Snowflake API Call
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
                st.sidebar.write(f"API Response Status: {resp.status_code}")
                st.sidebar.write(f"API Raw Response: {resp.text}")
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

    # Summarize Unstructured Answer
    def summarize_unstructured_answer(answer):
        sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|")\s', answer)
        return "\n".join(f"• {sent.strip()}" for sent in sentences[:6])

    # Suggest Sample Questions
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

    # Display Chart Function
    def display_chart_tab(df: pd.DataFrame, prefix: str = "chart", query: str = ""):
        try:
            if df is None or df.empty or len(df.columns) < 2:
                st.error.header("Invalid or insufficient data for visualization.")
                if st.session_state.debug:
                    st.error(f"Chart Data Issue: df={df}, columns={df.columns if df is not None else 'None'}")
                return
            query_lower = query.lower()
            if re.search(r'\b(county|county|jurisdiction)\b', query_lower):
                default_data = "Pie Chart"
            elif re.search(r'\b(month|year|date)\b', query_lower):
                default_data = "Line Chart"
            else:
                default_data = "Bar Chart"
            all_cols = list(df.columns)
            col1, col2, col3 = st.columns(3)
            default_x = st.session_state.get(f"{prefix}_x", all_cols[0])
            try:
                x_index = all_cols.index(default_x)
            except ValueError:
                x_index = 0
            x_col = col1.selectbox("X axis", all_cols, index=x_index, key=f"{prefix}_x")
            remaining_cols = [c for c in all_cols if c != x_col]
            default_y = st.session_state.get(f"{prefix}_y", remaining_cols[0] if remaining_cols else all_cols[0])
            try:
                y_index = remaining_cols.index(default_y)
            except ValueError:
                y_index = 0
            y_col = col2.selectbox("Y axis", remaining_cols, index=y_index, key=f"{prefix}_y")
            chart_options = ["Line Chart", "Bar Chart", "Pie Chart", "Scatter Chart", "Histogram Chart"]
            chart_type = col3.selectbox("Chart Type", chart_options, index=chart_options.index(default_data), key=f"{prefix}_type")
            if st.session_state.debug:
                st.sidebar.write(f"Chart Config: X={x_col}, Y={y_col}, Type={chart_type}")
            if chart_type == "Line Chart":
                fig = px.line(df, x=x_col, y=y_col, title=chart_type)
                st.plotly_chart(fig, key=f"Line")
            elif chart_type == "Bar Chart":
                fig = px.bar(df, x=x_col, y=y_col, title=chart_type)
                st.plotly_chart(fig, key=f"Bar")
            elif chart_type == "Pie Chart":
                fig = px.pie(df, names=x_col, values=y_col, title=chart_type)
                st.plotly_chart(fig, key=f"Pie")
            elif chart_type == "Scatter Chart":
                fig = px.scatter(df, x=x_col, y_col=y_col, title=chart_type)
                st.plotly_chart(fig, key=f"Scatter")
            elif chart_type == "Histogram Chart":
                fig = px.histogram(df, x=x_col, title=chart_type)
                st.plotly_chart(fig, key=f"Hist")
        except Exception as e:
            st.error(f"⚠️ Error generating chart: {str(e)}")
            if st.session_state.debug_mode:
                st.sidebar.error(f"Chart Error: {str(e)}")

    # Sidebar UI
    with st.sidebar:
        st.markdown("
        """
<style>
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
</style>
        """, unsafe_allow_html=True)
        logo_container = st.container()
        button_container = st.container()
        about_container = st.container()
        help_container = st.container()
        with logo_container:
            logo_url = "https://www.snowflake.com/wp-content/themes/snowflake/assets/img/logo-blue.svg"
            st.image(logo_url, width=250)
        with button_container:
            init_config_options()
            st.radio("Select Data Source:", ["Database", "Document"], key="data_source")
            st.selectbox(
                "Select Cortex Search Service:",
                [CORTEX_SEARCH_SERVICES],
                key="selected_cortex_search_service"
            )
            st.toggle("Debug", key="debug_mode", value=st.session_state.debug_mode)
            if st.session_state.debug_mode:
                st.expander("Session State").write(st.session_state)
            st.subheader("Sample Questions")
            sample_questions = [
                "What is Property Management?",
                "Total number of properties currently occupied?",
                "What is the number of properties by occupancy status?",
                "What is the number of properties currently leased?",
                "What are the supplier payments compared to customer billing by month?",
                "What is the total number of suppliers?",
                "What is the average amount of supplier payment per property?",
                "What are the details of lease execution, commencement, and termination?",
                "What are the customer billing and supplier payment details by location and purpose?",
                "What is the budget recovery by billing purpose?",
                "What are the details of customer billing?",
                "What are the details of supplier payments?"
            ]
            for sample in sample_questions:
                if st.button(sample, key=f"sample_{sample}"):
                    st.session_state.query = sample
                    st.session_state.show_greeting = False
        with about_container:
            st.markdown("### About")
            st.markdown(
                "This application uses **Snowflake Cortex Analyst** to interpret "
                "your natural language questions and generate data insights. "
                Simply ask a question below to see relevant answers and visualizations."
            )
        with help_container:
            st.markdown("### Help & Documentation")
            st.markdown(
                "- [User Guide](https://docs.snowflake.com/en/guides-overview-ai-features)  \n"
                "- [Snowflake Cortex Analyst Docs](https://docs.snowflake.com/)  \n"
                "- [Contact Support](https://support.snowflake.com/)  \n"
            )
        # Maintenance Request Submission
        with st.expander("Submit Maintenance Request"):
            st.markdown("### Submit a Maintenance Request")
            property_id = st.text_input("Supplier ID", key="maint_property_id")
            tenant_name = st.text_input("Supplier Name", key="maint_tenant_name")
            issue_description = st.text_area("Purchase Order Description", key="maint_issue_description")
            if st.button("Submit Request", key="submit_maintenance_request"):
                if not property_id or not tenant_name or not issue_description:
                    st.error("Error: Not all fields filled out. Please fill in all fields to submit a maintenance request.")
                else:
                    success, message = submit_maintenance_request(session, property_id, tenant_name, issue_description)
                    if success:
                        st.success(message)
                    else:
                        st.error(message)

    # Main UI
    st.markdown(
        f'<img src="https://raw.githubusercontent.com/nkumbala129/30-sup-sup/05/2025/main/DiLytics_logo.png" id="dilytics-logo">',
        unsafe_allow_html=True
    )
    semantic_model_filename = SEMANTIC_MODEL.split("/")[-1]
    st.markdown(
        """
        <div class="fixed-header">
            <h1>Cortex AI-Powered Property Management Assistant by DiLytics</h1>
            <p><strong>Welcome to property management.</strong> I am here to help with DiLytics Property Management Insights Solutions</p>
        </div>
    """
        ,
        unsafe_allow_html=True
    )

    if st.session_state.show_greeting and not st.session_state.chat_history:
        st.markdown("Welcome! I’m the Snowflake AI Assistant, ready to assist you with Property management. Property management is all about keeping your properties in tip-top shape—leasing, tenant screening, property payment collection, and maintenance, with transparency and efficiency. 🏠 Returns about your rent, supplier, or maintenance request to get started!")
    else:
        st.session_state.show_greeting = False

    for message in st.session_state.messages:
        content = message.get("content", "")
        with st.chat_message(message["role"])):
            st.markdown(content, unsafe_allow_html=True)
        if message["role"] == "assistant" and "sql" in message and message["sql"]:
            with st.expander("View SQL Query", expanded=False):
                st.code(message["sql"], language="sql")
            st.markdown(f"**Query Results** ({len(message['results'])} rows):")
            st.dataframe(message['results'])
            if message["results"] is not message["results"].empty and len(message["results"].columns) >= 2:
                st.markdown("### Visualization:")
                display_chart(st.session_state.messages["results"], prefix=f"query_{hash(str(message["content"])}", query=message.get("query", ""))
        if (message["role"] == "assistant" and "follow_up" in message and
            not is_greeting(message.get("query", "")) and
            not is_generic_query(message.get("query", "")) and 
            not is_question_suggestion["query"](message.get("query", "")))):
            st.markdown("### Follow-Up Questions:")
            for i, q in enumerate(message["follow_up"]):
                if st.button(q["question"], key=f"follow_up_{hash(str(message["content"])}_{i}", help=f"Ask: {q['question']}}"):
                    st.session_state.query = q["question"]
                    st.session_state.show_greeting = False

    chat_input_query = st.chat_input("What would you like to explore?")
    if chat_input_query:
        st.session_state.query = chat_input_query

    if st.session_state.query:
        query = st.query
        if query.lower().startswith("no of"):
            query = query.replace("no of", "number of", 1)
        st.session_state.show_greeting = False
        st.session_state.chart_x = None
        st.session_state.chart_y = None
        st.session_state.chart_type = "Bar Chart"
        original_query = query
        if query.strip().isdigit() and st.session_state.last_suggestions:
            try:
                index = int(query.strip()) - 1
                if 0 <= index <= len(st.session_state.last_suggestions):
                    query = st.session_state.last_suggestions[index]
                    else:
                        query = original_query
                    except ValueError:
                        query = original_query

                is_follow = False
        follow_up_patterns = [
            r'^\bby\b\s+\w+$',
            r'^\bgroup by\s+\w+$'
        ]
        if any(re.search(pattern, query.lower()) for pattern in follow_up_patterns) and st.session_state.previous_query:
            is_follow = True

        combined_query = query
        if st.session_state.use_chat_history:
            chat_history = get_chat_history()
            if chat_history and is_follow:
                combined_query = make_chat_history_summary(chat_history, query)
                if st.session_state.debug_mode:
                    st.session_state.text_area("Combined Query", expanded_query, height=100)

        st.session_state.chat_history.append({"role": "user", "content": original_query})
        st.session_state.messages.append({"role": "user", "content": original_query})
        with st.chat_message("user"):
            st.markdown(original_query)
        with st.chat_message("assistant"):
            with st.spinner("Processing..."):
                response_placeholder = st.empty()
                if st.session_state.data_source not in ["Database", "Document"]:
                    st.session_state.data_source = "Database"
                    is_structured = is_structured_query(combined_query)
                    is_complete = is_complete_query(combined_query)
                    is_summarize = is_summarize_query(combined_query)
                    is_suggestion = is_question_suggestion_query(combined_query)
                    is_greeting = is_greeting_query(combined_query)
                    if st.session_state.debug_mode:
                        st.session_state.debug({"is_structured_query": is_structured, "is_complete": is_complete, "is_summarize": is_summarize, "is_suggestion": is_suggestion, "is_greeting": is_greeting})
                    assistant_response = {"role": "assistant", "content": {"query": ""}}
                    response_content = None
                    failed_response = False

                    if is_greeting or is_generic or is_suggestion:
                        greeting = combined_query.lower().split()[0] if is_greeting else ""
                        if greeting not in ["hi", "hello", "hey", "greetings"]:
                            greeting = "hello"
                        response_content = (
                            f"{greeting.capitalize()}!\n\n"
                            "I’m here to assist with your Property Management questions. Here are some questions you might want to ask:\n\n"
                            )
                        suggestions = get_questions(combined_query)
                        for j, suggestion in enumerate(suggestions, 1):
                            response_content += f"{j}. {suggestion}\n"
                        response_content += "\nFeel free to ask any of these or come up with your own property management questions!"
                        with response_placeholder(response_content):
                            st.write_stream(stream(response_content))
                            st.markdown(response_content, unsafe_content=True)
                        assistant_response["content"] = response_content
                        st.session_state.last_suggestions = suggestions
                        st.session_state.messages.append({"role": "assistant", "content": {"query": suggestion}})

                    elif is_complete:
                        content = make_prompt(combined_query)
                        if content:
                            response_content = f"\n**Generated Response:**\n\n{content}"
                            with response_content(response_content, unsafe_allow_html=True):
                                st.write_stream(stream(response_content))
                                st.markdown(response_content)
                            assistant_response["content"] = response_content
                            st.session_state.messages.append({"role": "assistant", "content": content})
                        else:
                            response_content = "⚠️ Unable to generate response."
                            failed_response = True
                            with response_content(response_content, unsafe_content_html=True):
                                st.write_stream(stream(response_content))
                                st.markdown(response_content)
                            assistant_response["content"] = response_content
                            st.session_state.messages.append({"role": "assistant", "content": content})

                    elif is_summerize:
                        summary = get_summary(content)
                        if summary:
                            response_content = f"\n\n**Summary:**\n{summary}"
                            with response_content(response_content, unsafe_content=True):
                                st.write_stream(stream(response_content))
                                st.markdown(response_content)
                            assistant_response.append["content"] = response_content
                            st.session_state.messages.append({"role": "assistant", "content": content})
                        else:
                            response_content = "⚠️ Unable to generate summary."
                            failed_response = True
                            with response_content(response_content, unsafe_content_html=True):
                                st.write_stream(stream(response_content))
                                st.markdown(response_content)
                            assistant_response["content"] = response_content
                            st.session_state.messages.append({"role": "assistant", "content": content})

                    elif is_structured:
                        content = snowflake_api_call(combined_query, {"is_structured": True})
                        sql_content, _, _ = parse_sse(content, is_s_structured=True)
                        if sql:
                            if st.session_state.debug_mode:
                                st.session_state.json({"sql": {"query": sql_content}})
                            results = run_snowflake_query(sql_content)
                            if results is not None and not results.empty:
                                results_text = results.to_string(index=False))
                                    prompt = f"""
                                    Provide a natural language response for query '{combined_query}' using the data:\n{results_text}"
                                    """
                                    summary = generate_summary(content, prompt)
                                    if not summary:
                                        summary = "⚠️ Unable to generate summary."
                                    response_content = summary
                                    with response_content(response_content, unsafe_content=True):
                                        st.write_stream(stream(response_content))
                                    with st.expander("View SQL:", expanded):
                                            False):
                                        st.code(sql_content, language="sql")
                                    st.markdown(f"**{'Query results': {len(results)} rows}:**")
                                    st.dataframe(results)
                                    if len(results) >= 2:
                                        st.markdown("**📈:**")
                                        display_chart(results, prefix=f"results_{hash(combined_query)}"", query=combined_query")
                                    assistant_response["content"] = response_content
                                    assistant_response["sql"] = {"query": sql_content}
                                    assistant_response["results"] = results
                                    assistant_response["summary"] = summary
                                    st.session_state.messages.append({"content": response_content, "sql": sql_content, "results": results, "summary": summary})
                                else:
                                    response_content = "⚠️ No results found."
                                    failed_response = True
                                    with response_content(response_content, unsafe_content=True):
                                        st.write_stream(stream(response_content))
                                        st.markdown(response_content)
                                    assistant_response["content"] = response_content
                                    st.session_state.messages.append({"role": "assistant", "content": content})

                            else:
                                response_content = "⚠️ Failed to generate SQL."
                                failed_response = True
                                with response_content(response_content, unsafe_content=True):
                                    st.write_stream(stream(response_content))
                                    st.markdown(response_content)
                                assistant_response["content"] = response_content
                                st.session_state.messages.append({"role": "assistant", "content": content})

                    else:
                        content = snowflake_content_call(combined_query, {"is_structured": False})
                        _, _, search_results = content
                        if search_results:
                            raw_result = search_results[0]["result"]
                            summary = generate_prompt(summarycombined_query)
                            if summary:
                                response_content = f"\n{summary}"
                            else:
                                    response_content = f"\n**Unsummarized Response:**\n{summarize_raw(raw_result)}"
                            with response_content(response_content, unsafe_content=True):
                                st.write_stream(stream(response_content))
                            response_content(response_content)
                            st.session_state.messages.append({"                                role="assistant":", "content": response_content})
                            else:
                                response_content = "❌ No results."
                                failed_response = True
                                with response_content(response_content, unsafe_content=True):
                                    st.write_stream(stream(response_content))
                                    st.markdown(response_content)
                                assistant_response["content"] = response_content
                                st.session_state.messages.append({"role": "assistant", content="content": response_content})

                        if failed_response:
                            suggestions = get_sample_questions(suggestionscombined_query)
                            response_content = "\n⚠️ I’m not sure what you meant. Here are some suggestions:\n\n"
                            for j, suggestion in enumerate(suggestions, 1):
                                response_content += f"{j}. {suggestion}\n"
                            response_content += "\nTry one of these or rephrase your question."
                            with response_content(response_content, unsafe_content=True):
                                st.write_stream(stream(response_content))
                                st.markdown(response_content)
                            assistant_response["content"] = response_content
                            st.session_state.last_suggestions = suggestions
                            st.session_state.messages.append({"role": "assistant", "content": response})

                            st.session_state["content"] = response_content
                            if not is_greeting or not is_generic or not is_suggestion:
                                suggestions = get_followup_questions(combined_query)
                                st.session_state["followup_questions"] = suggestions

                            st.session_state.chat_history.append({"role": "assistant", "content": response_content})
                            st.session_state.current_query = combined_query
                            st.session_state.current_results = response_content.get("results")
                            st.session_state.current_sql = response_content["sql"]
                            st.session_state.current_summary = response_content.get("summary")
                            st.session_state.previous_query = combined_query
                            st.session_state.previous_sql = response_content["sql"]
                            st.session_state.previous_results = response_content.get("results")
                            st.session_state.query = None

                            if st.session_state.query == None:
                                st.markdown("**Follow-Up Questions:**")
                                for i, suggestion in enumerate(suggestions):
                                    if st.button(suggestion["question"], key=f"question_{i}", help=f"Ask: {suggestion['question']}"):                                        st.session_state.query = suggestion
                                    st.session_state.show_greeting = ""
