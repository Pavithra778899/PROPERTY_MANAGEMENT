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
import timeimport streamlit as st
import json
import re
import requests
from snowflake.snowpark import Session
from snowflake.core import Root
import pandas as pd
import plotly.express as px
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple
from tenacity import retry, stop_after_attempt, wait_exponential
import os

# --- Snowflake/Cortex Configuration ---
HOST = os.getenv("SNOWFLAKE_HOST", "QNWFESR-LKB66742.snowflakecomputing.com")
DATABASE = "AI"
SCHEMA = "DWH_MART"
API_ENDPOINT = "/api/v2/cortex/agent:run"
API_TIMEOUT = 50  # Seconds
CORTEX_SEARCH_SERVICES = None  # Set dynamically
SEMANTIC_MODEL = '@"AI"."DWH_MART"."PROPERTY_MANAGEMENT"/property_management (1).yaml'

# --- Model Options ---
MODELS = [
    "mistral-large",
    "snowflake-arctic",
    "llama3-70b",
    "llama3-8b",
]

# --- Streamlit Page Config ---
st.set_page_config(
    page_title="Cortex AI-Property Management Assistant",
    layout="wide",
    initial_sidebar_state="auto"
)

# --- Session State Initialization ---
def init_session_state():
    defaults = {
        "authenticated": False,
        "username": "",
        "password": "",
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
        "show_greeting": True,
        "show_about": False,
        "show_help": False,
        "show_history": False,
        "query": None,
        "previous_query": None,
        "previous_sql": None,
        "previous_results": None,
        "show_sample_questions": False,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

init_session_state()

# --- CSS Styling ---
st.markdown("""
<style>
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
}
.stApp {
    padding-top: 100px;
}
[data-testid="stSidebar"] [data-testid="stButton"] > button {
    background-color: #29B5E8 !important;
    color: white !important;
    font-weight: bold !important;
    width: 100% !important;
    border-radius: 5px !important;
    margin: 5px 0 !important;
}
</style>
""", unsafe_allow_html=True)

# --- Add Logo ---
if st.session_state.authenticated:
    st.markdown(
        '<img src="https://raw.githubusercontent.com/nkumbala129/30-05-2025/main/Dilytics_logo.png" class="dilytics-logo">',
        unsafe_allow_html=True
    )

# --- Utility Functions ---
def stream_text(text: str, chunk_size: int = 10, delay: float = 0.02):
    for i in range(0, len(text), chunk_size):
        yield text[i:i + chunk_size]
        time.sleep(delay)

def submit_maintenance_request(property_id: str, tenant_name: str, issue_description: str):
    try:
        request_id = str(uuid.uuid4())
        data = pd.DataFrame([{
            "REQUEST_ID": request_id,
            "PROPERTY_ID": property_id,
            "TENANT_NAME": tenant_name,
            "ISSUE_DESCRIPTION": issue_description,
            "SUBMITTED_AT": pd.Timestamp.now(),
            "STATUS": "PENDING"
        }])
        session.write_dataframe(data).to_table("MAINTENANCE_REQUESTS")
        return True, f"📝 Maintenance request submitted successfully! Request ID: {request_id}"
    except Exception as e:
        return False, f"❌ Failed to submit maintenance request: {str(e)}"

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
    st.session_state.show_greeting = True
    st.session_state.query = None
    st.session_state.show_history = False
    st.session_state.previous_query = None
    st.session_state.previous_sql = None
    st.session_state.previous_results = None
    st.switch_page(st.__file__)

def init_service_metadata():
    global CORTEX_SEARCH_SERVICES
    try:
        services = session.sql('SHOW CORTEX SEARCH SERVICES IN SCHEMA "AI"."DWH_MART";').collect()
        if not services:
            st.error("❌ No Cortex Search Services found in AI.DWH_MART.")
            return
        service_name = None
        for svc in services:
            svc_name = svc["name"]
            full_name = f'"AI"."DWH_MART"."{svc_name}"'
            desc_result = session.sql(f'DESC CORTEX SEARCH SERVICE {full_name};').collect()
            if desc_result:
                service_name = full_name
                break
        if not service_name:
            st.error("❌ No valid Cortex Search Service found.")
            return
        CORTEX_SEARCH_SERVICES = service_name
        desc_result = session.sql(f'DESC CORTEX SEARCH SERVICE {service_name};').collect()
        svc_search_col = desc_result[0]["search_column"] if desc_result else ""
        st.session_state.service_metadata = [{"name": service_name, "search_column": svc_search_col}]
        st.session_state.selected_cortex_search_service = service_name
    except Exception as e:
        st.error(f"❌ Failed to initialize Cortex Search Service: {str(e)}")

def query_cortex_search_service(query: str) -> str:
    try:
        if not st.session_state.selected_cortex_search_service:
            raise ValueError("No Cortex Search Service selected.")
        root = Root(session)
        service_name = st.session_state.selected_cortex_search_service.split('.')[-1].strip('"')
        cortex_search_service = root.databases[DATABASE].schemas[SCHEMA].cortex_search_services[service_name]
        desc_result = session.sql(f'DESC CORTEX SEARCH SERVICE {st.session_state.selected_cortex_search_service};').collect()
        if not desc_result:
            raise ValueError(f"Cortex Search Service {st.session_state.selected_cortex_search_service} does not exist.")
        columns = [row["search_column"] for row in desc_result]
        if not columns:
            raise ValueError("No search columns defined for Cortex Search Service.")
        context_documents = cortex_search_service.search(
            query, columns=columns, limit=st.session_state.num_retrieved_chunks
        )
        results = context_documents.results
        search_col = st.session_state.service_metadata[0]["search_column"] or columns[0]
        context_str = "\n".join(f"Context document {i+1}: {r.get(search_col, '')}" for i, r in enumerate(results))
        return context_str
    except Exception as e:
        st.error(f"❌ Error querying Cortex Search Service: {str(e)}")
        return ""

def get_chat_history():
    start_index = max(0, len(st.session_state.chat_history) - st.session_state.num_chat_messages)
    return st.session_state.chat_history[start_index:len(st.session_state.chat_history) - 1]

def make_chat_history_summary(chat_history: List[Dict], question: str) -> str:
    chat_history_str = "\n".join([f"{msg['role']}: {msg['content']}" for msg in chat_history])
    prompt = f"""
[INST]
You are an AI assistant specializing in property management queries. Rewrite the latest user question into a clear, standalone query by incorporating relevant context from the chat history. Ensure the rewritten query is precise, complete, and reflects the user's intent.

Examples:
- Chat history: 
  user: What is the total number of leases?
  assistant: There are 150 leases.
  user: by state
  Rewritten query: What is the total number of leases by state?
- Chat history:
  user: List properties with high occupancy.
  assistant: Properties with occupancy above 90% are listed.
  user: for last year
  Rewritten query: List properties with high occupancy for last year.

<chat_history>
{chat_history_str}
</chat_history>
<question>
{question}
</question>

Output only the rewritten standalone query.
[/INST]
"""
    summary = complete(st.session_state.model_name, prompt.strip())
    if st.session_state.debug_mode:
        st.sidebar.text_area("Resolved Question", summary.replace("$", "\\$"), height=150)
    return summary.strip()

def create_prompt(user_question: str) -> str:
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
    
    if not prompt_context.strip():
        return complete(st.session_state.model_name, user_question)
    
    prompt = f"""
[INST]
You are a property management AI assistant. Provide a concise, accurate answer to the user's question using the provided context and chat history.

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

def get_user_questions(limit: int = 10) -> List[str]:
    return [msg["content"] for msg in st.session_state.chat_history if msg["role"] == "user"][-limit:][::-1]

# --- Main Application Logic ---
if not st.session_state.authenticated:
    st.title("Welcome to Snowflake Cortex AI")
    st.markdown("Please login to interact with your data")
    st.session_state.username = st.text_input("Enter Snowflake Username:", value=st.session_state.username)
    st.session_state.password = st.text_input("Enter Password:", type="password")
    if st.button("Login"):
        try:
            session = Session.builder.configs({
                "account": HOST.split('.')[0],
                "user": st.session_state.username,
                "password": st.session_state.password,
                "host": HOST,
                "port": 443,
                "warehouse": "COMPUTE_WH",
                "role": "ACCOUNTADMIN",
                "database": DATABASE,
                "schema": SCHEMA,
            }).create()
            session.sql("ALTER SESSION SET TIMEZONE = 'UTC'").collect()
            session.sql("ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE").collect()
            st.session_state.snowpark_session = session
            st.session_state.authenticated = True
            st.success("Authentication successful! Redirecting...")
            st.switch_page(st.__file__)
        except Exception as e:
            st.error(f"Authentication failed: {str(e)}")
else:
    session = st.session_state.snowpark_session
    init_service_metadata()

    def run_snowflake_query(query: str) -> Optional[pd.DataFrame]:
        try:
            df = session.sql(query).to_pandas()
            return df if not df.empty else None
        except Exception as e:
            st.error(f"❌ SQL Execution Error: {str(e)}")
            return None

    def is_structured_query(query: str) -> bool:
        structured_patterns = [
            r'\b(count|number|total|sum|avg|max|min|how many|which|show|list|group by|order by)\b',
            r'\b(property|properties|tenant|tenants|lease|leases|rent|occupancy|maintenance|billing|payment)\b',
            r'\b(by state|by city|by property|by year|by month)\b'
        ]
        return any(re.search(pattern, query.lower()) for pattern in structured_patterns)

    def is_complete_query(query: str) -> bool:
        complete_patterns = [r'\b(generate|write|create|describe|explain)\b']
        return any(re.search(pattern, query.lower()) for pattern in complete_patterns)

    def is_summarize_query(query: str) -> bool:
        summarize_patterns = [r'\b(summarize|summary|condense)\b']
        return any(re.search(pattern, query.lower()) for pattern in summarize_patterns)

    def is_question_suggestion_query(query: str) -> bool:
        suggestion_patterns = [
            r'\b(what|which|how)\b.*\b(questions|queries)\b.*\b(ask|can i ask)\b',
            r'\b(give me|show me|list)\b.*\b(questions|examples)\b'
        ]
        return any(re.search(pattern, query.lower()) for pattern in suggestion_patterns)

    def is_greeting_query(query: str) -> bool:
        greeting_patterns = [
            r'^\s*(hi|hello|hey|greetings)\s*$',
            r'\bhow are you\b',
            r'\bwhat’s up\b',
            r'\bgood (morning|afternoon|evening)\b',
            r'\bthank(s| you)\b',
            r'\bwho are you\b',
            r'\bwhat can you do\b',
            r'\bhow can you help\b',
        ]
        return any(re.search(pattern, query.lower()) for pattern in greeting_patterns)

    def complete(model: str, prompt: str) -> Optional[str]:
        try:
            result = session.sql(
                "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?)",
                params=[model, prompt]
            ).collect()
            return result[0]["COMPLETE"]
        except Exception as e:
            st.error(f"❌ COMPLETE Function Error: {str(e)}")
            return None

    def summarize(text: str) -> Optional[str]:
        try:
            result = session.sql(
                "SELECT SNOWFLAKE.CORTEX.SUMMARIZE(?)",
                params=[text]
            ).collect()
            return result[0]["SUMMARIZE"]
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
                        st.error(f"❌ Failed to parse SSE data: {str(e)}")
        return events

    def process_sse_response(response: List[Dict], is_structured: bool) -> Tuple[str, List[str]]:
        sql = ""
        search_results = []
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
        return sql.strip(), search_results

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
    def snowflake_api_call(query: str, is_structured: bool = False) -> List[Dict]:
        payload = {
            "model": st.session_state.model_name,
            "messages": [{"role": "user", "content": [{"type": "text", "text": query}]}],
            "tools": []
        }
        if is_structured:
            payload["tools"].append({"tool_spec": {"type": "cortex_analyst_text_to_sql", "name": "analyst1"}})
            payload["tool_resources"] = {"analyst1": {"semantic_model_file": SEMANTIC_MODEL}}
        else:
            if not st.session_state.selected_cortex_search_service:
                raise ValueError("No Cortex Search Service configured.")
            payload["tools"].append({"tool_spec": {"type": "cortex_search", "name": "search1"}})
            payload["tool_resources"] = {"search1": {"name": st.session_state.selected_cortex_search_service, "max_results": st.session_state.num_retrieved_chunks}}
        try:
            resp = requests.post(
                url=f"https://{HOST}{API_ENDPOINT}",
                json=payload,
                headers={
                    "Authorization": f'Snowflake Token="{session.get_session_token()}"',
                    "Content-Type": "application/json",
                },
                timeout=API_TIMEOUT
            )
            resp.raise_for_status()
            if not resp.text.strip():
                raise ValueError("API returned an empty response.")
            return parse_sse_response(resp.text)
        except Exception as e:
            st.error(f"❌ API Request Error: {str(e)}")
            raise

    def summarize_unstructured_answer(answer: str) -> str:
        sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|")\s', answer)
        return "\n".join(f"• {sent.strip()}" for sent in sentences[:6] if sent.strip())

    def suggest_sample_questions(query: str) -> List[str]:
        try:
            prompt = (
                f"The user asked: '{query}'. Generate 3-5 clear, concise sample questions related to properties, leases, tenants, rent, or occupancy metrics. "
                f"Format as a numbered list."
            )
            response = complete(st.session_state.model_name, prompt)
            if response:
                questions = [re.sub(r'^\d+\.\s*', '', line.strip()) for line in response.split("\n") if re.match(r'^\d+\.\s*.+', line)]
                return questions[:5]
            return [
                "What are the total number of active leases?",
                "What is the average rent collected per tenant?",
                "Total number of properties?",
                "What’s the total rental income by property?",
                "Which tenants have pending rent payments?"
            ]
        except Exception:
            return [
                "What are the total number of active leases?",
                "What is the average rent collected per tenant?",
                "Total number of properties?",
                "What’s the total rental income by property?",
                "Which tenants have pending rent payments?"
            ]

    def display_chart_tab(df: pd.DataFrame, prefix: str = "chart", query: str = ""):
        try:
            if df is None or df.empty or len(df.columns) < 2:
                st.warning("No valid data available for visualization.")
                return
            query_lower = query.lower()
            chart_type = (
                "Pie Chart" if re.search(r'\b(county|jurisdiction)\b', query_lower) else
                "Line Chart" if re.search(r'\b(month|year|date)\b', query_lower) else
                "Bar Chart"
            )
            all_cols = list(df.columns)
            col1, col2, col3 = st.columns(3)
            x_col = col1.selectbox("X axis", all_cols, index=0, key=f"{prefix}_x")
            remaining_cols = [c for c in all_cols if c != x_col]
            y_col = col2.selectbox("Y axis", remaining_cols, index=0, key=f"{prefix}_y")
            chart_options = ["Line Chart", "Bar Chart", "Pie Chart", "Scatter Chart", "Histogram Chart"]
            chart_type = col3.selectbox("Chart Type", chart_options, index=chart_options.index(chart_type), key=f"{prefix}_type")
            if df[x_col].nunique() < 1 or df[y_col].empty:
                st.warning("Insufficient or invalid data for selected axes.")
                return
            if chart_type != "Histogram Chart" and not pd.api.types.is_numeric_dtype(df[y_col]):
                st.warning("Y-axis must be numeric for this chart type.")
                return
            st.markdown(f"### 📊 {chart_type}")
            fig = {
                "Line Chart": px.line,
                "Bar Chart": px.bar,
                "Pie Chart": px.pie,
                "Scatter Chart": px.scatter,
                "Histogram Chart": px.histogram
            }[chart_type](df, x=x_col, y=y_col if chart_type != "Histogram Chart" else None, names=x_col if chart_type == "Pie Chart" else None, values=y_col if chart_type == "Pie Chart" else None, title=chart_type)
            st.plotly_chart(fig, key=f"{prefix}_{chart_type.lower().replace(' ', '_')}")
        except Exception as e:
            st.error(f"❌ Error generating chart: {str(e)}")

    def toggle_about():
        st.session_state.show_about = not st.session_state.show_about
        st.session_state.show_help = False
        st.session_state.show_history = False

    def toggle_help():
        st.session_state.show_help = not st.session_state.show_help
        st.session_state.show_about = False
        st.session_state.show_history = False

    def toggle_history():
        st.session_state.show_history = not st.session_state.show_history
        st.session_state.show_about = False
        st.session_state.show_help = False

    # --- Sidebar ---
    with st.sidebar:
        st.image("https://www.snowflake.com/wp-content/themes/snowflake/assets/img/logo-blue.svg", width=250)
        if st.button("Clear conversation"):
            start_new_conversation()
        if CORTEX_SEARCH_SERVICES:
            st.selectbox(
                "Select Cortex Search Service:",
                [CORTEX_SEARCH_SERVICES],
                index=0,
                key="selected_cortex_search_service"
            )
        else:
            st.warning("⚠️ No Cortex Search Service available.")
        st.toggle("Debug", key="debug_mode")
        with st.expander("Advanced options"):
            st.selectbox("Select model:", MODELS, key="model_name")
            st.number_input(
                "Number of context chunks",
                value=100,
                min_value=1,
                max_value=400,
                key="num_retrieved_chunks"
            )
            st.number_input(
                "Number of chat history messages",
                value=10,
                min_value=1,
                max_value=100,
                key="num_chat_messages"
            )
        if st.button("Sample Questions"):
            st.session_state.show_sample_questions = not st.session_state.show_sample_questions
        if st.session_state.show_sample_questions:
            st.markdown("### Sample Questions")
            sample_questions = [
                "What is Property Management?",
                "Total number of properties currently occupied?",
                "What is the number of properties by occupancy status?",
                "What is the number of properties currently leased?",
                "What is the average rent collected per tenant?",
                "Total number of properties?",
                "What’s the total rental income by property?",
                "Which tenants have pending rent payments?"
            ]
            for sample in sample_questions:
                if st.button(sample, key=f"sidebar_{sample}"):
                    st.session_state.query = sample
                    st.session_state.show_greeting = False
        with st.expander("Submit Maintenance Request"):
            property_id = st.text_input("Property ID")
            tenant_name = st.text_input("Tenant Name")
            issue_description = st.text_area("Issue Description")
            if st.button("Submit Request"):
                if not all([property_id, tenant_name, issue_description]):
                    st.error("❌ Please fill in all fields.")
                else:
                    success, message = submit_maintenance_request(property_id, tenant_name, issue_description)
                    if success:
                        st.success(message)
                    else:
                        st.error(message)
        st.markdown("---")
        if st.button("History"):
            toggle_history()
        if st.session_state.show_history:
            st.markdown("### Recent Questions")
            user_questions = get_user_questions()
            if not user_questions:
                st.write("No questions in history yet.")
            else:
                for idx, question in enumerate(user_questions):
                    if st.button(question, key=f"history_{idx}"):
                        st.session_state.query = question
                        st.session_state.show_greeting = False
        if st.button("About"):
            toggle_about()
        if st.session_state.show_about:
            st.markdown("### About")
            st.write("This application uses Snowflake Cortex Analyst to provide property management insights.")
        if st.button("Help & Documentation"):
            toggle_help()
        if st.session_state.show_help:
            st.markdown("### Help & Documentation")
            st.write(
                "- [User Guide](https://docs.snowflake.com/en/guides-overview-ai-features)\n"
                "- [Snowflake Cortex Analyst Docs](https://docs.snowflake.com/)\n"
                "- [Contact Support](https://www.snowflake.com/en/support/)"
            )

    # --- Main UI ---
    st.markdown(
        """
        <div class="fixed-header">
            <h1 style='font-size: 30px; color: #29B5E8;'>
                Cortex AI – Property Management Insights by DiLytics
            </h1>
            <p style='font-size: 18px; color: #333;'>
                Welcome to Cortex AI. I am here to help with DiLytics Property Management Insights Solutions.
            </p>
        </div>
        """,
        unsafe_allow_html=True
    )

    semantic_model_filename = SEMANTIC_MODEL.split("/")[-1]
    st.markdown(f"Semantic Model: `{semantic_model_filename}`")

    if st.session_state.show_greeting and not st.session_state.chat_history:
        st.markdown("Welcome! I’m the Snowflake AI Assistant, ready to assist you with property management. Ask about your rent, properties, leases, occupancy, or submit a maintenance request!")
    else:
        st.session_state.show_greeting = False

    for message in st.session_state.chat_history:
        with st.chat_message(message["role"]):
            st.markdown(message["content"], unsafe_allow_html=True)
            if message["role"] == "assistant" and "results" in message and message["results"] is not None:
                with st.expander("View SQL Query"):
                    st.code(message["sql"], language="sql")
                st.markdown(f"**Query Results ({len(message['results'])} rows):**")
                st.dataframe(message["results"])
                if not message["results"].empty and len(message["results"].columns) >= 2:
                    st.markdown("**📈 Visualization:**")
                    display_chart_tab(message["results"], prefix=f"chart_{hash(message['content'])}", query=message.get("query", ""))

    if query := st.chat_input("Ask your question..."):
        st.session_state.query = query

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
            except ValueError:
                pass
        is_follow_up = any(re.search(pattern, query.lower()) for pattern in [r'^\bby\b\s+\w+$', r'^\bgroup by\b\s+\w+$']) and st.session_state.previous_query
        combined_query = make_chat_history_summary(get_chat_history(), query) if is_follow_up and st.session_state.use_chat_history else query
        st.session_state.chat_history.append({"role": "user", "content": original_query})
        st.session_state.messages.append({"role": "user", "content": original_query})
        with st.chat_message("user"):
            st.markdown(original_query)
        with st.chat_message("assistant"):
            with st.spinner("Generating Response..."):
                response_placeholder = st.empty()
                is_structured = is_structured_query(combined_query)
                is_complete = is_complete_query(combined_query)
                is_summarize = is_summarize_query(combined_query)
                is_suggestion = is_question_suggestion_query(combined_query)
                is_greeting = is_greeting_query(combined_query)
                assistant_response = {"role": "assistant", "content": "", "query": combined_query}

                if is_greeting or is_suggestion:
                    response_content = (
                        "Hello! Welcome to the Property Management AI Assistant!\n"
                        "Here are some questions you can try:\n"
                    )
                    suggestions = [
                        "Total number of properties currently occupied?",
                        "What is the average rent collected per tenant?",
                        "Total number of properties?",
                        "What’s the total rental income by property?"
                    ]
                    for i, suggestion in enumerate(suggestions, 1):
                        response_content += f"{i}. {suggestion}\n"
                    with response_placeholder:
                        for chunk in stream_text(response_content):
                            response_placeholder.markdown(response_content[:response_content.index(chunk) + len(chunk)], unsafe_allow_html=True)
                    assistant_response["content"] = response_content
                    st.session_state.last_suggestions = suggestions
                    st.session_state.messages.append({"role": "assistant", "content": response_content})

                elif is_complete:
                    response = create_prompt(combined_query)
                    if response:
                        response_content = f"**✍️ Generated Response:**\n{response}"
                        with response_placeholder:
                            for chunk in stream_text(response_content):
                                response_placeholder.markdown(response_content[:response_content.index(chunk) + len(chunk)], unsafe_allow_html=True)
                        assistant_response["content"] = response_content
                        st.session_state.messages.append({"role": "assistant", "content": response_content})
                    else:
                        response_content = "Could not generate a response."
                        assistant_response["content"] = response_content

                elif is_summarize:
                    summary = summarize(combined_query)
                    if summary:
                        response_content = f"**Summary:**\n{summary}"
                        with response_placeholder:
                            for chunk in stream_text(response_content):
                                response_placeholder.markdown(response_content[:response_content.index(chunk) + len(chunk)], unsafe_allow_html=True)
                        assistant_response["content"] = response_content
                        st.session_state.messages.append({"role": "assistant", "content": response_content})
                    else:
                        response_content = "Could not generate a summary."
                        assistant_response["content"] = response_content

                elif is_structured:
                    response = snowflake_api_call(combined_query, is_structured=True)
                    sql, _ = process_sse_response(response, is_structured=True)
                    if sql:
                        results = run_snowflake_query(sql)
                        if results is not None and not results.empty:
                            results_text = results.to_string(index=False)
                            prompt = f"Provide a concise natural language answer to the query '{combined_query}' using the following data:\n\n{results_text}"
                            summary = complete(st.session_state.model_name, prompt) or "Unable to generate a summary."
                            response_content = f"**✍️ Generated Response:**\n{summary}"
                            with response_placeholder:
                                for chunk in stream_text(response_content):
                                    response_placeholder.markdown(response_content[:response_content.index(chunk) + len(chunk)], unsafe_allow_html=True)
                            with st.expander("View SQL Query"):
                                st.code(sql, language="sql")
                            st.markdown(f"**Query Results ({len(results)} rows):**")
                            st.dataframe(results)
                            if len(results.columns) >= 2:
                                st.markdown("**📈 Visualization:**")
                                display_chart_tab(results, prefix=f"chart_{hash(combined_query)}", query=combined_query)
                            assistant_response.update({
                                "content": response_content,
                                "sql": sql,
                                "results": results,
                                "summary": summary
                            })
                            st.session_state.messages.append(assistant_response)
                        else:
                            response_content = "No data returned for the query."
                            assistant_response["content"] = response_content
                    else:
                        response_content = "Failed to generate SQL query."
                        assistant_response["content"] = response_content

                else:
                    response = snowflake_api_call(combined_query, is_structured=False)
                    _, search_results = process_sse_response(response, {})
                    if search_results:
                        response_content = f"**🔍 Generated Response:**\n{summarize_unstructured(search_results[0])}"
                        with response_placeholder:
                            for chunk in stream_text(response_content):
                                response_placeholder.markdown(response_content[:response_content.index(chunk) + len(chunk)], unsafe_allow_html=True)
                        assistant_response["content"] = response_content
                        st.session_state.messages.append({"role": "assistant", "content": response_content})
                    else:
                        response_content = "No search results found."

                if not assistant_response["content"]:
                    suggestions = suggest_sample_questions(combined_query)
                    response_content = "Could not understand your query, please try these suggestions:\n\n"
                    for i, suggestion in enumerate(suggestions, 1):
                        response_content += f"{i}. {suggestion}\n"
                    with response_placeholder:
                        for chunk in stream_text(response_content):
                            response_placeholder.markdown(response_content[:response_content.index(chunk) + len(chunk)], unsafe_allow_html=True)
                    assistant_response["content"] = response_content
                    st.session_state.last_suggestions = suggestions
                    st.session_state.messages.append({"role": "assistant", "content": response_content})

                st.session_state.chat_history.append(assistant_response)
                st.session_state.current_query = None
                st.session_state.current_results = assistant_response.get("results")
                st.session_state.current_sql = assistant_response.get("sql")
                st.session_state.current_summary = assistant_response.get("summary")
                st.session_state.previous_query = combined_query
                st.session_state.previous_sql = assistant_response.get("sql")
                st.session_state.previous_results = assistant_response.get("results")
                st.session_state.query = None

# Snowflake/Cortex Configuration
HOST = "GBJYVCT-LSB50763.snowflakecomputing.com"
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
if "previous_query" not in st.session_state:
    st.session_state.previous_query = None
if "previous_sql" not in st.session_state:
    st.session_state.previous_sql = None
if "previous_results" not in st.session_state:
    st.session_state.previous_results = None
if "show_sample_questions" not in st.session_state:
    st.session_state.show_sample_questions = False

# Hide Streamlit branding noted in earlier conversation and prevent chat history shading
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

import uuid

def submit_maintenance_request(property_id: str, tenant_name: str, issue_description: str):
    try:
        # Generate a unique request ID using UUID
        request_id = str(uuid.uuid4())

        # Use parameterized query to avoid SQL injection
        insert_query = """
        INSERT INTO MAINTENANCE_REQUESTS 
        (REQUEST_ID, PROPERTY_ID, TENANT_NAME, ISSUE_DESCRIPTION, SUBMITTED_AT, STATUS)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP(), 'PENDING')
        """
        session.sql(insert_query).bind((request_id, property_id, tenant_name, issue_description)).collect()

        return True, f"📝 Maintenance request submitted successfully! Request ID: {request_id}"
    except Exception as e:
        return False, f"❌ Failed to submit maintenance request: {str(e)}"


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
    st.session_state.previous_query = None
    st.session_state.previous_sql = None
    st.session_state.previous_results = None
    st.rerun()

def init_service_metadata():
    if not st.session_state.service_metadata:
        try:
            # Use quoted identifiers to handle case-sensitivity or special characters
            services = session.sql("SHOW CORTEX SEARCH SERVICES;").collect()
            service_metadata = []
            target_service = CORTEX_SEARCH_SERVICES.strip('"')  # Remove quotes for comparison
            if services:
                for s in services:
                    svc_name = s["name"]
                    if svc_name.lower() == target_service.lower():  # Case-insensitive comparison
                        svc_search_col = session.sql(
                            f"DESC CORTEX SEARCH SERVICE {CORTEX_SEARCH_SERVICES};"
                        ).collect()
                        if svc_search_col:
                            service_metadata.append(
                                {"name": svc_name, "search_column": svc_search_col[0]["search_column"]}
                            )
                        else:
                            st.error(f"❌ Cortex Search Service {CORTEX_SEARCH_SERVICES} exists but cannot be described.")
            if not service_metadata:
                st.error(f"❌ Cortex Search Service {CORTEX_SEARCH_SERVICES} not found. Please verify the service name.")
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
        # Ensure the service name is properly formatted
        service_name = st.session_state.selected_cortex_search_service
        cortex_search_service = (
            root.databases[db]
            .schemas[schema]
            .cortex_search_services[service_name]
        )
        context_documents = cortex_search_service.search(
            query, columns=[], limit=st.session_state.num_retrieved_chunks
        )
        results = context_documents.results
        service_metadata = st.session_state.service_metadata
        search_col = [s["search_column"] for s in service_metadata
                      if s["name"].lower() == service_name.lower()][0]
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

        # Maintenance Request Submission
        with st.expander("Submit Maintenance Request"):
            st.markdown("### Submit a Maintenance Request")
            property_id = st.text_input("Property ID", key="maint_property_id")
            tenant_name = st.text_input("Tenant Name", key="maint_tenant_name")
            issue_description = st.text_area("Issue Description", key="maint_issue_description")
            if st.button("Submit Request", key="submit_maintenance_request"):
                if not property_id or not tenant_name or not issue_description:
                    st.error("❌ Please fill in all fields to submit a maintenance request.")
                else:
                    success, message = submit_maintenance_request(property_id, tenant_name, issue_description)
                    if success:
                        st.success(message)
                    else:
                        st.error(message)

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

        # Check if this is a follow-up question
        is_follow_up = False
        follow_up_patterns = [
            r'^\bby\b\s+\w+$',  # e.g., "by state"
            r'^\bgroup by\b\s+\w+$'  # e.g., "group by state"
        ]
        if any(re.search(pattern, query.lower()) for pattern in follow_up_patterns) and st.session_state.previous_query:
            is_follow_up = True

        # If this is a follow-up and chat history is enabled, use the chat history to generate a combined query
        combined_query = query
        if st.session_state.use_chat_history:
            chat_history = get_chat_history()
            if chat_history and is_follow_up:
                combined_query = make_chat_history_summary(chat_history, query)
                if st.session_state.debug_mode:
                    st.sidebar.text_area("Combined Query", combined_query, height=100)

        st.session_state.chat_history.append({"role": "user", "content": original_query})
        st.session_state.messages.append({"role": "user", "content": original_query})
        with st.chat_message("user"):
            st.markdown(original_query)
        with st.chat_message("assistant"):
            with st.spinner("Generating Response..."):
                response_placeholder = st.empty()
                if st.session_state.data_source not in ["Database", "Document"]:
                    st.session_state.data_source = "Database"
                is_structured = is_structured_query(combined_query) and st.session_state.data_source == "Database"
                is_complete = is_complete_query(combined_query)
                is_summarize = is_summarize_query(combined_query)
                is_suggestion = is_question_suggestion_query(combined_query)
                is_greeting = is_greeting_query(combined_query)
                if st.session_state.debug_mode:
                    st.sidebar.text_area("Debug Info", f"is_structured: {is_structured}\nData Source: {st.session_state.data_source}\nis_follow_up: {is_follow_up}", height=150)
                assistant_response = {"role": "assistant", "content": "", "query": combined_query}
                response_content = ""
                failed_response = False

                if is_greeting and original_query.lower().strip() == "hi":
                    response_content = """
                    Hello! Welcome to the Property Management AI Assistant! I'm here to help you explore and analyze property-related data, answer questions about leasing, tenant screening, rent collection, and maintenance, or provide insights from documents.
                    
                    Here are some questions you can try:
                    """
                    suggestions = [
                        "Total number of properties currently occupied?",
                        "What is the number of properties currently leased?",
                        "What are the details of lease execution, commencement, and termination?",
                        "What is the average supplier payment per property?"
                    ]
                    # Add numbered suggestions
                    for i, suggestion in enumerate(suggestions, 1):
                        response_content += f"\n{i}. {suggestion}"
                    response_content += "\n\nFeel free to ask anything, or pick one of the suggested questions to get started!"
                    with response_placeholder:
                        st.write_stream(stream_text(response_content))
                        st.markdown(response_content, unsafe_allow_html=True)
                    assistant_response["content"] = response_content
                    st.session_state.messages.append({"role": "assistant", "content": response_content})
                    st.session_state.last_suggestions = suggestions

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
                        "Feel free to ask anything, or try one of these sample questions:"
                    )
                    suggestions = [
                        "Total number of properties currently occupied?",
                        "What are the details of lease execution, commencement, and termination?",
                        "What is the average supplier payment per property?",
                        "Which tenants have pending rent payments?"
                    ]
                    # Add numbered suggestions
                    for i, suggestion in enumerate(suggestions, 1):
                        response_content += f"\n{i}. {suggestion}"
                    with response_placeholder:
                        st.write_stream(stream_text(response_content))
                        st.markdown(response_content, unsafe_allow_html=True)
                    assistant_response["content"] = response_content
                    st.session_state.last_suggestions = suggestions
                    st.session_state.messages.append({"role": "assistant", "content": response_content})

                elif is_complete:
                    response = create_prompt(combined_query)
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
                    summary = summarize(combined_query)
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
                    response = snowflake_api_call(combined_query, is_structured=True)
                    sql, _ = process_sse_response(response, is_structured=True)
                    if sql:
                        if st.session_state.debug_mode:
                            st.sidebar.text_area("Generated SQL", sql, height=150)
                        results = run_snowflake_query(sql)
                        if results is not None and not results.empty:
                            results_text = results.to_string(index=False)
                            prompt = f"Provide a concise natural language answer to the query '{combined_query}' using the following data, avoiding phrases like 'Based on the query results':\n\n{results_text}"
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
                                display_chart_tab(results, prefix=f"chart_{hash(combined_query)}", query=combined_query)
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
                    response = snowflake_api_call(combined_query, is_structured=False)
                    _, search_results = process_sse_response(response, is_structured=False)
                    if search_results:
                        raw_result = search_results[0]
                        summary = create_prompt(combined_query)
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
                    suggestions = suggest_sample_questions(combined_query)
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
                st.session_state.current_query = combined_query
                st.session_state.current_results = assistant_response.get("results")
                st.session_state.current_sql = assistant_response.get("sql")
                st.session_state.current_summary = assistant_response.get("summary")
                # Store the previous query context
                st.session_state.previous_query = combined_query
                st.session_state.previous_sql = assistant_response.get("sql")
                st.session_state.previous_results = assistant_response.get("results")
                st.session_state.query = None
