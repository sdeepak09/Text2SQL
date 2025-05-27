import sys
# Increase recursion limit
sys.setrecursionlimit(10000)

import streamlit as st
import os
import traceback
import logging
import time
from db_setup import initialize_database, get_table_info
from ui_components import (
    display_schema_sidebar,
    display_chat_messages,
    display_query_results,
    display_debug_info,
    display_feedback_buttons,
    display_clarification_form,
    display_intent_detection
)
from handlers import (
    process_query,
    process_new_query_simple,
    process_feedback_simple,
    process_clarification_simple
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="Text2SQL Assistant",
    page_icon="🤖",
    layout="wide"
)

# Try to import from graph_builder, but fall back to legacy mode if it fails
try:
    from graph_builder import build_simple_graph
    USE_LANGGRAPH = True
    
    # Only show success messages in debug mode
    if st.session_state.get("show_debug", False):
        st.success("Successfully imported LangGraph components")
except ImportError as e:
    USE_LANGGRAPH = False
    st.error(f"Error importing LangGraph components: {str(e)}")
    st.info("Falling back to legacy mode without interactive feedback")

# Initialize session state variables
if "messages" not in st.session_state:
    st.session_state.messages = []

if "db_path" not in st.session_state:
    st.session_state.db_path = initialize_database()

if "table_info" not in st.session_state:
    st.session_state.table_info = get_table_info()

# Initialize LangGraph related session state variables if available
if USE_LANGGRAPH:
    if "graph" not in st.session_state or st.button("Reset Graph"):
        try:
            # Use the simple graph by default as it's more reliable
            st.session_state.graph = build_simple_graph().compile()
            
            # Only show success message in debug mode
            if st.session_state.get("show_debug", False):
                st.success("Using simplified LangGraph with feedback support")
            
            # Reset all graph-related state
            st.session_state.graph_state = None
            st.session_state.awaiting_feedback = False
            st.session_state.awaiting_clarification = False
            st.session_state.messages = []
            
        except Exception as e:
            st.error(f"Error building graph: {str(e)}")
            st.code(traceback.format_exc())
            USE_LANGGRAPH = False

    if "graph_state" not in st.session_state:
        st.session_state.graph_state = None

    if "awaiting_feedback" not in st.session_state:
        st.session_state.awaiting_feedback = False

    if "awaiting_clarification" not in st.session_state:
        st.session_state.awaiting_clarification = False

    if "debug_info" not in st.session_state:
        st.session_state.debug_info = {}

# Check if OpenAI API key is set
if USE_LANGGRAPH:
    openai_api_key = os.getenv("OPENAI_API_KEY")
    if not openai_api_key:
        st.error("OPENAI_API_KEY environment variable not set. LangGraph will not work without it.")
    else:
        # Only show success message in debug mode
        if st.session_state.get("show_debug", False):
            st.success(f"OPENAI_API_KEY is set (starts with {openai_api_key[:4]}...)")

# --- FAISS Index Check ---
# This path should be consistent with where main_pipeline.py and query_embedding_store.py save the index.
FAISS_INDEX_FOLDER_PATH = "data/context_faiss_store_v1"
faiss_actual_index_file = os.path.join(FAISS_INDEX_FOLDER_PATH, "index.faiss")

if not os.path.exists(faiss_actual_index_file):
    st.error(
        f"""🛑 **FAISS Vector Index Not Found!** 🛑

The Text2SQL RAG pipeline requires a FAISS vector index at `{FAISS_INDEX_FOLDER_PATH}`
to function correctly. This index stores embeddings of your schema and example queries.

**To build the index, please run the following command in your terminal from the project root directory:**
```
python main_pipeline.py
```

After running the command, please refresh this page.
The application's SQL generation capabilities will be limited or non-functional until the index is built.
"""
    )
# --- End FAISS Index Check ---

# Initialize debug mode
if "show_debug" not in st.session_state:
    st.session_state.show_debug = False

# Display database schema information in the sidebar
display_schema_sidebar(st.session_state.table_info)

# Main app interface
col1, col2 = st.columns([5, 1])
with col1:
    st.title("🤖 Interactive Text2SQL Assistant")
    st.caption("Ask questions about your company data in natural language")
with col2:
    # Place debug checkbox in top right
    show_debug = st.checkbox("Debug", value=st.session_state.show_debug, key="debug_checkbox_top")
    if show_debug != st.session_state.show_debug:
        st.session_state.show_debug = show_debug
        st.rerun()

# Override the default welcome expander with custom HTML
st.markdown("""
<style>
.streamlit-expanderHeader {
    display: none !important;
}
</style>
""", unsafe_allow_html=True)

# Custom welcome expander
st.markdown("""
<details>
<summary style="cursor: pointer; font-weight: bold; padding: 10px; background-color: #f0f2f6; border-radius: 5px;">
👋 Welcome! Learn how to use this assistant
</summary>
<div style="padding: 15px; background-color: #f9f9f9; border-radius: 5px; margin-top: 10px;">
<h2>What is this Text2SQL Assistant?</h2>
<p>This tool helps you generate SQL queries for your healthcare database using natural language. Simply ask questions about your data, and the assistant will:</p>

<ol>
<li><strong>Analyze your question</strong> and confirm its understanding</li>
<li><strong>Generate an SQL query</strong> based on your question</li>
</ol>

<h2>How to use it:</h2>
<ol>
<li><strong>Ask a question</strong> about your healthcare data in the chat box below</li>
<li><strong>Confirm or correct</strong> the assistant's understanding of your question</li>
<li><strong>View the SQL query</strong> generated to answer your question</li>
</ol>

<h2>Example questions to try:</h2>
<ul>
<li>Show all admission records where the total allowed amount is greater than 500.</li>
<li>List the first name and last name of patients for admissions with an ID less than 10.</li>
<li>What are the procedure codes (PROC_CD, ICD_PROC_CD) for admission ID 75?</li>
<li>Show responsible provider IDs and a count of admissions for each.</li>
<li>List admission IDs and their admit dates for admissions after January 1, 2023.</li>
<li>Which patient (MEMBER ID) has the most entries in the clinical markers table (CLINMARK_T)?</li>
<li>What are the distinct categories (CAT_DESC) available in the case data (CASD)?</li>
</ul>
</div>
</details>
""", unsafe_allow_html=True)

# Display chat messages
display_chat_messages(st.session_state.messages)

# Display query results if LangGraph is available
if USE_LANGGRAPH:
    display_query_results(st.session_state.graph_state)

# Display debug info if enabled
if USE_LANGGRAPH:
    display_debug_info(
        st.session_state.show_debug, 
        st.session_state.graph_state, 
        st.session_state.debug_info
    )

# Display feedback buttons if awaiting feedback
if USE_LANGGRAPH:
    # Add debug info about the awaiting_feedback state
    print(f"Awaiting feedback state: {st.session_state.awaiting_feedback}")
    
    display_feedback_buttons(
        st.session_state.awaiting_feedback,
        process_feedback_simple
    )

# Display clarification form if awaiting clarification
if USE_LANGGRAPH:
    display_clarification_form(
        st.session_state.awaiting_clarification,
        process_clarification_simple
    )

# Display intent detection result (for debugging only)
if USE_LANGGRAPH and st.session_state.show_debug:
    display_intent_detection(st.session_state.graph_state)

# Chat input
if not (USE_LANGGRAPH and st.session_state.awaiting_clarification):
    if prompt := st.chat_input("Ask a question about your data..."):
        # Add user message to chat history with avatar
        st.chat_message("user", avatar="👤").markdown(prompt)
        
        if USE_LANGGRAPH:
            # Process the new query using the simplified approach
            process_new_query_simple(prompt)
        else:
            # Process the query using the legacy method
            response = process_query(prompt)
            
            # Display assistant response with avatar
            with st.chat_message("assistant", avatar="🤖"):
                if response["sql_query"]:
                    st.code(response["sql_query"], language="sql")
                
                if response["error"]:
                    st.error(response["error"])
                elif response["results"] is not None:
                    st.dataframe(response["results"])
                
                # Generate a text explanation of the results
                if response["results"] is not None and not response["error"]:
                    row_count = len(response["results"])
                    col_count = len(response["results"].columns)
                    content = f"Query returned {row_count} rows with {col_count} columns."
                elif response["error"]:
                    content = "There was an error executing the query."
                else:
                    content = "No results returned."
                    
                st.markdown(content)
                
                # Add assistant response to chat history
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": content,
                    "sql_query": response["sql_query"],
                    "results": response["results"],
                    "error": response["error"]
                })
        
        # Force a rerun to update the UI
        st.rerun()

# Move the debug options to the sidebar but remove the checkbox (now in top right)
with st.sidebar:
    # Only show debug options when debug mode is enabled
    if st.session_state.show_debug:
        st.divider()
        st.subheader("Debug Options")
        
        # Only show the Test LangGraph button in debug mode
        if USE_LANGGRAPH:
            if st.button("Test LangGraph", key="test_langgraph_button"):
                try:
                    from graph_builder import initialize_state
                    # Create a simple test state
                    test_state = initialize_state("How many employees are in each department?")
                    
                    # Run the graph with the test state
                    result = st.session_state.graph.invoke(test_state, {"recursion_limit": 5})
                    
                    # Display the result
                    st.success("LangGraph test successful!")
                    st.json(result)
                except Exception as e:
                    st.error(f"LangGraph test failed: {str(e)}")
                    st.code(traceback.format_exc())

# Welcome message (shown only once) - simplified to just set the flag
if "welcome_shown" not in st.session_state:
    # Set the flag without showing a duplicate message
    st.session_state.welcome_shown = True 