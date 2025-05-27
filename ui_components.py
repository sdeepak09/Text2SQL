import streamlit as st
import pandas as pd
import re

def display_schema_sidebar(table_info):
    """Display example questions in the sidebar."""
    with st.sidebar:
        st.title("Text2SQL Assistant")
        
        st.markdown("""
        This assistant helps you query databases using natural language. 
        Just ask a question about the data, and the assistant will generate 
        the appropriate SQL query.
        """)
        
        st.divider()
        st.subheader("Example Questions")
        st.markdown("""
        - Show all admission records where the total allowed amount is greater than 500.
        - List the first name and last name of patients for admissions with an ID less than 10.
        - What are the procedure codes (PROC_CD, ICD_PROC_CD) for admission ID 75?
        - Show responsible provider IDs and a count of admissions for each.
        - List admission IDs and their admit dates for admissions after January 1, 2023.
        - Which patient (MEMBER ID) has the most entries in the clinical markers table (CLINMARK_T)?
        - What are the distinct categories (CAT_DESC) available in the case data (CASD)?
        """)

def display_chat_messages(messages):
    """Display chat messages with avatars and styling."""
    for message in messages:
        avatar = "👤" if message["role"] == "user" else "🤖"
        with st.chat_message(message["role"], avatar=avatar):
            if message["role"] == "assistant" and message.get("type") == "query_understanding":
                # Display new structured explanation
                if message.get("summary"):
                    st.markdown("**I understand your query as follows:**") # Main section header
                    st.markdown(message["summary"])
                    st.markdown("---") # Add a separator
                if message.get("breakdown"):
                    st.markdown("**Here's my plan to answer it:**") # Main section header
                    # Format the breakdown, assuming it might be a multi-line string or list of steps
                    if isinstance(message["breakdown"], list):
                        for i, step in enumerate(message["breakdown"]):
                            st.markdown(f"{i+1}. {step}")
                    else: # message["breakdown"] is a string
                        breakdown_text = message["breakdown"]
                        # Replace " 2." with "
                        # 2.", " 3." with "
                        # 3." etc.
                        # This aims to make "1. Step one. 2. Step two." display as:
                        # 1. Step one.
                        # 2. Step two.
                        # by ensuring each numbered item starts on a new line for markdown.
                        formatted_breakdown = breakdown_text
                        # Iterating from higher numbers to lower (e.g., 10 down to 2)
                        # to avoid issues where replacing " 2." might affect a " 12."
                        for i in range(10, 1, -1): # Numbers 10 down to 2
                            formatted_breakdown = formatted_breakdown.replace(f' {i}.', f'\n{i}.')
                        # For "1.", if it's not already at the beginning of a line (e.g. after a header)
                        # we don't necessarily need to add a newline before it,
                        # but if the whole string is "Summary text 1. first step" it might be needed.
                        # However, the current prompt structure implies "step_by_step_breakdown_llm" will *be* the list.
                        # This formatting is a fallback if the LLM doesn't use actual newlines in its string.
                        st.markdown(formatted_breakdown)
                
                # Optional: For debugging, display the raw structured explanation if show_debug is active
                # This requires access to st.session_state, which might be better handled in streamlit_app.py
                # For now, we'll omit this part from the direct subtask to keep it focused on ui_components.py
                # if st.session_state.get("show_debug") and message.get("structured_explanation_raw"):
                #    with st.expander("View Detailed Structured Explanation (Debug)"):
                #        st.json(message["structured_explanation_raw"])

            elif message["role"] == "assistant" and message.get("type") == "simple_explanation":
                # Handle the old simple explanation or fallbacks
                st.markdown(message["content"])
            elif message["role"] == "user":
                # Display user messages as before
                st.markdown(message["content"])
            else:
                # Default display for any other assistant messages (e.g., SQL query, results)
                # or messages that don't have the new 'type' field yet from older history.
                if "content" in message: # Check if content key exists
                    st.markdown(message["content"])
                # else:
                #    st.markdown("(Unsupported assistant message structure)") # Optional: for debugging

def display_query_results(graph_state):
    """Display query results from the graph state."""
    if not graph_state:
        return
    
    # Display the query explanation if available
    if "query_explanation" in graph_state and graph_state["query_explanation"]:
        # Query explanation is already displayed in the chat history
        pass
    
    # We'll remove this section since the SQL is already in the conversation history
    # The SQL will only be displayed as part of the conversation
    
    # Display error message if there was an error
    if "error_message" in graph_state and graph_state["error_message"]:
        st.error(graph_state["error_message"])

def display_debug_info(show_debug, graph_state, debug_info):
    """Display debug information if enabled."""
    if not show_debug:
        return
    
    # Create a dedicated debug section with an expander to keep the UI clean
    with st.expander("Debug Information", expanded=False):
        st.write("### Basic Debug Info")
        st.write("- LangGraph enabled: Yes")
        st.write(f"- Awaiting feedback: {st.session_state.awaiting_feedback}")
        st.write(f"- Awaiting clarification: {st.session_state.awaiting_clarification}")
        st.write(f"- Graph state available: {graph_state is not None}")
        
        if graph_state:
            st.write("### Graph State Details")
            st.write(f"- Has query explanation: {'query_explanation' in graph_state and graph_state['query_explanation'] is not None}")
            st.write(f"- Has generated SQL: {'generated_sql' in graph_state and graph_state['generated_sql'] is not None}")
            st.write(f"- Has query result: {'query_result' in graph_state and graph_state['query_result'] is not None}")
            st.write(f"- User feedback: {graph_state.get('user_feedback', 'None')}")
            
            # Show a safe version of the graph state (without the large objects)
            st.write("### Full Graph State")
            safe_state = {k: (str(v) if k in ["query_explanation", "generated_sql", "query_result"] else v) 
                         for k, v in graph_state.items()}
            st.json(safe_state)
        
        # Show debug info
        if debug_info:
            st.write("### Debug Info")
            st.json(debug_info)

def display_feedback_buttons(awaiting_feedback, process_feedback_func):
    """Display feedback buttons if awaiting feedback."""
    if not awaiting_feedback:
        return
    
    # Create a more prominent feedback section with a light background
    st.markdown("""
    <div style="padding: 15px; background-color: #f0f2f6; border-radius: 10px; margin-bottom: 20px;">
        <h4 style="margin-top: 0;">Is this understanding correct?</h4>
    </div>
    """, unsafe_allow_html=True)
    
    # Use clear, descriptive buttons
    col1, col2 = st.columns(2)
    with col1:
        if st.button("✓ Yes, that's right", key="good_feedback", use_container_width=True):
            process_feedback_func("good")
            st.rerun()
    with col2:
        if st.button("✗ No, that's not right", key="bad_feedback", use_container_width=True):
            process_feedback_func("not_good")
            st.rerun()

def display_clarification_form(awaiting_clarification, process_clarification_func):
    """Display clarification form if awaiting clarification."""
    if not awaiting_clarification:
        return
    
    # More compact clarification form
    with st.form(key="clarification_form"):
        st.markdown("##### Please clarify your query:")
        clarification = st.text_area("", height=80, placeholder="Explain what you're looking for...")
        submit_button = st.form_submit_button(label="Submit")
        if submit_button and clarification:
            process_clarification_func(clarification)
            st.rerun()

def display_intent_detection(graph_state):
    """Display the intent detection result (for debugging only)."""
    if not graph_state or "sql_intent" not in graph_state or graph_state["sql_intent"] is None:
        return
    
    with st.expander("SQL Intent Detection (Debug)", expanded=False):
        if graph_state["sql_intent"]:
            st.success("✓ Your query can be answered with SQL")
        else:
            st.warning("⚠️ Your query may not be suitable for SQL. Please rephrase your question.")
        if "intent_explanation" in graph_state and graph_state["intent_explanation"]:
            st.info(graph_state["intent_explanation"]) 