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
                summary_text = message.get("summary", "")
                breakdown_text_raw = message.get("breakdown", "") 

                html_breakdown_lines = []
                if isinstance(breakdown_text_raw, str) and breakdown_text_raw.strip():
                    steps = re.findall(r'(\d+\.\s+[\s\S]*?(?=(\s*\d+\.\s+)|$))', breakdown_text_raw)
                    if steps:
                        for step_match in steps:
                            step_text = step_match[0].strip() 
                            if step_text:
                                html_breakdown_lines.append(f"<p style='margin: 0.2em 0;'>{step_text}</p>")
                    elif breakdown_text_raw.strip(): 
                        html_breakdown_lines.append(f"<p style='margin: 0.2em 0;'>{breakdown_text_raw.strip()}</p>")
                elif isinstance(breakdown_text_raw, list): 
                    for i, item in enumerate(breakdown_text_raw):
                        item_stripped = str(item).strip()
                        if item_stripped:
                            html_breakdown_lines.append(f"<p style='margin: 0.2em 0;'>{i+1}. {item_stripped}</p>")
                
                html_breakdown = "".join(html_breakdown_lines)
                if not html_breakdown.strip() and breakdown_text_raw.strip(): 
                    html_breakdown = f"<p style='margin: 0.2em 0;'>{breakdown_text_raw.strip()}</p>"

                understanding_html_content = f"""
                <div style="background-color: #f0f2f6; padding: 15px; border-radius: 10px; border: 1px solid #dfe1e5; margin-bottom: 10px;">
                    <h4 style="margin-top: 0; margin-bottom: 10px;">I understand your query as follows:</h4>
                    <p>{summary_text}</p>
                    <hr style="border-top: 1px solid #dfe1e5; margin-top: 10px; margin-bottom: 10px;">
                    <h4 style="margin-top: 0; margin-bottom: 10px;">Here's my plan to answer it:</h4>
                    {html_breakdown}
                </div>
                """
                st.markdown(understanding_html_content, unsafe_allow_html=True)

            elif message["role"] == "assistant" and message.get("type") == "simple_explanation":
                st.markdown(message["content"])
            elif message["role"] == "user":
                st.markdown(message["content"])
            else:
                if "content" in message: 
                    st.markdown(message["content"])

def display_query_results(graph_state):
    """Display query results from the graph state."""
    if not graph_state:
        return
    
    if "query_explanation" in graph_state and graph_state["query_explanation"]:
        pass
    
    if "error_message" in graph_state and graph_state["error_message"]:
        st.error(graph_state["error_message"])

def display_debug_info(show_debug, graph_state, debug_info):
    """Display debug information if enabled."""
    if not show_debug:
        return
    
    with st.expander("Debug Information", expanded=False):
        st.write("### Basic Debug Info")
        st.write("- LangGraph enabled: Yes") # Assuming True if this UI is active
        st.write(f"- Awaiting feedback: {st.session_state.get('awaiting_feedback', False)}")
        st.write(f"- Awaiting clarification: {st.session_state.get('awaiting_clarification', False)}")
        st.write(f"- Graph state available: {graph_state is not None}")
        
        if graph_state:
            st.write("### Graph State Details")
            st.write(f"- Has query explanation: {'query_explanation' in graph_state and graph_state['query_explanation'] is not None}")
            st.write(f"- Has generated SQL: {'generated_sql' in graph_state and graph_state['generated_sql'] is not None}")
            st.write(f"- Has query result: {'query_result' in graph_state and graph_state['query_result'] is not None}")
            st.write(f"- User feedback: {graph_state.get('user_feedback', 'None')}")
            
            st.write("### Full Graph State")
            safe_state = {k: (str(v)[:500] + '...' if isinstance(v, str) and len(str(v)) > 500 else v) 
                         for k, v in graph_state.items()}
            st.json(safe_state)
        
        if debug_info:
            st.write("### Debug Info")
            st.json(debug_info)

def display_feedback_buttons(awaiting_feedback, process_feedback_func):
    """Display feedback buttons if awaiting feedback."""
    if not awaiting_feedback:
        return
    
    # User preference: Comment out the header above the buttons
    # st.markdown("""
    # <div style="padding: 10px 0px; margin-bottom: 10px; text-align: left;">
    #     <h4 style="margin-top: 0; margin-bottom: 5px;">Is this understanding correct and ready to proceed?</h4>
    # </div>
    # """, unsafe_allow_html=True)
    
    col_spacer, col_btn1, col_btn2 = st.columns([2, 1, 1])

    with col_btn1:
        if st.button("Yes, I approve the understanding", key="approve_understanding_feedback", use_container_width=True):
            process_feedback_func("good")
            st.rerun()
            
    with col_btn2:
        if st.button("No, I want to modify the query", key="modify_query_feedback", use_container_width=True):
            process_feedback_func("not_good")
            st.rerun()

def display_clarification_form(awaiting_clarification, process_clarification_func):
    """Display clarification form if awaiting clarification."""
    if not awaiting_clarification: # This should be awaiting_clarification
        return
    
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