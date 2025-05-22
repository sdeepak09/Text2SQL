import streamlit as st
import pandas as pd
import traceback
import re
import logging
from graph_builder import (
    initialize_state, 
    update_state_with_feedback, 
    update_state_with_clarification,
    explain_query_node,
    generate_sql_node,
    execute_query_node
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def execute_query(query, db_path):
    """Execute an SQL query and return the results as a DataFrame."""
    try:
        import sqlite3
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df, None
    except Exception as e:
        return None, str(e)

def process_query(query):
    """Process a query using the legacy method."""
    try:
        from langchain_utils import create_text2sql_agent
        
        # Create a Text2SQL agent
        agent_func, _ = create_text2sql_agent(st.session_state.db_path)
        
        # Generate SQL from the query
        response = agent_func(query)
        
        # Extract the SQL query from the response
        sql_query = None
        if "intermediate_steps" in response and response["intermediate_steps"]:
            sql_query = response["intermediate_steps"][0]
        
        # Execute the query if we have one
        results = None
        error = None
        if sql_query and not sql_query.startswith("-- Error"):
            results, error = execute_query(sql_query, st.session_state.db_path)
        
        return {
            "sql_query": sql_query,
            "results": results,
            "error": error
        }
    except Exception as e:
        return {
            "sql_query": None,
            "results": None,
            "error": str(e)
        }

# Add a simpler approach that doesn't rely on LangGraph's complex features
def process_new_query_simple(query):
    """Process a new query using the simplified LangGraph approach."""
    from graph_builder import initialize_state
    
    # Initialize the graph state with the new query
    initial_state = initialize_state(query)
    
    # Run the graph with the initial state
    try:
        # Increase the recursion limit to handle more complex conversations
        result = st.session_state.graph.invoke(initial_state, {"recursion_limit": 10})
        
        # Update the session state with the result
        st.session_state.graph_state = result
        st.session_state.messages = result["conversation_history"]
        
        # Check if we're awaiting feedback - only if SQL intent is True
        if (result.get("sql_intent", False) and  # Only if SQL intent is True
            "query_explanation" in result and 
            result["query_explanation"] is not None and 
            ("user_feedback" not in result or result["user_feedback"] is None)):
            st.session_state.awaiting_feedback = True
            print(f"Awaiting feedback set to TRUE. SQL intent is True, query_explanation exists, and user_feedback is None")
        else:
            st.session_state.awaiting_feedback = False
            # Add debug info about why not awaiting feedback
            if not result.get("sql_intent", False):
                print(f"Awaiting feedback set to FALSE. SQL intent is {result.get('sql_intent')}")
            elif "query_explanation" not in result or result["query_explanation"] is None:
                print(f"Awaiting feedback set to FALSE. No query_explanation in state")
            elif "user_feedback" in result and result["user_feedback"] is not None:
                print(f"Awaiting feedback set to FALSE. user_feedback already exists: {result['user_feedback']}")
            
        # Add debug info
        st.session_state.debug_info = {
            "graph_run_success": True,
            "awaiting_feedback": st.session_state.awaiting_feedback,
            "sql_intent": result.get("sql_intent", False),
            "has_query_explanation": "query_explanation" in result and result["query_explanation"] is not None,
            "has_user_feedback": "user_feedback" in result and result["user_feedback"] is not None,
        }
        
    except Exception as e:
        st.error(f"Error processing query: {str(e)}")
        st.code(traceback.format_exc())
        
        # Add a fallback response when there's an error
        st.session_state.messages.append({
            "role": "assistant",
            "content": "I encountered an error processing your query. Please try rephrasing your question or asking something else."
        })

# Add a simpler approach for processing feedback
def process_feedback_simple(feedback):
    """Process feedback using a simplified approach."""
    try:
        # Update the state with the feedback
        updated_state = update_state_with_feedback(st.session_state.graph_state, feedback)
        
        # If feedback is good, run the generate_sql_node only
        if feedback == "good":
            try:
                # Generate SQL but don't execute it
                final_result = generate_sql_node(updated_state)
                
                # Update the state
                st.session_state.graph_state = final_result
                
                # Update the messages
                st.session_state.messages = []
                if final_result and "conversation_history" in final_result:
                    for message in final_result["conversation_history"]:
                        st.session_state.messages.append(message)
                
                # Reset awaiting feedback
                st.session_state.awaiting_feedback = False
                
            except Exception as e:
                st.error(f"Error generating SQL: {str(e)}")
                st.code(traceback.format_exc())
                st.session_state.graph_state = updated_state
                st.session_state.messages = updated_state["conversation_history"]
                st.session_state.awaiting_feedback = False
        else:
            # If feedback is not good, set awaiting clarification to True
            st.session_state.graph_state = updated_state
            st.session_state.messages = updated_state["conversation_history"]
            st.session_state.awaiting_feedback = False
            st.session_state.awaiting_clarification = True
    
    except Exception as e:
        st.error(f"Error processing feedback: {str(e)}")
        st.code(traceback.format_exc())

# Add a simpler approach for processing clarification
def process_clarification_simple(clarification):
    """Process clarification using a simplified approach."""
    try:
        # Update the state with the clarification
        updated_state = update_state_with_clarification(st.session_state.graph_state, clarification)
        
        # Run the explain_query_node again with the clarified query
        try:
            result = explain_query_node(updated_state)
            
            # Update the state
            st.session_state.graph_state = result
            
            # Update the messages
            st.session_state.messages = []
            if result and "conversation_history" in result:
                for message in result["conversation_history"]:
                    st.session_state.messages.append(message)
            
            # Set awaiting feedback to True and awaiting clarification to False
            st.session_state.awaiting_feedback = True
            st.session_state.awaiting_clarification = False
            
        except Exception as e:
            st.error(f"Error explaining clarified query: {str(e)}")
            st.code(traceback.format_exc())
            st.session_state.graph_state = updated_state
            st.session_state.messages = updated_state["conversation_history"]
            st.session_state.awaiting_feedback = False
            st.session_state.awaiting_clarification = False
    
    except Exception as e:
        st.error(f"Error processing clarification: {str(e)}")
        st.code(traceback.format_exc()) 