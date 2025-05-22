from typing import TypedDict, List, Optional, Dict, Any, Literal
from langgraph.graph import StateGraph, END
from pydantic_models import QueryExplanation, SQLOutput, QueryResult
from llm_utils import (
    get_llm, get_query_explanation_prompt, get_sql_generation_prompt,
    parse_query_explanation, validate_sql_query, execute_sql_query, get_relevant_schema_context
)
from db_setup import get_formatted_schema
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the state type
class GraphState(TypedDict):
    """Type for the state of the Text2SQL graph."""
    original_query: str
    current_query: str
    sql_intent: Optional[bool]
    intent_explanation: Optional[str]
    query_explanation: Optional[Any]
    user_feedback: Optional[Literal["good", "not_good"]]
    generated_sql: Optional[Any]
    query_result: Optional[Any]
    error_message: Optional[str]
    _recursion_count: int
    conversation_history: List[Dict[str, Any]]

# Define the graph nodes
def explain_query_node(state: GraphState) -> GraphState:
    """Node that explains the query."""
    logger.info(f"explain_query_node called with state: {state}")
    
    # Get the current query
    query = state["current_query"]
    
    # Get the LLM
    llm = get_llm()
    
    # Get relevant schema context using RAG
    context = get_relevant_schema_context(query)
    relevant_schema = context["relevant_schema"]
    relevant_statements = context.get("relevant_statements", "")
    
    # Generate the explanation
    try:
        # Get the prompt for query explanation
        prompt = get_query_explanation_prompt()
        
        # Generate the explanation
        response = llm.invoke(prompt.format(
            relevant_schema=relevant_schema,
            relevant_statements=relevant_statements,
            query=query
        ))
        
        # Log the response
        logger.debug(f"LLM response: {response.content}")
        print(f"LLM response: {response.content}")
        
        # Parse the explanation
        explanation, error = parse_query_explanation(response.content)
        
        # If there's an error, provide a fallback explanation
        if error:
            logger.warning(f"Error parsing explanation: {error}")
            # Create a fallback explanation as a dictionary
            explanation_dict = {
                "explanation": f"I'll try to answer your question about: '{query}'",
                "tables": [],
                "columns": []
            }
            # Update the state with the explanation dictionary
            state["query_explanation"] = explanation_dict
            
            # Add the explanation to the conversation history
            state["conversation_history"].append({
                "role": "assistant",
                "content": f"I understand your query as follows:\n\n{explanation_dict['explanation']}"
            })
        else:
            # Convert the Pydantic model to a dictionary
            explanation_dict = explanation.dict() if hasattr(explanation, 'dict') else explanation
            
            # Update the state with the explanation dictionary
            state["query_explanation"] = explanation_dict
            
            # Add the explanation to the conversation history
            explanation_text = explanation.summary_of_understanding if hasattr(explanation, 'summary_of_understanding') else explanation_dict.get('explanation', f"I'll analyze your question about: '{query}'")
            state["conversation_history"].append({
                "role": "assistant",
                "content": f"I understand your query as follows:\n\n{explanation_text}"
            })
        
    except Exception as e:
        logger.error(f"Error in explain_query_node: {str(e)}")
        # Provide a fallback explanation in case of any error
        explanation_dict = {
            "explanation": f"Attempting to answer: '{query}'",
            "tables": [],
            "columns": []
        }
        state["query_explanation"] = explanation_dict
        state["conversation_history"].append({
            "role": "assistant",
            "content": f"I'll try to answer your question about: '{query}'\n\n(Note: I encountered an issue while analyzing your query, but I'll do my best to answer it.)"
        })
    
    return state

def generate_sql_node(state: GraphState) -> GraphState:
    """Node that generates SQL from the query explanation."""
    logger.info(f"generate_sql_node called with state: {state}")
    
    # Get the query explanation
    query_explanation = state["query_explanation"]
    
    # Get the current query
    query = state["current_query"]
    
    # Get the LLM
    llm = get_llm()
    
    # Get relevant schema context using RAG
    context = get_relevant_schema_context(query)
    relevant_schema = context["relevant_schema"]
    relevant_statements = context.get("relevant_statements", "")
    
    # Generate the SQL
    try:
        # Get the prompt for SQL generation
        prompt = get_sql_generation_prompt()
        
        # Generate the SQL
        response = llm.invoke(prompt.format(
            relevant_schema=relevant_schema,
            relevant_statements=relevant_statements,
            query=query,
            explanation=json.dumps(query_explanation)
        ))
        
        # Log the response
        logger.debug(f"LLM response: {response.content}")
        
        # Parse the SQL
        sql_query = response.content.strip()
        
        # Validate the SQL query
        is_valid, error = validate_sql_query(sql_query, "data/company.db")
        
        if is_valid:
            logger.info(f"Successfully generated SQL: {sql_query}")
            
            # Update the state with the SQL
            state["generated_sql"] = {"sql_query": sql_query}
            
            # Format SQL for better readability
            formatted_sql = sql_query
            
            # Add the SQL to the conversation history
            state["conversation_history"].append({
                "role": "assistant",
                "content": f"Here's the SQL query to answer your question:\n\n```sql\n{formatted_sql}\n```"
            })
        else:
            logger.warning(f"Generated invalid SQL: {sql_query}. Error: {error}")
            
            # Update the state with the error
            state["error_message"] = f"Generated invalid SQL: {error}"
            
            # Add the error to the conversation history
            state["conversation_history"].append({
                "role": "assistant",
                "content": f"I tried to generate SQL but encountered an error: {error}\n\nHere's what I came up with, but it's not valid:\n\n```sql\n{sql_query}\n```"
            })
    
    except Exception as e:
        logger.error(f"Error in generate_sql_node: {str(e)}")
        
        # Update the state with the error
        state["error_message"] = f"Error generating SQL: {str(e)}"
        
        # Add the error to the conversation history
        state["conversation_history"].append({
            "role": "assistant",
            "content": f"I encountered an error while trying to generate SQL: {str(e)}"
        })
    
    return state

def execute_query_node(state: GraphState) -> GraphState:
    """Node that executes the SQL query."""
    try:
        # Get the SQL from the state
        sql_output_dict = state["generated_sql"]
        
        # Convert back to Pydantic model if needed
        if sql_output_dict and not isinstance(sql_output_dict, SQLOutput):
            sql_output = SQLOutput(**sql_output_dict)
        else:
            sql_output = sql_output_dict
        
        # Execute the query
        try:
            query_result = execute_sql_query(sql_output.sql_query, "data/company.db")
            
            # Update the state with the result
            logger.info(f"Successfully executed query. Found {len(query_result.data) if query_result.data else 0} results.")
            return {
                **state,
                "query_result": query_result.dict(),  # Store as dict
                "error_message": None,
                "conversation_history": state["conversation_history"] + [
                    {"role": "assistant", "content": format_query_result_message(query_result)}
                ]
            }
        except Exception as e:
            error_message = str(e)
            logger.error(f"Error executing query: {error_message}")
            
            # Check if the error is due to missing columns or tables
            if "no such column:" in error_message or "no such table:" in error_message:
                # Try to regenerate the SQL with corrected schema information
                return retry_sql_generation(state, error_message)
            
            # For other errors, just update the state
            return {
                **state,
                "query_result": None,
                "error_message": f"Error executing query: {error_message}",
                "conversation_history": state["conversation_history"] + [
                    {"role": "assistant", "content": f"I encountered an error while executing the query: {error_message}"}
                ]
            }
    except Exception as e:
        logger.error(f"Error in execute_query_node: {str(e)}")
        return {
            **state,
            "query_result": None,
            "error_message": f"Error in execute_query_node: {str(e)}",
            "conversation_history": state["conversation_history"] + [
                {"role": "assistant", "content": f"I encountered an error while processing the query: {str(e)}"}
            ]
        }

def retry_sql_generation(state: GraphState, error_message: str) -> GraphState:
    """Retry SQL generation with corrected schema information."""
    try:
        # Get the database schema
        schema = get_formatted_schema()
        
        # Get the LLM
        llm = get_llm(temperature=0)
        
        # Create a prompt for SQL correction
        correction_prompt = f"""
You need to fix an SQL query that failed with the following error:
{error_message}

The database schema is:
{schema}

The original query was:
{state["generated_sql"]["sql_query"]}

Please generate a corrected SQL query that will work with the given schema.
Only return the SQL query, nothing else.
"""
        
        # Generate the corrected SQL
        response = llm.invoke(correction_prompt).content
        
        # Extract just the SQL (remove any markdown or explanations)
        import re
        sql_match = re.search(r'```sql\n(.*?)\n```', response, re.DOTALL)
        if sql_match:
            corrected_sql = sql_match.group(1)
        else:
            # Try to find SQL without markdown
            sql_lines = [line for line in response.split('\n') if line.strip() and not line.startswith('#')]
            corrected_sql = '\n'.join(sql_lines)
        
        # Validate the corrected SQL
        sql_output = validate_sql_query(corrected_sql, "data/company.db")
        
        if not sql_output.query_valid:
            # If still invalid, update the state with the error
            logger.error(f"Corrected SQL is still invalid: {sql_output.validation_error}")
            return {
                **state,
                "generated_sql": sql_output.dict(),
                "error_message": f"Could not correct SQL: {sql_output.validation_error}",
                "conversation_history": state["conversation_history"] + [
                    {"role": "assistant", "content": f"I tried to fix the query but encountered another error: {sql_output.validation_error}"}
                ]
            }
        
        # Try to execute the corrected query
        try:
            query_result = execute_sql_query(sql_output.sql_query, "data/company.db")
            
            # Update the state with the result
            logger.info(f"Successfully executed corrected query. Found {len(query_result.data) if query_result.data else 0} results.")
            return {
                **state,
                "generated_sql": sql_output.dict(),
                "query_result": query_result.dict(),
                "error_message": None,
                "conversation_history": state["conversation_history"] + [
                    {"role": "assistant", "content": f"I had to adjust the query to match the available columns. Here's the corrected SQL:\n\n```sql\n{sql_output.sql_query}\n```"},
                    {"role": "assistant", "content": format_query_result_message(query_result)}
                ]
            }
        except Exception as e:
            # If execution still fails, update the state with the error
            error_message = str(e)
            logger.error(f"Error executing corrected query: {error_message}")
            return {
                **state,
                "generated_sql": sql_output.dict(),
                "query_result": None,
                "error_message": f"Error executing corrected query: {error_message}",
                "conversation_history": state["conversation_history"] + [
                    {"role": "assistant", "content": f"I tried to correct the query but still encountered an error: {error_message}"}
                ]
            }
    except Exception as e:
        # If correction fails, update the state with the error
        logger.error(f"Error in retry_sql_generation: {str(e)}")
        return {
            **state,
            "query_result": None,
            "error_message": f"Error in retry_sql_generation: {str(e)}",
            "conversation_history": state["conversation_history"] + [
                {"role": "assistant", "content": f"I tried to correct the query but encountered an error: {str(e)}"}
            ]
        }

def format_query_result_message(query_result):
    """Format a message to display query results."""
    if not query_result.success:
        return f"Query execution failed: {query_result.error}"
    
    if not query_result.data:
        return "The query executed successfully but returned no results."
    
    row_count = len(query_result.data)
    if row_count == 1:
        return f"The query returned 1 row."
    else:
        return f"The query returned {row_count} rows."

# Define the conditional routing function
def should_continue_to_sql(state: GraphState) -> str:
    """Determine whether to continue to SQL generation or wait for user feedback."""
    logger.info(f"User feedback: {state.get('user_feedback')}")
    
    # Add a safety check to prevent infinite recursion
    recursion_count = state.get("_recursion_count", 0) + 1
    
    # If we've exceeded the recursion limit, force SQL generation
    if recursion_count > 5:  # Lower this threshold to be more aggressive
        logger.warning(f"Recursion limit reached ({recursion_count}), forcing end of graph")
        return "generate_sql"
    
    # Normal routing logic
    if state.get("user_feedback") == "good":
        return "generate_sql"
    elif state.get("user_feedback") == "not_good":
        return "wait_for_clarification"
    else:
        # Only wait for feedback if we haven't exceeded a lower threshold
        if recursion_count <= 3:  # Even lower threshold for feedback
            return "wait_for_feedback"
        else:
            # Force SQL generation after a few iterations
            logger.warning(f"Feedback threshold reached ({recursion_count}), forcing SQL generation")
            return "generate_sql"

# Build the graph
def build_graph():
    """Build and return the LangGraph for the Text2SQL application."""
    # Create a new graph without the recursion_limit parameter
    graph = StateGraph(GraphState)
    
    # Add the nodes
    graph.add_node("explain_query", explain_query_node)
    graph.add_node("generate_sql", generate_sql_node)
    graph.add_node("execute_query", execute_query_node)
    
    # Define placeholder nodes that properly handle the state
    def wait_for_feedback_node(state: GraphState) -> GraphState:
        """Node that waits for user feedback."""
        logger.info(f"wait_for_feedback_node called with state: {state}")
        
        # Just return the state unchanged, but increment recursion count
        recursion_count = state.get("_recursion_count", 0) + 1
        
        # If we've been waiting too long, force an end to avoid infinite recursion
        if recursion_count > 3:
            logger.warning(f"Forcing end of feedback wait after {recursion_count} iterations")
            # Add a message to let the user know we're proceeding without feedback
            state["conversation_history"].append({
                "role": "assistant",
                "content": "I haven't received feedback, but I'll proceed with generating SQL based on my understanding."
            })
            return {**state, "_recursion_count": recursion_count, "_force_end": True, "user_feedback": "good"}
        
        return {**state, "_recursion_count": recursion_count}
    
    def wait_for_clarification_node(state: GraphState) -> GraphState:
        """Placeholder node that waits for user clarification."""
        # Just return the state unchanged, but ensure _recursion_count is present
        recursion_count = state.get("_recursion_count", 0) + 1
        return {**state, "_recursion_count": recursion_count}
    
    graph.add_node("wait_for_feedback", wait_for_feedback_node)
    graph.add_node("wait_for_clarification", wait_for_clarification_node)
    
    # Add the conditional edges
    graph.add_conditional_edges(
        "explain_query",
        should_continue_to_sql,
        {
            "generate_sql": "generate_sql",
            "wait_for_feedback": "wait_for_feedback",
            "wait_for_clarification": "wait_for_clarification"
        }
    )
    
    graph.add_conditional_edges(
        "wait_for_feedback",
        should_continue_to_sql,
        {
            "generate_sql": "generate_sql",
            "wait_for_feedback": "wait_for_feedback",
            "wait_for_clarification": "wait_for_clarification"
        }
    )
    
    # Add the regular edges
    graph.add_edge("generate_sql", "execute_query")
    graph.add_edge("execute_query", END)
    graph.add_edge("wait_for_clarification", "explain_query")
    
    # Set the entry point
    graph.set_entry_point("explain_query")
    
    return graph

# Helper functions for Streamlit
def initialize_state(query: str) -> GraphState:
    """Initialize the graph state with a new query."""
    return {
        "original_query": query,
        "current_query": query,
        "sql_intent": None,
        "intent_explanation": None,
        "query_explanation": None,
        "user_feedback": None,
        "generated_sql": None,
        "query_result": None,
        "error_message": None,
        "_recursion_count": 0,
        "conversation_history": [
            {"role": "user", "content": query}
        ]
    }

def update_state_with_feedback(state: GraphState, feedback: str) -> GraphState:
    """Update the state with user feedback."""
    # Ensure _recursion_count is preserved
    recursion_count = state.get("_recursion_count", 0)
    
    return {
        **state,
        "user_feedback": feedback,
        "_recursion_count": recursion_count,
        "conversation_history": state["conversation_history"] + [
            {"role": "user", "content": f"Feedback: {feedback}"}
        ]
    }

def update_state_with_clarification(state: GraphState, clarification: str) -> GraphState:
    """Update the state with a clarified query."""
    # Ensure _recursion_count is preserved
    recursion_count = state.get("_recursion_count", 0)
    
    return {
        **state,
        "current_query": clarification,
        "user_feedback": None,  # Reset feedback
        "_recursion_count": recursion_count,
        "conversation_history": state["conversation_history"] + [
            {"role": "user", "content": f"Clarification: {clarification}"}
        ]
    }

def detect_sql_intent_node(state: GraphState) -> GraphState:
    """Node that detects if the user's query is intended for SQL generation."""
    logger.info(f"detect_sql_intent_node called with state: {state}")
    
    # Get the current query
    query = state["current_query"]
    
    # Get the LLM
    llm = get_llm()
    
    # Create a prompt for intent detection
    intent_prompt = f"""
You are an intent detection agent for a Text-to-SQL system. Your job is to determine if the user's query is asking for information that can be answered with an SQL query against a database.

The database contains information about a company, including employees, departments, projects, and sales data.

User query: "{query}"

First, analyze if this query is asking for information from a database. Then respond with one of these options:
1. SQL_INTENT: Yes, this query is asking for database information and can be answered with SQL.
2. NOT_SQL_INTENT: No, this query is not asking for database information or cannot be answered with SQL.

For each option, provide a brief explanation of your reasoning.

Format your response as:
INTENT: [SQL_INTENT or NOT_SQL_INTENT]
EXPLANATION: [Your explanation]
"""
    
    # Generate the intent detection
    response = llm.invoke(intent_prompt).content
    
    # Parse the response
    intent = "NOT_SQL_INTENT"  # Default to not SQL intent
    explanation = "I couldn't determine if your query is asking for database information."
    
    # Extract the intent and explanation using regex
    import re
    intent_match = re.search(r'INTENT:\s*(SQL_INTENT|NOT_SQL_INTENT)', response)
    explanation_match = re.search(r'EXPLANATION:\s*(.*?)(?:\n|$)', response, re.DOTALL)
    
    if intent_match:
        intent = intent_match.group(1)
    
    if explanation_match:
        explanation = explanation_match.group(1).strip()
    
    # Update the state with the intent detection result
    state["sql_intent"] = intent == "SQL_INTENT"
    state["intent_explanation"] = explanation
    
    # Add the intent detection to the conversation history
    if intent == "SQL_INTENT":
        state["conversation_history"].append({
            "role": "assistant",
            "content": "✓ I can answer this query with SQL. Let me analyze what you're asking for."
        })
    else:
        state["conversation_history"].append({
            "role": "assistant",
            "content": f"⚠️ I'm not sure if your query is asking for database information. {explanation}\n\nCould you please rephrase your question to clearly ask for specific information from the database?"
        })
    
    return state

def wait_for_feedback_node(state: GraphState) -> GraphState:
    """Node that waits for user feedback."""
    logger.info(f"wait_for_feedback_node called with state: {state}")
    
    # Just return the state unchanged, but increment recursion count
    recursion_count = state.get("_recursion_count", 0) + 1
    
    # If we've been waiting too long, force an end to avoid infinite recursion
    if recursion_count > 3:
        logger.warning(f"Forcing end of feedback wait after {recursion_count} iterations")
        # Add a message to let the user know we're proceeding without feedback
        state["conversation_history"].append({
            "role": "assistant",
            "content": "I haven't received feedback, but I'll proceed with generating SQL based on my understanding."
        })
        return {**state, "_recursion_count": recursion_count, "_force_end": True, "user_feedback": "good"}
    
    return {**state, "_recursion_count": recursion_count}

# Define a simpler graph that still waits for user feedback
def build_simple_graph() -> StateGraph:
    """Build a simplified graph for the Text2SQL assistant."""
    # Create a new graph
    graph = StateGraph(GraphState)
    
    # Add the nodes
    graph.add_node("detect_intent", detect_sql_intent_node)
    graph.add_node("explain_query", explain_query_node)
    graph.add_node("wait_for_feedback", wait_for_feedback_node)
    graph.add_node("generate_sql", generate_sql_node)
    # Remove execute_query node from the graph
    
    # Add conditional edges from intent detection
    graph.add_conditional_edges(
        "detect_intent",
        lambda state: "explain_query" if state.get("sql_intent", False) else END,
        {
            "explain_query": "explain_query",
            END: END
        }
    )
    
    # Add conditional edges from explain_query to wait_for_feedback
    graph.add_conditional_edges(
        "explain_query",
        lambda state: "wait_for_feedback",
        {
            "wait_for_feedback": "wait_for_feedback"
        }
    )
    
    # Add conditional edges from wait_for_feedback
    graph.add_conditional_edges(
        "wait_for_feedback",
        lambda state: "generate_sql" if state.get("user_feedback") == "good" or state.get("_force_end") else END,
        {
            "generate_sql": "generate_sql",
            END: END
        }
    )
    
    # Add the final edge from generate_sql to END (instead of execute_query)
    graph.add_edge("generate_sql", END)
    
    # Set the entry point
    graph.set_entry_point("detect_intent")
    
    return graph 