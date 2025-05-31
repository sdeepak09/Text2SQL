from typing import TypedDict, List, Optional, Dict, Any, Literal
from langgraph.graph import StateGraph, END
from pydantic_models import QueryExplanation, SQLOutput, QueryResult
from llm_utils import (
    get_llm, get_query_explanation_prompt, get_sql_generation_prompt,
    parse_query_explanation, get_relevant_schema_context # Removed validate_sql_query, execute_sql_query
)
from db_setup import get_formatted_schema
import json
import logging
import pathlib
from langchain_openai import OpenAIEmbeddings
from langchain.vectorstores import FAISS
from langchain.schema import Document

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

# Replace any existing embedding model with OpenAI
embeddings = OpenAIEmbeddings(model="text-embedding-3-small")  # or text-embedding-3-large for better quality

# Define the graph nodes
def explain_query_node(state: GraphState) -> GraphState:
    """Node that explains the query."""
    logger.info(f"explain_query_node: Called with current_query: '{state['current_query'][:100]}...'")
    
    query = state["current_query"]
    llm = get_llm()
    
    logger.debug("explain_query_node: Retrieving relevant schema context...")
    context = get_relevant_schema_context(query)
    relevant_schema = context["relevant_schema"]
    relevant_statements = context.get("relevant_statements", "") # Ensure it's a string, even if empty

    # Log details of the context
    logger.debug(f"explain_query_node: Type of relevant_schema: {type(relevant_schema)}, Length: {len(relevant_schema) if isinstance(relevant_schema, (str, list, dict)) else 'N/A'}")
    logger.debug(f"explain_query_node: Snippet of relevant_schema: {str(relevant_schema)[:200]}...")
    logger.debug(f"explain_query_node: Type of relevant_statements: {type(relevant_statements)}, Length: {len(relevant_statements) if isinstance(relevant_statements, (str, list, dict)) else 'N/A'}")
    logger.debug(f"explain_query_node: Snippet of relevant_statements: {str(relevant_statements)[:200]}...")
    logger.debug(f"explain_query_node: Type of query: {type(query)}, Query: {query}")

    try: # Outer try for the whole explanation generation
        logger.debug("explain_query_node: Getting query explanation prompt template...")
        prompt_template = get_query_explanation_prompt()
        
        # ---> ADD NEW LOGGING HERE <---
        logger.debug("------------- PROMPT TEMPLATE TO BE FORMATTED -------------")
        logger.debug(f"TEMPLATE STRING: {prompt_template.template}")
        logger.debug("---------------------------------------------------------")
        logger.debug("------------- RELEVANT SCHEMA FOR FORMATTING -------------")
        logger.debug(relevant_schema) # Log full content
        logger.debug("----------------------------------------------------------")
        logger.debug("------------- RELEVANT STATEMENTS FOR FORMATTING -------------")
        logger.debug(relevant_statements) # Log full content
        logger.debug("--------------------------------------------------------------")
        logger.debug("------------- QUERY FOR FORMATTING -------------")
        logger.debug(query) # Log full content
        logger.debug("------------------------------------------------")
        
        formatted_prompt_str = "" # Initialize
        
        try:
            logger.debug("explain_query_node: Attempting to format prompt...")
            formatted_prompt_str = prompt_template.format(
                relevant_schema=relevant_schema,
                relevant_statements=relevant_statements,
                query=query
            )
            logger.debug(f"explain_query_node: Successfully formatted prompt. Length: {len(formatted_prompt_str)}")
            logger.debug(f"explain_query_node: Formatted prompt (first 500 chars): {formatted_prompt_str[:500]}...")
        except Exception as format_exception:
            logger.error(f"explain_query_node: ERROR DURING PROMPT FORMATTING: {format_exception}", exc_info=True)
            logger.error(f"explain_query_node: String of format_exception: {str(format_exception)}")
            # This error will be caught by the outer try/except, which logs str(e) and sets fallback
            raise # Rethrow to be caught by the outer try/except

        logger.debug("explain_query_node: Invoking LLM for query explanation...")
        response = llm.invoke(formatted_prompt_str)
        # Log the raw response immediately
        logger.debug(f"explain_query_node: Raw LLM response content: {response.content}")
        
        # The existing print(f"LLM response: {response.content}") can be removed if too verbose for production
        # print(f"LLM response: {response.content}") # Retaining for now as per instructions
        
        logger.debug("explain_query_node: Attempting to parse LLM response.")
        explanation, error = parse_query_explanation(response.content)
        
        if error:
            logger.warning(f"explain_query_node: Error parsing explanation from LLM: {error}")
            # Fallback logic when 'error' from parse_query_explanation is set
            explanation_dict = {
                "explanation": f"I'll try to answer your question about: '{query}'", # Using current query
                "tables": [], # Ensure these are present as per QueryExplanation model, even if empty
                "columns": []
            }
            state["query_explanation"] = explanation_dict
            state["conversation_history"].append({
                "role": "assistant",
                "type": "simple_explanation", # Using consistent type
                "content": f"I understand your query as follows:\n\n{explanation_dict['explanation']}\n(Note: There was an issue parsing the detailed explanation.)"
            })
        else:
            logger.info("explain_query_node: Successfully parsed LLM explanation.")
            explanation_dict = explanation.dict() # Pydantic model to dict
            state["query_explanation"] = explanation_dict
            
            query_summary = explanation_dict.get("query_summary_llm")
            step_by_step_breakdown = explanation_dict.get("step_by_step_breakdown_llm")

            if query_summary and step_by_step_breakdown:
                logger.debug("explain_query_node: Using new structured explanation for conversation history.")
                state["conversation_history"].append({
                    "role": "assistant",
                    "type": "query_understanding",
                    "summary": query_summary,
                    "breakdown": step_by_step_breakdown,
                    "structured_explanation_raw": explanation_dict
                })
            elif explanation_dict.get("summary_of_understanding"):
                logger.debug("explain_query_node: Falling back to 'summary_of_understanding' for conversation history.")
                explanation_text = explanation_dict["summary_of_understanding"]
                state["conversation_history"].append({
                    "role": "assistant",
                    "type": "simple_explanation",
                    "content": f"I understand your query as follows:\n\n{explanation_text}"
                })
            else:
                logger.warning("explain_query_node: No detailed summary found in parsed explanation. Using generic fallback.")
                fallback_text = f"I'll try to answer your question about: '{query}'"
                state["conversation_history"].append({
                    "role": "assistant",
                    "type": "simple_explanation",
                    "content": fallback_text
                })

    except Exception as e: # Outer catch-all
        logger.error(f"explain_query_node: Unhandled error during query explanation: {str(e)}", exc_info=True)
        # Provide a fallback explanation in case of any error
        explanation_dict = {
            "explanation": f"Attempting to answer: '{query}'",
            "tables": [],
            "columns": []
        }
        state["query_explanation"] = explanation_dict
        state["conversation_history"].append({
            "role": "assistant",
            "type": "simple_explanation", # Consistent type
            "content": f"I'll try to answer your question about: '{query}'\n\n(Note: I encountered an unexpected issue while analyzing your query, but I'll do my best.)"
        })
    
    logger.info("explain_query_node: Exiting.")
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
        
        # (after sql_query = response.content.strip())
        logger.info(f"Successfully generated SQL (validation step removed): {sql_query}")
        state["generated_sql"] = {"sql_query": sql_query}
        # Format SQL for better readability (this can be kept)
        formatted_sql = sql_query 
        state["conversation_history"].append({
            "role": "assistant",
            "content": f"Here's the SQL query to answer your question:\n\n```sql\n{formatted_sql}\n```"
        })
        # No error handling here for invalid SQL for now, as validation was removed.
        # The responsibility shifts to the LLM to produce valid SQL based on good context.
    
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
    # graph.add_node("execute_query", execute_query_node) # Removed
    
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
    # graph.add_edge("generate_sql", "execute_query") # Removed
    graph.add_edge("generate_sql", END) # New edge
    # graph.add_edge("execute_query", END) # Removed
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
    prompt_file_path = pathlib.Path("prompts") / "intent_detection.txt"
    try:
        template_string = prompt_file_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        logger.error(f"Prompt file not found: {prompt_file_path}")
        raise FileNotFoundError(f"Prompt file not found: {prompt_file_path}. Please ensure it exists.")
    
    intent_prompt_formatted = template_string.format(query=query) 
    
    # Generate the intent detection
    response = llm.invoke(intent_prompt_formatted).content
    
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