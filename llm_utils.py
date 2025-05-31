import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain.prompts import PromptTemplate
from pydantic_models import QueryExplanation, SQLOutput, QueryResult
import json
import pathlib
import logging
import re
import pandas as pd
from db_setup import get_relevant_schema_context

# Load environment variables
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)

def get_llm(temperature=0):
    """Initialize and return the OpenAI LLM."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY environment variable not set")
    
    return ChatOpenAI(
        model="gpt-3.5-turbo",  # You can also use "gpt-4" if you have access
        api_key=api_key,
        temperature=temperature,
    )

def get_query_explanation_prompt():
    """Get the prompt template for explaining a query."""
    prompt_file_path = pathlib.Path("prompts") / "query_explanation.txt"
    try:
        template_string = prompt_file_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        logger.error(f"Prompt file not found: {prompt_file_path}")
        raise FileNotFoundError(f"Prompt file not found: {prompt_file_path}. Please ensure it exists.")
    
    return PromptTemplate(
        input_variables=["relevant_schema", "relevant_statements", "query"],
        template=template_string
    )

def get_sql_generation_prompt():
    """Get the prompt template for generating a SQL query."""
    prompt_file_path = pathlib.Path("prompts") / "sql_generation.txt"
    try:
        template_string = prompt_file_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        logger.error(f"Prompt file not found: {prompt_file_path}")
        raise FileNotFoundError(f"Prompt file not found: {prompt_file_path}. Please ensure it exists.")

    return PromptTemplate(
        input_variables=["relevant_schema", "relevant_statements", "explanation", "query"],
        template=template_string
    )

def parse_query_explanation(response_text):
    """Parse the LLM response into a QueryExplanation object."""
    try:
        # Extract JSON from the response
        json_match = re.search(r'```(?:json)?\s*(.*?)```', response_text, re.DOTALL)
        if json_match:
            json_str = json_match.group(1).strip()
        else:
            json_str = response_text.strip()
        
        # Clean up the JSON string
        json_str = re.sub(r'```.*?```', '', json_str, flags=re.DOTALL)
        
        print(f"Attempting to parse JSON: {json_str}")
        
        # Parse the JSON
        explanation_dict = json.loads(json_str)
        
        # Create a QueryExplanation object
        explanation = QueryExplanation(**explanation_dict)
        
        return explanation, None
    except Exception as e:
        return None, f"Error parsing explanation: {str(e)}"

def get_embeddings_model():
    """Returns the OpenAI embeddings model."""
    return OpenAIEmbeddings(
        model="text-embedding-3-small",  # or text-embedding-3-large
        openai_api_key=os.getenv("OPENAI_API_KEY")
    )

def embed_text(text):
    """Embeds a single text string using OpenAI."""
    embeddings_model = get_embeddings_model()
    return embeddings_model.embed_query(text)

def embed_texts(texts):
    """Embeds a list of text strings using OpenAI."""
    embeddings_model = get_embeddings_model()
    return embeddings_model.embed_documents(texts) 