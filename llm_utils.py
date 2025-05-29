import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_community.utilities import SQLDatabase
from langchain.prompts import PromptTemplate
from pydantic_models import QueryExplanation, SQLOutput, QueryResult
import json
import pathlib
import logging
import re
import sqlite3
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

def create_db_connection(db_path):
    """Create a connection to the SQLite database."""
    return SQLDatabase.from_uri(f"sqlite:///{db_path}")

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

def validate_sql_query(sql_query, schema_path):
    """Validate an SQL query using LLM-based validation."""
    try:
        # Basic syntax validation
        if not re.match(r'^\s*SELECT', sql_query, re.IGNORECASE):
            return False, "Query must start with SELECT"
        
        # Check for common SQL syntax errors
        common_errors = [
            (r'FROM\s+(\w+)\s+WHERE\s+\1\.\w+', "Table referenced incorrectly in WHERE clause"),
            (r'JOIN\s+(\w+)\s+ON\s+\1\.\w+', "Table referenced incorrectly in JOIN clause"),
            (r'SELECT\s+\*\s+FROM\s+\w+\s+GROUP\s+BY', "Cannot use * with GROUP BY without aggregation"),
            (r'ORDER\s+BY\s+\d+', "ORDER BY using column position instead of name")
        ]
        
        for pattern, error_msg in common_errors:
            if re.search(pattern, sql_query, re.IGNORECASE):
                return False, error_msg
        
        # If no obvious errors, consider it valid
        return True, None
    except Exception as e:
        return False, str(e)

def execute_sql_query(query, db_path):
    """Execute a SQL query and return the results."""
    try:
        # Create a database connection
        conn = sqlite3.connect(db_path)
        
        # Execute the query
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Convert DataFrame to list of dictionaries
        data = df.to_dict(orient='records')
        
        return QueryResult(
            success=True,
            data=data,
            row_count=len(data),
            column_names=df.columns.tolist()
        )
    except Exception as e:
        return QueryResult(
            success=False,
            error_message=str(e)
        )

def create_text2sql_agent(db_path):
    """Create a Text2SQL agent using LangChain."""
    from langchain.chains import SQLDatabaseChain
    from langchain.prompts import PromptTemplate
    
    # Create a database connection
    db = create_db_connection(db_path)
    
    # Create an LLM
    llm = get_llm()
    
    # Create a prompt template
    prompt_file_path = pathlib.Path("prompts") / "text2sql_agent.txt"
    try:
        template_string = prompt_file_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        logger.error(f"Prompt file not found: {prompt_file_path}")
        raise FileNotFoundError(f"Prompt file not found: {prompt_file_path}. Please ensure it exists.")

    prompt = PromptTemplate(
        input_variables=["input", "table_info"],
        template=template_string
    )
    
    # Create a function to generate SQL
    def generate_sql(query):
        try:
            # Create a SQLDatabaseChain
            db_chain = SQLDatabaseChain.from_llm(
                llm=llm,
                db=db,
                prompt=prompt,
                verbose=True,
                return_intermediate_steps=True
            )
            
            # Run the chain
            result = db_chain(query)
            return result
        except Exception as e:
            return {
                "intermediate_steps": ["-- Error generating SQL query"],
                "result": f"Error: {str(e)}"
            }
    
    return generate_sql, db

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