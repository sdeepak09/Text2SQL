import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain.sql_database import SQLDatabase
from langchain.prompts import PromptTemplate
import time
import hashlib
import json

# Load environment variables
load_dotenv()

# Create a simple cache
query_cache = {}

def get_llm():
    """Initialize and return the OpenAI LLM."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY environment variable not set")
    
    return ChatOpenAI(
        model="gpt-3.5-turbo",  # You can also use "gpt-4" if you have access
        api_key=api_key,
        temperature=0.1,  # Lower temperature for more deterministic SQL generation
    )

def create_db_connection(db_path):
    """Create a connection to the SQLite database."""
    return SQLDatabase.from_uri(f"sqlite:///{db_path}")

def create_text2sql_agent(db_path):
    """Create a custom Text2SQL function using the LLM directly."""
    db = create_db_connection(db_path)
    llm = get_llm()
    
    # Get the database schema
    db_schema = db.get_table_info()
    
    # Create a prompt template for SQL generation
    template = """
    You are a SQL expert. Given the following database schema and a question, 
    generate a SQLite SQL query that answers the question.
    
    Database Schema:
    {schema}
    
    Question: {question}
    
    SQL Query (only provide the SQL query, no explanation):
    """
    
    prompt = PromptTemplate(
        input_variables=["schema", "question"],
        template=template,
    )
    
    def generate_sql(question):
        """Generate SQL from a natural language question with caching."""
        # Create a cache key from the question
        cache_key = hashlib.md5(question.encode()).hexdigest()
        
        # Check if we have a cached result
        if cache_key in query_cache:
            return query_cache[cache_key]
        
        # If not in cache, generate the SQL
        formatted_prompt = prompt.format(schema=db_schema, question=question)
        
        try:
            sql_query = llm.invoke(formatted_prompt).content.strip()
            
            # Execute the query
            import sqlite3
            import pandas as pd
            
            try:
                conn = sqlite3.connect(db_path)
                df = pd.read_sql_query(sql_query, conn)
                conn.close()
                
                # Generate a human-readable explanation of the results
                explanation_prompt = f"""
                The following SQL query was executed:
                {sql_query}
                
                It returned {len(df)} rows of data.
                
                Please explain these results in a clear, concise way that answers the original question: {question}
                """
                
                explanation = llm.invoke(explanation_prompt).content
                
                result = {
                    "intermediate_steps": [sql_query],
                    "result": explanation
                }
                
                # Cache the result
                query_cache[cache_key] = result
                
                return result
            except Exception as e:
                result = {
                    "intermediate_steps": [sql_query],
                    "result": f"Error executing query: {str(e)}"
                }
                return result
        except Exception as e:
            return {
                "intermediate_steps": ["-- Error generating SQL query"],
                "result": f"Error: {str(e)}"
            }
    
    return generate_sql, db 