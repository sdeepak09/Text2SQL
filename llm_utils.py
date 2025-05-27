import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_community.utilities import SQLDatabase
from langchain.prompts import PromptTemplate
from pydantic_models import QueryExplanation, SQLOutput, QueryResult
import json
import re
import sqlite3
import pandas as pd
from db_setup import get_relevant_schema_context

# Load environment variables
load_dotenv()

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
    template = """
You are an expert in SQL and database schema analysis. Your task is to analyze a natural language query and explain how it relates to the database schema.

{relevant_schema}

{relevant_statements}

User Query: {query}

Analyze the query and provide a structured explanation in JSON format with the following fields:
- query_summary_llm: A user-facing natural language summary of what the query is asking for.
- step_by_step_breakdown_llm: A user-facing natural language step-by-step plan of how you will approach generating the SQL query.
- identified_intent: A brief description of what the user is asking for (e.g., 'retrieve data', 'aggregate data').
- target_tables: An array of table names that are relevant to the query.
- target_columns: An array of column names that should be included in the result or used in calculations.
- filter_conditions: List of filter objects (e.g., `[{{""column"": ""age"", ""operator"": "">"", ""value"": 30}}]`) or an empty list `[]` if no filters apply.
- join_conditions: List of join objects (e.g., `[{{""table1"": ""Orders"", ""column1"": ""CustomerID"", ""table2"": ""Customers"", ""column2"": ""ID""}}]`) or an empty list `[]` if no joins.
- group_by: List of column names to group by (e.g., `["department"]`), or an empty list `[]` if no grouping.
- order_by: An order_by object (e.g., `{{""column"": ""name"", ""direction"": ""ASC""}}`) or `null` if no ordering is needed.
- limit: An integer specifying the number of results (e.g., `10`) or `null` if no limit.
- summary_of_understanding: A concise technical summary of your understanding, focusing on how the query maps to schema elements.

Return your response **only in the specified JSON format**. Ensure the JSON is well-formed.
Example for the new fields:
  "query_summary_llm": "You want to find all employees in the 'Sales' department who were hired after January 1, 2022, and list their names and hire dates.",
  "step_by_step_breakdown_llm": "1. I will select the employee's name and hire date. 2. I will look at the 'Employees' table. 3. I will filter for employees in the 'Sales' department. 4. I will further filter for employees hired after 2022-01-01. 5. The results will be presented as requested.",

Make sure to include these new fields along with the existing ones in your JSON response.
Return your response in this JSON format:
"""
    return PromptTemplate(
        input_variables=["relevant_schema", "relevant_statements", "query"],
        template=template
    )

def get_sql_generation_prompt():
    """Get the prompt template for generating a SQL query."""
    template = """
You are an expert SQL developer. Your task is to generate a T-SQL query for Microsoft SQL Server based on a natural language query and its structured explanation.

{relevant_schema}

{relevant_statements}

Original Query: {query}

Query Explanation:
{explanation}

Generate a valid T-SQL query that answers the original query based on the explanation provided.
Only return the SQL query without any additional explanation or markdown formatting.
Ensure the query is compatible with Microsoft SQL Server syntax.
"""
    return PromptTemplate(
        input_variables=["relevant_schema", "relevant_statements", "explanation", "query"],
        template=template
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
    prompt = PromptTemplate(
        input_variables=["input", "table_info"],
        template="""
You are a SQL expert. Given an input question, create a syntactically correct SQLite query to run.

Only use the following tables:
{table_info}

Question: {input}
SQL Query:"""
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