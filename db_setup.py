import sqlite3
import os
import shutil
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import random
from rag_context import RAGContextProvider

# Ensure data directory exists
os.makedirs('data', exist_ok=True)

# Database path
DB_PATH = 'data/company.db'

FAISS_STORE_PATH = "data/schema_embeddings_faiss/" # Define the path

# Ensure data directory exists for the FAISS store path itself
os.makedirs(os.path.dirname(FAISS_STORE_PATH), exist_ok=True)

if os.path.exists(FAISS_STORE_PATH):
    print(f"--- Deleting existing FAISS store at {FAISS_STORE_PATH} for a clean startup ---")
    shutil.rmtree(FAISS_STORE_PATH)
else:
    print(f"--- No existing FAISS store found at {FAISS_STORE_PATH}. Proceeding with creation. ---")

# Get the directory where db_setup.py is located
_current_dir = os.path.dirname(os.path.abspath(__file__))
# Construct the absolute path to the DDL file relative to db_setup.py
# Assuming 'data/database_schema.sql' is in a subdirectory 'data'
# at the same level as the directory containing db_setup.py,
# or more robustly, if 'data' is at the project root and db_setup.py is also at the project root.
# Given the project structure, 'data' is a top-level directory.
# If db_setup.py is in the root, this is:
# _project_root = os.path.dirname(os.path.abspath(__file__)) # if db_setup.py is in root
# _ddl_file_path = os.path.join(_project_root, "data", "database_schema.sql")

# Let's assume db_setup.py is in the project root directory alongside the 'data' folder.
_project_root = os.path.dirname(os.path.realpath(__file__)) # Get directory of db_setup.py
_ddl_file_path = os.path.join(_project_root, "data", "database_schema.sql")
# Normalize the path to handle any OS differences
_ddl_file_path = os.path.normpath(_ddl_file_path)
print(f"--- [db_setup.py] Using DDL file path: {_ddl_file_path} ---") # Diagnostic print

# Initialize the RAG context provider
rag_provider = RAGContextProvider(ddl_file_path=_ddl_file_path)

def initialize_database():
    """Initialize the database (not needed with RAG approach)."""
    return "data/database_schema.sql"

def get_table_info():
    """Get information about all tables and their columns."""
    return rag_provider.get_table_info()

def get_formatted_schema():
    """Get a formatted string representation of the schema."""
    return rag_provider.full_schema

def get_relevant_schema_context(query: str):
    """Get relevant schema context for a query."""
    return rag_provider.get_relevant_context(query)

def get_formatted_schema():
    """Get a formatted string representation of the database schema."""
    table_info = get_table_info()
    
    schema_str = "Database Schema:\n\n"
    
    # Add table and column information
    for table_name, columns in table_info.items():
        if table_name != 'foreign_keys':
            schema_str += f"Table: {table_name}\n"
            for col in columns:
                pk_str = " (Primary Key)" if col['pk'] else ""
                null_str = " NOT NULL" if col['notnull'] else ""
                schema_str += f"  - {col['name']} ({col['type']}){pk_str}{null_str}\n"
            schema_str += "\n"
    
    # Add foreign key information
    schema_str += "Foreign Keys:\n"
    for fk in table_info.get('foreign_keys', []):
        schema_str += f"  - {fk['table']}.{fk['from']} -> {fk['to']}\n"
    
    return schema_str

if __name__ == "__main__":
    initialize_database()
    print("Table information:", get_table_info())
    print("\nFormatted Schema:")
    print(get_formatted_schema()) 