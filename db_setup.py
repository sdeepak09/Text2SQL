import sqlite3
import os
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import random
from rag_context import RAGContextProvider

# Ensure data directory exists
os.makedirs('data', exist_ok=True)

# Database path
DB_PATH = 'data/company.db'

# Initialize the RAG context provider
rag_provider = RAGContextProvider()

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