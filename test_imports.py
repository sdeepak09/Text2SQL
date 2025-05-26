"""
Test script to verify imports and file structure
"""
import os
from pathlib import Path

def test_imports():
    print("=== IMPORT AND FILE STRUCTURE TEST ===")
    
    # Check current directory
    print(f"Current directory: {os.getcwd()}")
    
    # Check for key files
    files_to_check = [
        "rag_context.py",
        "schema_parser.py", 
        "schema_embedding_store.py",
        "data/database_schema.sql",
        "test_full_sql_generation.py"
    ]
    
    print("\nFile existence check:")
    for file_path in files_to_check:
        exists = Path(file_path).exists()
        print(f"  {file_path}: {'✅' if exists else '❌'}")
    
    # Try imports
    print("\nImport tests:")
    try:
        from rag_context import RAGContextProvider
        print("  rag_context.RAGContextProvider: ✅")
    except ImportError as e:
        print(f"  rag_context.RAGContextProvider: ❌ ({e})")
    
    try:
        from schema_parser import SchemaParser
        print("  schema_parser.SchemaParser: ✅")
    except ImportError as e:
        print(f"  schema_parser.SchemaParser: ❌ ({e})")
    
    try:
        from schema_embedding_store import SchemaEmbeddingStore
        print("  schema_embedding_store.SchemaEmbeddingStore: ✅")
    except ImportError as e:
        print(f"  schema_embedding_store.SchemaEmbeddingStore: ❌ ({e})")
    
    # Test basic initialization if possible
    print("\nBasic initialization test:")
    try:
        ddl_path = "data/database_schema.sql"
        if Path(ddl_path).exists():
            from rag_context import RAGContextProvider
            rag_provider = RAGContextProvider(ddl_file_path=ddl_path)
            print("  RAGContextProvider initialization: ✅")
        else:
            print(f"  RAGContextProvider initialization: ❌ (DDL file not found)")
    except Exception as e:
        print(f"  RAGContextProvider initialization: ❌ ({e})")

if __name__ == "__main__":
    test_imports() 