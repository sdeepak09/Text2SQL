"""
Compare test environment vs Streamlit environment behavior
"""
import logging
from pathlib import Path
from rag_context import RAGContextProvider

# Configure detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_environment_comparison():
    """Run the same test as test_full_sql_generation.py with detailed logging"""
    
    print("=== ENVIRONMENT COMPARISON TEST ===")
    
    # Same setup as working test - using data/ prefix like the test
    ddl_path = "data/database_schema.sql"
    test_query = "How many appointments were scheduled for each day last week?"
    
    print(f"DDL Path: {ddl_path}")
    print(f"DDL Path exists: {Path(ddl_path).exists()}")
    print(f"DDL Path absolute: {Path(ddl_path).absolute()}")
    
    # Initialize RAG provider - using same parameter name as test
    print("\n1. Initializing RAG Provider...")
    rag_provider = RAGContextProvider(ddl_file_path=ddl_path)
    
    # Test the get_relevant_context method like the test does
    print("\n2. Testing get_relevant_context...")
    try:
        rag_context_data = rag_provider.get_relevant_context(test_query)
        print(f"RAG context data keys: {list(rag_context_data.keys())}")
        
        # Extract the components like the test does
        relevant_schema = rag_context_data.get("relevant_schema", "")
        formatted_relevant_statements = rag_context_data.get("formatted_relevant_statements", "")
        
        print(f"\nRelevant Schema length: {len(relevant_schema)} characters")
        print("Relevant Schema preview:")
        print(relevant_schema[:300] + "..." if len(relevant_schema) > 300 else relevant_schema)
        
        print(f"\nFormatted Relevant Statements length: {len(formatted_relevant_statements)} characters")
        print("Formatted Relevant Statements preview:")
        print(formatted_relevant_statements[:300] + "..." if len(formatted_relevant_statements) > 300 else formatted_relevant_statements)
        
        # Check for success indicators
        if "Relevant statements not found" in formatted_relevant_statements or not formatted_relevant_statements.strip():
            print("\n❌ FAILURE: No relevant statements found")
        else:
            print("\n✅ SUCCESS: Relevant statements found")
            
    except Exception as e:
        print(f"Error in get_relevant_context: {e}")
        import traceback
        traceback.print_exc()
    
    return rag_context_data if 'rag_context_data' in locals() else None

if __name__ == "__main__":
    test_environment_comparison() 