"""
Debug script to isolate search function issues in Streamlit environment
"""
import streamlit as st
import logging
from pathlib import Path
from rag_context import RAGContextProvider

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def debug_search_functions():
    """Debug the search functions step by step"""
    
    st.title("Search Functions Debug")
    
    # Initialize components - using same path as test
    ddl_path = "data/database_schema.sql"
    
    st.write(f"**DDL Path:** {ddl_path}")
    st.write(f"**DDL Exists:** {Path(ddl_path).exists()}")
    
    if not Path(ddl_path).exists():
        st.error(f"DDL file not found at {ddl_path}")
        return
    
    try:
        rag_provider = RAGContextProvider(ddl_file_path=ddl_path)
        st.success("RAG Provider initialized successfully")
    except Exception as e:
        st.error(f"Failed to initialize RAG Provider: {e}")
        return
    
    # Test query - same as test file
    test_query = "How many appointments were scheduled for each day last week?"
    
    st.subheader("1. Full RAG Context Test")
    
    # Test the main method like the test does
    try:
        rag_context_data = rag_provider.get_relevant_context(test_query)
        
        st.write("**RAG Context Data Keys:**")
        st.write(list(rag_context_data.keys()))
        
        relevant_schema = rag_context_data.get("relevant_schema", "")
        formatted_statements = rag_context_data.get("formatted_relevant_statements", "")
        
        st.write("**Relevant Schema:**")
        if relevant_schema:
            st.code(relevant_schema, language="sql")
        else:
            st.warning("No relevant schema found")
        
        st.write("**Formatted Relevant Statements:**")
        if formatted_statements and formatted_statements.strip() and "not found" not in formatted_statements.lower():
            st.code(formatted_statements, language="text")
            st.success("✅ Relevant statements found!")
        else:
            st.error("❌ No relevant statements found")
            st.write(f"Raw value: '{formatted_statements}'")
            
    except Exception as e:
        st.error(f"Error in get_relevant_context: {e}")
        import traceback
        st.code(traceback.format_exc())
    
    st.subheader("2. Component Analysis")
    
    # Check if we can access the internal components
    try:
        if hasattr(rag_provider, 'schema_parser'):
            parser = rag_provider.schema_parser
            st.write(f"**Schema Parser Tables:** {len(parser.tables) if hasattr(parser, 'tables') else 'Unknown'}")
            if hasattr(parser, 'tables'):
                st.write(f"**Table Names:** {list(parser.tables.keys())}")
        
        if hasattr(rag_provider, 'embedding_store'):
            store = rag_provider.embedding_store
            st.write(f"**Embedding Store Type:** {type(store).__name__}")
            if hasattr(store, 'vector_store') and store.vector_store:
                st.write(f"**Vector Store Type:** {type(store.vector_store).__name__}")
                # Try to get vector count
                try:
                    if hasattr(store.vector_store, 'index'):
                        st.write(f"**Vector Count:** {store.vector_store.index.ntotal}")
                    elif hasattr(store.vector_store, '_index'):
                        st.write(f"**Vector Count:** {store.vector_store._index.ntotal}")
                except:
                    st.write("**Vector Count:** Unable to determine")
            else:
                st.warning("Vector store not initialized")
                
    except Exception as e:
        st.error(f"Error analyzing components: {e}")

if __name__ == "__main__":
    debug_search_functions() 