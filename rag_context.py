import os # Added import
from schema_parser import SchemaParser
# from schema_embedding_store import SchemaEmbeddingStore # Removed
from query_retriever import QueryRetriever # Added import
from typing import Dict, Any, List, Optional
import re
import logging # Added for logging warnings

logger = logging.getLogger(__name__) # Added for logging warnings

class RAGContextProvider:
    """Provide relevant context for SQL generation using RAG approach with QueryRetriever."""
    
    def __init__(self, ddl_file_path: str):
        """Initialize with the path to the DDL file."""
        self.schema_parser = SchemaParser(ddl_file_path)
        self.full_schema = self.schema_parser.get_formatted_schema()

        openai_api_key = os.getenv("OPENAI_API_KEY")
        if not openai_api_key:
            # QueryRetriever will raise an error if API key is missing for its embeddings
            # Alternatively, we could raise an error here or log more prominently.
            logger.error("OPENAI_API_KEY not found in environment. QueryRetriever may fail to initialize.")
            # For now, we let QueryRetriever handle the error if api_key is vital for its __init__
            # If QueryRetriever is robust to this (e.g. for loading a pre-built index without needing embeddings at init)
            # then this is just a warning. Given QueryRetriever init, it will likely fail if API key is bad/missing.

        faiss_index_folder_path = "data/context_faiss_store_v1" 
        
        try:
            self.query_retriever = QueryRetriever(
                openai_api_key=openai_api_key, 
                faiss_index_folder_path=faiss_index_folder_path
            )
            if self.query_retriever.vector_store is None:
                logger.warning(f"QueryRetriever failed to load FAISS index from {faiss_index_folder_path}. Relevant statements retrieval will be impacted.")
        except Exception as e:
            logger.error(f"Error initializing QueryRetriever in RAGContextProvider: {e}", exc_info=True)
            # Depending on desired behavior, might want to make self.query_retriever None or re-raise
            # For now, if QueryRetriever fails, subsequent calls will likely fail.
            self.query_retriever = None # Ensure it's None if init fails badly
    
    def get_relevant_context(self, query: str) -> Dict[str, Any]:
        """Get relevant schema context based on the query using embeddings."""
        retrieved_docs = []
        if self.query_retriever:
            try:
                retrieved_docs = self.query_retriever.retrieve_relevant_documents(query, k=5)
            except Exception as e:
                logger.error(f"Error retrieving documents from QueryRetriever: {e}", exc_info=True)
                retrieved_docs = [] # Ensure it's an empty list on error
        else:
            logger.warning("QueryRetriever not initialized. Cannot retrieve documents.")
            
        relevant_schema = self.schema_parser.search_schema(query)
        
        formatted_context = self._format_relevant_schema(relevant_schema)
        # statement_context = self._format_relevant_statements(relevant_statements) # Old
        statement_context = self._format_retrieved_documents(retrieved_docs) # New
        
        return {
            "relevant_schema": formatted_context, # This comes from _format_relevant_schema
            "relevant_statements": statement_context, # This now comes from _format_retrieved_documents
            "full_schema": self.full_schema,
            "relevant_tables": list(relevant_schema["tables"].keys()),
            "query_terms": self._extract_query_terms(query)
        }
    
    def _format_relevant_schema(self, relevant_schema: Dict[str, Any]) -> str:
        """Format the relevant schema for the LLM. (This method remains largely the same)"""
        formatted = "Relevant Database Schema:\n\n"
        
        # Ensure relevant_schema["tables"] is a dictionary as expected
        tables_data = relevant_schema.get("tables", {})
        if not isinstance(tables_data, dict):
            logger.warning(f"Expected 'tables' to be a dict in relevant_schema, got {type(tables_data)}. Returning minimal schema.")
            return formatted # Or some default error string

        for table_name, columns in tables_data.items():
            formatted += f"Table: {table_name}\n"
            # Ensure columns is a list as expected
            if isinstance(columns, list):
                for column in columns:
                    if isinstance(column, dict) and 'name' in column and 'type' in column:
                         formatted += f"  - {column['name']} ({column['type']})\n"
                    else:
                        logger.warning(f"Skipping malformed column data for table {table_name}: {column}")
            else:
                logger.warning(f"Expected 'columns' to be a list for table {table_name}, got {type(columns)}")
            formatted += "\n"
        
        relationships_data = relevant_schema.get("relationships", [])
        if relationships_data: # Ensure it's a list and not empty
            formatted += "Relationships:\n"
            if isinstance(relationships_data, list):
                for rel in relationships_data:
                    if isinstance(rel, dict) and all(k in rel for k in ['source_table', 'source_column', 'target_table', 'target_column']):
                        formatted += f"  - {rel['source_table']}.{rel['source_column']} -> {rel['target_table']}.{rel['target_column']}\n"
                    else:
                        logger.warning(f"Skipping malformed relationship data: {rel}")
            else:
                logger.warning(f"Expected 'relationships' to be a list, got {type(relationships_data)}")

        return formatted
    
    # def _format_relevant_statements(self, statements: List[Dict[str, Any]]) -> str: # Old
    def _format_retrieved_documents(self, retrieved_docs: List[Dict[str, Any]]) -> str: # New
        if not retrieved_docs:
            return "-- No specific statements/examples retrieved." # Provide a clear message
        
        formatted_parts = []
        for doc in retrieved_docs:
            content = doc.get('content', '').strip() # Ensure content is stripped
            metadata = doc.get('metadata', {})
            meta_type = metadata.get('type', 'unknown')
            
            # Only add if content is not empty
            if content:
                if meta_type == 'table':
                    table_name = metadata.get('table_name', 'Unknown Table')
                    formatted_parts.append(f"-- Retrieved Table Description for {table_name}:\n{content}")
                elif meta_type == 'column':
                    table_name = metadata.get('table_name', 'Unknown Table')
                    column_name = metadata.get('column_name', 'Unknown Column')
                    formatted_parts.append(f"-- Retrieved Column Description for {table_name}.{column_name}:\n{content}")
                elif meta_type == 'example_query':
                    formatted_parts.append(f"-- Retrieved Example SQL Query:\n{content}")
                else: # Fallback for any other type or if type is missing in metadata
                    formatted_parts.append(f"-- Retrieved Context:\n{content}")
        
        if not formatted_parts: # If all docs had empty content or were filtered.
             return "-- No relevant statements/examples retrieved with content."

        return "\n\n".join(formatted_parts)

    def _extract_query_terms(self, query: str) -> List[str]:
        """Extract key terms from the query."""
        # Remove common words and keep potential database-related terms
        common_words = {"the", "a", "an", "in", "on", "at", "by", "for", "with", "about", "from", "to", "of"}
        terms = [term.lower() for term in re.findall(r'\b\w+\b', query) 
                if term.lower() not in common_words and len(term) > 2]
        return terms
    
    def get_table_info(self) -> Dict[str, List[Dict[str, str]]]:
        """Get information about all tables and their columns."""
        return self.schema_parser.get_table_info() 