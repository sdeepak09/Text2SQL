import json # Added import
from schema_parser import SchemaParser
from schema_embedding_store import SchemaEmbeddingStore
from typing import Dict, Any, List, Optional
import re
import logging

# Configure logging at INFO level
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RAGContextProvider:
    """Provide relevant context for SQL generation using RAG approach with embeddings."""
    
    def __init__(self, ddl_file_path: str):
        """Initialize with the path to the DDL file."""
        self.schema_parser = SchemaParser(ddl_file_path)
        # Only log important initialization info
        logger.info("Initializing RAGContextProvider with schema parser")
        
        self.embedding_store = SchemaEmbeddingStore("data/schema_embeddings_faiss/")
        self.full_schema = self.schema_parser.get_formatted_schema()

        # Populate FAISS store with schema elements
        schema_elements_for_embedding = self.schema_parser.get_elements_for_embedding()
        if schema_elements_for_embedding:
            self.embedding_store.add_schema_elements(schema_elements_for_embedding)
            logger.info(f"Added {len(schema_elements_for_embedding)} schema elements to embedding store")
        else:
            logger.warning("No schema elements found to add to the embedding store")
    
    def get_relevant_context(self, query: str) -> Dict[str, str]:
        """Get relevant context for a query."""
        # Get relevant schema elements (returns list of table names)
        relevant_table_names = self.schema_parser.search_schema(query)
        
        # Get relevant statements from embeddings
        relevant_statements = self.embedding_store.search(query, k=5)
        
        # Format the context
        formatted_schema = self._format_relevant_schema(relevant_table_names)
        formatted_statements = self._format_relevant_statements(relevant_statements)
        
        return {
            "relevant_schema": formatted_schema,
            "formatted_relevant_statements": formatted_statements,
            "raw_relevant_statements": relevant_statements
        }
    
    def _format_relevant_schema(self, relevant_table_names) -> str:
        """Format the relevant schema for display."""
        # Handle case where relevant_table_names is a list of table names
        if isinstance(relevant_table_names, list):
            if not relevant_table_names:
                return "No relevant schema found."
            
            formatted_parts = []
            
            for table_name in relevant_table_names:
                if table_name in self.schema_parser.tables:
                    columns = self.schema_parser.tables[table_name]
                    formatted_parts.append(f"Table: {table_name}")
                    for column in columns:
                        formatted_parts.append(f"  - {column['name']} ({column['type']})")
                    formatted_parts.append("")  # Empty line between tables
            
            return "\n".join(formatted_parts)
        
        # Handle legacy dict format if it exists
        elif isinstance(relevant_table_names, dict) and "tables" in relevant_table_names:
            formatted_parts = []
            for table_name, columns in relevant_table_names["tables"].items():
                formatted_parts.append(f"Table: {table_name}")
                for column in columns:
                    formatted_parts.append(f"  - {column['name']} ({column['type']})")
                formatted_parts.append("")
            return "\n".join(formatted_parts)
        
        else:
            return "No relevant schema found."
    
    def _format_relevant_statements(self, relevant_statements: List[Dict[str, Any]]) -> str:
        """Format the relevant statements for display."""
        if not relevant_statements:
            return "Relevant statements not found in context."
        
        formatted_parts = []
        for i, statement in enumerate(relevant_statements, 1):
            if isinstance(statement, dict) and "content" in statement:
                formatted_parts.append(f"{i}. {statement['content']}")
            elif isinstance(statement, str):
                formatted_parts.append(f"{i}. {statement}")
            else:
                formatted_parts.append(f"{i}. {str(statement)}")
        
        return "\n".join(formatted_parts)
    
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