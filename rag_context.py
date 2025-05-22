from schema_parser import SchemaParser
from schema_embedding_store import SchemaEmbeddingStore
from typing import Dict, Any, List, Optional
import re

class RAGContextProvider:
    """Provide relevant context for SQL generation using RAG approach with embeddings."""
    
    def __init__(self, ddl_file_path: str):
        """Initialize with the path to the DDL file."""
        self.schema_parser = SchemaParser(ddl_file_path)
        self.embedding_store = SchemaEmbeddingStore(ddl_file_path)
        self.full_schema = self.schema_parser.get_formatted_schema()
    
    def get_relevant_context(self, query: str) -> Dict[str, Any]:
        """Get relevant schema context based on the query using embeddings."""
        # Get relevant statements using embeddings
        relevant_statements = self.embedding_store.search(query, top_k=5)
        
        # Also get relevant tables using the existing method
        relevant_schema = self.schema_parser.search_schema(query)
        
        # Combine the information
        formatted_context = self._format_relevant_schema(relevant_schema)
        statement_context = self._format_relevant_statements(relevant_statements)
        
        return {
            "relevant_schema": formatted_context,
            "relevant_statements": statement_context,
            "full_schema": self.full_schema,
            "relevant_tables": list(relevant_schema["tables"].keys()),
            "query_terms": self._extract_query_terms(query)
        }
    
    def _format_relevant_schema(self, relevant_schema: Dict[str, Any]) -> str:
        """Format the relevant schema for the LLM."""
        formatted = "Relevant Database Schema:\n\n"
        
        for table_name, columns in relevant_schema["tables"].items():
            formatted += f"Table: {table_name}\n"
            for column in columns:
                formatted += f"  - {column['name']} ({column['type']})\n"
            formatted += "\n"
        
        if relevant_schema["relationships"]:
            formatted += "Relationships:\n"
            for rel in relevant_schema["relationships"]:
                formatted += f"  - {rel['source_table']}.{rel['source_column']} -> {rel['target_table']}.{rel['target_column']}\n"
        
        return formatted
    
    def _format_relevant_statements(self, statements: List[Dict[str, Any]]) -> str:
        """Format the relevant DDL statements for the LLM."""
        if not statements:
            return ""
            
        formatted = "Relevant Schema Statements:\n\n"
        
        for stmt in statements:
            formatted += f"-- {stmt['type']} for table {stmt['table']}\n"
            formatted += f"{stmt['text']}\n\n"
        
        return formatted
    
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