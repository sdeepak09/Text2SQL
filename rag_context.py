import os # Added import
from csv_schema_loader import CSVSchemaLoader, TableInfo, ColumnInfo, JoinInfo
# from schema_embedding_store import SchemaEmbeddingStore # Removed
from query_retriever import QueryRetriever # Added import
from typing import Dict, Any, List, Optional
import re
import logging # Added for logging warnings

logger = logging.getLogger(__name__) # Added for logging warnings

class RAGContextProvider:
    """Provide relevant context for SQL generation using RAG approach with QueryRetriever."""
    
    def __init__(self):
        """Initialize with CSVSchemaLoader."""
        self.csv_loader = CSVSchemaLoader(data_folder_path="data/")
        self.full_schema = self._get_full_schema_from_csv()

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

    def _get_full_schema_from_csv(self) -> str:
        """Constructs the full schema string from CSVSchemaLoader."""
        schema_parts = []
        tables = self.csv_loader.get_tables()
        if not tables:
            logger.warning("No tables found by CSVSchemaLoader. Full schema will be empty.")
            return "Database schema is empty or could not be loaded."

        schema_parts.append("Database Schema:")
        for table_info in tables:
            schema_parts.append(f"\nTable: {table_info.name}")
            if table_info.description:
                schema_parts.append(f"  Description: {table_info.description}")
            
            columns = self.csv_loader.get_columns_for_table(table_info.name)
            if columns:
                schema_parts.append("  Columns:")
                for col in columns:
                    col_desc = f"    - {col.column_name} ({col.data_type})"
                    if col.description:
                        col_desc += f" # {col.description}"
                    schema_parts.append(col_desc)
            else:
                schema_parts.append("  No columns defined for this table.")

        relationships = self.csv_loader.get_foreign_keys()
        if relationships:
            schema_parts.append("\nRelationships:")
            for rel in relationships:
                schema_parts.append(
                    f"  - {rel.primary_table_name}.{rel.primary_table_column} "
                    f"-> {rel.foreign_table_name}.{rel.foreign_table_column}"
                )
        else:
            schema_parts.append("\nNo relationships defined.")
            
        return "\n".join(schema_parts)

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
            
        relevant_schema_data = self._search_csv_schema(query)
        
        formatted_context = self._format_relevant_schema(relevant_schema_data)
        # statement_context = self._format_relevant_statements(relevant_statements) # Old
        statement_context = self._format_retrieved_documents(retrieved_docs) # New
        
        return {
            "relevant_schema": formatted_context, # This comes from _format_relevant_schema
            "relevant_statements": statement_context, # This now comes from _format_retrieved_documents
            "full_schema": self.full_schema,
            "relevant_tables": list(relevant_schema_data["tables"].keys()) if relevant_schema_data and "tables" in relevant_schema_data else [],
            "query_terms": self._extract_query_terms(query)
        }

    def _search_csv_schema(self, query: str) -> Dict[str, Any]:
        """Mimics search_schema using CSVSchemaLoader."""
        query_terms = {term.lower() for term in re.findall(r'\b\w+\b', query)}
        
        relevant_tables_data = {}
        all_identified_table_names = set()

        # Iterate through all tables to find matches in table names or column names
        for table_info in self.csv_loader.get_tables():
            table_name_lower = table_info.name.lower()
            table_added = False
            
            # Check if table name itself is in query terms
            if table_name_lower in query_terms:
                if table_info.name not in relevant_tables_data:
                    relevant_tables_data[table_info.name] = []
                    all_identified_table_names.add(table_info.name)
                table_added = True

            columns_for_table = self.csv_loader.get_columns_for_table(table_info.name)
            for col_info in columns_for_table:
                col_name_lower = col_info.column_name.lower()
                # Check if column name is in query terms or if table was already added (to include all its columns)
                if col_name_lower in query_terms or table_name_lower in query_terms:
                    if table_info.name not in relevant_tables_data:
                        relevant_tables_data[table_info.name] = []
                        all_identified_table_names.add(table_info.name)
                    
                    # Avoid duplicating columns if table was added due to its name, then a column matched
                    # This logic ensures columns are added once per table
                    found_col = any(c['name'] == col_info.column_name for c in relevant_tables_data[table_info.name])
                    if not found_col:
                         relevant_tables_data[table_info.name].append({
                            "name": col_info.column_name,
                            "type": col_info.data_type,
                            "description": col_info.description
                        })

        # Filter relationships based on identified tables
        relevant_relationships = []
        for join_info in self.csv_loader.get_foreign_keys():
            if (join_info.primary_table_name in all_identified_table_names and \
                join_info.foreign_table_name in all_identified_table_names):
                relevant_relationships.append({
                    "source_table": join_info.primary_table_name,
                    "source_column": join_info.primary_table_column,
                    "target_table": join_info.foreign_table_name,
                    "target_column": join_info.foreign_table_column,
                    "description": join_info.join_description 
                })
        
        return {
            "tables": relevant_tables_data,
            "relationships": relevant_relationships
        }

    def _format_relevant_schema(self, relevant_schema_data: Dict[str, Any]) -> str:
        """Format the relevant schema from CSVSchemaLoader for the LLM."""
        formatted = "Relevant Database Schema:\n"
        
        tables_data = relevant_schema_data.get("tables", {})
        if not tables_data: # Check if tables_data is empty
            formatted += "-- No relevant tables found for the query.\n"
        
        for table_name, columns in tables_data.items():
            formatted += f"\nTable: {table_name}\n"
            table_info = self.csv_loader.get_table_by_name(table_name)
            if table_info and table_info.description:
                 formatted += f"  Description: {table_info.description}\n"

            if columns:
                formatted += "  Columns:\n"
                for col_data in columns: # col_data is now a dict like {"name": ..., "type": ...}
                    col_desc_str = f"    - {col_data['name']} ({col_data['type']})"
                    # Include column description if available from _search_csv_schema
                    if col_data.get('description'):
                        col_desc_str += f" # {col_data['description']}"
                    formatted += col_desc_str + "\n"
            else:
                formatted += "  No columns found or selected for this table.\n"
            
        relationships_data = relevant_schema_data.get("relationships", [])
        if relationships_data:
            formatted += "\nRelationships:\n"
            for rel in relationships_data: # rel is already a dict
                formatted += (
                    f"  - {rel['source_table']}.{rel['source_column']} "
                    f"-> {rel['target_table']}.{rel['target_column']}"
                )
                if rel.get('description'):
                    formatted += f" # {rel['description']}"
                formatted += "\n"
        else:
            if tables_data: # Only add this if there were tables but no relationships
                 formatted += "\n-- No relevant relationships found for the selected tables.\n"
        
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
        """Get information about all tables and their columns using CSVSchemaLoader."""
        all_tables_info = {}
        tables = self.csv_loader.get_tables()
        if not tables:
            logger.warning("No tables found by CSVSchemaLoader for get_table_info.")
            return {}

        for table_info in tables:
            columns_data = []
            columns = self.csv_loader.get_columns_for_table(table_info.name)
            for col_info in columns:
                columns_data.append({
                    "name": col_info.column_name,
                    "type": col_info.data_type
                    # Optionally include description: col_info.description
                })
            all_tables_info[table_info.name] = columns_data
        return all_tables_info