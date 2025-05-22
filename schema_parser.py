import re
import os
from typing import Dict, List, Any, Optional

class SchemaParser:
    """Parse SQL DDL files to extract schema information."""
    
    def __init__(self, ddl_file_path: str):
        """Initialize with the path to the DDL file."""
        self.ddl_file_path = ddl_file_path
        self.tables = {}
        self.relationships = []
        self._parse_ddl()
    
    def _parse_ddl(self):
        """Parse the DDL file to extract table and column information."""
        if not os.path.exists(self.ddl_file_path):
            raise FileNotFoundError(f"DDL file not found: {self.ddl_file_path}")
        
        with open(self.ddl_file_path, 'r') as f:
            ddl_content = f.read()
        
        # Extract CREATE TABLE statements
        table_pattern = r'CREATE TABLE\s+\[?(\w+)\]?\s*\(([\s\S]*?)\);'
        table_matches = re.finditer(table_pattern, ddl_content, re.IGNORECASE)
        
        for match in table_matches:
            table_name = match.group(1)
            columns_text = match.group(2)
            
            # Extract columns
            columns = []
            column_pattern = r'\[?(\w+)\]?\s+(\w+(?:\(\d+(?:,\s*\d+)?\))?)\s*(?:,|$)'
            column_matches = re.finditer(column_pattern, columns_text, re.IGNORECASE)
            
            for col_match in column_matches:
                column_name = col_match.group(1)
                column_type = col_match.group(2)
                columns.append({
                    "name": column_name,
                    "type": column_type
                })
            
            self.tables[table_name] = columns
        
        # Extract foreign key relationships
        fk_pattern = r'ALTER TABLE\s+\[?(\w+)\]?\s+ADD\s+CONSTRAINT\s+\[?\w+\]?\s+FOREIGN KEY\s*\(\[?(\w+)\]?\)\s+REFERENCES\s+\[?(\w+)\]?\s*\(\[?(\w+)\]?\)'
        fk_matches = re.finditer(fk_pattern, ddl_content, re.IGNORECASE)
        
        for match in fk_matches:
            source_table = match.group(1)
            source_column = match.group(2)
            target_table = match.group(3)
            target_column = match.group(4)
            
            self.relationships.append({
                "source_table": source_table,
                "source_column": source_column,
                "target_table": target_table,
                "target_column": target_column
            })
    
    def get_table_info(self) -> Dict[str, List[Dict[str, str]]]:
        """Get information about all tables and their columns."""
        table_info = {table_name: columns for table_name, columns in self.tables.items()}
        table_info["foreign_keys"] = self.relationships
        return table_info
    
    def get_formatted_schema(self) -> str:
        """Get a formatted string representation of the schema."""
        schema_str = "Database Schema:\n\n"
        
        for table_name, columns in self.tables.items():
            schema_str += f"Table: {table_name}\n"
            for column in columns:
                schema_str += f"  - {column['name']} ({column['type']})\n"
            schema_str += "\n"
        
        if self.relationships:
            schema_str += "Relationships:\n"
            for rel in self.relationships:
                schema_str += f"  - {rel['source_table']}.{rel['source_column']} -> {rel['target_table']}.{rel['target_column']}\n"
        
        return schema_str
    
    def search_schema(self, query: str) -> Dict[str, Any]:
        """Search the schema for relevant tables and columns based on a query."""
        query_terms = set(re.findall(r'\b\w+\b', query.lower()))
        
        relevant_tables = {}
        relevant_relationships = []
        
        # Find relevant tables and columns
        for table_name, columns in self.tables.items():
            if table_name.lower() in query_terms:
                relevant_tables[table_name] = columns
                continue
                
            relevant_columns = []
            for column in columns:
                if column['name'].lower() in query_terms:
                    relevant_columns.append(column)
            
            if relevant_columns:
                relevant_tables[table_name] = relevant_columns
        
        # If no direct matches, include tables with semantic similarity
        if not relevant_tables:
            # Simple heuristic for semantic matching
            semantic_matches = {
                "employee": ["employees", "staff", "personnel"],
                "department": ["departments", "divisions", "teams"],
                "salary": ["compensation", "pay", "wage"],
                "sale": ["sales", "revenue", "transaction"],
                "product": ["products", "items", "goods"],
                "customer": ["customers", "clients", "buyers"]
            }
            
            for term in query_terms:
                for key, values in semantic_matches.items():
                    if term in values or key == term:
                        for table_name, columns in self.tables.items():
                            if key in table_name.lower():
                                relevant_tables[table_name] = columns
        
        # Find relationships involving the relevant tables
        for rel in self.relationships:
            if rel['source_table'] in relevant_tables or rel['target_table'] in relevant_tables:
                relevant_relationships.append(rel)
                
                # Add the related tables if they're not already included
                if rel['source_table'] not in relevant_tables:
                    relevant_tables[rel['source_table']] = self.tables[rel['source_table']]
                if rel['target_table'] not in relevant_tables:
                    relevant_tables[rel['target_table']] = self.tables[rel['target_table']]
        
        # If still no tables found, return a subset of the schema
        if not relevant_tables and self.tables:
            # Return a few important tables as fallback
            table_names = list(self.tables.keys())
            for i in range(min(3, len(table_names))):
                relevant_tables[table_names[i]] = self.tables[table_names[i]]
        
        return {
            "tables": relevant_tables,
            "relationships": relevant_relationships
        } 