import re
import os
from typing import Dict, List, Any, Optional
import logging

# Configure logging at INFO level
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        # Updated table_pattern to handle optional schema like [dbo]. or dbo.
        table_pattern = r'CREATE TABLE\s+((?:\[?dbo\]?\.)?\[?\w+\]?)\s*\(([\s\S]*?)\);'
        table_matches = re.finditer(table_pattern, ddl_content, re.IGNORECASE)
        
        for match in table_matches:
            full_table_name_with_brackets = match.group(1)
            columns_text = match.group(2)

            # Remove schema prefix "dbo." or "[dbo]." and brackets for the clean table name
            cleaned_name = full_table_name_with_brackets
            if cleaned_name.lower().startswith(("[dbo].", "dbo.")):
                parts = cleaned_name.split('.', 1)
                if len(parts) > 1: # Check if split produced more than one part
                    cleaned_name = parts[1]
            table_name = cleaned_name.replace('[', '').replace(']', '')
            
            # Extract columns - New line-by-line parsing logic
            columns = []
            # Regex to capture column name and type from the start of a line.
            # Group 1: Column Name
            # Group 2: Full Data Type (e.g., "NVARCHAR(10)", "UNIQUEIDENTIFIER")
            line_col_pattern = r'^\s*\[?(\w+)\]?\s+([a-zA-Z_][\w\s]*(?:\(\s*(?:\d+|MAX|\d+\s*,\s*\d+)\s*\))?)'

            lines = columns_text.splitlines()
            for line in lines:
                trimmed_line = line.strip()
                
                # Skip empty lines, comment lines, or constraint definitions (FKs are handled separately by inline_fk_pattern)
                if not trimmed_line or trimmed_line.startswith('--') or trimmed_line.upper().startswith('CONSTRAINT'):
                    continue

                col_match = re.match(line_col_pattern, trimmed_line, re.IGNORECASE)
                if col_match:
                    column_name = col_match.group(1)
                    column_type = col_match.group(2).strip() # Ensure type is stripped of trailing spaces
                    
                    # Basic filtering for common SQL keywords that might be accidentally captured
                    common_sql_keywords = {"PRIMARY", "KEY", "NOT", "NULL", "DEFAULT", "CHECK", "UNIQUE", "FOREIGN", "REFERENCES", "CONSTRAINT"}
                    if column_name.upper() in common_sql_keywords:
                        # This is likely a mis-parse, skip this line
                        # print(f"Warning: Possible mis-parse of column name '{column_name}' in table '{table_name}'. Line: '{trimmed_line}'")
                        continue

                    columns.append({
                        "name": column_name,
                        "type": column_type
                    })
                # else:
                    # Optional: print a warning for lines that were not skipped but didn't match column pattern
                    # print(f"Warning: Line did not match column pattern in table '{table_name}': '{trimmed_line}'")
            
            self.tables[table_name] = columns # Assign the newly parsed columns

            # New: Parse inline foreign keys from columns_text (original multi-line block)
            # This pattern assumes FK constraints are defined after all column definitions or intermingled.
            # It needs to be robust enough not to misinterpret column definitions.
            inline_fk_pattern = r'CONSTRAINT\s+\[?(\w*)\]?\s+FOREIGN\s+KEY\s*\(\s*\[?(\w+)\]?\s*\)\s*REFERENCES\s+(?:\[?(dbo)\]?\.)?\[?(\w+)\]?\s*\(\s*\[?(\w+)\]?\s*\)'
            fk_matches_inline = re.finditer(inline_fk_pattern, columns_text, re.IGNORECASE | re.MULTILINE)
            current_table_name = table_name # The table being processed

            for fk_match in fk_matches_inline:
                # fk_constraint_name = fk_match.group(1) # e.g., FK_Claims_Patients (optional capture)
                source_column = fk_match.group(2)    # e.g., Patient_ID
                # target_schema_prefix = fk_match.group(3) # 'dbo' or None, captured by (dbo)?
                target_table_raw = fk_match.group(4) # e.g., Patients
                target_column = fk_match.group(5)    # e.g., Patient_ID

                # Clean target_table_raw name (remove potential schema and brackets)
                # The regex for target_table_raw is just (\w+), so it should be clean.
                # If it could capture [dbo].[Table], similar cleaning as source table_name would be needed.
                # For now, assume target_table_raw is clean as per (\w+).
                
                self.relationships.append({
                    "source_table": current_table_name,
                    "source_column": source_column,
                    "target_table": target_table_raw, 
                    "target_column": target_column
                })

        # Comment out or delete old ALTER TABLE foreign key parsing
        # fk_pattern = r'ALTER TABLE\s+\[?(\w+)\]?\s+ADD\s+CONSTRAINT\s+\[?\w+\]?\s+FOREIGN KEY\s*\(\[?(\w+)\]?\)\s+REFERENCES\s+\[?(\w+)\]?\s*\(\[?(\w+)\]?\)'
        # fk_matches_alter = re.finditer(fk_pattern, ddl_content, re.IGNORECASE)
        
        # for match in fk_matches_alter:
        #     source_table = match.group(1)
        #     source_column = match.group(2)
        #     target_table = match.group(3)
        #     target_column = match.group(4)
            
        #     self.relationships.append({
        #         "source_table": source_table,
        #         "source_column": source_column,
        #         "target_table": target_table,
        #         "target_column": target_column
        #     })
    
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
    
    def search_schema(self, query: str) -> List[str]:
        """Search for relevant tables based only on table/column name matches."""
        query_lower = query.lower()
        relevant_tables = set()
        query_tokens = set(re.findall(r'\w+', query_lower))

        # Match query tokens with table names (partial/fuzzy match)
        for table_name in self.tables.keys():
            table_name_lower = table_name.lower()
            if any(token in table_name_lower for token in query_tokens):
                relevant_tables.add(table_name)

        # Match query tokens with column names (partial/fuzzy match)
        for table_name, columns_list in self.tables.items():
            for column_dict in columns_list:
                column_name_lower = column_dict['name'].lower()
                if any(token in column_name_lower for token in query_tokens):
                    relevant_tables.add(table_name)

        # If nothing found, return all tables (or empty list if you prefer)
        if not relevant_tables:
            return list(self.tables.keys())
        return list(relevant_tables)
    
    def get_elements_for_embedding(self) -> List[Dict[str, Any]]:
        """Extract schema elements for embedding, using only schema structure."""
        elements = []

        # Tables and columns
        for table_name, columns_list in self.tables.items():
            # Table element (generic description)
            table_content = f"Table: {table_name} with columns: {', '.join([col['name'] for col in columns_list])}."
            elements.append({
                "type": "table",
                "name": table_name,
                "content": table_content,
                "metadata": {"table_name": table_name, "columns": columns_list}
            })

            # Column elements (generic description)
            for column_dict in columns_list:
                column_content = (
                    f"Column: {column_dict['name']} of type {column_dict['type']}, part of table {table_name}."
                )
                elements.append({
                    "type": "column",
                    "name": column_dict['name'],
                    "content": column_content,
                    "metadata": {
                        "table_name": table_name,
                        "column_name": column_dict['name'],
                        "column_type": column_dict['type']
                    }
                })

        # Foreign key relationships (generic)
        for rel_dict in self.relationships:
            rel_content = (
                f"Relationship: {rel_dict['source_table']}.{rel_dict['source_column']} "
                f"references {rel_dict['target_table']}.{rel_dict['target_column']}."
            )
            elements.append({
                "type": "relationship",
                "name": f"{rel_dict['source_table']}.{rel_dict['source_column']}_to_{rel_dict['target_table']}.{rel_dict['target_column']}",
                "content": rel_content,
                "metadata": rel_dict
            })

        return elements
