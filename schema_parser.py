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
    
    def get_elements_for_embedding(self) -> List[Dict[str, Any]]:
        """Extract schema elements for embedding."""
        elements = []
        
        # Process tables and columns
        for table_name, columns_list in self.tables.items():
            # Table element
            table_content = f"Database table named '{table_name}'. It contains the following columns: {', '.join([col['name'] for col in columns_list])}."
            if table_name == "Claims":
                table_content += " This table contains records of patient claims, which can correspond to appointments or service encounters. It includes dates and links to patients and providers."
            elif table_name == "Claim_Lines":
                table_content += " This table provides detailed lines for each claim, including specific services or procedures performed, relevant to appointments. It includes service dates."
            
            elements.append({
                "type": "table",
                "name": table_name,
                "content": table_content,
                "metadata": {"table_name": table_name, "columns": columns_list}
            })
            
            # Column elements
            for column_dict in columns_list:
                column_content = f"Database column named '{column_dict['name']}' of type '{column_dict['type']}', part of the table '{table_name}'."
                if table_name == "Claims" and column_dict['name'] == "Claim_Date":
                    column_content += " This date likely represents when the claim was filed or the primary date of service for the claim/appointment."
                elif table_name == "Claim_Lines" and column_dict['name'] == "Service_Date":
                    column_content += " This date specifies when a particular service or procedure on the claim line was rendered, corresponding to an appointment or part of one."
                
                elements.append({
                    "type": "column",
                    "name": column_dict['name'],
                    "content": column_content,
                    "metadata": {"table_name": table_name, "column_name": column_dict['name'], "column_type": column_dict['type']}
                })
        
        # Process relationships
        for rel_dict in self.relationships:
            elements.append({
                "type": "relationship",
                "name": f"{rel_dict['source_table']}.{rel_dict['source_column']}_to_{rel_dict['target_table']}.{rel_dict['target_column']}",
                "content": f"Database relationship: The column '{rel_dict['source_column']}' in table '{rel_dict['source_table']}' is linked to column '{rel_dict['target_column']}' in table '{rel_dict['target_table']}'.",
                "metadata": rel_dict
            })
            
        return elements
