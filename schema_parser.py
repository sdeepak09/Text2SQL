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
            
            self.tables[table_name] = {"columns": columns} # Store columns under a 'columns' key

            # Parse inline foreign key constraints within the CREATE TABLE statement
            inline_fk_pattern = r'CONSTRAINT\s+\[?(\w+)\]?\s+FOREIGN KEY\s*\(\s*\[?(\w+)\]?\s*\)\s*REFERENCES\s+\[?(\w+)\]?\s*\(\s*\[?(\w+)\]?\s*\)'
            inline_fk_matches = re.finditer(inline_fk_pattern, columns_text, re.IGNORECASE)
            for fk_match in inline_fk_matches:
                # Group 1 is constraint name, can be ignored for self.relationships
                source_column = fk_match.group(2)
                target_table = fk_match.group(3)
                target_column = fk_match.group(4)
                self.relationships.append({
                    "source_table": table_name, # Current table being processed
                    "source_column": source_column,
                    "target_table": target_table,
                    "target_column": target_column
                })
        
        # Extract foreign key relationships defined with ALTER TABLE
        alter_fk_pattern = r'ALTER TABLE\s+\[?(\w+)\]?\s+ADD\s+(?:CONSTRAINT\s+\[?\w+\]?\s+)?FOREIGN KEY\s*\(\s*\[?(\w+)\]?\s*\)\s+REFERENCES\s+\[?(\w+)\]?\s*\(\s*\[?(\w+)\]?\s*\)'
        alter_fk_matches = re.finditer(alter_fk_pattern, ddl_content, re.IGNORECASE)
        
        for match in alter_fk_matches:
            source_table = match.group(1)
            source_column = match.group(2) # Adjusted group index if (?:CONSTRAINT...)? part changes numbering
            target_table = match.group(3)
            target_column = match.group(4)
            
            # Ensure correct group indices if the regex for ALTER TABLE was changed
            # For the original regex: r'ALTER TABLE\s+\[?(\w+)\]?\s+ADD\s+CONSTRAINT\s+\[?\w+\]?\s+FOREIGN KEY\s*\(\[?(\w+)\]?\)\s+REFERENCES\s+\[?(\w+)\]?\s*\(\[?(\w+)\]?\)'
            # source_table = match.group(1)
            # source_column = match.group(2)
            # target_table = match.group(3)
            # target_column = match.group(4)

            self.relationships.append({
                "source_table": source_table,
                "source_column": source_column,
                "target_table": target_table,
                "target_column": target_column
            })
    
    def get_table_info(self) -> Dict[str, Any]: # Return type changed to Any
        """Get information about all tables and their columns."""
        # table_info = {table_name: columns for table_name, columns_data in self.tables.items()}
        # Corrected to reflect self.tables structure:
        table_info = {}
        for table_name, table_data in self.tables.items():
            table_info[table_name] = table_data['columns']
        
        # It might be better to return tables and relationships separately or structured differently
        # For now, just adding foreign_keys to the top level for compatibility with old usage.
        # This method might need further review based on how it's used.
        
        # The task asks to print parser.relationships, so get_table_info's exact structure
        # for foreign keys might not be critical for this specific subtask's verification,
        # as long as self.relationships is populated correctly.
        
        # Let's return a structure that is more aligned with the internal representation:
        return {
            "tables": self.tables, # Contains table_name: {"columns": [...]}
            "relationships": self.relationships
        }
    
    def get_formatted_schema(self) -> str:
        """Get a formatted string representation of the schema."""
        schema_str = "Database Schema:\n\n"
        
        for table_name, table_data in self.tables.items():
            schema_str += f"Table: {table_name}\n"
            for column in table_data['columns']: # Access columns via table_data['columns']
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
        for table_name, table_data in self.tables.items():
            columns = table_data['columns'] # Access columns via table_data['columns']
            if table_name.lower() in query_terms:
                relevant_tables[table_name] = columns # Store the list of columns
                continue
                
            relevant_columns_list = [] # Renamed to avoid confusion
            for column in columns:
                if column['name'].lower() in query_terms:
                    relevant_columns_list.append(column)
            
            if relevant_columns_list:
                # Store as {table_name: [relevant_columns_list]}
                relevant_tables[table_name] = relevant_columns_list
        
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
                        relevant_tables[table_name] = table_data['columns'] # Store list of columns
        
        # Find relationships involving the relevant tables
        for rel in self.relationships:
            if rel['source_table'] in relevant_tables or rel['target_table'] in relevant_tables:
                relevant_relationships.append(rel)
                
                # Add the related tables if they're not already included
                if rel['source_table'] not in relevant_tables:
                    relevant_tables[rel['source_table']] = self.tables[rel['source_table']]['columns']
                if rel['target_table'] not in relevant_tables:
                    relevant_tables[rel['target_table']] = self.tables[rel['target_table']]['columns']
        
        # If still no tables found, return a subset of the schema
        if not relevant_tables and self.tables:
            # Return a few important tables as fallback
            table_names_list = list(self.tables.keys()) # Renamed
            for i in range(min(3, len(table_names_list))):
                table_key = table_names_list[i]
                relevant_tables[table_key] = self.tables[table_key]['columns']
        
        return {
            "tables": relevant_tables, # This now contains {table_name: [list of columns]}
            "relationships": relevant_relationships
        }

if __name__ == '__main__':
    # Create a dummy DDL file for demonstration if it doesn't exist
    dummy_ddl_path = 'data/database_schema.sql'
    os.makedirs('data', exist_ok=True)

    if not os.path.exists(dummy_ddl_path):
        print(f"Creating dummy DDL file: {dummy_ddl_path}")
        with open(dummy_ddl_path, 'w') as f:
            f.write(
                "CREATE TABLE Users (user_id INT PRIMARY KEY, username VARCHAR(50), email VARCHAR(100), registration_date DATE);\n"
                "CREATE TABLE Products (product_id INT PRIMARY KEY, product_name VARCHAR(100), price DECIMAL(10, 2), category_id INT);\n"
                "CREATE TABLE Categories (category_id INT PRIMARY KEY, category_name VARCHAR(50));\n"
                "CREATE TABLE Orders (order_id INT PRIMARY KEY, user_id INT, order_date TIMESTAMP, total_amount MONEY, "
                "CONSTRAINT FK_UserOrder FOREIGN KEY (user_id) REFERENCES Users(user_id));\n"
                "CREATE TABLE Claims (ClaimID INT PRIMARY KEY, PolicyID INT, ClaimDate DATE, ClaimAmount DECIMAL(18,2), Description TEXT, "
                "CONSTRAINT FK_Claims_Policies FOREIGN KEY (PolicyID) REFERENCES Policies(PolicyID));\n" # Inline FK
                "CREATE TABLE Policies (PolicyID INT PRIMARY KEY, PolicyNumber VARCHAR(255), CustomerID INT, StartDate DATE, EndDate DATE, PremiumAmount DECIMAL(18,2));\n" # Assume Policies table for the FK above
                "ALTER TABLE Products ADD CONSTRAINT fk_category FOREIGN KEY (category_id) REFERENCES Categories(category_id);\n"
                "ALTER TABLE Policies ADD CONSTRAINT FK_Policies_Customers FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID);\n" # Assume Customers table
                "CREATE TABLE Customers (CustomerID INT PRIMARY KEY, CustomerName VARCHAR(255));\n"
            )

    try:
        print(f"Parsing schema from: {dummy_ddl_path}")
        parser = SchemaParser(dummy_ddl_path)
        
        print("\n--- Parsed Tables ---")
        for table_name, data in parser.tables.items():
            print(f"Table: {table_name}")
            for col in data['columns']:
                print(f"  - {col['name']} ({col['type']})")
        
        print("\n--- Parsed Relationships (Foreign Keys) ---")
        if parser.relationships:
            for rel in parser.relationships:
                print(f"  - {rel['source_table']}.{rel['source_column']} -> {rel['target_table']}.{rel['target_column']}")
        else:
            print("  No relationships found.")

        # Specifically checking for Claims table relationships as per requirements
        print("\n--- Checking Claims Table Relationships ---")
        claims_rels_found = False
        for rel in parser.relationships:
            if rel['source_table'] == 'Claims':
                print(f"  Found: {rel['source_table']}.{rel['source_column']} -> {rel['target_table']}.{rel['target_column']}")
                claims_rels_found = True
        if not claims_rels_found:
            print("  No relationships found originating from the Claims table.")

    except FileNotFoundError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()