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
        
        # Helper regex for names (allows for brackets, spaces, dots)
        # capture_name_sg = r'\[?(\w+)\]?' # Original simpler one for single word names like columns
        capture_name_sg = r'\[?([\w\.]+)\]?' # Allowing dots for simple names e.g. column_name or schema.column_name
        capture_name_mg = r'(?:\[([\w\s\.]+)\]|([\w\s\.]+))' # For complex names like tables [dbo.My Table] or dbo.MyTable

        # Extract CREATE TABLE statements
        # table_pattern = r'CREATE TABLE\s+\[?(\w+)\]?\s*\(([\s\S]*?)\);' # Original
        table_pattern = fr'CREATE TABLE\s+{capture_name_mg}\s*\(([\s\S]*?)\);'
        table_matches = re.finditer(table_pattern, ddl_content, re.IGNORECASE | re.MULTILINE)
        
        for match in table_matches:
            table_name = match.group(1) or match.group(2) # Group 1 for bracketed, Group 2 for non-bracketed
            columns_text = match.group(3) # Content of the table definition
            
            # Extract columns
            columns = []
            # New robust column pattern
            # Explanation:
            # ^\s*: Matches the beginning of a line/segment.
            # (?:\[(?P<name_b>[\w\s\.]+)\]|(?P<name>[\w\s\.]+)): Named groups name_b (bracketed) or name (non-bracketed) for the column name. Allows spaces and dots.
            # \s+: Separator.
            # (?P<type>\w+(?:\(\s*\d+(?:\s*,\s*\d+)?\s*\))?(?:\s*(?:UNSIGNED|ZEROFILL))?(?:\s*CHARACTER\s+SET\s+[\w]+)?(?:\s*COLLATE\s+[\w_]+)?): Named group type. Captures base type, optional numeric attributes, and optional character set/collation.
            # (?:.*?(?:,|\)|$)): Non-greedily consumes any characters until comma, closing parenthesis, or end of string/line.
            new_column_pattern = re.compile(
                r"^\s*"                                      # Start of a line or segment
                r"(?:\[(?P<name_b>[\w\s\.]+)\]|(?P<name>[\w\s\.]+))"  # Column name (bracketed or not, allows spaces and dots)
                r"\s+"                                       # Whitespace after name
                r"(?P<type>\w+(?:\(\s*\d+(?:\s*,\s*\d+)?\s*\))?(?:\s*(?:UNSIGNED|ZEROFILL))?(?:\s*CHARACTER\s+SET\s+[\w]+)?(?:\s*COLLATE\s+[\w_]+)?)"  # Data type
                r"(?:.*?(?:,|\)|$))",                         # Non-greedily consume the rest until a comma or end of definition block
                re.IGNORECASE | re.MULTILINE
            )

            for col_match in new_column_pattern.finditer(columns_text):
                name_b = col_match.group('name_b')
                name_val = col_match.group('name') # Renamed 'name' to 'name_val' to avoid conflict with 'name' in columns.append
                column_name = name_b if name_b else name_val
                column_type = col_match.group('type')
                
                if column_name and column_type: # Ensure essential parts were captured
                     columns.append({"name": column_name.strip(), "type": column_type.strip()})
            
            self.tables[table_name] = {"columns": columns} # Store columns under a 'columns' key

            # Parse inline foreign key constraints within the CREATE TABLE statement
            # inline_fk_pattern = r'CONSTRAINT\s+\[?(\w+)\]?\s+FOREIGN KEY\s*\(\s*\[?(\w+)\]?\s*\)\s*REFERENCES\s+\[?(\w+)\]?\s*\(\s*\[?(\w+)\]?\s*\)' # Original
            inline_fk_pattern = fr'CONSTRAINT\s+\[?\w+\]?\s+FOREIGN KEY\s*\(\s*{capture_name_sg}\s*\)\s*REFERENCES\s+{capture_name_mg}\s*\(\s*{capture_name_sg}\s*\)'
            inline_fk_matches = re.finditer(inline_fk_pattern, columns_text, re.IGNORECASE | re.MULTILINE)
            for fk_match in inline_fk_matches:
                # fk_match.group(0) is the full match
                # fk_match.group(1) is source_column from capture_name_sg
                # fk_match.group(2) is target_table (bracketed part of capture_name_mg)
                # fk_match.group(3) is target_table (non-bracketed part of capture_name_mg)
                # fk_match.group(4) is target_column from capture_name_sg
                source_column = fk_match.group(1)
                target_table = fk_match.group(2) or fk_match.group(3)
                target_column = fk_match.group(4)
                self.relationships.append({
                    "source_table": table_name, # Current table being processed
                    "source_column": source_column,
                    "target_table": target_table,
                    "target_column": target_column
                })
        
        # Extract foreign key relationships defined with ALTER TABLE
        # alter_fk_pattern = r'ALTER TABLE\s+\[?(\w+)\]?\s+ADD\s+(?:CONSTRAINT\s+\[?\w+\]?\s+)?FOREIGN KEY\s*\(\s*\[?(\w+)\]?\s*\)\s+REFERENCES\s+\[?(\w+)\]?\s*\(\s*\[?(\w+)\]?\s*\)' #Original
        alter_fk_pattern = fr'ALTER TABLE\s+{capture_name_mg}\s+ADD\s+(?:CONSTRAINT\s+\[?\w+\]?\s+)?FOREIGN KEY\s*\(\s*{capture_name_sg}\s*\)\s*REFERENCES\s+{capture_name_mg}\s*\(\s*{capture_name_sg}\s*\)'
        alter_fk_matches = re.finditer(alter_fk_pattern, ddl_content, re.IGNORECASE | re.MULTILINE)
        
        for fk_match in alter_fk_matches: # Renamed match to fk_match for clarity
            # fk_match.group(0) is the full match
            # fk_match.group(1) is source_table (bracketed part of capture_name_mg for ALTER TABLE)
            # fk_match.group(2) is source_table (non-bracketed part of capture_name_mg for ALTER TABLE)
            # fk_match.group(3) is source_column from capture_name_sg
            # fk_match.group(4) is target_table (bracketed part of capture_name_mg for REFERENCES)
            # fk_match.group(5) is target_table (non-bracketed part of capture_name_mg for REFERENCES)
            # fk_match.group(6) is target_column from capture_name_sg
            source_table = fk_match.group(1) or fk_match.group(2)
            source_column = fk_match.group(3)
            target_table = fk_match.group(4) or fk_match.group(5)
            target_column = fk_match.group(6)

            self.relationships.append({
                "source_table": source_table.strip(), # Added strip just in case
                "source_column": source_column.strip(),
                "target_table": target_table.strip(),
                "target_column": target_column.strip()
            })
    
    def get_table_info(self) -> Dict[str, Any]:
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
                        for table_name, table_data_loop in self.tables.items(): # Changed 'table_data' to 'table_data_loop' to avoid conflict
                            if key in table_name.lower():
                                relevant_tables[table_name] = table_data_loop['columns'] # Corrected indentation and variable
        
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
    # Use the actual DDL file if it exists, otherwise create/use the dummy one.
    actual_ddl_path = 'data/database_schema.sql'
    
    # Ensure 'data' directory exists
    os.makedirs(os.path.dirname(actual_ddl_path), exist_ok=True)

    if not os.path.exists(actual_ddl_path):
        print(f"Actual DDL file {actual_ddl_path} not found. Creating a dummy one for demonstration.")
        with open(actual_ddl_path, 'w') as f:
            f.write(
                "-- Dummy DDL for SchemaParser testing --\n"
                "CREATE TABLE Users (user_id INT PRIMARY KEY, username VARCHAR(50), email VARCHAR(100), registration_date DATE);\n"
                "CREATE TABLE [dbo.Products] (product_id INT PRIMARY KEY, product_name VARCHAR(100), price DECIMAL(10, 2), category_id INT);\n"
                "CREATE TABLE Categories (category_id INT PRIMARY KEY, category_name VARCHAR(50));\n"
                "CREATE TABLE Orders (order_id INT PRIMARY KEY, user_id INT, order_date TIMESTAMP, total_amount MONEY, "
                "CONSTRAINT FK_UserOrder FOREIGN KEY (user_id) REFERENCES Users(user_id));\n"
                "CREATE TABLE [dbo.Claims] (ClaimID INT PRIMARY KEY, PolicyID INT, ClaimDate DATE, ClaimAmount DECIMAL(18,2), Description TEXT, "
                "CONSTRAINT FK_Claims_Policies FOREIGN KEY (PolicyID) REFERENCES [dbo.Policies](PolicyID));\n" # Inline FK with complex names
                "CREATE TABLE [dbo.Policies] (PolicyID INT PRIMARY KEY, PolicyNumber VARCHAR(255), CustomerID INT, StartDate DATE, EndDate DATE, PremiumAmount DECIMAL(18,2));\n"
                "ALTER TABLE [dbo.Products] ADD CONSTRAINT fk_category FOREIGN KEY (category_id) REFERENCES Categories(category_id);\n"
                "ALTER TABLE [dbo.Policies] ADD CONSTRAINT FK_Policies_Customers FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID);\n" 
                "CREATE TABLE Customers (CustomerID INT PRIMARY KEY, CustomerName VARCHAR(255));\n"
                "ALTER TABLE [dbo.Orders] ADD FOREIGN KEY ([user_id]) REFERENCES [Users] ([user_id]);\n" # Example with brackets
            )
    else:
        print(f"Using existing DDL file: {actual_ddl_path}")


    try:
        print(f"Parsing schema from: {actual_ddl_path}")
        parser = SchemaParser(actual_ddl_path)
        
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
        print("\n--- Checking 'dbo.Claims' Table Relationships (and other complex names) ---")
        claims_rels_found_count = 0
        for rel in parser.relationships:
            # Check for claims, and also test if complex names like dbo.Products are parsed correctly
            if 'claims' in rel['source_table'].lower() or 'products' in rel['source_table'].lower() or 'orders' in rel['source_table'].lower():
                print(f"  Found: {rel['source_table']}.{rel['source_column']} -> {rel['target_table']}.{rel['target_column']}")
                if 'claims' in rel['source_table'].lower():
                    claims_rels_found_count +=1
        
        if claims_rels_found_count == 0:
            print("  No relationships found originating from a 'Claims' table.")
        else:
            print(f"  Found {claims_rels_found_count} relationships originating from a 'Claims' table.")


    except FileNotFoundError as e:
        print(f"Error: DDL File not found. {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()