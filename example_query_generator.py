"""
This script generates example SQL queries based on a DDL schema.
"""
import json
import random
from schema_parser import SchemaParser

class ExampleQueryGenerator:
    """
    Generates example SQL queries from a DDL file.
    """
    def __init__(self, ddl_file_path: str):
        """
        Initializes the ExampleQueryGenerator with a DDL file path.

        Args:
            ddl_file_path: The path to the DDL file.
        """
        self.schema_parser = SchemaParser(ddl_file_path)
        if not self.schema_parser.tables:
            # Handle case where schema parsing might have failed or yielded no tables
            print("Warning: SchemaParser found no tables. Query generation will be limited.")


    def _get_columns_by_type(self, table_name: str, col_type_check: callable) -> list[str]:
        """Helper to get column names of a specific type from a table."""
        if table_name not in self.schema_parser.tables:
            return []
        cols = []
        for col in self.schema_parser.tables[table_name]['columns']:
            if col_type_check(col['type'].lower()):
                cols.append(col['name'])
        return cols

    def _get_text_columns(self, table_name: str) -> list[str]:
        return self._get_columns_by_type(table_name, lambda t: any(s in t for s in ['char', 'text', 'varchar']))

    def _get_numeric_columns(self, table_name: str) -> list[str]:
        return self._get_columns_by_type(table_name, lambda t: any(s in t for s in ['int', 'num', 'dec', 'float', 'double', 'money', 'real']))

    def _get_date_columns(self, table_name: str) -> list[str]:
        return self._get_columns_by_type(table_name, lambda t: any(s in t for s in ['date', 'time']))

    def _get_random_table(self) -> str | None:
        """Selects a random table name from the schema."""
        table_names = list(self.schema_parser.tables.keys())
        return random.choice(table_names) if table_names else None
    
    def _get_random_columns(self, table_name: str, count: int = 2) -> list[str]:
        """Selects random columns from a table."""
        if table_name not in self.schema_parser.tables:
            return []
        
        available_columns = [col['name'] for col in self.schema_parser.tables[table_name]['columns']]
        if not available_columns:
            return []
        
        return random.sample(available_columns, min(len(available_columns), count))


    def generate_example_queries(self, num_queries_per_type: int = 3) -> list[str]:
        """
        Generates a list of diverse SQL query strings.
        """
        queries = []
        if not self.schema_parser.tables:
            return ["-- No tables found in schema to generate queries."]

        table_names = list(self.schema_parser.tables.keys())
        if not table_names:
            return ["-- No tables available for query generation."]

        # 1. Simple SELECT statements
        for _ in range(num_queries_per_type):
            table = self._get_random_table()
            if not table: continue

            # Query type 1.1: SELECT *
            queries.append(f"SELECT * FROM {table} LIMIT 10;")

            # Query type 1.2: SELECT col1, col2 WHERE text_col LIKE ...
            cols_for_select = self._get_random_columns(table, 2)
            text_cols = self._get_text_columns(table)
            if cols_for_select and text_cols:
                col_str = ", ".join(cols_for_select)
                text_col = random.choice(text_cols)
                queries.append(f"SELECT {col_str} FROM {table} WHERE {text_col} LIKE '%sample%' LIMIT 10;")

            # Query type 1.3: SELECT col1, col2 WHERE num_col > val ORDER BY date_col
            numeric_cols = self._get_numeric_columns(table)
            date_cols = self._get_date_columns(table)
            if cols_for_select and numeric_cols and date_cols:
                col_str = ", ".join(cols_for_select)
                num_col = random.choice(numeric_cols)
                date_col = random.choice(date_cols)
                queries.append(f"SELECT {col_str} FROM {table} WHERE {num_col} > 100 ORDER BY {date_col} DESC LIMIT 10;")
        
        # 2. JOIN statements
        for _ in range(num_queries_per_type):
            if not self.schema_parser.relationships: continue
            
            relationship = random.choice(self.schema_parser.relationships)
            from_table = relationship['source_table'] # Key corrected
            to_table = relationship['target_table']   # Key corrected
            from_col = relationship['source_column'] # Key corrected
            to_col = relationship['target_column']   # Key corrected

            # Ensure tables exist before trying to get columns
            if from_table not in self.schema_parser.tables or to_table not in self.schema_parser.tables:
                continue

            t1_cols = self._get_random_columns(from_table, 1)
            t2_cols = self._get_random_columns(to_table, 1)

            if t1_cols and t2_cols:
                queries.append(
                    f"SELECT T1.{t1_cols[0]}, T2.{t2_cols[0]} "
                    f"FROM {from_table} T1 "
                    f"JOIN {to_table} T2 ON T1.{from_col} = T2.{to_col} "
                    f"LIMIT 10;"
                )

        # 3. Aggregation statements
        for _ in range(num_queries_per_type):
            table = self._get_random_table()
            if not table: continue

            # Query type 3.1: SELECT COUNT(*)
            queries.append(f"SELECT COUNT(*) FROM {table};")
            
            numeric_cols = self._get_numeric_columns(table)
            # Use any column for grouping, preferably not the numeric one being aggregated
            all_cols = [col['name'] for col in self.schema_parser.tables[table]['columns']]
            
            if numeric_cols:
                num_col = random.choice(numeric_cols)
                
                group_by_cols = [col for col in all_cols if col != num_col] # Avoid grouping by the aggregated column
                # Prefer text or date columns for grouping
                potential_group_cols = self._get_text_columns(table) + self._get_date_columns(table)
                if not potential_group_cols: # fallback to any other column if no text/date
                    potential_group_cols = [col for col in all_cols if col != num_col]

                if potential_group_cols:
                    group_col = random.choice(potential_group_cols)
                    # Query type 3.2: SELECT group_by_col, SUM(numeric_col)
                    queries.append(f"SELECT {group_col}, SUM({num_col}) FROM {table} GROUP BY {group_col};")
                    # Query type 3.3: SELECT group_by_col, AVG(numeric_col)
                    queries.append(f"SELECT {group_col}, AVG({num_col}) FROM {table} GROUP BY {group_col};")

        # 4. Queries with multiple JOINs (basic attempt)
        # This is a simplified version. True multi-join requires graph traversal of relationships.
        if len(self.schema_parser.relationships) >= 2 and num_queries_per_type > 0:
            for _ in range(min(num_queries_per_type, len(self.schema_parser.relationships) -1 )): # Limit attempts
                # Pick two distinct relationships
                if len(self.schema_parser.relationships) < 2: break
                rel1, rel2 = random.sample(self.schema_parser.relationships, 2)

                # Try to chain them: T1 -> T2 -> T3
                # If rel1: T1.col1 -> T2.col2  AND rel2: T2.col3 -> T3.col4
                # Corrected keys for rel1 and rel2
                if rel1['target_table'] == rel2['source_table'] and \
                   rel1['source_table'] != rel2['target_table']: # Avoid self-joins for simplicity here
                    
                    t1 = rel1['source_table']
                    t2 = rel1['target_table'] # Same as rel2['source_table']
                    t3 = rel2['target_table']

                    t1_pk = rel1['source_column']
                    t2_fk_for_t1 = rel1['target_column']
                    t2_pk_for_t3 = rel2['source_column']
                    t3_fk = rel2['target_column']
                    
                    # Ensure tables exist
                    if not all(tbl in self.schema_parser.tables for tbl in [t1, t2, t3]):
                        continue

                    t1_sel_col = self._get_random_columns(t1, 1)
                    t3_sel_col = self._get_random_columns(t3, 1)

                    if t1_sel_col and t3_sel_col:
                        queries.append(
                            f"SELECT T1.{t1_sel_col[0]}, T3.{t3_sel_col[0]} "
                            f"FROM {t1} T1 "
                            f"JOIN {t2} T2 ON T1.{t1_pk} = T2.{t2_fk_for_t1} "
                            f"JOIN {t3} T3 ON T2.{t2_pk_for_t3} = T3.{t3_fk} "
                            f"LIMIT 10;"
                        )
        
        # Remove duplicates if any, while preserving order for consistency in testing/review
        seen = set()
        unique_queries = [q for q in queries if not (q in seen or seen.add(q))]
        return unique_queries


    def save_queries_to_file(self, queries: list[str], output_filepath: str = "data/example_queries.jsonl"):
        """
        Saves the list of query strings to the specified file in JSONL format.
        """
        # Ensure directory exists (though for this task, 'data/' should be there)
        # import os
        # os.makedirs(os.path.dirname(output_filepath), exist_ok=True)
        with open(output_filepath, 'w') as f:
            for query_str in queries:
                json.dump({"query": query_str}, f)
                f.write('\n')

if __name__ == '__main__':
    # Create a dummy DDL file for demonstration if it doesn't exist
    # This is just to make the example runnable without manual file creation.
    # In a real scenario, 'data/database_schema.sql' should be provided.
    try:
        with open('data/database_schema.sql', 'r') as f:
            pass # File exists
    except FileNotFoundError:
        print("Creating dummy 'data/database_schema.sql' for demonstration.")
        import os
        os.makedirs('data', exist_ok=True)
        with open('data/database_schema.sql', 'w') as f:
            f.write(
                "CREATE TABLE Users (user_id INT PRIMARY KEY, username VARCHAR(50), email VARCHAR(100), registration_date DATE);\n"
                "CREATE TABLE Products (product_id INT PRIMARY KEY, product_name VARCHAR(100), price DECIMAL(10, 2), category_id INT);\n"
                "CREATE TABLE Orders (order_id INT PRIMARY KEY, user_id INT, order_date TIMESTAMP, total_amount MONEY, "
                "FOREIGN KEY (user_id) REFERENCES Users(user_id));\n"
                "ALTER TABLE Products ADD CONSTRAINT fk_category FOREIGN KEY (category_id) REFERENCES Categories(category_id);\n" # Assume Categories table exists
                "CREATE TABLE Categories (category_id INT PRIMARY KEY, category_name VARCHAR(50));\n"
            )
        print("Dummy DDL created. Please re-run the script if SchemaParser previously failed due to missing file.")


    try:
        generator = ExampleQueryGenerator('data/database_schema.sql')
        # Check if tables were parsed
        if not generator.schema_parser.tables:
            print("No tables parsed from DDL. Cannot generate meaningful queries.")
            print("Please ensure 'data/database_schema.sql' is valid and contains CREATE TABLE statements.")
        else:
            print(f"Schema loaded. Tables: {list(generator.schema_parser.tables.keys())}")
            print(f"Relationships: {generator.schema_parser.relationships}")
            
            example_queries = generator.generate_example_queries(num_queries_per_type=2) # Generate 2 of each type

            print("\nGenerated Example Queries:")
            if not example_queries:
                print("-- No queries were generated. Check schema and generation logic.")
            for i, query in enumerate(example_queries):
                print(f"{i+1}. {query}")

            output_file = "data/example_queries.jsonl"
            generator.save_queries_to_file(example_queries, output_file)
            print(f"\nSuccessfully saved {len(example_queries)} queries to {output_file}")

    except FileNotFoundError:
        print("Error: The DDL file 'data/database_schema.sql' was not found.")
        print("Please create this file with your database schema or ensure the path is correct.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()
