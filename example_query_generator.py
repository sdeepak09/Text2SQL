"""
This script generates example SQL queries based on schema information from CSV files.
"""
import json
import random
# from schema_parser import SchemaParser # Removed
from csv_schema_loader import CSVSchemaLoader, TableInfo, ColumnInfo, JoinInfo # Added

class ExampleQueryGenerator:
    """
    Generates example SQL queries using schema data from CSVSchemaLoader.
    """
    def __init__(self, ddl_file_path: str = None, data_folder_path: str = "data/"): # ddl_file_path made optional
        """
        Initializes the ExampleQueryGenerator.
        ddl_file_path is now optional and not directly used.
        data_folder_path points to the directory containing CSV schema files.
        """
        # self.schema_parser = SchemaParser(ddl_file_path) # Removed
        self.csv_loader = CSVSchemaLoader(data_folder_path=data_folder_path) # Added
        if not self.csv_loader.get_tables(): # Updated check
            print("Warning: CSVSchemaLoader found no tables. Query generation will be limited.")


    def _get_columns_by_type(self, table_name: str, col_type_check: callable) -> list[str]:
        """Helper to get column names of a specific type from a table."""
        table_info = self.csv_loader.get_table_by_name(table_name)
        if not table_info:
            return []
        
        columns_for_table = self.csv_loader.get_columns_for_table(table_name)
        cols = []
        for col in columns_for_table: # Iterate through ColumnInfo objects
            if col_type_check(col.data_type.lower()): # Use col.data_type
                cols.append(col.column_name) # Use col.column_name
        return cols

    def _get_text_columns(self, table_name: str) -> list[str]:
        return self._get_columns_by_type(table_name, lambda t: any(s in t for s in ['char', 'text', 'varchar']))

    def _get_numeric_columns(self, table_name: str) -> list[str]:
        return self._get_columns_by_type(table_name, lambda t: any(s in t for s in ['int', 'num', 'dec', 'float', 'double', 'money', 'real', 'decimal'])) # Added decimal

    def _get_date_columns(self, table_name: str) -> list[str]:
        return self._get_columns_by_type(table_name, lambda t: any(s in t for s in ['date', 'time', 'smalldatetime', 'datetime2'])) # Added smalldatetime, datetime2

    def _get_random_table(self) -> str | None:
        """Selects a random table name from the schema."""
        all_tables = self.csv_loader.get_tables()
        if not all_tables:
            return None
        table_names = [table.name for table in all_tables] # Get names from TableInfo objects
        return random.choice(table_names) if table_names else None
    
    def _get_random_columns(self, table_name: str, count: int = 2) -> list[str]:
        """Selects random columns from a table."""
        table_info = self.csv_loader.get_table_by_name(table_name)
        if not table_info:
            return []
        
        columns_for_table = self.csv_loader.get_columns_for_table(table_name)
        available_columns = [col.column_name for col in columns_for_table] # Get names from ColumnInfo
        if not available_columns:
            return []
        
        return random.sample(available_columns, min(len(available_columns), count))


    def generate_example_queries(self, num_queries_per_type: int = 3) -> list[str]:
        """
        Generates a list of diverse SQL query strings.
        """
        queries = []
        all_tables_info = self.csv_loader.get_tables()
        if not all_tables_info: # Updated check
            return ["-- No tables found in schema to generate queries."]

        table_names = [table.name for table in all_tables_info] # Updated access
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
        foreign_keys = self.csv_loader.get_foreign_keys() # Use JoinInfo
        for _ in range(num_queries_per_type):
            if not foreign_keys: continue
            
            join_info = random.choice(foreign_keys) # This is a JoinInfo object
            from_table = join_info.primary_table_name
            to_table = join_info.foreign_table_name
            from_col = join_info.primary_table_column
            to_col = join_info.foreign_table_column

            # Ensure tables exist before trying to get columns
            # Check against the list of table names we derived earlier
            if from_table not in table_names or to_table not in table_names:
                continue

            t1_cols = self._get_random_columns(from_table, 1)
            t2_cols = self._get_random_columns(to_table, 1)

            if t1_cols and t2_cols:
                queries.append(
                    f"SELECT T1.{t1_cols[0]}, T2.{t2_cols[0]} "
                    f"FROM {from_table} AS T1 " # Added AS for alias
                    f"JOIN {to_table} AS T2 ON T1.{from_col} = T2.{to_col} "
                    f"LIMIT 10;"
                )

        # 3. Aggregation statements
        for _ in range(num_queries_per_type):
            table = self._get_random_table()
            if not table: continue

            # Query type 3.1: SELECT COUNT(*)
            queries.append(f"SELECT COUNT(*) FROM {table};")
            
            numeric_cols = self._get_numeric_columns(table)
            columns_for_table = self.csv_loader.get_columns_for_table(table)
            all_cols = [col.column_name for col in columns_for_table] # Use ColumnInfo
            
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
        if len(foreign_keys) >= 2 and num_queries_per_type > 0:
            for _ in range(min(num_queries_per_type, len(foreign_keys) - 1)): # Limit attempts
                if len(foreign_keys) < 2: break
                rel1, rel2 = random.sample(foreign_keys, 2) # These are JoinInfo objects

                # Try to chain them: T1 -> T2 -> T3
                # If rel1: T1.col1 -> T2.col2  AND rel2: T2.col3 -> T3.col4
                if rel1.foreign_table_name == rel2.primary_table_name and \
                   rel1.primary_table_name != rel2.foreign_table_name: # Avoid self-joins for simplicity here
                    
                    t1 = rel1.primary_table_name
                    t2 = rel1.foreign_table_name # Same as rel2.primary_table_name
                    t3 = rel2.foreign_table_name

                    t1_pk = rel1.primary_table_column
                    t2_fk_for_t1 = rel1.foreign_table_column
                    t2_pk_for_t3 = rel2.primary_table_column
                    t3_fk = rel2.foreign_table_column
                    
                    # Ensure tables exist (check against derived table_names list)
                    if not all(tbl in table_names for tbl in [t1, t2, t3]):
                        continue

                    t1_sel_col = self._get_random_columns(t1, 1)
                    t3_sel_col = self._get_random_columns(t3, 1)

                    if t1_sel_col and t3_sel_col:
                        queries.append(
                            f"SELECT T1.{t1_sel_col[0]}, T3.{t3_sel_col[0]} "
                            f"FROM {t1} AS T1 " # Added AS for alias
                            f"JOIN {t2} AS T2 ON T1.{t1_pk} = T2.{t2_fk_for_t1} "
                            f"JOIN {t3} AS T3 ON T2.{t2_pk_for_t3} = T3.{t3_fk} "
                            f"LIMIT 10;"
                        )
        
        # Remove duplicates if any, while preserving order for consistency in testing/review
        seen = set()
        unique_queries = [q for q in queries if not (q in seen or seen.add(q))]
        return unique_queries


    def save_queries_to_file(self, queries: list[str], output_filepath: str = "data/example_queries.jsonl"): # Path might need adjustment if data/ is not CWD
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
    # CSVSchemaLoader expects CSV files in "data/" relative to its data_folder_path.
    # No dummy DDL creation is needed here.
    # ExampleQueryGenerator now defaults to data_folder_path="data/"
    
    # For robust execution if script is not in project root:
    # import pathlib
    # script_dir = pathlib.Path(__file__).parent
    # data_path = script_dir / "data" # Assuming data is relative to script
    # generator = ExampleQueryGenerator(data_folder_path=str(data_path))
    # Or if data is always relative to a project root from where script is run:
    
    try:
        generator = ExampleQueryGenerator() # Uses default data_folder_path="data/"
        
        # Check if tables were loaded by CSVSchemaLoader
        if not generator.csv_loader.get_tables():
            print("No tables loaded from CSV files. Cannot generate meaningful queries.")
            print("Please ensure 'table_related_information.csv', 'column_related_information.csv', "
                  "and 'join_related_information.csv' exist in the 'data/' directory and are readable.")
        else:
            loaded_tables = [table.name for table in generator.csv_loader.get_tables()]
            print(f"Schema loaded from CSVs. Tables: {loaded_tables}")
            
            loaded_fks = generator.csv_loader.get_foreign_keys()
            print(f"Number of foreign keys loaded: {len(loaded_fks)}")
            if loaded_fks:
                 print("Sample Foreign Keys (first 3):")
                 for fk_info in loaded_fks[:3]:
                     print(f"  {fk_info.primary_table_name}.{fk_info.primary_table_column} -> "
                           f"{fk_info.foreign_table_name}.{fk_info.foreign_table_column}")
            
            example_queries = generator.generate_example_queries(num_queries_per_type=2) 

            print("\nGenerated Example Queries:")
            if not example_queries or all(q.startswith("-- No") for q in example_queries):
                print("-- No meaningful queries were generated. Check CSV data and generation logic.")
            for i, query in enumerate(example_queries):
                print(f"{i+1}. {query}")

            output_file = "data/example_queries.jsonl" # Assumes data/ is writable
            generator.save_queries_to_file(example_queries, output_file)
            print(f"\nSuccessfully saved {len(example_queries)} queries to {output_file}")

    except FileNotFoundError: # This might be caught by CSVSchemaLoader now
        print("Error: One or more required CSV schema files were not found in 'data/'.")
        print("Please ensure 'table_related_information.csv', 'column_related_information.csv', "
              "and 'join_related_information.csv' exist.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()
