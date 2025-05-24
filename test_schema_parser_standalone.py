import json # For pretty printing
from schema_parser import SchemaParser

def main():
    ddl_file = "data/database_schema.sql"
    print(f"--- Testing SchemaParser with DDL file: {ddl_file} ---")

    try:
        schema_parser = SchemaParser(ddl_file_path=ddl_file)

        print("\n--- Parsed Tables (schema_parser.tables) ---")
        if schema_parser.tables:
            # Pretty print the tables dictionary
            print(json.dumps(schema_parser.tables, indent=2))
        else:
            print("No tables were parsed.")

        print("\n--- Parsed Relationships (schema_parser.relationships) ---")
        if schema_parser.relationships:
            # Pretty print the relationships list
            print(json.dumps(schema_parser.relationships, indent=2))
        else:
            print("No relationships were parsed.")

        print("\n--- Output of get_elements_for_embedding() ---")
        elements = schema_parser.get_elements_for_embedding()
        if elements:
            # Pretty print the elements list
            print(json.dumps(elements, indent=2))
            print(f"Total elements for embedding: {len(elements)}")
        else:
            print("No elements generated for embedding. This means 'schema_parser.tables' was likely empty.")
            
        # Also test get_formatted_schema to ensure it runs
        print("\n--- Output of get_formatted_schema() ---")
        formatted_schema = schema_parser.get_formatted_schema()
        if formatted_schema.strip() == "Database Schema:": # Check if only the header is there
            print("Formatted schema is empty (only header).")
        else:
            print(formatted_schema)


    except FileNotFoundError:
        print(f"ERROR: DDL file not found at {ddl_file}")
    except Exception as e:
        print(f"An error occurred during SchemaParser testing: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
