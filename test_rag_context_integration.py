import os
from rag_context import RAGContextProvider
from csv_schema_loader import CSVSchemaLoader # For potential direct checks

# Ensure environment variable for OpenAI API key is set, even if not strictly needed for this test,
# as RAGContextProvider's __init__ checks for it.
os.environ["OPENAI_API_KEY"] = "test_key_not_used_for_this_schema_test"

print("Starting RAGContextProvider integration test...")

# --- Test 1: Instantiate RAGContextProvider ---
try:
    print("\n--- Test 1: Attempting to instantiate RAGContextProvider ---")
    # This will implicitly test if CSVSchemaLoader can find and load its CSV files
    # (table_related_information.csv, column_related_information.csv, join_related_information.csv)
    # from the default "data/" directory.
    # If any CSV is missing, CSVSchemaLoader should now raise FileNotFoundError.
    context_provider = RAGContextProvider()
    print("SUCCESS: RAGContextProvider instantiated successfully.")
    print(f"Full schema loaded (first 300 chars):\n{context_provider.full_schema[:300]}...")
except FileNotFoundError as fnf_error:
    print(f"FAILURE: FileNotFoundError during RAGContextProvider instantiation. This likely means a CSV file was not found.")
    print(f"Error details: {fnf_error}")
    # Exit if instantiation fails, as other tests depend on it.
    exit(1) 
except Exception as e:
    print(f"FAILURE: An unexpected error occurred during RAGContextProvider instantiation.")
    print(f"Error details: {e}")
    exit(1)

# --- Test 2: Check basic schema loading via CSVSchemaLoader directly (optional sanity check) ---
# This is to ensure CSVSchemaLoader itself is working as expected, as a baseline.
try:
    print("\n--- Test 2: Sanity check CSVSchemaLoader directly ---")
    csv_loader = CSVSchemaLoader(data_folder_path="data/")
    if not csv_loader.get_tables() or not csv_loader.get_all_columns():
        print("WARNING: CSVSchemaLoader loaded, but no tables or columns found. Check 'data/' CSV files content and paths.")
    else:
        print(f"SUCCESS: CSVSchemaLoader loaded {len(csv_loader.get_tables())} tables and {len(csv_loader.get_all_columns())} columns directly.")
except FileNotFoundError as fnf_error:
    print(f"FAILURE: FileNotFoundError during direct CSVSchemaLoader instantiation. This likely means a CSV file was not found.")
    print(f"Error details: {fnf_error}")
except Exception as e:
    print(f"FAILURE: An unexpected error occurred during direct CSVSchemaLoader instantiation.")
    print(f"Error details: {e}")


# --- Test 3: Call get_table_info ---
try:
    print("\n--- Test 3: Calling get_table_info() ---")
    table_info = context_provider.get_table_info()
    if not table_info:
        print("WARNING: get_table_info() returned empty. This might be okay if CSVs are empty, but check if data was expected.")
    else:
        print(f"SUCCESS: get_table_info() returned {len(table_info)} tables.")
        # Print info for the first table if available
        if table_info:
            first_table_name = list(table_info.keys())[0]
            print(f"Schema for first table '{first_table_name}': {table_info[first_table_name][:2]}...") # Print first 2 columns
except Exception as e:
    print(f"FAILURE: An error occurred while calling get_table_info().")
    print(f"Error details: {e}")

# --- Test 4: Call get_relevant_context with a sample query ---
try:
    print("\n--- Test 4: Calling get_relevant_context() ---")
    # Use a query that is likely to match some terms in your CSV schema if it has data
    # e.g., if you have a 'claims' table or 'policy' column.
    sample_query = "details about claims" 
    relevant_context = context_provider.get_relevant_context(sample_query)
    if not relevant_context.get("relevant_schema") or relevant_context.get("relevant_schema") == "Relevant Database Schema:\n\n":
        print(f"WARNING: get_relevant_context() with query '{sample_query}' returned no specific relevant schema. This might be okay if the query doesn't match or CSVs are sparse.")
    else:
        print(f"SUCCESS: get_relevant_context() returned a relevant schema for query '{sample_query}'.")
        print(f"Relevant schema (first 300 chars):\n{relevant_context['relevant_schema'][:300]}...")
    
    if not relevant_context.get("relevant_statements") or relevant_context.get("relevant_statements") == "-- No specific statements/examples retrieved.": # Default QueryRetriever response
        print(f"INFO: get_relevant_context() with query '{sample_query}' returned no specific relevant statements (this part depends on QueryRetriever & FAISS index, not CSVSchemaLoader directly).")

except Exception as e:
    print(f"FAILURE: An error occurred while calling get_relevant_context().")
    print(f"Error details: {e}")

print("\nTest script finished.")
