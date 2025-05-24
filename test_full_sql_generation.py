import os
import json # For pretty printing dicts
from dotenv import load_dotenv
import shutil # For directory deletion

# Attempt to import from local modules
try:
    from rag_context import RAGContextProvider
    from llm_utils import get_llm, get_query_explanation_prompt, get_sql_generation_prompt, parse_query_explanation
    from pydantic_models import QueryExplanation 
except ImportError as e:
    print(f"Error importing local modules: {e}")
    print("Please ensure you are running this script from the root of the repository and PYTHONPATH is set up correctly if needed.")
    exit(1)

def main():
    load_dotenv() # Ensure OPENAI_API_KEY is loaded from .env if present

    ddl_file = "data/database_schema.sql"
    user_query = "How many appointments were scheduled for each day last week?" # Used for FAISS search test
    faiss_store_path = "data/schema_embeddings_faiss/" # Default path used by SchemaEmbeddingStore

    # Ensure OPENAI_API_KEY is available for RAGContextProvider initialization
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key or "dummy" in api_key.lower() or len(api_key) < 10:
        print(f"CRITICAL ERROR: A valid OPENAI_API_KEY was not found or appears to be a placeholder: {api_key}")
        print("Please ensure it is correctly set as an environment variable or in a .env file.")
        print("Script cannot proceed without a valid key for RAGContextProvider initialization.")
        exit(1)
    
    print(f"Using OpenAI API Key: {api_key[:5]}...{api_key[-4:]}")
    print(f"Target DDL file: {ddl_file}")
    print(f"FAISS Store Path: {faiss_store_path}")
    print(f"User Query for FAISS test: {user_query}\n")

    try:
        # Delete Existing Store (for a clean test)
        if os.path.exists(faiss_store_path):
            print(f"--- Deleting existing FAISS store at {faiss_store_path} for a clean test ---")
            shutil.rmtree(faiss_store_path)
        else:
            print(f"--- No existing FAISS store found at {faiss_store_path}. Proceeding with creation. ---")

        # First RAGContextProvider Instance (Create and Save)
        print("\n--- Initializing first RAGContextProvider (will create and save FAISS store) ---")
        rag_provider_first_instance = RAGContextProvider(ddl_file_path=ddl_file)
        print("First RAGContextProvider initialized. FAISS store should have been populated and saved if DDL parsing yielded elements.")
        
        # Second RAGContextProvider Instance (Load)
        print("\n--- Initializing second RAGContextProvider (should LOAD the saved FAISS store) ---")
        rag_provider_second_instance = RAGContextProvider(ddl_file_path=ddl_file) 
        print("Second RAGContextProvider initialized.")

        # Test FAISS Search with Second Instance
        print("\n--- Testing FAISS search with the second (loaded) RAGContextProvider instance ---")
        # user_query is already defined
        rag_context_data_loaded = rag_provider_second_instance.get_relevant_context(user_query)
        
        # The task description asked for "relevant_statements". In the current RAGContextProvider,
        # this corresponds to "formatted_relevant_statements".
        relevant_statements_from_loaded_faiss = rag_context_data_loaded.get("formatted_relevant_statements", "") # Per task description
        # For more detailed verification, we can also look at raw_relevant_statements
        # raw_statements_from_loaded_faiss = rag_context_data_loaded.get("raw_relevant_statements", [])

        print("\n--- Relevant Statements (Formatted, from FAISS Search on loaded store) ---")
        if relevant_statements_from_loaded_faiss and relevant_statements_from_loaded_faiss.strip() and relevant_statements_from_loaded_faiss != "None":
            print(relevant_statements_from_loaded_faiss)
        else:
            print("No formatted relevant statements found from FAISS search on the loaded store, or result was 'None' or empty.")

        print("\n--- FAISS Save/Load test portion completed ---")

        # --- LLM-dependent parts are now re-enabled ---
        print("\n--- Initializing LLM Components ---")
        llm = get_llm() 
        explanation_prompt_template = get_query_explanation_prompt()
        sql_generation_prompt_template = get_sql_generation_prompt()
        print("LLM Components initialized successfully.\n")

        # RAG context for LLM calls will use the second (loaded) instance's data
        relevant_schema_from_parser = rag_context_data_loaded.get("relevant_schema", "")
        formatted_relevant_statements_from_faiss = rag_context_data_loaded.get("formatted_relevant_statements", "")
        
        print("\n--- Relevant Schema (Keyword Search - for LLM) ---")
        print(relevant_schema_from_parser if relevant_schema_from_parser else "None")
        print("\n--- Relevant Statements (FAISS - Formatted - for LLM) ---")
        print(formatted_relevant_statements_from_faiss if formatted_relevant_statements_from_faiss else "None")
        print("\n--- End of RAG Context for LLM ---")

        # Step 2: Generate Query Explanation
        print("\n--- Step 2: Generating Query Explanation ---")
        explanation_prompt_filled = explanation_prompt_template.format(
            query=user_query,
            relevant_schema=relevant_schema_from_parser,
            relevant_statements=formatted_relevant_statements_from_faiss # Use the formatted string
        )
        print("\n--- Explanation Prompt Sent to LLM ---")
        print(explanation_prompt_filled)
        
        explanation_response = llm.invoke(explanation_prompt_filled)
        explanation_content = explanation_response.content
        
        print("\n--- Explanation Response from LLM (Raw) ---")
        print(explanation_content)
        
        parsed_explanation, error = parse_query_explanation(explanation_content)
        explanation_for_sql_prompt = explanation_content # Fallback to raw content
        if error:
            print(f"Error parsing explanation: {error}")
        elif parsed_explanation:
            print("\n--- Parsed Explanation (JSON) ---")
            if hasattr(parsed_explanation, 'dict'):
                explanation_for_sql_prompt = json.dumps(parsed_explanation.dict(), indent=2)
                print(explanation_for_sql_prompt)
            else: 
                explanation_for_sql_prompt = str(parsed_explanation)
                print(explanation_for_sql_prompt)
        else: 
            print("Unknown state: No error but no parsed explanation. Using raw content for next step.")


        # Step 3: Generate SQL Query
        print("\n--- Step 3: Generating SQL Query ---")
        if parsed_explanation or explanation_for_sql_prompt: # Proceed if we have some form of explanation
            sql_generation_prompt_filled = sql_generation_prompt_template.format(
                query=user_query,
                relevant_schema=relevant_schema_from_parser,
                relevant_statements=formatted_relevant_statements_from_faiss, # Use the formatted string
                explanation=explanation_for_sql_prompt 
            )
            print("\n--- SQL Generation Prompt Sent to LLM ---")
            print(sql_generation_prompt_filled)

            sql_response = llm.invoke(sql_generation_prompt_filled)
            generated_sql = sql_response.content.strip()

            print("\n--- Generated SQL Query ---")
            print(generated_sql)
        else:
            print("Skipping SQL generation due to parsing error or no explanation.")
        
        print("\n--- Full SQL Generation Test Script Completed ---")

    except FileNotFoundError as e: 
        print(f"ERROR: DDL file not found at {ddl_file}. Cannot proceed with RAGContextProvider initialization.")
        print(f"Please ensure the DDL file '{ddl_file}' exists.")
    except Exception as e:
        print(f"An error occurred during the test execution: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
