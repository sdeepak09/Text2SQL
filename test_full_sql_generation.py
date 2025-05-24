import os
import json # For pretty printing dicts
from dotenv import load_dotenv

# Attempt to import from local modules
try:
    from rag_context import RAGContextProvider
    from llm_utils import get_llm, get_query_explanation_prompt, get_sql_generation_prompt, parse_query_explanation
    from pydantic_models import QueryExplanation # Needed for parsed_explanation.dict()
except ImportError as e:
    print(f"Error importing local modules: {e}")
    print("Please ensure you are running this script from the root of the repository and PYTHONPATH is set up correctly if needed.")
    exit(1)

def main():
    load_dotenv() # Ensure OPENAI_API_KEY is loaded from .env if present

    ddl_file = "data/database_schema.sql"
    user_query = "How many appointments were scheduled for each day last week?"

    # Ensure OPENAI_API_KEY is available
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key or "dummy" in api_key.lower() or len(api_key) < 10: # Basic check for placeholder/invalid key
        print(f"CRITICAL ERROR: A valid OPENAI_API_KEY was not found or appears to be a placeholder: {api_key}")
        print("Please ensure it is correctly set as an environment variable or in a .env file.")
        print("Script cannot proceed without a valid key for live OpenAI API calls.")
        exit(1)
    
    print(f"Using OpenAI API Key: {api_key[:5]}...{api_key[-4:]}") # Print partial key for confirmation
    print(f"Target DDL file: {ddl_file}")
    print(f"User Query: {user_query}\n")

    try:
        # Initialize Components
        print("--- Initializing Components ---")
        rag_provider = RAGContextProvider(ddl_file_path=ddl_file)
        llm = get_llm() 
        explanation_prompt_template = get_query_explanation_prompt()
        sql_generation_prompt_template = get_sql_generation_prompt()
        print("Components initialized successfully.\n")

        # Step 1: Get RAG Context
        print("--- Step 1: Getting RAG Context ---")
        rag_context_data = rag_provider.get_relevant_context(user_query)
        relevant_schema_from_parser = rag_context_data.get("relevant_schema", "") # Keyword search
        relevant_statements_from_faiss = rag_context_data.get("relevant_statements", "") # FAISS search
        
        print("\n--- Relevant Schema (Keyword Search Result from SchemaParser) ---")
        print(relevant_schema_from_parser if relevant_schema_from_parser else "None")
        print("\n--- Relevant Statements (FAISS Search Result from SchemaEmbeddingStore) ---")
        print(relevant_statements_from_faiss if relevant_statements_from_faiss else "None")
        print("\n--- End of RAG Context ---")

        # Step 2: Generate Query Explanation
        print("\n--- Step 2: Generating Query Explanation ---")
        explanation_prompt_filled = explanation_prompt_template.format(
            query=user_query,
            relevant_schema=relevant_schema_from_parser,
            relevant_statements=relevant_statements_from_faiss
        )
        print("\n--- Explanation Prompt Sent to LLM ---")
        print(explanation_prompt_filled)
        
        explanation_response = llm.invoke(explanation_prompt_filled)
        explanation_content = explanation_response.content
        
        print("\n--- Explanation Response from LLM (Raw) ---")
        print(explanation_content)
        
        parsed_explanation, error = parse_query_explanation(explanation_content)
        if error:
            print(f"Error parsing explanation: {error}")
            # Allow script to continue to SQL generation if explanation_content is available
            # as the LLM might sometimes return valid SQL explanation without perfect JSON.
            explanation_for_sql_prompt = explanation_content 
        elif parsed_explanation:
            print("\n--- Parsed Explanation (JSON) ---")
            # Ensure parsed_explanation is a Pydantic model with .dict() or handle appropriately
            if hasattr(parsed_explanation, 'dict'):
                explanation_for_sql_prompt = json.dumps(parsed_explanation.dict(), indent=2)
                print(explanation_for_sql_prompt)
            else: # Fallback if it's not a Pydantic model but some other structure
                explanation_for_sql_prompt = str(parsed_explanation)
                print(explanation_for_sql_prompt)
        else: # Should not happen if no error and no parsed_explanation
            print("Unknown state: No error but no parsed explanation. Using raw content for next step.")
            explanation_for_sql_prompt = explanation_content


        # Step 3: Generate SQL Query
        print("\n--- Step 3: Generating SQL Query ---")
        sql_generation_prompt_filled = sql_generation_prompt_template.format(
            query=user_query,
            relevant_schema=relevant_schema_from_parser,
            relevant_statements=relevant_statements_from_faiss,
            explanation=explanation_for_sql_prompt 
        )
        print("\n--- SQL Generation Prompt Sent to LLM ---")
        print(sql_generation_prompt_filled)

        sql_response = llm.invoke(sql_generation_prompt_filled)
        generated_sql = sql_response.content.strip()

        print("\n--- Generated SQL Query ---")
        print(generated_sql)
        print("\n--- End of Test ---")

    except Exception as e:
        print(f"An error occurred during the test execution: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
```
