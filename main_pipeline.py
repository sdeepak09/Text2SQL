import os
from dotenv import load_dotenv

# Assuming these files are in the same directory or accessible via PYTHONPATH
from query_embedding_store import QueryEmbeddingStore
from rag_sql_llm import RAGSQLGenerator

# --- Configuration Variables ---
DDL_FILE_PATH = "data/database_schema.sql"
FAISS_INDEX_FOLDER_PATH = "data/context_faiss_store_v1" # Consistent with other scripts

def create_dummy_ddl_if_not_exists():
    """Creates a dummy DDL file if the specified one doesn't exist."""
    if not os.path.exists(DDL_FILE_PATH):
        print(f"Warning: DDL file {DDL_FILE_PATH} not found. Creating a dummy file for demonstration.")
        os.makedirs(os.path.dirname(DDL_FILE_PATH), exist_ok=True)
        with open(DDL_FILE_PATH, 'w') as f:
            f.write(
                "-- Dummy DDL for main_pipeline.py testing --\n"
                "CREATE TABLE Users (user_id INT PRIMARY KEY, full_name VARCHAR(100), email VARCHAR(100), birth_date DATE, insurance_id VARCHAR(50));\n"
                "CREATE TABLE Policies (policy_id VARCHAR(50) PRIMARY KEY, policy_type VARCHAR(50), start_date DATE, end_date DATE, premium_amount DECIMAL(10,2));\n"
                "CREATE TABLE Claims (claim_id VARCHAR(50) PRIMARY KEY, policy_id VARCHAR(50), claim_date DATE, total_amount DECIMAL(10,2), description TEXT, "
                "CONSTRAINT FK_Claims_Policies FOREIGN KEY (policy_id) REFERENCES Policies(policy_id));\n"
                "CREATE TABLE Providers (provider_id INT PRIMARY KEY, provider_name VARCHAR(100), specialty VARCHAR(50));\n"
                "CREATE TABLE Procedures (procedure_id INT PRIMARY KEY, procedure_name VARCHAR(100), cost DECIMAL(10,2));\n"
                "CREATE TABLE ClaimProcedures (claim_id VARCHAR(50), procedure_id INT, provider_id INT, "
                "PRIMARY KEY (claim_id, procedure_id), "
                "FOREIGN KEY (claim_id) REFERENCES Claims(claim_id), "
                "FOREIGN KEY (procedure_id) REFERENCES Procedures(procedure_id), "
                "FOREIGN KEY (provider_id) REFERENCES Providers(provider_id));\n"
                "ALTER TABLE Users ADD CONSTRAINT FK_Users_Policies FOREIGN KEY (insurance_id) REFERENCES Policies(policy_id);\n" # Simplified link
            )
        print(f"Dummy DDL created at {DDL_FILE_PATH}")


def main():
    """
    Main function to run the RAG SQL generation pipeline.
    """
    print("--- Starting RAG SQL Generation Pipeline ---")
    load_dotenv()
    openai_api_key = os.getenv("OPENAI_API_KEY")

    if not openai_api_key:
        print("Error: OPENAI_API_KEY environment variable not set. Please set it in your .env file or environment.")
        exit(1)

    # Ensure a DDL file exists for the embedding store to process
    create_dummy_ddl_if_not_exists()

    # --- Step 1: Ensure FAISS Index Exists or Build It ---
    print("\n--- Step 1: Checking/Building FAISS Index ---")
    faiss_actual_index_file = os.path.join(FAISS_INDEX_FOLDER_PATH, "index.faiss")

    if not os.path.exists(faiss_actual_index_file):
        print(f"FAISS index not found at {faiss_actual_index_file}. Building and saving new index...")
        try:
            embedding_store = QueryEmbeddingStore(
                ddl_file_path=DDL_FILE_PATH,
                openai_api_key=openai_api_key,
                faiss_folder_path=FAISS_INDEX_FOLDER_PATH
            )
            embedding_store.build_and_save_store()

            if not os.path.exists(faiss_actual_index_file):
                print(f"Error: FAISS index still not found after build attempt at {faiss_actual_index_file}. Exiting.")
                exit(1)
            print("FAISS index built and saved successfully.")
        except Exception as e:
            print(f"Error during FAISS index building: {e}")
            import traceback
            traceback.print_exc()
            exit(1)
    else:
        print(f"FAISS index found at {FAISS_INDEX_FOLDER_PATH}.")

    # --- Step 2: Initialize RAG SQL Generator ---
    print("\n--- Step 2: Initializing RAG SQL Generator ---")
    try:
        rag_generator = RAGSQLGenerator(
            openai_api_key=openai_api_key,
            faiss_index_folder_path=FAISS_INDEX_FOLDER_PATH
        )
        if rag_generator.query_retriever.vector_store is None:
            print("Error: RAGSQLGenerator failed to load the FAISS index, though the index file might exist. Check retriever logic. Exiting.")
            # This could happen if FAISS.load_local fails for some reason in QueryRetriever
            exit(1)
        print("RAGSQLGenerator initialized successfully.")
    except Exception as e:
        print(f"Error initializing RAGSQLGenerator: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

    # --- Step 3: Get User Question and Generate SQL ---
    print("\n--- Step 3: Generating SQL Queries from User Questions ---")
    
    # Sample questions tailored to the dummy DDL (adjust if using a different DDL)
    user_questions = [
        # Example 1: Based on ADMISSIONS table (financials)
        # Assumes ADMISSIONS.TOTAL_ALLOWED or ADMISSIONS.TOTAL_PAID can be used for 'claim amount'.
        "Show all admission records where the total allowed amount is greater than 500.",

        # Example 2: Based on Patients table (demographics) and ADMISSIONS (linking)
        # The 'insurance policy ID' concept is not directly in the CSV schema as described.
        # This question is modified to use existing fields.
        "List the first name, last name, and date of birth for patients associated with admissions having an ADMISSION_ID less than 10.",

        # Example 3: Procedures for a given ADMISSION_ID (integer)
        # This now uses an integer ID and refers to procedure codes.
        "What are the procedure codes (PROC_CD, ICD_PROC_CD) for admission ID 75?", # Using an example integer ID

        # Example 4: Querying responsible provider IDs (specialty is not available)
        # This avoids asking for specialty directly.
        "Show the responsible provider IDs (RESPONSIBLE_PROV_ID) and a count of admissions for each from the ADMISSIONS table.",

        # Example 5: Based on ADMISSIONS table (dates)
        # 'Policy types' and 'policy end dates' are not in the CSV ADMISSIONS table.
        # This question is modified to use ADMISSIONS.ADMIT_DT or DISCHARGE_DT.
        "List admission IDs and their admit dates for admissions that occurred after January 1, 2023."
    ]
    print("NOTE: The following questions are tailored for a schema derived from CSV files (Patients, Admissions, etc.).")
    print("For these to work correctly with main_pipeline.py, the FAISS index (context store) would also need to be built from the CSV schema descriptions, not the dummy DDL.")

    for user_question in user_questions:
        print(f"\n--- Processing Question: \"{user_question}\" ---")
        try:
            # k_retrieved_items can be tuned
            sql_query = rag_generator.generate_sql_query(user_question, k_retrieved_items=7) 
            print(f"User Question: {user_question}")
            print(f"Generated SQL Query:\n{sql_query}")
            print("\n--- Evaluation Suggestions (Manual) ---")
            print("- Is the SQL syntactically correct?")
            print("- Does the query answer the user's question based on the (dummy) schema?")
            print("- Are table and column names used correctly (e.g., Users.full_name, Claims.total_amount)?")
            print("- Are JOINs (if needed) present and correct?")
            print("- Are aggregations (COUNT, SUM, etc.) and filters (WHERE clauses) appropriate?")
            print("---------------------------------------\n")

        except Exception as e:
            print(f"An error occurred while processing question '{user_question}': {e}")
            import traceback
            traceback.print_exc()

    print("\n--- RAG SQL Generation Pipeline Finished ---")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"An unexpected error occurred in the main pipeline: {e}")
        import traceback
        traceback.print_exc()
