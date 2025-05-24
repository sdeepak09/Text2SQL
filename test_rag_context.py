import sys
import os

# Ensure the script can find other modules in the repository root
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from rag_context import RAGContextProvider

if __name__ == "__main__":
    ddl_file = "data/database_schema.sql"

    print("Initializing RAGContextProvider...")
    try:
        rag_provider = RAGContextProvider(ddl_file_path=ddl_file)
    except FileNotFoundError as e:
        print(f"Error during RAGContextProvider initialization: {e}")
        print("Please ensure the DDL file exists at the specified path.")
        print("You might need to create a dummy 'data/database_schema.sql' file for this test.")
        # Create a dummy DDL file if it doesn't exist, so the test can proceed further
        # to test the embedding store and FAISS parts.
        if not os.path.exists("data"):
            os.makedirs("data")
        if not os.path.exists(ddl_file):
            with open(ddl_file, "w") as f:
                f.write("-- Dummy DDL file for testing\n")
                f.write("CREATE TABLE Patients (PatientID INT, LastName VARCHAR(255), FirstName VARCHAR(255));\n")
                f.write("CREATE TABLE Claims (ClaimID INT, PatientID INT, ClaimStatus VARCHAR(50), ClaimAmount DECIMAL(10,2));\n")
                f.write("CREATE TABLE Procedures (ProcedureID INT, ProcedureCode VARCHAR(50), Description VARCHAR(255));\n")
                f.write("CREATE TABLE Providers (ProviderID INT, NPI VARCHAR(20));\n")
            print(f"Created a dummy DDL file: {ddl_file}")
            # Try initializing again
            rag_provider = RAGContextProvider(ddl_file_path=ddl_file)
        else:
            sys.exit(1) # Exit if file not found and couldn't create a dummy

    print("RAGContextProvider initialization complete.")

    queries = [
        "How many appointments were scheduled for each day last week?"
    ]

    for query in queries:
        print(f"\nTesting query: \"{query}\"")
        context = rag_provider.get_relevant_context(query)
        
        print("\n" + "="*30 + " CONTEXT DETAILS " + "="*30)

        # Raw relevant statements from FAISS search
        raw_faiss_statements = context.get("raw_relevant_statements", [])
        print("\n--- Relevant Statements from FAISS (Raw) ---")
        if raw_faiss_statements:
            for i, stmt_info in enumerate(raw_faiss_statements): # Print all results
                content = stmt_info.get('content', 'N/A')
                score = stmt_info.get('score', 'N/A') 
                metadata = stmt_info.get('metadata', {})
                print(f"  Result {i+1}:")
                print(f"    Content: \"{content}\"")
                print(f"    Score: {score}")
                print(f"    Metadata: {metadata}")
        else:
            print("  No raw relevant statements found by FAISS.")

        # Formatted relevant statements (this is the string version of above)
        formatted_faiss_statements = context.get("formatted_relevant_statements", "N/A")
        print("\n--- Formatted Relevant Statements from FAISS ---")
        print(formatted_faiss_statements if formatted_faiss_statements else "  N/A")

        # Relevant schema from SchemaParser keyword search
        relevant_schema_keyword = context.get("relevant_schema", "N/A")
        print("\n--- Relevant Schema (Keyword Search) ---")
        print(relevant_schema_keyword if relevant_schema_keyword else "  N/A")

        # Full schema from SchemaParser
        full_schema_parsed = context.get("full_schema", "N/A")
        print("\n--- Full Schema ---")
        print(full_schema_parsed if full_schema_parsed else "  N/A")
        
        print("\n" + "="*30 + " END CONTEXT DETAILS " + "="*30)
        print("----")

    print("\nRAG context tests completed for the specific query.")
