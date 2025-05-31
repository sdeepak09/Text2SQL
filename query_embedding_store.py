import os
import pickle
import json
from dotenv import load_dotenv

try:
    from langchain_openai import OpenAIEmbeddings
    from langchain_community.vectorstores import FAISS
    from langchain.schema import Document
except ImportError:
    print("Langchain modules not fully available. Please ensure langchain, langchain_openai, langchain_community are installed.")
    # Define dummy classes for basic script structure to work if langchain is missing
    class OpenAIEmbeddings:
        def __init__(self, model, openai_api_key): pass
    class FAISS:
        @staticmethod
        def from_documents(documents, embedding): return FAISS()
        def save_local(self, folder_path): print(f"Dummy save to {folder_path}")
        @staticmethod
        def load_local(folder_path, embeddings, allow_dangerous_deserialization): print(f"Dummy load from {folder_path}"); return FAISS()
        def similarity_search(self, query, k): return []

    class Document:
        def __init__(self, page_content, metadata): pass


from schema_description_generator import SchemaDescriptionGenerator
from example_query_generator import ExampleQueryGenerator

class QueryEmbeddingStore:
    """
    Manages the creation, storage, and loading of query and schema embeddings
    using FAISS and OpenAI.
    """
    def __init__(self, ddl_file_path: str, openai_api_key: str, faiss_folder_path: str = "data/context_faiss_index"):
        """
        Initializes the QueryEmbeddingStore.

        Args:
            ddl_file_path: Path to the DDL schema file.
            openai_api_key: OpenAI API key.
            faiss_folder_path: Folder path to save/load the FAISS index.
        """
        self.faiss_folder_path = faiss_folder_path
        try:
            self.embeddings_model = OpenAIEmbeddings(model="text-embedding-3-small", openai_api_key=openai_api_key)
        except Exception as e:
            print(f"Error initializing OpenAIEmbeddings: {e}. Ensure OPENAI_API_KEY is valid.")
            self.embeddings_model = None # Or raise error

        self.vector_store = None
        
        try:
            self.schema_desc_generator = SchemaDescriptionGenerator(ddl_file_path)
            self.example_query_generator = ExampleQueryGenerator(ddl_file_path)
        except FileNotFoundError as e:
            print(f"Error: DDL file not found at {ddl_file_path}. {e}")
            # Depending on desired behavior, might re-raise or handle gracefully
            raise 
        except Exception as e:
            print(f"Error initializing schema/query generators: {e}")
            raise

    def _load_data(self) -> list[Document]:
        """
        Loads schema descriptions and example queries to be embedded.

        Returns:
            A list of Document objects.
        """
        documents = []

        # 1. Get schema descriptions
        try:
            all_descriptions = self.schema_desc_generator.get_all_descriptions()
            for desc in all_descriptions:
                metadata = {'type': desc['type']}
                if 'table_name' in desc:
                    metadata['table_name'] = desc['table_name']
                if 'column_name' in desc: # Only present for column type
                    metadata['column_name'] = desc['column_name']
                
                # Ensure content is a string
                page_content = str(desc.get('content', ''))
                documents.append(Document(page_content=page_content, metadata=metadata))
        except Exception as e:
            print(f"Error loading schema descriptions: {e}")

        # 2. Generate example queries
        try:
            # num_queries_per_type can be adjusted. 5 is a reasonable default.
            example_sql_queries = self.example_query_generator.generate_example_queries(num_queries_per_type=5)
            for query_str in example_sql_queries:
                 # Ensure query_str is a string
                documents.append(Document(page_content=str(query_str), metadata={'type': 'example_query'}))
        except Exception as e:
            print(f"Error generating example queries: {e}")
            
        if not documents:
            print("Warning: No documents (schema descriptions or example queries) were loaded/generated.")
            
        return documents

    def build_and_save_store(self):
        """
        Builds the FAISS vector store from schema and query data and saves it locally.
        """
        if not self.embeddings_model:
            print("Embeddings model not initialized. Cannot build store.")
            return

        documents = self._load_data()

        if documents:
            try:
                print(f"Building FAISS index with {len(documents)} documents...")
                self.vector_store = FAISS.from_documents(documents=documents, embedding=self.embeddings_model)
                
                # Ensure the directory for faiss_folder_path exists
                os.makedirs(self.faiss_folder_path, exist_ok=True)
                
                self.vector_store.save_local(folder_path=self.faiss_folder_path)
                print(f"FAISS index built and saved to folder: {self.faiss_folder_path}")
                # FAISS typically saves index.faiss and index.pkl within this folder.
            except Exception as e:
                print(f"Error building or saving FAISS store: {e}")
        else:
            print("No documents found to build the FAISS store.")

    def load_store(self) -> bool:
        """
        Loads an existing FAISS vector store from local storage.

        Returns:
            True if loading was successful, False otherwise.
        """
        if not self.embeddings_model:
            print("Embeddings model not initialized. Cannot load store.")
            return False

        index_file_path = os.path.join(self.faiss_folder_path, "index.faiss")
        if os.path.exists(index_file_path):
            try:
                self.vector_store = FAISS.load_local(
                    folder_path=self.faiss_folder_path, 
                    embeddings=self.embeddings_model,
                    allow_dangerous_deserialization=True 
                )
                print(f"FAISS index loaded from folder: {self.faiss_folder_path}")
                return True
            except Exception as e:
                print(f"Error loading FAISS index: {e}")
                return False
        else:
            print(f"FAISS index file not found at {index_file_path}. Cannot load store.")
            return False

if __name__ == '__main__':
    load_dotenv() # Load environment variables from .env file

    openai_api_key = os.getenv("OPENAI_API_KEY")
    if not openai_api_key:
        print("Error: OPENAI_API_KEY environment variable not set.")
        exit(1)

    # Define file paths
    # Ensure data/database_schema.sql exists from previous steps or create a dummy one.
    ddl_file = "data/database_schema.sql"
    faiss_idx_folder = "data/context_faiss_store_v1" # Changed name for clarity

    # Create dummy DDL if it doesn't exist for demonstration
    if not os.path.exists(ddl_file):
        print(f"Warning: DDL file {ddl_file} not found. Creating a dummy file for demonstration.")
        os.makedirs(os.path.dirname(ddl_file), exist_ok=True)
        with open(ddl_file, 'w') as f:
            f.write(
                "CREATE TABLE Users (user_id INT PRIMARY KEY, username VARCHAR(50), email VARCHAR(100));\n"
                "CREATE TABLE Products (product_id INT PRIMARY KEY, name VARCHAR(100), price DECIMAL(10,2));\n"
            )
    
    print("--- Building and Saving Store ---")
    try:
        store_builder = QueryEmbeddingStore(
            ddl_file_path=ddl_file,
            openai_api_key=openai_api_key,
            faiss_folder_path=faiss_idx_folder
        )
        store_builder.build_and_save_store()
    except FileNotFoundError:
        print(f"Exiting due to DDL file not found issue during store building.")
        exit(1)
    except Exception as e:
        print(f"An error occurred during store building: {e}")
        exit(1)

    print("\n--- Loading Store and Performing Search ---")
    try:
        store_loader = QueryEmbeddingStore(
            ddl_file_path=ddl_file, # DDL needed for generators, though not strictly for loading if pre-built
            openai_api_key=openai_api_key,
            faiss_folder_path=faiss_idx_folder
        )
        if store_loader.load_store():
            if store_loader.vector_store:
                print("Performing a sample similarity search...")
                try:
                    # Example search, adjust query as needed based on dummy DDL
                    search_results = store_loader.vector_store.similarity_search("find user name", k=2)
                    if search_results:
                        print("Search Results:")
                        for doc in search_results:
                            print(f"- Content: {doc.page_content[:100]}...") # Print first 100 chars
                            print(f"  Metadata: {doc.metadata}")
                    else:
                        print("No results found for the dummy search.")
                except Exception as e:
                    print(f"Error during similarity search: {e}")
            else:
                print("Vector store not available after loading.")
        else:
            print("Failed to load the FAISS store. Search cannot be performed.")
    except FileNotFoundError:
        print(f"Exiting due to DDL file not found issue during store loading.")
    except Exception as e:
        print(f"An error occurred during store loading or search: {e}")

    # To clean up the created dummy index after testing (optional):
    # import shutil
    # if os.path.exists(faiss_idx_folder):
    #     print(f"\nCleaning up dummy FAISS index folder: {faiss_idx_folder}")
    #     shutil.rmtree(faiss_idx_folder)
    # if os.path.exists(ddl_file) and "CREATE TABLE Users" in open(ddl_file).read(): # basic check if it's a dummy
    #     print(f"Cleaning up dummy DDL file: {ddl_file}")
    #     # os.remove(ddl_file) # Be careful with auto-removing files
    #     pass
