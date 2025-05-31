import os
from dotenv import load_dotenv

try:
    from langchain_openai import OpenAIEmbeddings
    from langchain_community.vectorstores import FAISS
    from langchain.schema import Document # For type hinting and if we need to construct Documents
except ImportError:
    print("Langchain modules not fully available. Please ensure langchain, langchain_openai, langchain_community are installed.")
    # Define dummy classes for basic script structure to work if langchain is missing
    class OpenAIEmbeddings:
        def __init__(self, model, openai_api_key): pass
        def embed_query(self, query_text): return [0.1] * 1536 # Dummy embedding
    class FAISS:
        @staticmethod
        def load_local(folder_path, embeddings, allow_dangerous_deserialization): print(f"Dummy load from {folder_path}"); return FAISS()
        def similarity_search_with_score(self, query, k): return []
    class Document:
        def __init__(self, page_content, metadata): self.page_content = page_content; self.metadata = metadata


class QueryRetriever:
    """
    Retrieves relevant documents from a FAISS index based on a user query.
    """
    def __init__(self, openai_api_key: str, faiss_index_folder_path: str = "data/context_faiss_index"):
        """
        Initializes the QueryRetriever.

        Args:
            openai_api_key: OpenAI API key.
            faiss_index_folder_path: Path to the folder containing the FAISS index.
        """
        self.faiss_index_folder_path = faiss_index_folder_path
        try:
            self.embeddings_model = OpenAIEmbeddings(model="text-embedding-3-small", openai_api_key=openai_api_key)
        except Exception as e:
            print(f"Error initializing OpenAIEmbeddings: {e}. Ensure OPENAI_API_KEY is valid.")
            self.embeddings_model = None # Or raise error
            # Depending on desired behavior, might re-raise or handle gracefully
            raise
            
        self.vector_store = None
        self._load_index()

    def _load_index(self) -> bool:
        """
        Loads the FAISS index from the specified folder path.

        Returns:
            True if the index was loaded successfully, False otherwise.
        """
        if not self.embeddings_model:
            print("Embeddings model not initialized. Cannot load FAISS index.")
            return False

        index_file = os.path.join(self.faiss_index_folder_path, "index.faiss")
        if os.path.exists(index_file):
            try:
                self.vector_store = FAISS.load_local(
                    folder_path=self.faiss_index_folder_path, 
                    embeddings=self.embeddings_model,
                    allow_dangerous_deserialization=True
                )
                print(f"FAISS index loaded successfully from {self.faiss_index_folder_path}")
                return True
            except Exception as e:
                print(f"Error loading FAISS index from {self.faiss_index_folder_path}: {e}")
                self.vector_store = None
                return False
        else:
            print(f"FAISS index not found at {index_file}. Retriever will not work until index is created.")
            self.vector_store = None
            return False

    def retrieve_relevant_documents(self, user_question: str, k: int = 5) -> list[dict]:
        """
        Retrieves documents relevant to the user_question from the vector store.

        Args:
            user_question: The question to find relevant documents for.
            k: The number of top relevant documents to retrieve.

        Returns:
            A list of dictionaries, where each dictionary contains 'content', 
            'metadata', and 'score' of a retrieved document.
        """
        if self.vector_store is None:
            print("Error: Vector store not loaded. Cannot retrieve.")
            return []
        
        if not self.embeddings_model:
            print("Error: Embeddings model not available. Cannot retrieve.")
            return []

        try:
            # Embed the user question (embed_query for single string)
            # No need to embed separately, similarity_search_with_score takes the query string directly
            # query_embedding = self.embeddings_model.embed_query(user_question) 
            
            results = self.vector_store.similarity_search_with_score(query=user_question, k=k)
            
            formatted_results = []
            for doc, score in results:
                formatted_results.append({
                    'content': doc.page_content,
                    'metadata': doc.metadata,
                    'score': float(score) # score is often distance, smaller is better for FAISS
                })
            return formatted_results
        except Exception as e:
            print(f"Error during similarity search: {e}")
            return []

if __name__ == '__main__':
    load_dotenv() # Load environment variables from .env file

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("Error: OPENAI_API_KEY environment variable not set. Please set it to run the retriever.")
        exit(1)

    # This path should match the one used in query_embedding_store.py
    faiss_path = "data/context_faiss_store_v1" 

    print(f"Attempting to load FAISS index from: {faiss_path}")
    try:
        retriever = QueryRetriever(openai_api_key=api_key, faiss_index_folder_path=faiss_path)

        if retriever.vector_store is not None:
            print("\n--- Testing Document Retrieval ---")
            # This query might need to be adjusted based on the actual content of your dummy DDL/data
            # For the dummy DDL in query_embedding_store.py, tables are Users, Products
            user_question = "Show me all products and their prices" 
            # Or for a more generic query if your dummy DDL is very simple:
            # user_question = "user information"

            print(f"Sample User Question: \"{user_question}\"")
            
            retrieved_docs = retriever.retrieve_relevant_documents(user_question, k=3)

            if retrieved_docs:
                print("\nRetrieved Documents (Top 3):")
                for i, doc_info in enumerate(retrieved_docs):
                    print(f"\nDocument {i+1}:")
                    print(f"  Content: {doc_info['content'][:200]}...") # Print first 200 chars
                    print(f"  Metadata: {doc_info['metadata']}")
                    print(f"  Score: {doc_info['score']:.4f}") # Format score
            else:
                print("No documents retrieved. This could be due to an empty index, a very specific query, or other issues.")
        else:
            print("\nRetriever could not be tested as the FAISS index was not loaded.")
            print("Please ensure 'query_embedding_store.py' has been run successfully to create the index,")
            print(f"or check the path '{faiss_path}' and any error messages above.")

    except Exception as e:
        print(f"An unexpected error occurred in the main block: {e}")
        import traceback
        traceback.print_exc()
