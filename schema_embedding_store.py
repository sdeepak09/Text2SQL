import os
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
import json
import pickle
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import FAISS # Updated import
from langchain.schema import Document
import logging

# Configure logging at INFO level
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Dummy Embedding class for testing without API keys
class DummyEmbeddings:
    def __init__(self, model="dummy_model", openai_api_key="dummy_key"):
        self.model = model
        self.openai_api_key = openai_api_key
        print("Initialized DummyEmbeddings.")

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        print(f"DummyEmbeddings: Embedding {len(texts)} documents.")
        # Return a list of fixed-size vectors (e.g., size 10)
        # The actual content doesn't matter much for testing structure.
        return [[0.1] * 10 for _ in texts]

    def embed_query(self, text: str) -> List[float]:
        print(f"DummyEmbeddings: Embedding query: '{text}'")
        return [0.1] * 10

class SchemaEmbeddingStore:
    """Store and retrieve embeddings for database schema elements."""
    
    def __init__(self, cache_path: str = "data/schema_embeddings_faiss/"): # Changed default to a directory path
        """Initialize the embedding store."""
        self.cache_path = cache_path
        self.ci_test_mode = os.environ.get("CI_TEST_MODE") == "true"

        if self.ci_test_mode:
            print("CI_TEST_MODE enabled: Using DummyEmbeddings.")
            self.embeddings_model = DummyEmbeddings()
        else:
            self.embeddings_model = OpenAIEmbeddings(
                model="text-embedding-ada-002",
                openai_api_key=os.getenv("OPENAI_API_KEY")
            )
        
        self.vector_store = None
        self._load_or_create_store()
    
    def _load_or_create_store(self):
        """Load existing vector store or create a new one."""
        if self.ci_test_mode:
            print("CI_TEST_MODE: Skipping load and creating a new dummy store.")
            self._create_new_store()
            return

        # Check if the FAISS index directory and a key file exist
        faiss_index_file = os.path.join(self.cache_path, "index.faiss")
        if os.path.isdir(self.cache_path) and os.path.exists(faiss_index_file):
            try:
                self.vector_store = FAISS.load_local(
                    folder_path=self.cache_path,
                    embeddings=self.embeddings_model
                )
                if self.vector_store: # Basic check after loading
                    print(f"Loaded schema embeddings from {self.cache_path}")
                else: # Should not happen if load_local succeeds without error
                    print(f"Failed to load a valid vector store from {self.cache_path}. Creating new store.")
                    self._create_new_store()
            except Exception as e:
                print(f"Error loading FAISS embeddings from {self.cache_path}: {e}. Creating new store.")
                self._create_new_store()
        else:
            self._create_new_store()
    
    def _create_new_store(self):
        """Create a new vector store (without injecting a dummy placeholder).
        (If no documents are provided, FAISS.from_documents will raise an error.)
        (In production, ensure that add_schema_elements is called with actual schema elements.)"""
        self.vector_store = FAISS.from_documents(
            documents=[], 
            embedding=self.embeddings_model
        )
        print("Created new (empty) schema embedding store (not saved yet).")

    def add_schema_elements(self, elements: List[Dict[str, Any]]):
        """Add schema elements to the vector store."""
        print("--- Elements for Embedding from SchemaEmbeddingStore ---")
        # Print each element for clarity, as the whole list can be large
        # Ensure json is imported in this file, or import it locally if preferred.
        for i, element in enumerate(elements):
            print(f"Element {i}: {json.dumps(element, indent=2)}")
        if not elements:
            print("No elements were generated for embedding.")
        print("--- End of Elements for Embedding ---")
        documents = []
        for element in elements: # element is like {"type": ..., "name": ..., "content": "DETAILED_STRING", "metadata": {"actual_meta": ...}}
            if 'content' not in element or 'metadata' not in element:
                print(f"Warning: Skipping element due to missing 'content' or 'metadata': {element}")
                continue

            doc = Document(
                page_content=element['content'], # Use the DETAILED descriptive string here
                metadata=element                 # Store the WHOLE element dict as metadata
            )
            documents.append(doc)
        
        # Create a new vector store with the documents
        if not documents:
            print("No documents to add to the vector store.")
            # (If no documents are provided, the vector store remains empty.)
            return

        self.vector_store = FAISS.from_documents(
            documents=documents,
            embedding=self.embeddings_model
        )
        
        # Save the vector store only if not in CI test mode
        if not self.ci_test_mode:
            self._save_store()
        else:
            print("CI_TEST_MODE: Skipping save of vector store.")

    def _save_store(self):
        """Save the vector store to disk using FAISS's method."""
        if self.ci_test_mode:
            print("CI_TEST_MODE: Skipping save of vector store.")
            return
        
        if self.vector_store is not None:
            os.makedirs(self.cache_path, exist_ok=True)
            self.vector_store.save_local(folder_path=self.cache_path)
            print(f"Saved schema embeddings to {self.cache_path}")
        else:
            print("Warning: Vector store not initialized, nothing to save.")
    
    def search(self, query: str, k: int = 5) -> List[Dict[str, Any]]:
        """Search for relevant schema elements."""
        if not self.vector_store:
            return []
        
        try:
            # Use similarity search with score to get better results
            results = self.vector_store.similarity_search_with_score(query, k=k*2)  # Get more results initially
            
            # Filter and format results
            formatted_results = []
            for doc, score in results:
                # Only include results with reasonable similarity scores
                if score < 1.5:  # Adjust threshold as needed
                    formatted_results.append({
                        "content": doc.page_content,
                        "metadata": doc.metadata,
                        "score": float(score)
                    })
            
            # Return top k results
            return formatted_results[:k]
            
        except Exception as e:
            print(f"Error in embedding search: {e}")
            return []
    
    def get_relevant_schema_context(self, query: str, k: int = 5) -> str:
        """Get a formatted string of schema elements relevant to the query."""
        results = self.search(query, k=k)
        
        if not results:
            return "No schema information available."
        
        context_parts = []
        for result in results:
            metadata = result["metadata"]
            if metadata["type"] == "table":
                # Format table information
                columns_str = "\n".join([f"  - {col['name']} ({col['type']})" for col in metadata.get("columns", [])])
                context_parts.append(f"Table: {metadata['name']}\nColumns:\n{columns_str}\n")
            else:
                # Format other schema elements
                context_parts.append(f"{metadata['type']}: {metadata['name']}\n{result['content']}\n")
        
        return "\n".join(context_parts)

    def search_vector_store(self, query: str, top_k: int = 5) -> List[str]:
        """Search the vector store for relevant statements"""
        if not self.vector_store:
            self._create_new_store()
            if not self.vector_store:
                return []
        
        try:
            results = self.vector_store.similarity_search_with_score(query, k=top_k)
            statements = []
            for doc, score in results:
                statements.append(doc.page_content)
            return statements
        except Exception as e:
            print(f"Error in embedding search: {e}")
            return [] 