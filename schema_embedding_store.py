import os
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
import json
import pickle
from langchain_openai import OpenAIEmbeddings
from langchain.vectorstores import FAISS
from langchain.schema import Document

# Load environment variables
load_dotenv()

class SchemaEmbeddingStore:
    """Store and retrieve embeddings for database schema elements."""
    
    def __init__(self, cache_path: str = "data/schema_embeddings.pkl"):
        """Initialize the embedding store."""
        self.cache_path = cache_path
        self.embeddings_model = OpenAIEmbeddings(
            model="text-embedding-3-small",
            openai_api_key=os.getenv("OPENAI_API_KEY")
        )
        self.vector_store = None
        self._load_or_create_store()
    
    def _load_or_create_store(self):
        """Load existing vector store or create a new one."""
        if os.path.exists(self.cache_path):
            try:
                with open(self.cache_path, "rb") as f:
                    self.vector_store = pickle.load(f)
                print(f"Loaded schema embeddings from {self.cache_path}")
            except Exception as e:
                print(f"Error loading embeddings: {e}")
                self._create_new_store()
        else:
            self._create_new_store()
    
    def _create_new_store(self):
        """Create a new vector store."""
        self.vector_store = FAISS.from_documents(
            documents=[Document(page_content="schema_placeholder")],
            embedding=self.embeddings_model
        )
        print("Created new schema embedding store")
    
    def add_schema_elements(self, elements: List[Dict[str, Any]]):
        """Add schema elements to the vector store."""
        documents = []
        for element in elements:
            # Create a document for each schema element
            content = f"{element['type']}: {element['name']}"
            if element.get('description'):
                content += f"\nDescription: {element['description']}"
            if element.get('columns'):
                columns_str = ", ".join([f"{col['name']} ({col['type']})" for col in element['columns']])
                content += f"\nColumns: {columns_str}"
            
            doc = Document(
                page_content=content,
                metadata=element
            )
            documents.append(doc)
        
        # Create a new vector store with the documents
        self.vector_store = FAISS.from_documents(
            documents=documents,
            embedding=self.embeddings_model
        )
        
        # Save the vector store
        self._save_store()
    
    def _save_store(self):
        """Save the vector store to disk."""
        os.makedirs(os.path.dirname(self.cache_path), exist_ok=True)
        with open(self.cache_path, "wb") as f:
            pickle.dump(self.vector_store, f)
        print(f"Saved schema embeddings to {self.cache_path}")
    
    def search(self, query: str, k: int = 5) -> List[Dict[str, Any]]:
        """Search for schema elements relevant to the query."""
        if not self.vector_store:
            return []
        
        results = self.vector_store.similarity_search_with_score(query, k=k)
        
        # Format the results
        formatted_results = []
        for doc, score in results:
            formatted_results.append({
                "content": doc.page_content,
                "metadata": doc.metadata,
                "score": float(score)
            })
        
        return formatted_results
    
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