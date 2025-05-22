import pandas as pd
import numpy as np
import re
import os
from typing import List, Dict, Any

# Try to import sentence_transformers and faiss
# Fall back to simpler implementation if not available
try:
    from sentence_transformers import SentenceTransformer
    import faiss
    EMBEDDINGS_AVAILABLE = True
except ImportError:
    print("Warning: sentence-transformers or faiss not available. Using fallback similarity.")
    EMBEDDINGS_AVAILABLE = False

class SchemaEmbeddingStore:
    def __init__(self, ddl_file_path, embedding_file="data/schema_embeddings.csv"):
        self.ddl_file_path = ddl_file_path
        self.embedding_file = embedding_file
        self.statements = []
        self.embeddings = None
        self.index = None
        
        # Initialize embedding model if available
        if EMBEDDINGS_AVAILABLE:
            self.model = SentenceTransformer('all-MiniLM-L6-v2')
        
        # Check if embeddings already exist
        if os.path.exists(embedding_file) and EMBEDDINGS_AVAILABLE:
            try:
                self.load_embeddings()
                print(f"Loaded existing embeddings from {embedding_file}")
            except Exception as e:
                print(f"Error loading embeddings: {str(e)}")
                self.create_embeddings()
                self.save_embeddings()
        else:
            print(f"Creating new schema information from {ddl_file_path}")
            self.parse_ddl_statements()
            
            if EMBEDDINGS_AVAILABLE:
                self.create_embeddings()
                self.save_embeddings()
                # Build the FAISS index
                self.build_index()
    
    def parse_ddl_statements(self):
        """Parse DDL file into individual statements."""
        if not os.path.exists(self.ddl_file_path):
            print(f"Warning: DDL file {self.ddl_file_path} not found.")
            return
            
        with open(self.ddl_file_path, 'r') as f:
            ddl_content = f.read()
        
        # Split by statement terminators
        raw_statements = re.split(r';(?=\s*(?:CREATE|ALTER|DROP|INSERT))', ddl_content)
        
        # Clean and categorize statements
        for stmt in raw_statements:
            stmt = stmt.strip()
            if not stmt:
                continue
                
            # Add metadata to each statement
            statement_info = {
                "text": stmt,
                "type": self._get_statement_type(stmt),
                "table": self._extract_table_name(stmt),
                "columns": self._extract_columns(stmt)
            }
            
            self.statements.append(statement_info)
    
    def _get_statement_type(self, stmt):
        """Determine the type of SQL statement."""
        if re.match(r'CREATE\s+TABLE', stmt, re.IGNORECASE):
            return "CREATE_TABLE"
        elif re.match(r'ALTER\s+TABLE', stmt, re.IGNORECASE):
            return "ALTER_TABLE"
        elif re.match(r'CREATE\s+INDEX', stmt, re.IGNORECASE):
            return "CREATE_INDEX"
        else:
            return "OTHER"
    
    def _extract_table_name(self, stmt):
        """Extract table name from statement."""
        match = re.search(r'(?:TABLE|INDEX)\s+\[?(\w+)\]?', stmt, re.IGNORECASE)
        return match.group(1) if match else ""
    
    def _extract_columns(self, stmt):
        """Extract column names from statement."""
        if "CREATE TABLE" in stmt.upper():
            # Extract column definitions
            match = re.search(r'CREATE\s+TABLE\s+\[?\w+\]?\s*\(([\s\S]*?)\)', stmt, re.IGNORECASE)
            if match:
                column_text = match.group(1)
                columns = []
                for col_match in re.finditer(r'\[?(\w+)\]?\s+(\w+)', column_text):
                    columns.append(col_match.group(1))
                return columns
        return []
    
    def create_embeddings(self):
        """Create embeddings for all statements."""
        if not EMBEDDINGS_AVAILABLE:
            print("Warning: Embeddings not available. Skipping embedding creation.")
            return
            
        # Create text representations for embedding
        texts = []
        for stmt in self.statements:
            # Create a rich text representation that includes metadata
            text = f"{stmt['type']} {stmt['table']} "
            if stmt['columns']:
                text += f"COLUMNS: {', '.join(stmt['columns'])} "
            text += stmt['text'][:500]  # Limit length for embedding
            texts.append(text)
        
        # Generate embeddings
        self.embeddings = self.model.encode(texts)
    
    def save_embeddings(self):
        """Save embeddings and statements to CSV."""
        if not EMBEDDINGS_AVAILABLE:
            return
            
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(self.embedding_file), exist_ok=True)
        
        df = pd.DataFrame({
            'statement_type': [s['type'] for s in self.statements],
            'table': [s['table'] for s in self.statements],
            'columns': [','.join(s['columns']) for s in self.statements],
            'text': [s['text'] for s in self.statements]
        })
        
        # Save embeddings as separate columns
        for i in range(self.embeddings.shape[1]):
            df[f'emb_{i}'] = self.embeddings[:, i]
        
        df.to_csv(self.embedding_file, index=False)
    
    def load_embeddings(self):
        """Load embeddings from CSV."""
        if not EMBEDDINGS_AVAILABLE:
            return
            
        df = pd.read_csv(self.embedding_file)
        
        # Reconstruct statements
        self.statements = []
        for _, row in df.iterrows():
            self.statements.append({
                'type': row['statement_type'],
                'table': row['table'],
                'columns': row['columns'].split(',') if pd.notna(row['columns']) and row['columns'] else [],
                'text': row['text']
            })
        
        # Extract embeddings
        emb_cols = [col for col in df.columns if col.startswith('emb_')]
        self.embeddings = df[emb_cols].values
    
    def build_index(self):
        """Build FAISS index for fast similarity search."""
        if not EMBEDDINGS_AVAILABLE or self.embeddings is None:
            return
            
        dimension = self.embeddings.shape[1]
        self.index = faiss.IndexFlatL2(dimension)
        self.index.add(self.embeddings.astype(np.float32))
    
    def search(self, query, top_k=5):
        """Search for most relevant schema elements."""
        if not EMBEDDINGS_AVAILABLE or self.index is None:
            # Fallback to keyword matching
            return self._keyword_search(query, top_k)
            
        # Encode the query
        query_embedding = self.model.encode([query])
        
        # Search the index
        distances, indices = self.index.search(query_embedding.astype(np.float32), top_k)
        
        # Return the relevant statements
        results = []
        for idx in indices[0]:
            results.append(self.statements[idx])
        
        return results
    
    def _keyword_search(self, query, top_k=5):
        """Fallback search using keyword matching."""
        query_terms = set(re.findall(r'\b\w+\b', query.lower()))
        
        # Score each statement based on term overlap
        scored_statements = []
        for stmt in self.statements:
            score = 0
            
            # Check table name
            if stmt['table'].lower() in query_terms:
                score += 3
            
            # Check columns
            for col in stmt['columns']:
                if col.lower() in query_terms:
                    score += 2
            
            # Check statement text
            stmt_terms = set(re.findall(r'\b\w+\b', stmt['text'].lower()))
            score += len(query_terms.intersection(stmt_terms))
            
            scored_statements.append((score, stmt))
        
        # Sort by score and return top_k
        scored_statements.sort(reverse=True, key=lambda x: x[0])
        return [stmt for score, stmt in scored_statements[:top_k]] 