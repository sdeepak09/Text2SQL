import os
from dotenv import load_dotenv

try:
    from langchain_openai import ChatOpenAI
    from langchain_core.prompts import ChatPromptTemplate
    # SystemMessage and HumanMessage are implicitly handled by ChatPromptTemplate.from_messages
    # from langchain_core.messages import HumanMessage, SystemMessage
except ImportError:
    print("Langchain modules not fully available. Please ensure langchain_openai and langchain_core are installed.")
    # Dummy classes for basic script structure
    class ChatOpenAI:
        def __init__(self, model_name, openai_api_key, temperature): pass
        def invoke(self, messages): return type('obj', (object,), {'content': '-- Dummy SQL Query'})() # Dummy response
    class ChatPromptTemplate:
        @staticmethod
        def from_messages(messages_list): return ChatPromptTemplate()
        def format_messages(self, **kwargs): return []


from query_retriever import QueryRetriever # Assuming query_retriever.py is in the same directory or PYTHONPATH

class RAGSQLGenerator:
    """
    Generates SQL queries based on user questions using a RAG approach
    with a FAISS vector store and an LLM.
    """
    def __init__(self, openai_api_key: str, faiss_index_folder_path: str, llm_model_name: str = "gpt-3.5-turbo"):
        """
        Initializes the RAGSQLGenerator.

        Args:
            openai_api_key: OpenAI API key.
            faiss_index_folder_path: Path to the folder containing the FAISS index.
            llm_model_name: The name of the LLM model to use (e.g., "gpt-3.5-turbo", "gpt-4").
        """
        self.query_retriever = QueryRetriever(
            openai_api_key=openai_api_key, 
            faiss_index_folder_path=faiss_index_folder_path
        )
        
        try:
            self.llm = ChatOpenAI(
                model_name=llm_model_name, 
                openai_api_key=openai_api_key, 
                temperature=0  # For deterministic SQL generation
            )
        except Exception as e:
            print(f"Error initializing ChatOpenAI: {e}. Ensure OPENAI_API_KEY is valid and model name is correct.")
            self.llm = None # Or raise error
            # Depending on desired behavior, might re-raise or handle gracefully
            raise

        if self.query_retriever.vector_store is None:
            print("Warning: FAISS index not loaded in QueryRetriever. RAG capabilities will be non-functional.")
            print("Please ensure the FAISS index exists at the specified path and was built successfully.")

    def _format_retrieved_context(self, retrieved_docs: list[dict]) -> str:
        """
        Formats the list of retrieved documents into a single string for the LLM context.
        """
        if not retrieved_docs:
            return "No relevant context found."

        context_parts = []
        for doc_info in retrieved_docs:
            content = doc_info.get('content', '')
            metadata_type = doc_info.get('metadata', {}).get('type', 'unknown')

            if metadata_type == 'table':
                context_parts.append(f"Table Schema: {content}")
            elif metadata_type == 'column':
                context_parts.append(f"Column Schema: {content}")
            elif metadata_type == 'example_query':
                context_parts.append(f"-- Example SQL Query:\n{content}")
            else:
                # Fallback for unknown types, or just skip
                context_parts.append(f"Context: {content}") 
        
        return "\n\n".join(context_parts)

    def generate_sql_query(self, user_question: str, k_retrieved_items: int = 5) -> str:
        """
        Generates an SQL query based on the user question and retrieved context.

        Args:
            user_question: The user's natural language question.
            k_retrieved_items: The number of items to retrieve from the vector store.

        Returns:
            The generated SQL query string.
        """
        if self.llm is None:
            print("Error: LLM not initialized. Cannot generate SQL query.")
            return "-- LLM not initialized"

        if self.query_retriever.vector_store is None:
            print("Warning: Vector store not available. Proceeding without RAG context (this may lead to poor results).")
            formatted_context = "No RAG context available due to missing vector store."
        else:
            retrieved_docs = self.query_retriever.retrieve_relevant_documents(user_question, k=k_retrieved_items)
            if not retrieved_docs:
                print("Warning: No relevant documents found in vector store for the question. Query will be generated without specific RAG context.")
                formatted_context = "No specific RAG context found for this question."
            else:
                formatted_context = self._format_retrieved_context(retrieved_docs)

        system_template = """
You are an expert SQL generation assistant. Your task is to generate a syntactically correct SQL query that answers the user's question.
You will be provided with context that includes relevant table schemas, column schemas, and example SQL queries.
Use the provided table and column information to ensure correct table and field names.
The example SQL queries can guide you on query structure, joins, and filters.
Only generate the SQL query. Do not include any explanations or markdown formatting (e.g., ```sql ... ```).
If the provided context is insufficient or no context is provided, try to generate the query based on the user's question alone, but prioritize using the context if available.
If you cannot generate a meaningful query, return an empty string or a standard message like "-- Unable to generate query based on the information."

Database Schema and Examples:
{context}
"""
        human_template = "User Question: {question}"

        try:
            chat_prompt = ChatPromptTemplate.from_messages([
                ("system", system_template),
                ("human", human_template)
            ])
            
            messages = chat_prompt.format_messages(context=formatted_context, question=user_question)
            
            response = self.llm.invoke(messages)
            sql_query = response.content
            
            # Strip potential markdown backticks and leading/trailing whitespace
            if sql_query.startswith("```sql"):
                sql_query = sql_query[len("```sql"):]
            if sql_query.endswith("```"):
                sql_query = sql_query[:-len("```")]
            sql_query = sql_query.strip()
            
            return sql_query

        except Exception as e:
            print(f"Error during LLM invocation or prompt formatting: {e}")
            return "-- Error generating SQL query"


if __name__ == '__main__':
    load_dotenv()

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("Error: OPENAI_API_KEY environment variable not set. Please set it to run the RAG SQL generator.")
        exit(1)

    # This path should match the one used in query_embedding_store.py and query_retriever.py
    faiss_path = "data/context_faiss_store_v1" 
    # Ensure this path exists and contains the FAISS index.
    # If not, query_retriever will print a warning, and RAG will be limited.

    print(f"Initializing RAGSQLGenerator with FAISS index from: {faiss_path}")
    try:
        rag_generator = RAGSQLGenerator(
            openai_api_key=api_key, 
            faiss_index_folder_path=faiss_path,
            llm_model_name="gpt-3.5-turbo" # Or "gpt-4" if preferred and available
        )
    except Exception as e:
        print(f"Failed to initialize RAGSQLGenerator: {e}")
        exit(1)

    if rag_generator.query_retriever.vector_store is None:
        print("--- RAG SQL Generator initialized, but FAISS index was not loaded. ---")
        print("--- Functionality will be limited (no RAG context).        ---")
    else:
        print("--- RAG SQL Generator initialized successfully with FAISS index. ---")

    print("\n--- Testing SQL Query Generation ---")
    
    # Example questions - adjust based on your actual schema in data/database_schema.sql
    # If using the dummy DDL from previous steps (Users, Products, Categories, Orders, Claims, Policies, Customers)
    questions = [
        "What are the names and email addresses of all users registered in 2023?", # Needs date functions
        "Show me all products in the 'Electronics' category.", # Needs Categories table if joined
        "List orders placed by user with id 123.",
        "What is the total claim amount for policy number 'POL987'?", # Needs Claims and Policies tables
        "Find customers who have policies starting after January 1, 2024."
    ]
    
    # A more generic question if using a very simple dummy schema:
    # questions = ["Show all users and their emails.", "List products with price greater than 50."]

    for user_q in questions:
        print(f"\nUser Question: \"{user_q}\"")
        try:
            sql_query = rag_generator.generate_sql_query(user_q, k_retrieved_items=5)
            print(f"Generated SQL Query:\n{sql_query}")
        except Exception as e:
            print(f"An error occurred while generating query for '{user_q}': {e}")
            import traceback
            traceback.print_exc()

    # Test with a question that might not have direct context, to see fallback
    print(f"\nUser Question (less context expected): \"Count all tables in the database.\"")
    try:
        sql_query_no_ctx = rag_generator.generate_sql_query("Count all tables in the database.", k_retrieved_items=2)
        print(f"Generated SQL Query:\n{sql_query_no_ctx}")
    except Exception as e:
        print(f"An error occurred: {e}")

    # To test the scenario where FAISS index is missing, you might temporarily rename the faiss_path folder
    # and re-run. The warnings in __init__ and generate_sql_query should appear.
