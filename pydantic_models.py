from pydantic import BaseModel, Field, validator
from typing import List, Dict, Optional, Union, Any

class FilterCondition(BaseModel):
    """A filter condition for a SQL query."""
    column: str
    operator: str
    value: Any

class JoinCondition(BaseModel):
    """A join condition for a SQL query."""
    table1: str
    table2: str
    column1: str
    column2: str

class OrderBy(BaseModel):
    """An order by clause for a SQL query."""
    column: str
    direction: str

class QueryExplanation(BaseModel):
    """A structured explanation of a natural language query."""
    identified_intent: str = Field(description="The main intent of the query (e.g., 'retrieve', 'aggregate', 'filter')")
    target_tables: List[str] = Field(description="List of tables that need to be queried")
    target_columns: List[str] = Field(description="List of columns that need to be retrieved or used in calculations")
    filter_conditions: Optional[List[FilterCondition]] = Field(default_factory=list, description="List of filter conditions to apply")
    join_conditions: Optional[List[JoinCondition]] = Field(default_factory=list, description="List of join conditions if multiple tables are involved")
    group_by: Optional[List[str]] = Field(default_factory=list, description="List of columns to group by, if any")
    order_by: Optional[OrderBy] = None
    limit: Optional[int] = None
    summary_of_understanding: str = Field(description="A human-readable summary of the query understanding")

    @validator('target_tables')
    def validate_tables(cls, v):
        """Validate that at least one table is specified."""
        if not v:
            raise ValueError("At least one target table must be specified")
        return v

    @validator('target_columns')
    def validate_columns(cls, v):
        """Validate that at least one column is specified."""
        if not v:
            raise ValueError("At least one target column must be specified")
        return v

    def dict(self, *args, **kwargs):
        """Override dict method to handle None values properly."""
        result = super().dict(*args, **kwargs)
        # Convert empty lists to None for better readability
        for key, value in result.items():
            if isinstance(value, list) and len(value) == 0:
                result[key] = None
        return result

class SQLOutput(BaseModel):
    """The output of SQL generation."""
    sql_query: str = Field(description="The generated SQL query")
    query_valid: bool = Field(description="Whether the query is valid")
    validation_error: Optional[str] = Field(default=None, description="Error message if the query is invalid")

class QueryResult(BaseModel):
    """The result of executing a SQL query."""
    success: bool = Field(description="Whether the query execution was successful")
    data: Optional[List[Dict[str, Any]]] = Field(default=None, description="The query results as a list of dictionaries")
    error_message: Optional[str] = Field(default=None, description="Error message if the query execution failed")
    row_count: Optional[int] = Field(default=None, description="Number of rows returned")
    column_names: Optional[List[str]] = Field(default=None, description="Names of the columns in the result") 