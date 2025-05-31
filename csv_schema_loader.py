import csv
import pathlib
import os # Optional, for path joining if needed, though pathlib is preferred
from dataclasses import dataclass, field
from typing import List, Dict, Optional

# --- Dataclasses ---
@dataclass
class TableInfo:
    source_database: str
    name: str
    category: Optional[str]
    description: Optional[str]

@dataclass
class ColumnInfo:
    database_name: str
    table_name: str
    column_name: str
    data_type: str  # From SQL_FORMAT
    description: Optional[str]
    valid_values: Optional[str] = None
    connect_portal_only: Optional[str] = None # Or bool if appropriate
    corresponding_map_table: Optional[str] = None

@dataclass
class JoinInfo:
    source_database: str
    primary_table_schema: Optional[str]
    primary_table_name: str
    primary_table_column: str
    relationship_type: Optional[str]
    foreign_table_schema: Optional[str]
    foreign_table_name: str
    foreign_table_column: str
    join_description: Optional[str]

# --- CSVSchemaLoader Class ---
class CSVSchemaLoader:
    def __init__(self, data_folder_path: str = "data/"):
        self.base_path = pathlib.Path(data_folder_path)
        self.tables: Dict[str, TableInfo] = {} # Store by table_name for easy lookup
        self.columns: List[ColumnInfo] = []
        self.joins: List[JoinInfo] = []
        
        self._load_tables()
        self._load_columns()
        self._load_joins()

    def _read_csv(self, file_name: str) -> List[Dict[str, str]]:
        file_path = self.base_path / file_name
        if not file_path.exists():
            raise FileNotFoundError(f"Error: CSV file not found at {file_path}")
        
        records = []
        try:
            # Using utf-8-sig to handle potential BOM (Byte Order Mark) in CSV files
            with open(file_path, mode='r', encoding='utf-8-sig', newline='') as infile:
                reader = csv.DictReader(infile)
                for row in reader:
                    records.append(row)
        except Exception as e:
            print(f"Error reading CSV {file_path}: {e}")
        return records

    def _load_tables(self):
        records = self._read_csv("table_related_information.csv")
        for rec in records:
            table = TableInfo(
                source_database=rec.get('Source_database', '').strip(),
                name=rec.get('table_name', '').strip(), # Ensure key matches CSV header
                category=rec.get('Table_category', '').strip(),
                description=rec.get('Description', '').strip()
            )
            if table.name: # Only add if name is present
                self.tables[table.name] = table
    
    def _load_columns(self):
        records = self._read_csv("column_related_information.csv")
        for rec in records:
            column = ColumnInfo(
                database_name=rec.get('Database_name', '').strip(),
                table_name=rec.get('Table_name', '').strip(),
                column_name=rec.get('column_name', '').strip(), # Ensure key matches CSV header
                data_type=rec.get('SQL_FORMAT', '').strip(),
                description=rec.get('Description (a very descriptive one)', '').strip(),
                valid_values=rec.get('Valid_values', '').strip(),
                connect_portal_only=rec.get('Connect_portal_only', '').strip(),
                corresponding_map_table=rec.get('corresponding_map_table', '').strip()
            )
            # Ensure essential fields are present before appending
            if column.table_name and column.column_name and column.data_type:
                self.columns.append(column)

    def _load_joins(self):
        records = self._read_csv("join_related_information.csv")
        for rec in records:
            join = JoinInfo(
                source_database=rec.get('Source_Database', '').strip(),
                primary_table_schema=rec.get('Primary_Table_Schema', '').strip(),
                primary_table_name=rec.get('Primary_Table_Name', '').strip(),
                primary_table_column=rec.get('Primary_Table_Column', '').strip(),
                relationship_type=rec.get('Relationship_Type', '').strip(),
                foreign_table_schema=rec.get('Foreign_Table_Schema', '').strip(),
                foreign_table_name=rec.get('Foreign_Table_Name', '').strip(),
                foreign_table_column=rec.get('Foreign_Table_Column', '').strip(),
                join_description=rec.get('Join_Description', '').strip()
            )
            # Ensure essential fields for a join are present
            if join.primary_table_name and join.primary_table_column and \
               join.foreign_table_name and join.foreign_table_column:
                self.joins.append(join)
    
    # Public access methods
    def get_tables(self) -> List[TableInfo]:
        return list(self.tables.values())

    def get_table_by_name(self, name: str) -> Optional[TableInfo]:
        return self.tables.get(name)

    def get_columns_for_table(self, table_name: str) -> List[ColumnInfo]:
        return [col for col in self.columns if col.table_name == table_name]

    def get_all_columns(self) -> List[ColumnInfo]:
        return self.columns

    def get_foreign_keys(self) -> List[JoinInfo]: # Renamed from get_joins to be more standard
        return self.joins
        
    def get_table_description(self, table_name: str) -> Optional[str]:
        table = self.get_table_by_name(table_name)
        return table.description if table else None

    def get_column_description(self, table_name: str, column_name: str) -> Optional[str]:
        for col in self.columns:
            if col.table_name == table_name and col.column_name == column_name:
                return col.description
        return None

# --- if __name__ == '__main__': Block ---
if __name__ == "__main__":
    # This assumes that the script is run from a directory where "data/" is a subdirectory,
    # or that "data/" is in the current working directory.
    # For robust execution, consider using absolute paths or paths relative to this script file.
    # script_dir = pathlib.Path(__file__).parent
    # data_path = script_dir / "data"
    # loader = CSVSchemaLoader(data_folder_path=str(data_path))
    
    # Per instructions, assume CSVs are in "data/" relative to CWD
    loader = CSVSchemaLoader(data_folder_path="data/") 
    
    print(f"Loaded {len(loader.get_tables())} tables.")
    for table in loader.get_tables()[:3]: # Print first 3 tables as sample
        print(f"  Table: {table.name} (Category: {table.category})")
        # print(f"    Desc: {table.description}") # Uncomment for more detail
        cols_for_table = loader.get_columns_for_table(table.name)
        print(f"    Columns ({len(cols_for_table)}):")
        for col_info in cols_for_table[:3]: # Print first 3 cols as sample
             print(f"      - {col_info.column_name} ({col_info.data_type}): {col_info.description[:50] if col_info.description else ''}...")
        if len(cols_for_table) > 3:
            print("      ...")
    if len(loader.get_tables()) > 3:
        print("  ...")
    
    print(f"\nLoaded {len(loader.get_all_columns())} columns in total.")
    
    print(f"\nLoaded {len(loader.get_foreign_keys())} foreign key relationships.")
    for fk in loader.get_foreign_keys()[:3]: # Print first 3 FKs
        print(f"  FK: {fk.primary_table_name}.{fk.primary_table_column} -> {fk.foreign_table_name}.{fk.foreign_table_column}")
    if len(loader.get_foreign_keys()) > 3:
        print("  ...")

    # Example of getting specific descriptions (assuming some table/column names)
    # Replace "YourTableName" and "YourColumnName" with actual names from your CSVs if you want to test these.
    # test_table_name = loader.get_tables()[0].name if loader.get_tables() else None
    # if test_table_name:
    #     print(f"\nDescription for '{test_table_name}' table:", loader.get_table_description(test_table_name))
    #     test_columns = loader.get_columns_for_table(test_table_name)
    #     if test_columns:
    #         test_column_name = test_columns[0].column_name
    #         print(f"Description for '{test_column_name}' column in '{test_table_name}':", loader.get_column_description(test_table_name, test_column_name))
    # else:
    #     print("\nNo tables loaded, skipping specific description examples.")
