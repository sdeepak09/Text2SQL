"""
This script generates descriptions for tables and columns using CSVSchemaLoader.
"""

from csv_schema_loader import CSVSchemaLoader # Updated import
# TableInfo and ColumnInfo might be useful for type hinting if used directly
# from csv_schema_loader import TableInfo, ColumnInfo 

class SchemaDescriptionGenerator:
    """
    Generates descriptions for tables and columns using CSVSchemaLoader.
    """
    def __init__(self, ddl_file_path: str = None, data_folder_path: str = "data/"): # ddl_file_path made optional
        """
        Initializes the SchemaDescriptionGenerator.
        ddl_file_path is now optional and not directly used if CSVSchemaLoader is self-contained.
        data_folder_path points to the directory containing CSV schema files.
        """
        # self.schema_parser = SchemaParser(ddl_file_path) # Removed
        self.csv_loader = CSVSchemaLoader(data_folder_path=data_folder_path)

    def get_table_descriptions(self) -> list[dict]:
        """
        Generates descriptions for each table using data from CSVSchemaLoader.
        """
        table_descriptions = []
        for table_info in self.csv_loader.get_tables():
            table_name = table_info.name
            
            columns_for_this_table = self.csv_loader.get_columns_for_table(table_name)
            cols_str = ", ".join([
                f"{col.column_name} ({col.data_type})" for col in columns_for_this_table
            ])
            
            # Construct description string, incorporating details from TableInfo
            description_string = f"Table '{table_name}'"
            if table_info.category:
                description_string += f" (Category: {table_info.category})"
            if table_info.description:
                description_string += f" has description: '{table_info.description}'."
            else:
                description_string += "." # End sentence if no specific description
            description_string += f" It contains columns: {cols_str}"

            table_descriptions.append({
                'type': 'table',
                'table_name': table_name,
                'content': description_string
            })
        return table_descriptions

    def get_column_descriptions(self) -> list[dict]:
        """
        Generates descriptions for each column using data from CSVSchemaLoader.
        """
        column_descriptions = []
        for col_info in self.csv_loader.get_all_columns():
            description_string = (
                f"Column '{col_info.column_name}' in table '{col_info.table_name}' "
                f"(Type: {col_info.data_type})"
            )
            if col_info.description:
                description_string += f" has description: '{col_info.description}'."
            else:
                description_string += "." # End sentence

            # Optionally add other info like valid_values if present
            if col_info.valid_values:
                description_string += f" Valid values include: '{col_info.valid_values}'."
            
            column_descriptions.append({
                'type': 'column',
                'table_name': col_info.table_name,
                'column_name': col_info.column_name,
                'content': description_string
            })
        return column_descriptions

    def get_all_descriptions(self) -> list[dict]:
        """
        Generates all table and column descriptions.

        Returns:
            A combined list of all table and column descriptions.
        """
        descriptions = []
        descriptions.extend(self.get_table_descriptions())
        descriptions.extend(self.get_column_descriptions())
        return descriptions

if __name__ == '__main__':
    try:
        # CSVSchemaLoader will print errors if CSV files are not found in "data/"
        # No ddl_file_path is strictly needed for instantiation if data_folder_path is fixed
        generator = SchemaDescriptionGenerator() 
        all_descriptions = generator.get_all_descriptions()

        table_desc_count = 0
        column_desc_count = 0

        print("Sample Descriptions:")
        for i, desc in enumerate(all_descriptions):
            if i < 5: # Print first 5 descriptions as examples
                print(f"- {desc['content']}")
            if desc['type'] == 'table':
                table_desc_count += 1
            elif desc['type'] == 'column':
                column_desc_count += 1

        print(f"\nTotal table descriptions generated: {table_desc_count}")
        print(f"Total column descriptions generated: {column_desc_count}")

    except FileNotFoundError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
