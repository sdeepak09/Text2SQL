"""
This script generates descriptions for tables and columns from a DDL file.
"""

from schema_parser import SchemaParser

class SchemaDescriptionGenerator:
    """
    Generates descriptions for tables and columns from a DDL file.
    """
    def __init__(self, ddl_file_path: str):
        """
        Initializes the SchemaDescriptionGenerator with a DDL file path.

        Args:
            ddl_file_path: The path to the DDL file.
        """
        self.schema_parser = SchemaParser(ddl_file_path)

    def get_table_descriptions(self) -> list[dict]:
        """
        Generates descriptions for each table in the schema.

        Returns:
            A list of dictionaries, where each dictionary represents a table
            description.
        """
        table_descriptions = []
        for table_name, table_data in self.schema_parser.tables.items():
            columns_str = ", ".join([
                f"{col['name']} ({col['type']})" for col in table_data['columns']
            ])
            description_string = (
                f"Table '{table_name}' contains columns: {columns_str}"
            )
            table_descriptions.append({
                'type': 'table',
                'table_name': table_name,
                'content': description_string
            })
        return table_descriptions

    def get_column_descriptions(self) -> list[dict]:
        """
        Generates descriptions for each column in each table of the schema.

        Returns:
            A list of dictionaries, where each dictionary represents a column
            description.
        """
        column_descriptions = []
        for table_name, table_data in self.schema_parser.tables.items():
            for column in table_data['columns']:
                column_name = column['name']
                # Basic description, can be improved later
                description_string = (
                    f"Column '{column_name}' in table '{table_name}' "
                    f"represents the {column_name.lower()} of the {table_name}."
                )
                column_descriptions.append({
                    'type': 'column',
                    'table_name': table_name,
                    'column_name': column_name,
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
        # Assuming 'data/database_schema.sql' exists for demonstration
        # In a real scenario, ensure this path is correct and the file exists.
        # For the purpose of this task, we'll assume SchemaParser handles
        # FileNotFoundError if the file is missing.
        generator = SchemaDescriptionGenerator('data/database_schema.sql')
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
