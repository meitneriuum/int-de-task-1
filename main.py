import json
import psycopg2
from psycopg2.extras import RealDictCursor
import xml.etree.ElementTree as ET
from pg_config import config
from const import *
import os
from datetime import datetime, timedelta


class DatabaseManager:
    def __init__(self) -> None:
        self.connection: psycopg2.extensions.connection = None
        self.cursor: psycopg2.extensions.cursor = None

    def connect(self) -> None:
        """Establishes connection to the db and returns the connection object."""
        conf: dict = config()
        self.connection = psycopg2.connect(dbname=conf['dbname'], user=conf['user'], password=conf['password'], port=conf['port'])
        self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)

    def disconnect(self) -> None:
        """Closes the cursor and connection."""
        self.cursor.close()
        self.connection.close()

    def create_table(self, table: str, drop: bool = True) -> None:
        """Creates table if not exists, drops table first if not specified otherwise"""
        if drop:
            self.cursor.execute(sql_drop_table % table)
            print(f'dropped {table}')
        self.cursor.execute(sql_create_table[table])
        self.connection.commit()

    def insert_values(self, data: list[dict], table: str, drop: bool = False) -> None:
        """Inserts values from data to the specified table."""
        column_names: list[str] = data[0].keys()
        query = "INSERT INTO {} ({}) VALUES ({})".format(
            table,
            ', '.join(column_names),
            ', '.join(['%s'] * len(column_names))
        )
        for row in data:
            self.cursor.execute(query, list(row.values()))
        self.connection.commit()

    def run_query(self, query: str) -> list[dict]:
        """Executes a query and returns the result as a list of dictionaries."""
        self.cursor.execute(query)
        return self.cursor.fetchall()


def read_json(path: str) -> list[dict]:
    """Performs json deserialization and returns a Python object"""
    with open(path, 'r') as file:
        return json.loads(file.read())


def default_encoder(obj):
    """JSON serializer for datetime and timedelta objects."""
    if isinstance(obj, (datetime, timedelta)):
        return obj.__str__()


def write_to_file(data: list[dict], file_path: str, output_format: str) -> None:
    """Writes data to a file in either JSON or XML format."""
    if output_format.lower() == 'json':
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4, default=default_encoder)
    elif output_format.lower() == 'xml':
        if not data:
            return
        
        # Get column names from the first row of data
        column_names = list(data[0].keys())
        
        root = ET.Element("data")
        for entry in data:
            item = ET.SubElement(root, "item")
            for col_name in column_names:
                field = ET.SubElement(item, col_name)
                field.text = str(entry[col_name])
        tree = ET.ElementTree(root)
        tree.write(file_path)


def main() -> None:
    # Prompting user to choose between default or custom paths
    path_choice = input("Do you want to use default data file paths? (yes/no): ").lower()
    
    if path_choice == "yes":
        # Using default paths
        rooms_data_path = ROOMS_DATA_PATH
        students_data_path = STUDENTS_DATA_PATH
    else:
        # Prompting user for custom data file paths
        rooms_data_path = input("Enter the path to the rooms data file: ")
        students_data_path = input("Enter the path to the students data file: ")

    db_manager = DatabaseManager()
    db_manager.connect()
    
    # Reading data files
    data_rooms: list[dict] = read_json(rooms_data_path)
    data_students: list[dict] = read_json(students_data_path)
    
    # Creating tables
    db_manager.create_table(ROOMS, drop=True)
    db_manager.create_table(STUDENTS, drop=True)
    
    # Inserting values into tables
    db_manager.insert_values(data_rooms, ROOMS, drop=True)
    db_manager.insert_values(data_students, STUDENTS, drop=True)

    # Running user-selected query
    query_choice = int(input("Enter the number of the query you want to run (1-4): "))
    query = QUERIES[query_choice - 1]
    query_result = db_manager.run_query(query)

    # Generating file name with datetime and format information
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_format = input("Enter the output format (JSON or XML): ").lower()
    query_file_name = f"query_{query_choice}_{timestamp}.{output_format}"

    # Saving result to file in 'queries' folder in cwd
    output_folder_path = os.path.join(os.getcwd(), "queries")
    if not os.path.exists(output_folder_path):
        os.makedirs(output_folder_path)
    output_file_path = os.path.join(output_folder_path, query_file_name)
    
    write_to_file(query_result, output_file_path, output_format)

    db_manager.disconnect()

if __name__ == '__main__':
    main()