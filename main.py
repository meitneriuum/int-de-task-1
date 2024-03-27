import json
import psycopg2
from pg_config import config
from const import *


class DatabaseManager:
    def __init__(self) -> None:
        self.connection: psycopg2.extensions.connection = None
        self.cursor: psycopg2.extensions.cursor = None

    def connect(self) -> None:
        """Establishes connection to the db and returns the connection object."""
        conf: dict = config()
        self.connection = psycopg2.connect(dbname=conf['dbname'], user=conf['user'], password=conf['password'], port=conf['port'])
        self.cursor = self.connection.cursor()

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


def read_json(path: str) -> list[dict]:
    """Performs json deserialization and returns a Python object"""
    with open(path, 'r') as file:
        return json.loads(file.read())


def main() -> None:
    db_manager = DatabaseManager()
    db_manager.connect()
    
    data_rooms: list[dict] = read_json(ROOMS_DATA_PATH)
    data_students: list[dict] = read_json(STUDENTS_DATA_PATH)
    
    db_manager.create_table(ROOMS, drop=True)
    db_manager.create_table(STUDENTS, drop=True)
    
    db_manager.insert_values(data_rooms, ROOMS, drop=True)
    db_manager.insert_values(data_students, STUDENTS, drop=True)
    
    db_manager.disconnect()


if __name__ == '__main__':
    main()
