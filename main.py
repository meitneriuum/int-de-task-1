import json
import functools
import psycopg2
from pg_config import config
from const import *


def db_connect():
    """Establishes connection to the db and returns the connection object."""

    conf = config()
    connection = psycopg2.connect(dbname=conf['dbname'], user=conf['user'], password=conf['password'], port=conf['port'])
    return connection


def connection_handler(func):
    """A decorator that establishes connection to the db and passes a created cursor to the called function. 
    When the func is executed, the transaction is being commited, and the connection is being closed."""

    @functools.wraps(func)
    def inner(*args, **kwargs):
        conf = config()
        connection = psycopg2.connect(dbname=conf['dbname'], user=conf['user'], password=conf['password'], port=conf['port'])
        cursor = connection.cursor()
        func(cursor, *args, **kwargs)
        connection.commit()
        connection.close()
    return inner


def read_json(path: str) -> list[dict]:
    """Performs json deserialization and returns a Python object"""

    file = open(path,'r')
    return json.loads(file.read())


@connection_handler
def create_table(cursor, table, drop=True):
    """Creates table if not exists, drops table first if not specified otherwise"""
    if drop:
        cursor.execute(sql_drop_table % table)
        print(f'dropped {table}')
    cursor.execute(sql_create_table[table])


@connection_handler
def insert_values(cursor, data: list, table: str, drop: bool = False) -> None:
    """Inserts values from data to the specified table."""

    column_names = data[0].keys()

    query = "INSERT INTO {} ({}) VALUES ({})".format(
        table,
        ', '.join(column_names),
        ', '.join(['%s'] * len(column_names))
    )

    for row in data:
        cursor.execute(query, list(row.values()))


def main():
    data_rooms = read_json("./data/rooms.json")
    data_students = read_json("./data/students.json")
    create_table(ROOMS, drop=True)
    create_table(STUDENTS, drop=True)
    insert_values(data_rooms, ROOMS, drop=True)
    insert_values(data_students, STUDENTS, drop=True)


if __name__ == '__main__':
    main()
