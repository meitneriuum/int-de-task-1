DATABASE = 'task1'
ROOMS = 'rooms'
STUDENTS = 'students'


# -------- QUERIES --------

sql_create_table = {
    ROOMS: "CREATE TABLE IF NOT EXISTS rooms (id INT PRIMARY KEY, name VARCHAR(9));",
    STUDENTS: """CREATE TABLE IF NOT EXISTS students (
	birthday timestamp,
	id INT PRIMARY KEY,
	name VARCHAR(30),
	room INT NOT NULL,
	sex VARCHAR(1),
	FOREIGN KEY (room) REFERENCES rooms(id)
    );
    """
}


sql_create_db = """
    CREATE DATABASE IF NOT EXISTS task1
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_United States.1251'
    LC_CTYPE = 'English_United States.1251'
    LOCALE_PROVIDER = 'libc'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;
    """

sql_drop_db = """DROP DATABASE IF EXISTS task1;"""

sql_drop_table = """DROP TABLE IF EXISTS %s CASCADE"""