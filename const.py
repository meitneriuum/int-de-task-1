DATABASE = 'task1'
ROOMS = 'rooms'
STUDENTS = 'students'
ROOMS_DATA_PATH = "./data/rooms.json"
STUDENTS_DATA_PATH = "./data/students.json"


# -------- QUERIES CREATE/DROP --------

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


# -------- QUERIES SELECT --------

sql_select_rooms = """
    SELECT r.name, COUNT(s.id) AS students_count
    FROM rooms r INNER JOIN students s ON r.id = s.room
    GROUP BY r.id, r.name
    ORDER BY r.id ASC;
"""

sql_select_5_rooms_min_age = """
    SELECT r.name as room_name, AVG(q.age) as avg_age
    FROM rooms r INNER JOIN (
                             SELECT 
                                name, 
                                AGE(CURRENT_DATE, students.birthday) as age, 
                                room
                             FROM students
                            ) q ON r.id = q.room
    GROUP BY r.name
    ORDER BY avg_age ASC
    LIMIT 5;
"""

sql_select_5_rooms_max_diff = """
    SELECT room_name, (max_age_days - min_age_days) as age_diff
    FROM (
        SELECT 
            r.id as room_id,
            r.name as room_name,
            MAX(age) AS max_age_days,
            MIN(age) AS min_age_days
        FROM rooms r INNER JOIN (SELECT name, AGE(CURRENT_DATE, birthday) as age, room FROM students) q 
        ON r.id = q.room
        GROUP BY r.id, r.name
        )
    ORDER BY age_diff DESC
    LIMIT 5;
"""

sql_select_rooms_diff_sex = """
    SELECT r.name
    FROM rooms r INNER JOIN students s ON r.id = s.room
    GROUP BY r.id, r.name
    HAVING COUNT(DISTINCT s.sex) = 2
    ORDER BY r.id;
"""

QUERIES = [sql_select_rooms, sql_select_5_rooms_min_age, sql_select_5_rooms_max_diff, sql_select_rooms_diff_sex]