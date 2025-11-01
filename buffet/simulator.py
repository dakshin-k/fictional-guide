
import sqlite3

schema_path = '/Users/dakshin/projects/darva/buffet/config/schema.sql'
simulation_db_path = '/Users/dakshin/projects/darva/buffet/simulation_data.sqlite3'

db = sqlite3.connect(':memory:')
cursor = db.cursor()

start_date = ''

def init_db():
    with open(schema_path, 'r') as f:
        schema_script = f.read()
        cursor.executescript(schema_script)

    disk_db = sqlite3.connect(simulation_db_path)
    disk_cursor = disk_db.cursor()

    disk_cursor.execute('SELECT * FROM historicals')
    rows = disk_cursor.fetchall()
    cursor.executemany('INSERT INTO historicals VALUES (?, ?, ?, ?, ?, ?, ?, ?)', rows)
    db.commit()
    disk_db.close()

