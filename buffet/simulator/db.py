import sqlite3


def init_simulation_db(start_date: str, cash: int) -> sqlite3.Connection:
    schema_path = '/Users/dakshin/projects/darva/buffet/config/schema.sql'
    simulation_db_path = '/Users/dakshin/projects/darva/buffet/simulation_data.sqlite3'

    db = sqlite3.connect(':memory:')
    cursor = db.cursor()
    with open(schema_path, 'r') as f:
        schema_script = f.read()
        cursor.executescript(schema_script)

    disk_db = sqlite3.connect(simulation_db_path)
    historicals = disk_db.cursor().execute('SELECT * FROM historicals where trade_date < ?', (start_date,)).fetchall()
    cursor.executemany('INSERT INTO historicals VALUES (?, ?, ?, ?, ?, ?, ?)', historicals)
    cursor.execute('INSERT INTO wallet VALUES (?)', (cash,))
    db.commit()
    disk_db.close()
    return db


def open_historical_db() -> sqlite3.Connection:
    simulation_db_path = '/Users/dakshin/projects/darva/buffet/simulation_data.sqlite3'
    return sqlite3.connect(simulation_db_path)
