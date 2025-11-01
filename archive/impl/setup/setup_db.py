from pathlib import Path
import sqlite3



def apply_schema(con: sqlite3.Connection, sql_path: str | Path) -> None:
    path = Path(sql_path)
    if not path.exists():
        raise FileNotFoundError(f"Schema SQL file not found: {path}")

    sql = path.read_text(encoding="utf-8")
    # executescript allows multiple statements separated by ';' in SQLite
    con.executescript(sql)


