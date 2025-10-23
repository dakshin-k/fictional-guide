from pathlib import Path
from typing import Optional
import duckdb





def apply_schema(con: duckdb.DuckDBPyConnection, sql_path: str | Path) -> None:
    path = Path(sql_path)
    if not path.exists():
        raise FileNotFoundError(f"Schema SQL file not found: {path}")

    sql = path.read_text(encoding="utf-8")
    con.execute(sql)


