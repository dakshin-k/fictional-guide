from datetime import datetime, timedelta
import sqlite3

import plan
from execute import execute_plan
from repository import DataRepository
from simulator import db
from simulator.mock_api import MockFinanceApi

start_date = '2023-10-26'
end_date = '2023-10-27'

def begin():
    conn = db.init_simulation_db(start_date, 1_00_000)
    repo = DataRepository(conn)
    mock_api = MockFinanceApi()
    tickers = repo.fetch_all_tickers()
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    date = start
    while date <= end:
        plan.run(conn, tickers, date, mock_api)


        execute_plan(conn, today=date.date(), api=mock_api)
        date += timedelta(days=1)


if __name__ == '__main__':
    begin()
