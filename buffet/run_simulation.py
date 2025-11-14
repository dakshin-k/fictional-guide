from datetime import datetime, timedelta
from decimal import Decimal
import sqlite3

import plan
from execute import execute_plan
from repository import DataRepository
from simulator import db
from simulator.mock_api import MockFinanceApi

start_date = "2023-10-26"
end_date = "2023-11-10"
starting_cash = 1_00_000


def begin():
    conn = db.init_simulation_db(start_date, starting_cash)
    repo = DataRepository(conn)
    tickers = repo.fetch_all_tickers()
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    date = start
    while date <= end:
        mock_api = MockFinanceApi(date)
        plan.run(conn, tickers, date, mock_api)

        tomorrow = date + timedelta(days=1)
        execute_plan(conn, date=tomorrow, api=mock_api)
        date += timedelta(days=1)

    wallet = repo.get_wallet_amount()
    for trade in repo.get_active_trades():
        closing_price = mock_api.get_trading_price(end, trade.ticker)
        if closing_price is None:
            print(f"Warning: No closing price for {trade.ticker} on {end}")
            continue
        value = Decimal(trade.qty) * closing_price
        print(f"CLosing value of {trade.ticker}: {round(value)}")
        wallet += value
    print(f"Closing Portfolio value: {round(wallet)}")
    print(f"Total Profit: {round(wallet - Decimal(starting_cash))}")
    print(
        f"Total Return: {round((wallet - Decimal(starting_cash)) / Decimal(starting_cash), 2) * 100}%"
    )


if __name__ == "__main__":
    begin()
