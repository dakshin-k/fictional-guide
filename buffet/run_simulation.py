from datetime import datetime, timedelta
from decimal import Decimal
import config
import plan
from execute import execute_plan
from repository import DataRepository
from simulator import db
from simulator.mock_api import MockFinanceApi

start_date = "2023-10-26"
end_date = "2024-02-28"


def begin():
    conn = db.init_simulation_db(start_date, config.starting_cash)
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
    profit_loss = wallet - config.starting_cash
    print(f"Profit/Loss: {round(profit_loss)}")
    stcg = Decimal(0.2) * profit_loss if profit_loss > 0 else Decimal(0)
    print(f"STCG: {round(stcg)}")
    gross = wallet - stcg
    print(f"Gross After Taxes: {round(gross)}")
    print(
        f"Total Return: {round((gross - Decimal(config.starting_cash)) / Decimal(config.starting_cash), 2) * 100}%"
    )


if __name__ == "__main__":
    begin()
