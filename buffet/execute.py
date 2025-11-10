import sqlite3
from datetime import date
from decimal import Decimal
from typing import Optional

from api import FinanceApi, GrowwApi
from repository import TradingPlan, DataRepository, ActiveTrade


def update_wallet(db: sqlite3.Connection, amount: Decimal) -> Decimal:
    cursor = db.cursor()
    cursor.execute("UPDATE wallet SET available_cash = available_cash + ?", (float(amount),))
    db.commit()
    cursor.execute("SELECT available_cash FROM wallet")
    return Decimal(cursor.fetchone()[0])


def execute_plan(
        db: sqlite3.Connection, today: date, api: Optional[FinanceApi] = None
):
    if api is None:
        api = GrowwApi()
    repo = DataRepository(db)

    cursor = db.cursor()
    cursor.execute("SELECT * FROM trading_plan WHERE order_type = 'BUY' and date = ?", (today,))
    plans = map(TradingPlan.from_row, cursor.fetchall())
    for plan in plans:
        if plan.qty is None:
            print(f"ERROR: No quantity specified for {plan.ticker} in trading plan")
            continue
        cost = api.buy(plan.ticker, int(plan.qty), today)
        wallet = update_wallet(db, -cost)
        repo.add_active_trade(ActiveTrade(
            ticker=plan.ticker,
            qty_owned=plan.qty,
            cost=cost,
            stop_loss_amt=plan.,
        ))
        print(f"Bought shares of {plan.ticker}. Wallet: {wallet}")
