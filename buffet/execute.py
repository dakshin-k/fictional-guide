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
    cursor.execute("SELECT * FROM trading_plan WHERE date = ?", (today,))
    plans = map(TradingPlan.from_row, cursor.fetchall())
    for plan in plans:
        if plan.order_type == 'BUY':
            if plan.qty is None:
                print(f"ERROR: No quantity specified for {plan.ticker} in trading plan")
                continue
            cost = api.buy(plan.ticker, int(plan.qty), today)
            wallet = update_wallet(db, -cost)
            repo.add_active_trade(ActiveTrade(
                ticker=plan.ticker,
                buy_cost=float(cost),
                buy_date=today,
                stop_loss=float(plan.stop_loss) if plan.stop_loss is not None else None,
            ))
        elif plan.order_type == 'UPDATE_STOP_LOSS':
            api.update_stop_loss(plan.ticker, Decimal(plan.stop_loss))
            repo.update_trade_stop_loss(plan.ticker, float(plan.stop_loss))
        print(f"Bought shares of {plan.ticker}. Wallet: {wallet}")
