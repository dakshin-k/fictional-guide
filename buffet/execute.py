import sqlite3
from datetime import date as Date
from decimal import Decimal
from typing import Optional

from api import FinanceApi, GrowwApi
from repository import TradingPlan, DataRepository, ActiveTrade




def execute_plan(
        db: sqlite3.Connection, date: Date, api: Optional[FinanceApi] = None
):
    if api is None:
        api = GrowwApi(date)
    repo = DataRepository(db)

    cursor = db.cursor()
    cursor.execute("SELECT * FROM trading_plan WHERE date = ?", (date,))
    plans = map(TradingPlan.from_row, cursor.fetchall())
    for plan in plans:
        if plan.order_type == 'BUY':
            if plan.qty is None:
                print(f"ERROR: No quantity specified for {plan.ticker} in trading plan")
                continue
            cost = api.get_buy_cost(plan.ticker, int(plan.qty), date)
            if repo.get_wallet_amount() < cost:
                print(f"ERROR: Not enough money to buy {plan.ticker} {int(plan.qty)} shares. Cost: {round(cost)}. Wallet: {round(repo.get_wallet_amount())}")
                continue
            api.buy(plan.ticker, int(plan.qty), date)
            wallet = repo.update_wallet(-cost)
            repo.add_active_trade(ActiveTrade(
                qty=int(plan.qty),
                ticker=plan.ticker,
                buy_cost=float(cost),
                buy_date=date,
                stop_loss=plan.stop_loss,
            ))
            print(f"Bought {int(plan.qty)} shares of {plan.ticker} at SL {plan.stop_loss}. Cost: {round(cost)}. Wallet: {round(wallet)}")
        elif plan.order_type == 'UPDATE_STOP_LOSS':
            api.update_stop_loss(plan.ticker, Decimal(plan.stop_loss))
            repo.update_trade_stop_loss(plan.ticker, plan.stop_loss)
            print(f"Updated stop loss for {plan.ticker} to {plan.stop_loss}")
