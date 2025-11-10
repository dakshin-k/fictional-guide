import sqlite3
from datetime import date, timedelta
from typing import List
from typing import Optional

from tqdm import tqdm

import config
import decision
from api import FinanceApi
from api import GrowwApi
from repository import DataRepository
from repository import TradingPlan
from utils import max_affordable_buy_qty


def run(
    connection: sqlite3.Connection,
    tickers: List[str],
    today: Optional[date],
    api: Optional[FinanceApi] = None,
):
    """
    1. Get today's date
    2. If today was a trading day, update historicals and portfolio
    3. If tomorrow is a trading, generate decision plan and send email
    """

    if api is None:
        api = GrowwApi()
    if today is None:
        today = date.today()

    repo = DataRepository(connection)
    tomorrow = today + timedelta(days=1)
    todays_losses = dict()

    print(f'>>> Planning for date {today}')

    if api.is_trading_day(today):
        """
        1. For each ticker, fetch and update historicals
        2. For each pending sell order, check status and update order and portfolio. Make note of losses for the decision engine
        """
        pass

    if api.is_trading_day(tomorrow):
        trading_prices = {
            ticker: api.get_trading_price(tomorrow, ticker) for ticker in tqdm(tickers)
        }

        decisions = []
        for ticker in tickers:
            open_price = trading_prices[ticker]
            if open_price is None:
                continue
            order_decision = decision.get_decision(
                connection,
                ticker,
                str(tomorrow),
                open_price,
                config.leader_lookback_days,
                config.breakout_streak,
                config.default_height_pct,
                config.height_increment_pct,
                todays_losses.get(ticker, False),
            )
            decisions.append(order_decision)
            # Insert TradingPlan rows for BUY or UPDATE_STOP_LOSS decisions
            if order_decision.decision == "BUY":
                wallet_cash = repo.get_wallet_amount()
                invest_cap = config.max_invest_per_stock
                qty = max_affordable_buy_qty(wallet_cash, open_price, invest_cap)
                repo.create_trading_plan(
                    TradingPlan(
                        date=tomorrow,
                        ticker=ticker,
                        order_type="BUY",
                        qty=qty,
                    )
                )
            elif order_decision.decision == "UPDATE_STOP_LOSS":
                repo.create_trading_plan(
                    TradingPlan(
                        ticker=ticker,
                        order_type="UPDATE_STOP_LOSS",
                        qty=None,
                    )
                )
            if order_decision.decision != "NO_OP":
                print(
                    f"Ticker: {ticker}, Decision: {order_decision.decision}, Stop Loss: {order_decision.stop_loss}"
                )
