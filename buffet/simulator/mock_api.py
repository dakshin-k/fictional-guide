from datetime import datetime, date as date_type
from decimal import Decimal
from functools import lru_cache
from typing import Optional

from api import FinanceApi, StopLossStatus
from repository import ActiveTrade
from utils import calculate_transaction_charges
from .db import open_historical_db


class MockFinanceApi(FinanceApi):
    def __init__(self, today: date_type):
        self.conn = open_historical_db()
        self.today = today

    @lru_cache(maxsize=10)
    def get_all_open_prices(self, date: date_type) -> dict[str, Optional[Decimal]]:
        sql = """
        select ticker, open from historicals
        where trade_date = ?
        """
        rows = self.conn.execute(sql, (date.strftime("%Y-%m-%d"),)).fetchall()
        return {row[0]: Decimal(row[1]) if row[1] else None for row in rows}

    def get_trading_price(self, date: date_type, ticker: str) -> Optional[Decimal]:
        return self.get_all_open_prices(date)[ticker]

    def is_trading_day(self, date: date_type) -> bool:
        sql = """
        select count(*) from historicals
        where trade_date = ?
        """
        row = self.conn.execute(sql, (date.strftime("%Y-%m-%d"),)).fetchone()
        return row[0] > 0

    def get_buy_cost(self, ticker: str, qty: int, today: date_type) -> Decimal:
        price = self.get_trading_price(today, ticker)
        if not price:
            raise ValueError(f"Price not available for {ticker} on {today}")

        cost = price * Decimal(qty)
        return cost + calculate_transaction_charges(cost)

    def buy(self, ticker: str, qty: int, today: date_type) -> None:
        pass

    def update_stop_loss(self, ticker: str, stop_loss: Decimal) -> None:
        pass

    def get_stop_loss_status(self, trade: ActiveTrade) -> StopLossStatus:
        sql = """
        select min(low) from historicals
        where ticker = ? and trade_date >= ? and trade_date <= ?
        """

        row = self.conn.execute(
            sql,
            (
                trade.ticker,
                trade.buy_date.strftime("%Y-%m-%d"),
                self.today.strftime("%Y-%m-%d"),
            ),
        ).fetchone()

        lowest_price = Decimal(row[0]) if row[0] else None
        stop_loss_decimal = Decimal(str(trade.stop_loss))

        if lowest_price and lowest_price <= stop_loss_decimal:
            trade_value = stop_loss_decimal * Decimal(trade.qty)
            charges = calculate_transaction_charges(trade_value, is_buy=False)
            return StopLossStatus(triggered=True, amount=trade_value - charges)
        else:
            return StopLossStatus(triggered=False, amount=None)
