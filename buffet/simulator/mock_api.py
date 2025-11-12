from datetime import datetime, date as date_type
from decimal import Decimal
from typing import Optional

from api import FinanceApi
from utils import calculate_transaction_charges
from .db import open_historical_db


class MockFinanceApi(FinanceApi):
    def __init__(self):
        self.conn = open_historical_db()

    def get_trading_price(self, date: date_type, ticker: str) -> Optional[Decimal]:
        sql = '''
        select open from historicals
        where ticker = ? and trade_date = ?
        '''
        row = self.conn.execute(sql, (ticker, date.strftime('%Y-%m-%d'))).fetchone()
        if not row or not row[0]:
            return None
        return Decimal(row[0])

    def is_trading_day(self, date: datetime) -> bool:
        sql = '''
        select count(*) from historicals
        where trade_date = ?
        '''
        row = self.conn.execute(sql, (date.strftime('%Y-%m-%d'),)).fetchone()
        return row[0] > 0

    def buy(self, ticker: str, qty: int, today: datetime) -> Decimal:
        price = self.get_trading_price(today, ticker)
        if not price:
            raise ValueError(f"Price not available for {ticker} on {today}")

        cost = price * Decimal(qty)
        return cost + calculate_transaction_charges(cost)
    
    def update_stop_loss(self, ticker: str, stop_loss: Decimal) -> None:
        pass
