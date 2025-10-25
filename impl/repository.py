from dataclasses import dataclass
from datetime import date
from typing import Optional

import duckdb


@dataclass
class PriceData:
    trade_date: date
    open: float
    high: float
    low: float
    close: float


@dataclass
class ActiveTrade:
    qty_owned: int
    buy_price: Optional[float]
    stop_loss_amt: Optional[float]


@dataclass
class StrategyState:
    breakout_streak: int


class DataRepository:
    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con

    def get_day_price(self, ticker: str, on_date: str) -> Optional[PriceData]:
        row = self.con.execute(
            """
            SELECT trade_date, open, high, low, close
            FROM historicals
            WHERE ticker = ? AND trade_date = ?
            """,
            [ticker, on_date],
        ).fetchone()
        if not row:
            return None
        return PriceData(
            trade_date=row[0], open=float(row[1]), high=float(row[2]), low=float(row[3]), close=float(row[4])
        )

    def get_prev_trading_day(self, ticker: str, before_date: str) -> Optional[date]:
        # We need to fetch this from historicals to account for market holidays
        row = self.con.execute(
            """
            SELECT MAX(trade_date)
            FROM historicals
            WHERE ticker = ? AND trade_date < ?
            """,
            [ticker, before_date],
        ).fetchone()
        if not row or row[0] is None:
            return None
        return row[0]

    def get_recent_closes(self, ticker: str, before_date: str, lookback: int) -> list[float]:
        rows = self.con.execute(
            """
            SELECT close
            FROM historicals
            WHERE ticker = ? AND trade_date < ?
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            [ticker, before_date, lookback],
        ).fetchall()
        return [float(r[0]) for r in rows]

    def get_active_trade(self, ticker: str) -> Optional[ActiveTrade]:
        row = self.con.execute(
            """
            SELECT qty_owned, buy_price, stop_loss_amt
            FROM active_trades
            WHERE ticker = ?
            """,
            [ticker],
        ).fetchone()
        if not row:
            return None
        qty = int(row[0]) if row[0] is not None else 0
        buy = float(row[1]) if row[1] is not None else None
        stop = float(row[2]) if row[2] is not None else None
        return ActiveTrade(qty_owned=qty, buy_price=buy, stop_loss_amt=stop)

    def get_last_buy_date(self, ticker: str) -> Optional[date]:
        row = self.con.execute(
            """
            SELECT MAX(txn_date)
            FROM transactions
            WHERE ticker = ? AND txn_type = 'BUY'
            """,
            [ticker],
        ).fetchone()
        if not row or row[0] is None:
            return None
        return row[0]

    def get_high_since(self, ticker: str, start_date: str, end_date: str) -> Optional[float]:
        row = self.con.execute(
            """
            SELECT MAX(high)
            FROM historicals
            WHERE ticker = ? AND trade_date >= ? AND trade_date <= ?
            """,
            [ticker, start_date, end_date],
        ).fetchone()
        if not row or row[0] is None:
            return None
        return float(row[0])

    # --- Strategy state (breakout streak) ---
    def get_breakout_streak(self, ticker: str) -> int:
        row = self.con.execute(
            """
            SELECT breakout_streak
            FROM strategy_state
            WHERE ticker = ?
            """,
            [ticker],
        ).fetchone()
        if not row or row[0] is None:
            return 0
        return int(row[0])

    def set_breakout_streak(self, ticker: str, streak: int) -> None:
        self.con.execute(
            """
            INSERT OR REPLACE INTO strategy_state (ticker, breakout_streak)
            VALUES (?, ?)
            """,
            [ticker, streak],
        )