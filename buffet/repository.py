from decimal import Decimal
from typing import List
from dataclasses import dataclass
from datetime import date
from typing import Any, Optional, Sequence
import logging
from django.db import connection


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


# Darvas box representation
@dataclass
class DarvasBox:
    box_id: int
    ticker: str
    start_date: date
    end_date: Optional[date]
    min_price: float
    max_price: float
    base_close: float
    is_active: bool


@dataclass
class TradingPlan:
    ticker: str
    order_type: str
    qty: Optional[int] = None


class DataRepository:
    def __init__(self) -> None:
        pass

    def _get_cursor(self):
        return connection.cursor()

    def _prep_sql(self, sql: str) -> str:
        return sql.replace("?", "%s")

    def _fetchone(self, sql: str, params: Sequence[Any]):
        cur = self._get_cursor()
        sql = self._prep_sql(sql)
        cur.execute(sql, params)
        try:
            return cur.fetchone()
        finally:
            try:
                cur.close()
            except Exception:
                pass

    def _fetchall(self, sql: str, params: Sequence[Any]):
        cur = self._get_cursor()
        sql = self._prep_sql(sql)
        cur.execute(sql, params)
        try:
            return cur.fetchall()
        finally:
            try:
                cur.close()
            except Exception:
                pass

    def _execute(self, sql: str, params: Sequence[Any]):
        cur = self._get_cursor()
        sql = self._prep_sql(sql)
        cur.execute(sql, params)
        return cur

    def get_day_price(self, ticker: str, on_date: str) -> Optional[PriceData]:
        row = self._fetchone(
            """
            SELECT trade_date, open, high, low, close
            FROM historicals
            WHERE ticker = ? AND trade_date = ?
            """,
            [ticker, on_date],
        )
        if not row:
            return None
        return PriceData(
            trade_date=row[0],
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
        )

    def get_prev_trading_day(self, ticker: str, before_date: str) -> Optional[date]:
        # We need to fetch this from historicals to account for market holidays
        row = self._fetchone(
            """
            SELECT MAX(trade_date)
            FROM historicals
            WHERE ticker = ? AND trade_date < ?
            """,
            [ticker, before_date],
        )
        if not row or row[0] is None:
            return None
        return row[0]

    def get_prev_close(self, ticker: str, before_date: str) -> Optional[float]:
        # Fetch the closing price on the previous trading day
        row = self._fetchone(
            """
            SELECT close
            FROM historicals
            WHERE ticker = ? AND trade_date = (
                SELECT MAX(trade_date)
                FROM historicals
                WHERE ticker = ? AND trade_date < ?
            )
            """,
            [ticker, ticker, before_date],
        )
        if not row or row[0] is None:
            return None
        return float(row[0])

    def get_recent_closes(
        self, ticker: str, before_date: str, lookback: int
    ) -> list[float]:
        rows = self._fetchall(
            """
            SELECT close
            FROM historicals
            WHERE ticker = ? AND trade_date < ?
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            [ticker, before_date, lookback],
        )
        return [float(r[0]) for r in rows]

    def get_recent_volumes(
        self, ticker: str, before_date: str, lookback: int
    ) -> list[float]:
        rows = self._fetchall(
            """
            SELECT volume
            FROM historicals
            WHERE ticker = ? AND trade_date < ? AND volume IS NOT NULL
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            [ticker, before_date, lookback],
        )
        return [float(r[0]) for r in rows]

    def get_max_high_lookback(
        self, ticker: str, before_date: str, lookback: int
    ) -> Optional[float]:
        row = self._fetchone(
            """
            SELECT MAX(high)
            FROM (
                SELECT high
                FROM historicals
                WHERE ticker = ? AND trade_date < ?
                ORDER BY trade_date DESC
                LIMIT ?
            )
            """,
            [ticker, before_date, lookback],
        )
        if not row or row[0] is None:
            return None
        return float(row[0])

    def get_active_trade(self, ticker: str) -> Optional[ActiveTrade]:
        row = self._fetchone(
            """
            SELECT qty_owned, buy_price, stop_loss_amt
            FROM active_trades
            WHERE ticker = ?
            """,
            [ticker],
        )
        if not row:
            return None
        qty = int(row[0]) if row[0] is not None else 0
        buy = float(row[1]) if row[1] is not None else None
        stop = float(row[2]) if row[2] is not None else None
        return ActiveTrade(qty_owned=qty, buy_price=buy, stop_loss_amt=stop)

    def get_last_buy_date(self, ticker: str) -> Optional[date]:
        row = self._fetchone(
            """
            SELECT MAX(txn_date)
            FROM transactions
            WHERE ticker = ? AND txn_type = 'BUY'
            """,
            [ticker],
        )
        if not row or row[0] is None:
            return None
        return row[0]

    def get_high_since(
        self, ticker: str, start_date: str, end_date: str
    ) -> Optional[float]:
        row = self._fetchone(
            """
            SELECT MAX(high)
            FROM historicals
            WHERE ticker = ? AND trade_date >= ? AND trade_date <= ?
            """,
            [ticker, start_date, end_date],
        )
        if not row or row[0] is None:
            return None
        return float(row[0])

    # --- Strategy state (breakout streak) ---
    def get_breakout_streak(self, ticker: str) -> int:
        row = self._fetchone(
            """
            SELECT breakout_streak
            FROM strategy_state
            WHERE ticker = ?
            """,
            [ticker],
        )
        if not row or row[0] is None:
            return 0
        return int(row[0])

    def set_breakout_streak(self, ticker: str, streak: int) -> None:
        cur = self._execute(
            """
            INSERT OR REPLACE INTO strategy_state (ticker, breakout_streak)
            VALUES (?, ?)
            """,
            [ticker, streak],
        )
        try:
            cur.close()
        except Exception:
            pass

    # --- Darvas boxes helpers ---
    def get_earliest_day_close(self, ticker: str) -> Optional[tuple[date, float]]:
        # TODO: This gives the earliest price of all time! Should respect the lookback days in config
        row = self._fetchone(
            """
            SELECT trade_date, close
            FROM historicals
            WHERE ticker = ?
            AND close is not NULL
            ORDER BY trade_date ASC
            LIMIT 1
            """,
            [ticker],
        )
        if not row:
            return None
        return row[0], float(row[1])

    def get_current_darvas_box(self, ticker: str) -> Optional[DarvasBox]:
        row = self._fetchone(
            """
            SELECT box_id, ticker, start_date, end_date, min_price, max_price, base_close, is_active
            FROM darvas_boxes
            WHERE ticker = ? AND is_active = 1
            ORDER BY box_id DESC
            LIMIT 1
            """,
            [ticker],
        )
        if not row:
            return None
        return DarvasBox(
            box_id=int(row[0]),
            ticker=row[1],
            start_date=row[2],
            end_date=row[3],
            min_price=float(row[4]),
            max_price=float(row[5]),
            base_close=float(row[6]),
            is_active=bool(row[7]),
        )

    def deactivate_active_darvas_box(self, ticker: str, end_date: date) -> None:
        cur = self._execute(
            """
            UPDATE darvas_boxes
            SET is_active = 0, end_date = ?
            WHERE ticker = ? AND is_active = 1
            """,
            [end_date, ticker],
        )
        try:
            cur.close()
        except Exception:
            pass

    def create_darvas_box(
        self, ticker: str, start_date: date, base_close: float, height_pct: float
    ) -> DarvasBox:
        logger = logging.getLogger(__name__)
        logger.debug(
            "Creating Darvas box for ticker=%s with height_pct=%.2f", ticker, height_pct
        )
        min_price = base_close * (1 - height_pct)
        max_price = base_close * (1 + height_pct)
        cur = self._execute(
            """
            INSERT INTO darvas_boxes (ticker, start_date, end_date, min_price, max_price, base_close, is_active)
            VALUES (?, ?, NULL, ?, ?, ?, 1)
            """,
            [ticker, start_date, float(min_price), float(max_price), float(base_close)],
        )
        try:
            box_id = (
                int(cur.lastrowid) if getattr(cur, "lastrowid", None) is not None else 0
            )
        finally:
            try:
                cur.close()
            except Exception:
                pass
        return DarvasBox(
            box_id=box_id,
            ticker=ticker,
            start_date=start_date,
            end_date=None,
            min_price=float(min_price),
            max_price=float(max_price),
            base_close=float(base_close),
            is_active=True,
        )

    def update_active_box_end_date(self, ticker: str, end_date: date) -> None:
        cur = self._execute(
            """
            UPDATE darvas_boxes
            SET end_date = ?
            WHERE ticker = ? AND is_active = 1
            """,
            [end_date, ticker],
        )
        try:
            cur.close()
        except Exception:
            pass

    def fetch_all_tickers(self) -> List[str]:
        rows = self._fetchall(
            """
            SELECT DISTINCT ticker FROM historicals
            """,
            [],
        )
        return [row[0] for row in rows]

    def get_wallet_amount(self) -> Decimal:
        row = self._fetchone(
            """
            SELECT available_cash FROM wallet
            """,
            [],
        )
        if not row or row[0] is None:
            raise Exception("Wallet amount not found")
        return Decimal(row[0])

    def create_trading_plan(self, plan: TradingPlan) -> None:
        self._execute(
            """
            INSERT INTO trading_plan (ticker, order_type, qty)
            VALUES (?, ?, ?)
            """,
            [plan.ticker, plan.order_type, plan.qty],
        )
