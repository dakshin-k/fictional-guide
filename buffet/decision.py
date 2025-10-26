from decimal import Decimal
import sqlite3
import logging
from typing import Optional, Dict

from repository import DataRepository


logger = logging.getLogger(__name__)
_height_pct_by_ticker: Dict[str, float] = {}


class Decision:
    decision: str
    stop_loss: Optional[float]

    def __init__(self, decision: str, stop_loss: Optional[float] = None) -> None:
        if decision == "BUY" and stop_loss is None:
            raise ValueError("Stop loss must be provided for BUY decision")
        self.decision = decision
        self.stop_loss = stop_loss


def get_decision(
    con: sqlite3.Connection,
    ticker: str,
    trade_date: str,
    open_price: Decimal,
    leader_lookback_days: int,
    breakout_streak: int = 1,
    default_height_pct: float = 0.01,
    height_increment_pct: float = 0.0,
    loss_occurred: bool = False,
) -> Decision:
    
    """
    Determine trading decision at the start of the trading day using Darvas boxes.

    Rules:
    - Maintain an active Darvas box per ticker defined by:
      min_price = base_close * (1 - height_pcct),
      max_price = base_close * (1 + height_pcct),
      where base_close is the closing price of the box's first day.
    - For each day:
      * If open is within [min_price, max_price], extend box end_date.
      * If open > max_price, declare breakout and increment streak (if not in position).
        If in position, advance to a new box anchored at today's close and raise
        stop-loss to the new box's lower limit.
      * If open < min_price, reset the breakout streak to 0, close current box,
        and start a new box anchored to today's close.
    - BUY stop-loss equals the lower limit (min_price) of the current box at breakout.
    - When holding a position: also update trailing stop when highest high since entry rises.
    """
    repo = DataRepository()

    # Maintain per-ticker Darvas height in-memory
    current_height = _height_pct_by_ticker.get(ticker, default_height_pct)
    if loss_occurred and height_increment_pct > 0:
        current_height = current_height + height_increment_pct
    _height_pct_by_ticker[ticker] = current_height

    # Ensure we have a current Darvas box
    box = repo.get_current_darvas_box(ticker)
    prev_day = repo.get_prev_trading_day(ticker, trade_date)
    prev_closing = repo.get_prev_close(ticker, trade_date)

    if box is None:
        earliest = repo.get_earliest_day_close(ticker)
        if earliest is None:
            repo.set_breakout_streak(ticker, 0)
            logger.debug(f"{trade_date}: [NO_OP] No earliest close to start box for {ticker}")
            return Decision("NO_OP")
        earliest_date, earliest_close = earliest
        box = repo.create_darvas_box(ticker, earliest_date, earliest_close, current_height)

    active = repo.get_active_trade(ticker)
    if active and active.qty_owned > 0:
        if open_price > box.max_price:
            repo.deactivate_active_darvas_box(ticker, prev_day if prev_day else box.start_date)
            new_box = repo.create_darvas_box(ticker, trade_date, float(prev_closing), current_height)
            new_stop = new_box.min_price
            current_stop = active.stop_loss_amt if active.stop_loss_amt is not None else 0.0
            if new_stop > current_stop:
                logger.debug(f"{trade_date}: [UPDATE_STOP_LOSS] Price above box; new stop {new_stop:.4f} > current {current_stop:.4f} for {ticker}")
                return Decision("UPDATE_STOP_LOSS", stop_loss=round(new_stop, 4))
        return Decision("NO_OP")

    # Not in a position: evaluate Darvas box behavior
    # Below box -> reset streak and start a new box
    if open_price < box.min_price:
        # Close current box on the previous trading day if possible
        repo.deactivate_active_darvas_box(ticker, prev_day if prev_day else box.start_date)
        # Start new box anchored to today's close
        repo.create_darvas_box(ticker, trade_date, float(prev_closing), current_height)
        repo.set_breakout_streak(ticker, 0)
        logger.debug(f"{trade_date}: [NO_OP] Open {open_price:.4f} below box min {box.min_price:.4f}; reset streak and start new box for {ticker}")
        return Decision("NO_OP")

    # Within box -> extend end date and NO_OP
    if box.min_price <= open_price <= box.max_price:
        repo.update_active_box_end_date(ticker, trade_date)
        # logger.debug(f"{date}: [NO_OP] Open {open_price:.4f} within box [{box.min_price:.4f}, {box.max_price:.4f}]; extend end_date for {ticker}")
        return Decision("NO_OP")

    # Above box -> breakout handling
    if open_price > box.max_price:
        streak = repo.get_breakout_streak(ticker)
        new_streak = streak + 1

        # Leader lookback gating conditions for BUY
        allow_buy = True
        if leader_lookback_days and leader_lookback_days > 0:
            # Price condition: open within 5% of max high in lookback window (prior days)
            max_high = repo.get_max_high_lookback(ticker, trade_date, leader_lookback_days)
            price_ok = (max_high is not None) and (open_price >= 0.95 * float(max_high))
            if price_ok:
                logger.debug(f"{trade_date}: [GATING] Open {open_price:.4f} within 5% of max high {float(max_high):.4f} for {ticker}")

            # Volume condition: previous day's volume >= 30% above average of lookback (excluding previous day)
            vols = repo.get_recent_volumes(ticker, trade_date, leader_lookback_days)
            volume_ok = False
            if len(vols) > 1:
                prev_vol = float(vols[0])
                avg_vol = sum(float(v) for v in vols[1:]) / len(vols[1:])
                volume_ok = prev_vol >= 1.3 * avg_vol
                if volume_ok:
                    logger.debug(f"{trade_date}: [GATING] Volume {prev_vol:.4f} above 30% avg {avg_vol:.4f} for {ticker}")

            allow_buy = price_ok and volume_ok

        if new_streak >= breakout_streak and allow_buy:
            # BUY stop-loss: lower limit of the current (broken) box
            initial_stop = box.min_price
            repo.set_breakout_streak(ticker, 0)
            # Close the broken box and start a new one anchored to today's close
            repo.deactivate_active_darvas_box(ticker, prev_day if prev_day else box.start_date)
            repo.create_darvas_box(ticker, trade_date, float(prev_closing), current_height)
            logger.debug(f"{trade_date}: [BUY] Breakout; streak {new_streak}/{breakout_streak} met and leader gating passed; stop {initial_stop:.4f} for {ticker}. New box started at close {prev_closing:.4f}")
            return Decision("BUY", stop_loss=round(initial_stop, 4))
        else:
            # Either streak not met or leader gating failed -> treat as NO_OP, start new box, keep updated streak
            repo.set_breakout_streak(ticker, new_streak)
            repo.deactivate_active_darvas_box(ticker, prev_day if prev_day else box.start_date)
            repo.create_darvas_box(ticker, trade_date, float(prev_closing), current_height)
            logger.debug(f"{trade_date}: [NO_OP] Breakout but {'streak not met' if new_streak < breakout_streak else 'leader gating failed'} for {ticker}. New box started at close {prev_closing:.4f}")
            return Decision("NO_OP")

    logger.debug(f"{trade_date}: [NO_OP] Fallback path for {ticker}")
    return Decision("NO_OP")
