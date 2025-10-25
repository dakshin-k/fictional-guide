import duckdb
from typing import Optional

from repository import DataRepository


class Decision:
    decision: str
    stop_loss: Optional[float]

    def __init__(self, decision: str, stop_loss: Optional[float] = None) -> None:
        if decision == "BUY" and stop_loss is None:
            raise ValueError("Stop loss must be provided for BUY decision")
        self.decision = decision
        self.stop_loss = stop_loss


def get_decision(
    con: duckdb.DuckDBPyConnection,
    ticker: str,
    date: str,
    breakout_streak: int = 1,
    darvas_height_pct: float = 0.01,
) -> Decision:
    
    """
    Determine trading decision at the start of the trading day using Darvas boxes.

    Rules:
    - Maintain an active Darvas box per ticker defined by:
      min_price = base_close * (1 - darvas_height_pct),
      max_price = base_close * (1 + darvas_height_pct),
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
    repo = DataRepository(con)

    # Configurable parameter
    stop_pct = 0.1

    # Fetch current day (t) price for open
    day_price = repo.get_day_price(ticker, date)
    if not day_price:
        return Decision("NO_OP")

    # Ensure we have a current Darvas box
    box = repo.get_current_darvas_box(ticker)
    if box is None:
        earliest = repo.get_earliest_day_close(ticker)
        if earliest is None:
            repo.set_breakout_streak(ticker, 0)
            return Decision("NO_OP")
        earliest_date, earliest_close = earliest
        box = repo.create_darvas_box(ticker, earliest_date, earliest_close, darvas_height_pct)

    open_price = float(day_price.open)

    # Check if we already hold a position
    active = repo.get_active_trade(ticker)
    if active and active.qty_owned > 0:
        # If price rises above current box, move to a new box and lift stop to its lower bound
        if open_price > box.max_price:
            prev_day = repo.get_prev_trading_day(ticker, date)
            repo.deactivate_active_darvas_box(ticker, prev_day if prev_day else box.start_date)
            new_box = repo.create_darvas_box(ticker, day_price.trade_date, float(day_price.close), darvas_height_pct)
            new_stop = new_box.min_price
            current_stop = active.stop_loss_amt if active.stop_loss_amt is not None else 0.0
            if new_stop > current_stop:
                return Decision("UPDATE_STOP_LOSS", stop_loss=round(new_stop, 4))
            return Decision("NO_OP")

        # Otherwise, apply trailing stop based on highest high since entry
        buy_date = repo.get_last_buy_date(ticker)
        if not buy_date:
            return Decision("NO_OP")

        highest_high = repo.get_high_since(ticker, str(buy_date), date)
        if highest_high is None:
            return Decision("NO_OP")

        new_stop = highest_high * (1 - stop_pct)
        current_stop = active.stop_loss_amt if active.stop_loss_amt is not None else 0.0
        if new_stop > current_stop:
            return Decision("UPDATE_STOP_LOSS", stop_loss=round(new_stop, 4))
        return Decision("NO_OP")

    # Not in a position: evaluate Darvas box behavior
    # Below box -> reset streak and start a new box
    if open_price < box.min_price:
        prev_day = repo.get_prev_trading_day(ticker, date)
        # Close current box on the previous trading day if possible
        repo.deactivate_active_darvas_box(ticker, prev_day if prev_day else box.start_date)
        # Start new box anchored to today's close
        repo.create_darvas_box(ticker, day_price.trade_date, float(day_price.close), darvas_height_pct)
        repo.set_breakout_streak(ticker, 0)
        return Decision("NO_OP")

    # Within box -> extend end date and NO_OP
    if box.min_price <= open_price <= box.max_price:
        repo.update_active_box_end_date(ticker, day_price.trade_date)
        return Decision("NO_OP")

    # Above box -> breakout handling
    if open_price > box.max_price:
        streak = repo.get_breakout_streak(ticker)
        new_streak = streak + 1
        if new_streak >= breakout_streak:
            # BUY stop-loss: lower limit of the current (broken) box
            initial_stop = box.min_price
            repo.set_breakout_streak(ticker, 0)
            return Decision("BUY", stop_loss=round(initial_stop, 4))
        else:
            repo.set_breakout_streak(ticker, new_streak)
            return Decision("NO_OP")

    return Decision("NO_OP")
