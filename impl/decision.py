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
    breakout_streak: int = 3,
) -> Decision:
    
    """
    Determine trading decision at the start of the trading day.

    This uses only information available up to the previous trading day
    (for breakout confirmation) and the current day's open.

    Parameters:
    - breakout_streak_required: number of consecutive gap-open breakouts to wait for
      before acting (default 3).

    Rules:
    - Box High is max(close) over the previous `box_lookback` days (excluding today).
    - Gap-open breakout only: today's open > box_high * (1 + breakout_buffer).
    - React only on the N-th consecutive gap-open breakout when not in a position.
    - If breakout streak reaches `breakout_streak_required` and no current position:
      BUY at today's open, with initial stop-loss = open * (1 - stop_pct),
      and reset streak to 0.
    - If holding a position: UPDATE_STOP_LOSS when highest_high since entry increases,
      new stop-loss = highest_high * (1 - stop_pct).
    - Non-breakout days reset the breakout streak to 0 when not in a position.
    - Stop-loss SELL is handled by the simulator before calling this function.
    """
    repo = DataRepository(con)

    # Configurable parameters
    box_lookback = 5
    breakout_buffer = 0.01  # 1% above box high
    stop_pct = 0.1

    # Fetch current day (t) price for open
    day_price = repo.get_day_price(ticker, date)
    if not day_price:
        return Decision("NO_OP")

    # Check if we already hold a position
    active = repo.get_active_trade(ticker)
    if active and active.qty_owned > 0:
        # Find entry date and compute highest high since entry up to today
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

    # No position: evaluate gap-open breakout based on prior box and today's open
    closes = repo.get_recent_closes(ticker, date, box_lookback)
    if not closes:
        # Without recent closes we can't form a box; reset streak
        repo.set_breakout_streak(ticker, 0)
        return Decision("NO_OP")

    box_high = max(closes)
    breakout_threshold = box_high * (1 + breakout_buffer)

    gap_open_breakout = day_price.open > breakout_threshold

    if gap_open_breakout:
        streak = repo.get_breakout_streak(ticker)
        new_streak = streak + 1

        if new_streak >= breakout_streak:
            initial_stop = day_price.open * (1 - stop_pct)
            # Reset the streak upon making a BUY decision
            repo.set_breakout_streak(ticker, 0)
            return Decision("BUY", stop_loss=round(initial_stop, 4))
        else:
            # Increment streak and wait for subsequent breakouts
            repo.set_breakout_streak(ticker, new_streak)
            return Decision("NO_OP")

    return Decision("NO_OP")
