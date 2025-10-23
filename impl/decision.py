import duckdb
from typing import Optional


class Decision:
    decision: str
    stop_loss: Optional[float]

    def __init__(self, decision: str, stop_loss: Optional[float] = None) -> None:
        if decision == "BUY" and stop_loss is None:
            raise ValueError("Stop loss must be provided for BUY decision")
        self.decision = decision
        self.stop_loss = stop_loss

def get_decision(con: duckdb.DuckDBPyConnection, ticker: str, date: str) -> Decision:
    return Decision(
        decision="BUY",
        stop_loss=1,
    )
