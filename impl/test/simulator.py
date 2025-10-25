import sqlite3
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date
import logging
from decimal import Decimal

from tqdm import tqdm
from sqlalchemy import create_engine

from setup.setup_db import apply_schema
from decision import get_decision


class SimulationException(Exception):
    """Custom exception for simulation errors"""

    pass


class LogMessage:
    msg: str
    level: str

    def __init__(self, msg: str, level: str) -> None:
        self.msg = msg
        self.level = level


class TradingSimulator:
    """
    Trading simulator that tests the performance of the decisioning system.

    Features:
    - Initializes SQLite database with schema
    - Loads yfinance CSV data into database tables
    - Manages portfolio with ₹500 starting budget per ticker
    - Executes trading decisions (BUY/SELL/STOP_LOSS/NO_OP)
    - Tracks cash and positions per ticker
    - Logs events and generates performance reports
    """

    def __init__(
        self, db_path: str = ":memory:", initial_cash_per_ticker: float = 500.0
    ):
        """
        Initialize the trading simulator.

        Args:
            db_path: Path to SQLite database file (":memory:" for in-memory)
            initial_cash_per_ticker: Starting cash amount per ticker
        """
        self.db_path = db_path
        self.initial_cash_per_ticker = initial_cash_per_ticker
        self.con: Optional[sqlite3.Connection] = None
        self.tickers: List[str] = []
        self.trading_dates: List[date] = []
        self.log_messages: List[LogMessage] = []

        # Setup logging
        logging.basicConfig(level=logging.INFO, format="%(message)s")
        self.logger = logging.getLogger(__name__)

    def _ensure_connection(self) -> sqlite3.Connection:
        """Ensure database connection is available and return it."""
        if not self.con:
            raise SimulationException("Database connection not initialized")
        return self.con

    def initialize_database(self, schema_path: str) -> None:
        """Initialize SQLite database and apply schema."""
        self.con = sqlite3.connect(self.db_path)
        # Enable foreign key enforcement for SQLite and prep for bulk inserts
        self.con.execute("PRAGMA foreign_keys = ON")
        apply_schema(self.con, schema_path)
        self.con.commit()
        self.logger.info(f"Database initialized with schema from {schema_path}")

    def load_yfinance_data(self, data_dir: str) -> None:
        """
        Load yfinance CSV files into the database.

        Args:
            data_dir: Directory containing yfinance CSV files
        """
        data_path = Path(data_dir)

        # Expected CSV files
        csv_files = {
            "close": data_path / "yfinance_close.csv",
            "high": data_path / "yfinance_high.csv",
            "low": data_path / "yfinance_low.csv",
            "open": data_path / "yfinance_open.csv",
            "volume": data_path / "yfinance_volume.csv",
        }

        # Verify all files exist
        for file_type, file_path in csv_files.items():
            if not file_path.exists():
                raise FileNotFoundError(f"Required CSV file not found: {file_path}")

        # Load and process data
        self.logger.info("Loading yfinance data...")

        # Read the close data to get tickers and dates structure
        close_df = pd.read_csv(csv_files["close"])
        close_df["Date"] = pd.to_datetime(close_df["Date"]).dt.date

        # Extract tickers (all columns except Date)
        self.tickers = [col for col in close_df.columns if col != "Date"]
        self.trading_dates = sorted(close_df["Date"].unique())

        self.logger.info(
            f"Found {len(self.tickers)} tickers and {len(self.trading_dates)} trading days"
        )

        # Load all OHLCV data
        data_frames = {}
        for file_type, file_path in csv_files.items():
            df = pd.read_csv(file_path)
            df["Date"] = pd.to_datetime(df["Date"]).dt.date
            data_frames[file_type] = df

        # Transform data from wide to long format and insert into historicals table
        records = []
        for trade_date in self.trading_dates:
            for ticker in self.tickers:
                try:
                    record = {
                        "trade_date": trade_date,
                        "ticker": ticker,
                        "close": data_frames["close"][
                            data_frames["close"]["Date"] == trade_date
                        ][ticker].iloc[0],
                        "high": data_frames["high"][
                            data_frames["high"]["Date"] == trade_date
                        ][ticker].iloc[0],
                        "low": data_frames["low"][
                            data_frames["low"]["Date"] == trade_date
                        ][ticker].iloc[0],
                        "open": data_frames["open"][
                            data_frames["open"]["Date"] == trade_date
                        ][ticker].iloc[0],
                        "volume": data_frames["volume"][
                            data_frames["volume"]["Date"] == trade_date
                        ][ticker].iloc[0],
                    }

                    # Skip records with NaN values
                    if not any(
                        pd.isna(val)
                        for val in record.values()
                        if val != trade_date and val != ticker
                    ):
                        records.append(record)

                except (IndexError, KeyError):
                    # Skip missing data points
                    continue

        # Insert data into database
        if records:
            engine = create_engine(f"sqlite:///{self.db_path}")
            records_df = pd.DataFrame(records)
            records_df.to_sql(
                "historicals",
                engine,
                if_exists="replace",
                index=False,
                method="multi",
            )
            self.logger.info(f"Loaded {len(records)} historical data records")

    def initialize_portfolio_cash(self) -> None:
        """Initialize portfolio cash for all tickers."""
        engine = create_engine(f"sqlite:///{self.db_path}")
        df = pd.DataFrame(
            [
                {
                    "ticker": ticker,
                    "available_cash": float(self.initial_cash_per_ticker),
                    "is_active": True,
                }
                for ticker in self.tickers
            ]
        )
        df.to_sql(
            "portfolio_cash",
            engine,
            if_exists="replace",
            index=False,
            method="multi",
        )

        self.logger.info(
            f"Initialized portfolio with ₹{self.initial_cash_per_ticker} per ticker for {len(self.tickers)} tickers"
        )

    def _get_portfolio_status(self, ticker: str) -> Tuple[Decimal, bool]:
        """Get current cash and active status for a ticker."""
        con = self._ensure_connection()
        result = con.execute(
            """
            SELECT available_cash, is_active 
            FROM portfolio_cash 
            WHERE ticker = ?
        """,
            [ticker],
        ).fetchone()

        if result:
            return Decimal(result[0]), result[1]
        print("NO RESULT!")
        return Decimal(0.0), False

    def _update_portfolio_cash(
        self, ticker: str, new_cash: Decimal, is_active: bool = True
    ) -> None:
        """Update portfolio cash for a ticker."""
        con = self._ensure_connection()
        con.execute(
            """
            UPDATE portfolio_cash 
            SET available_cash = ?, is_active = ?
            WHERE ticker = ?
        """,
            [float(new_cash), is_active, ticker],
        )

    def _log_event(
        self, trade_date: date, ticker: str, message: str, log_type: str = "INFO"
    ) -> None:
        """Log an event to the simulation log."""
        con = self._ensure_connection()
        con.execute(
            """
            INSERT INTO simulation_log (log_date, ticker, message, log_type)
            VALUES (?, ?, ?, ?)
        """,
            [trade_date, ticker, message, log_type],
        )

        # Also log to console
        self.log_messages.append(
            LogMessage(msg=f"{trade_date} - {ticker}: {message}", level=log_type)
        )

    def _get_current_position(
        self, ticker: str
    ) -> Tuple[int, Optional[float], Optional[float]]:
        """Get current position for a ticker (qty, buy_price, stop_loss)."""
        con = self._ensure_connection()
        result = con.execute(
            """
            SELECT qty_owned, buy_price, stop_loss_amt
            FROM active_trades
            WHERE ticker = ?
        """,
            [ticker],
        ).fetchone()

        if result:
            return (
                int(result[0]),
                float(result[1]) if result[1] else None,
                float(result[2]) if result[2] else None,
            )
        return 0, None, None

    @staticmethod
    def _calculate_transaction_charges(trade_value, is_buy: bool = True) -> Decimal:
        """
        Calculate total transaction cost for a trade value.

        brokerage: 0.1% bounded between ₹5 and ₹20
        STT: 0.1%
        turnover: 0.0001%
        stamp duty: 0.1% only on buy
        """
        tv = Decimal(str(trade_value))
        brokerage = max(Decimal("5"), min(Decimal("20"), Decimal("0.001") * tv))
        stt = Decimal("0.001") * tv
        turnover = Decimal("0.000001") * tv
        stamp_duty = (Decimal("0.001") * tv) if is_buy else Decimal("0")
        return brokerage + stt + turnover + stamp_duty

    def _max_affordable_buy_qty(self, cash: Decimal, price: Decimal) -> int:
        """Return max integer qty such that qty*price + fees <= cash."""
        qty = int(cash / price)
        while qty > 0:
            trade_val = Decimal(qty) * price
            fees = self._calculate_transaction_charges(trade_val, is_buy=True)
            total_cost = trade_val + fees
            if total_cost <= cash:
                return qty
            qty -= 1
        return 0

    def _execute_buy(
        self, ticker: str, trade_date: date, price: Decimal, stop_loss: float
    ) -> None:
        """Execute a BUY transaction."""
        cash, is_active = self._get_portfolio_status(ticker)

        if not is_active:
            raise SimulationException(
                f"Cannot buy {ticker}: trading is inactive due to insufficient funds"
            )

        if cash <= 0:
            self._update_portfolio_cash(ticker, cash, False)
            self._log_event(
                trade_date,
                ticker,
                f"Trading deactivated: insufficient cash (₹{cash:.2f})",
                "WARNING",
            )
            return

        qty_owned, _, _ = self._get_current_position(ticker)
        if qty_owned > 0:
            return

        qty_to_buy = self._max_affordable_buy_qty(cash, price)
        if qty_to_buy <= 0:
            return

        total_trade_value = Decimal(qty_to_buy) * price
        buy_fees = self._calculate_transaction_charges(total_trade_value, is_buy=True)
        total_cost = total_trade_value + buy_fees
        remaining_cash = cash - total_cost

        self._update_portfolio_cash(ticker, remaining_cash)

        con = self._ensure_connection()
        con.execute(
            """
            INSERT OR REPLACE INTO active_trades (ticker, qty_owned, buy_price, stop_loss_amt)
            VALUES (?, ?, ?, ?)
            """,
            [ticker, qty_to_buy, float(price), float(stop_loss)],
        )

        con.execute(
            """
            INSERT INTO transactions (txn_date, ticker, txn_type, price, qty)
            VALUES (?, ?, 'BUY', ?, ?)
            """,
            [trade_date, ticker, float(price), qty_to_buy],
        )

        self._log_event(
            trade_date,
            ticker,
            f"BUY: {qty_to_buy} shares at ₹{price:.2f}, fees: ₹{buy_fees:.2f}, total cost: ₹{total_cost:.2f}, stop-loss: ₹{Decimal(str(stop_loss)):.2f}, remaining cash: ₹{remaining_cash:.2f}",
        )

    def _execute_sell(self, ticker: str, trade_date: date, price: float) -> None:
        qty_owned, buy_price, _ = self._get_current_position(ticker)

        if qty_owned <= 0:
            raise SimulationException(
                f"Invalid SELL decision for {ticker}: no shares owned"
            )

        total_trade_value = Decimal(qty_owned) * Decimal(price)
        sell_fees = self._calculate_transaction_charges(total_trade_value, is_buy=False)
        net_proceeds = total_trade_value - sell_fees

        cash, is_active = self._get_portfolio_status(ticker)
        new_cash = cash + net_proceeds

        self._update_portfolio_cash(ticker, new_cash)

        con = self._ensure_connection()
        con.execute("DELETE FROM active_trades WHERE ticker = ?", [ticker])

        con.execute(
            """
            INSERT INTO transactions (txn_date, ticker, txn_type, price, qty)
            VALUES (?, ?, 'SELL', ?, ?)
        """,
            [trade_date, ticker, price, qty_owned],
        )

        buy_price_dec = (
            Decimal(str(buy_price)) if buy_price is not None else Decimal("0")
        )
        profit_loss = net_proceeds - (Decimal(qty_owned) * buy_price_dec)
        self._log_event(
            trade_date,
            ticker,
            f"SELL: {qty_owned} shares at ₹{price:.2f}, fees: ₹{sell_fees:.2f}, net proceeds: ₹{net_proceeds:.2f}, P&L: ₹{profit_loss:.2f}, total cash: ₹{new_cash:.2f}",
        )

    def _update_stop_loss(
        self, ticker: str, trade_date: date, new_stop_loss: float
    ) -> None:
        """Update stop-loss for an existing position."""
        qty_owned, _, current_stop_loss = self._get_current_position(ticker)

        if qty_owned <= 0:
            self._log_event(
                trade_date,
                ticker,
                f"Cannot update stop-loss: no position held",
                "WARNING",
            )
            return

        con = self._ensure_connection()
        con.execute(
            """
            UPDATE active_trades 
            SET stop_loss_amt = ?
            WHERE ticker = ?
        """,
            [new_stop_loss, ticker],
        )

        self._log_event(
            trade_date,
            ticker,
            f"Updated stop-loss from ₹{current_stop_loss:.2f} to ₹{new_stop_loss:.2f}",
        )

    def _check_stop_loss(
        self, ticker: str, trade_date: date, current_price: float
    ) -> bool:
        """Check if stop-loss should be triggered and execute if needed."""
        qty_owned, buy_price, stop_loss = self._get_current_position(ticker)

        if qty_owned > 0 and stop_loss and current_price <= stop_loss:
            self._execute_sell(ticker, trade_date, stop_loss)
            return True

        return False

    def run_simulation(self) -> Dict:
        """
        Run the complete trading simulation.

        Returns:
            Dictionary containing simulation results and portfolio performance
        """
        if not self.con:
            raise SimulationException(
                "Database not initialized. Call initialize_database() first."
            )

        if not self.tickers or not self.trading_dates:
            raise SimulationException(
                "No data loaded. Call load_yfinance_data() first."
            )

        self.logger.info(
            f"Starting simulation for {len(self.tickers)} tickers over {len(self.trading_dates)} days"
        )

        simulation_start = datetime.now()

        # Process each trading day
        for ticker in tqdm(self.tickers):
            for trade_date in self.trading_dates:
                try:
                    # Check if trading is still active for this ticker
                    cash, is_active = self._get_portfolio_status(ticker)
                    if not is_active:
                        self._log_event(
                            trade_date, ticker, f"Trading inactive for {ticker}"
                        )
                        continue

                    # Get current price data
                    con = self._ensure_connection()
                    price_data = con.execute(
                        """
                        SELECT open, high, low, close
                        FROM historicals
                        WHERE ticker = ? AND trade_date = ?
                    """,
                        [ticker, trade_date],
                    ).fetchone()

                    if not price_data:
                        # self._log_event(trade_date, ticker, f"No price data for {ticker} on {trade_date}")
                        continue

                    open_price, high_price, low_price, close_price = map(Decimal, price_data)

                    # Check stop-loss first (using low price for worst case)
                    if self._check_stop_loss(ticker, trade_date, low_price):
                        self._log_event(
                            trade_date,
                            ticker,
                            f"Stop-loss triggered for {ticker} on {trade_date}",
                        )
                        continue  # Position was closed due to stop-loss

                    # Get decision from the decision engine
                    decision = get_decision(self.con, ticker, str(trade_date))

                    # Execute decision
                    if decision.decision == "BUY":
                        if decision.stop_loss is None:
                            raise SimulationException(
                                f"BUY decision for {ticker} missing required stop_loss"
                            )
                        self._execute_buy(
                            ticker, trade_date, open_price, decision.stop_loss
                        )

                    elif decision.decision == "SELL":
                        self._execute_sell(ticker, trade_date, open_price)

                    elif decision.decision == "UPDATE_STOP_LOSS":
                        if decision.stop_loss is None:
                            raise SimulationException(
                                f"UPDATE_STOP_LOSS decision for {ticker} missing required stop_loss"
                            )
                        self._update_stop_loss(ticker, trade_date, decision.stop_loss)

                    elif decision.decision == "NO_OP":
                        # No action needed
                        pass

                    else:
                        raise SimulationException(
                            f"Invalid decision '{decision.decision}' for {ticker} on {trade_date}"
                        )

                except Exception as e:
                    self._log_event(
                        trade_date, ticker, f"Error processing: {str(e)}", "ERROR"
                    )
                    raise

        simulation_end = datetime.now()
        simulation_duration = simulation_end - simulation_start

        self._flush_logs()

        # Generate final report
        results = self._generate_final_report()
        results["simulation_duration"] = simulation_duration.total_seconds()

        self.logger.info(
            f"Simulation completed in {simulation_duration.total_seconds():.2f} seconds"
        )

        return results

    def _generate_final_report(self) -> Dict:
        """Generate final portfolio performance report."""
        con = self._ensure_connection()

        # Get final portfolio values
        portfolio_summary = con.execute("""
            SELECT 
                pc.ticker,
                pc.available_cash,
                pc.is_active,
                COALESCE(active_trades.qty_owned, 0) as qty_owned,
                active_trades.buy_price,
                h.close as current_price
            FROM portfolio_cash pc
            LEFT JOIN active_trades ON pc.ticker = active_trades.ticker
            LEFT JOIN historicals h ON pc.ticker = h.ticker 
                AND h.trade_date = (SELECT MAX(trade_date) FROM historicals WHERE ticker = pc.ticker)
            ORDER BY pc.ticker
        """).fetchall()

        total_portfolio_value = 0.0
        active_tickers = 0
        inactive_tickers = 0
        total_cash = 0.0
        total_position_value = 0.0

        ticker_details = []

        for row in portfolio_summary:
            ticker, cash, is_active, qty_owned, buy_price, current_price = row

            cash = float(cash) if cash else 0.0
            qty_owned = int(qty_owned) if qty_owned else 0
            buy_price = float(buy_price) if buy_price else 0.0
            current_price = float(current_price) if current_price else 0.0

            position_value = qty_owned * current_price
            ticker_total_value = cash + position_value

            total_cash += cash
            total_position_value += position_value
            total_portfolio_value += ticker_total_value

            if is_active:
                active_tickers += 1
            else:
                inactive_tickers += 1

            unrealized_pnl = (
                (current_price - buy_price) * qty_owned
                if qty_owned > 0 and buy_price > 0
                else 0.0
            )

            ticker_details.append(
                {
                    "ticker": ticker,
                    "cash": cash,
                    "qty_owned": qty_owned,
                    "position_value": position_value,
                    "total_value": ticker_total_value,
                    "is_active": is_active,
                    "unrealized_pnl": unrealized_pnl,
                    "current_price": current_price,
                }
            )

        # Calculate overall performance
        initial_total_value = len(self.tickers) * self.initial_cash_per_ticker
        capital_gains_tax = max(0, 0.2 * (total_portfolio_value - initial_total_value))
        total_return = total_portfolio_value - initial_total_value - capital_gains_tax
        total_return_pct = (
            (total_return / initial_total_value) * 100
            if initial_total_value > 0
            else 0.0
        )

        # Get transaction summary
        transaction_summary = con.execute("""
            SELECT 
                txn_type,
                COUNT(*) as count,
                SUM(price * qty) as total_value
            FROM transactions
            GROUP BY txn_type
        """).fetchall()

        results = {
            "portfolio_summary": {
                "total_portfolio_value": total_portfolio_value,
                "total_cash": total_cash,
                "total_position_value": total_position_value,
                "initial_value": initial_total_value,
                "total_return": total_return,
                "total_return_pct": total_return_pct,
                "active_tickers": active_tickers,
                "inactive_tickers": inactive_tickers,
                "total_tickers": len(self.tickers),
            },
            "ticker_details": ticker_details,
            "transaction_summary": {
                row[0]: {"count": row[1], "total_value": float(row[2])}
                for row in transaction_summary
            },
        }

        # Print summary to console
        print("\n" + "=" * 80)
        print("TRADING SIMULATION RESULTS")
        print("=" * 80)
        print(f"Initial Portfolio Value: ₹{initial_total_value:,.2f}")
        print(f"Final Portfolio Value:   ₹{total_portfolio_value:,.2f}")
        print(
            f"Total Return:           ₹{total_return:,.2f} ({total_return_pct:+.2f}%)"
        )
        print(f"Cash:                   ₹{total_cash:,.2f}")
        print(f"Positions Value:        ₹{total_position_value:,.2f}")
        print(f"Active Tickers:         {active_tickers}/{len(self.tickers)}")
        print(f"Inactive Tickers:       {inactive_tickers}/{len(self.tickers)}")

        print("\n" + "=" * 80)
        print('Worst performing Stocks: ')
        for ticker_detail in sorted(ticker_details, key=lambda x: x['total_value'])[:3]:
            print(f"  {ticker_detail['ticker']}: Value ₹{ticker_detail['total_value']:,.2f}")

        if transaction_summary:
            print("\nTransaction Summary:")
            for txn_type, count, total_value in transaction_summary:
                print(
                    f"  {txn_type}: {count} transactions, ₹{total_value:,.2f} total value"
                )

        print("=" * 80)

        return results

    def close(self) -> None:
        """Close database connection."""
        if self.con:
            self.con.close()
            self.con = None

    def _flush_logs(self):
        for log in self.log_messages:
            logging.log(level=getattr(logging, log.level.upper()), msg=log.msg)


def initialize_db_from_files(
    data_dir: str,
    schema_path: str,
    db_path: str,
    initial_cash_per_ticker: float,
) -> None:
    base_db_path = Path(db_path)
    base_db_path.parent.mkdir(parents=True, exist_ok=True)
    builder = TradingSimulator(str(base_db_path), initial_cash_per_ticker)
    try:
        builder.initialize_database(schema_path)
        builder.load_yfinance_data(data_dir)
        builder.initialize_portfolio_cash()
    finally:
        builder.close()


def run_simulation_from_files(
    schema_path: str, db_path: str = ":memory:", initial_cash_per_ticker: float = 500.0
) -> Dict:
    """
    Convenience function to run a complete simulation using an existing test data DB.

    Args:
        data_dir: Unused in file-backed cloning, used for in-memory fallback initialization
        schema_path: Path to schema SQL file
        db_path: Target database path (":memory:" for in-memory)
        initial_cash_per_ticker: Starting cash per ticker

    Returns:
        Simulation results dictionary
    """
    # Hardcode source test DB path at project root
    source_db_path = Path(__file__).parent / "test_data.sqlite"
    if not source_db_path.exists():
        raise FileNotFoundError(f"Source test database not found: {source_db_path}")

    # Initialize schema, then attach source DB and copy data
    simulator = TradingSimulator(db_path, initial_cash_per_ticker)
    try:
        simulator.initialize_database(schema_path)
        con = simulator._ensure_connection()
        con.execute(f"ATTACH DATABASE '{str(source_db_path)}' AS src")
        # Copy historicals content from source DB
        con.execute("DELETE FROM historicals")
        con.execute("INSERT INTO historicals SELECT * FROM src.historicals")
        con.execute("INSERT INTO portfolio_cash SELECT * FROM src.portfolio_cash")

        # Derive tickers and dates
        tickers_rows = con.execute(
            "SELECT DISTINCT ticker FROM historicals ORDER BY ticker"
        ).fetchall()
        simulator.tickers = [row[0] for row in tickers_rows]

        dates_rows = con.execute(
            "SELECT DISTINCT trade_date FROM historicals ORDER BY trade_date"
        ).fetchall()
        simulator.trading_dates = [row[0] for row in dates_rows]

        # Reset simulation tables and initialize portfolio cash
        con.execute("DELETE FROM active_trades")
        con.execute("DELETE FROM transactions")
        con.execute("DELETE FROM simulation_log")

        results = simulator.run_simulation()
        return results
    finally:
        simulator.close()
