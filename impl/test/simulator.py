from typing import Set
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
    - Manages a global wallet with starting cash
    - Executes trading decisions (BUY/SELL/STOP_LOSS/NO_OP)
    - Tracks cash and positions
    - Logs events and generates performance reports
    """

    def __init__(
        self,
        db_path: str = ":memory:",
        initial_wallet_cash: float = 500.0,
        max_invest_per_stock: float = 10000.0,
        breakout_streak: int = 1,
        darvas_height_pct: float = 0.01,
        darvas_height_increment_pct: float = 0.0,
        leader_lookback_days: int = 20,
    ):
        """
        Initialize the trading simulator.

        Args:
            db_path: Path to SQLite database file (":memory:" for in-memory)
            initial_wallet_cash: Starting cash amount in the global wallet
            max_invest_per_stock: Maximum cash to invest per BUY when not holding
            breakout_streak: Required consecutive breakouts before BUY
            darvas_height_pct: Darvas box height as fraction of base close
            darvas_height_increment_pct: Increment to add to height after a loss
            leader_lookback_days: Lookback window (days) for leader checks
        """
        self.db_path = db_path
        self.initial_wallet_cash = initial_wallet_cash
        # Backward-compat to avoid accidental references
        self.initial_cash_per_ticker = initial_wallet_cash
        self.max_invest_per_stock = max_invest_per_stock
        self.con: Optional[sqlite3.Connection] = None
        self.tickers: List[str] = []
        self.trading_dates: List[date] = []
        self.log_messages: List[LogMessage] = []
        self.breakout_streak = breakout_streak
        self.darvas_height_pct = darvas_height_pct
        self.darvas_height_increment_pct = darvas_height_increment_pct
        self.leader_lookback_days = leader_lookback_days
        # Per-ticker Darvas height mapping moved to decision module
        self.loss_carryover_tickers: Set[str] = set()

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
                chunksize=max(1, 900 // len(records_df.columns)),
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
            chunksize=max(1, 900 // len(df.columns)),
        )

        self.logger.info(
            f"Initialized portfolio with ₹{self.initial_cash_per_ticker} per ticker for {len(self.tickers)} tickers"
        )

    def initialize_wallet_cash(self) -> None:
        """Initialize the global wallet cash."""
        con = self._ensure_connection()
        con.execute("DELETE FROM wallet")
        con.execute(
            "INSERT INTO wallet (available_cash) VALUES (?)",
            [float(self.initial_wallet_cash)],
        )
        self.logger.info(
            f"Initialized wallet with ₹{self.initial_wallet_cash:,.2f}"
        )

    def _get_wallet_cash(self) -> Decimal:
        con = self._ensure_connection()
        row = con.execute(
            "SELECT available_cash FROM wallet LIMIT 1"
        ).fetchone()
        return Decimal(row[0]) if row else Decimal("0.0")

    def _update_wallet_cash(self, new_cash: Decimal) -> None:
        con = self._ensure_connection()
        con.execute(
            "UPDATE wallet SET available_cash = ?",
            [float(new_cash)],
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
        self.logger.info(f"{trade_date} - {ticker}: {message}")

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
        """Execute a BUY transaction using wallet and invest cap."""
        # If we already own, do nothing
        qty_owned, _, _ = self._get_current_position(ticker)
        if qty_owned > 0:
            return

        wallet_cash = self._get_wallet_cash()
        if wallet_cash <= 0:
            self._log_event(
                trade_date,
                ticker,
                f"Insufficient wallet cash (₹{wallet_cash:.2f})",
                "WARNING",
            )
            return

        # Budget for this buy is capped at Y and limited by wallet cash
        budget = min(wallet_cash, Decimal(self.max_invest_per_stock))
        qty_to_buy = self._max_affordable_buy_qty(budget, price)
        if qty_to_buy <= 0:
            return

        total_trade_value = Decimal(qty_to_buy) * price
        buy_fees = self._calculate_transaction_charges(total_trade_value, is_buy=True)
        total_cost = total_trade_value + buy_fees
        new_wallet_cash = wallet_cash - total_cost

        self._update_wallet_cash(new_wallet_cash)

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
            f"BUY: {qty_to_buy} shares at ₹{price:.2f}, fees: ₹{buy_fees:.2f}, total cost: ₹{total_cost:.2f}, stop-loss: ₹{Decimal(str(stop_loss)):.2f}, wallet cash: ₹{new_wallet_cash:.2f}",
        )

    def _execute_sell(self, ticker: str, trade_date: date, price: float) -> Decimal:
        qty_owned, buy_price, _ = self._get_current_position(ticker)

        if qty_owned <= 0:
            raise SimulationException(
                f"Invalid SELL decision for {ticker}: no shares owned"
            )

        total_trade_value = Decimal(qty_owned) * Decimal(price)
        sell_fees = self._calculate_transaction_charges(total_trade_value, is_buy=False)
        net_proceeds = total_trade_value - sell_fees

        wallet_cash = self._get_wallet_cash()
        new_wallet_cash = wallet_cash + net_proceeds

        self._update_wallet_cash(new_wallet_cash)

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
            f"SELL: {qty_owned} shares at ₹{price:.2f}, fees: ₹{sell_fees:.2f}, net proceeds: ₹{net_proceeds:.2f}, P&L: ₹{profit_loss:.2f}, wallet cash: ₹{new_wallet_cash:.2f}",
        )
        return profit_loss

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
    ) -> Tuple[bool, Optional[Decimal]]:
        """Check if stop-loss should be triggered and execute if needed."""
        qty_owned, buy_price, stop_loss = self._get_current_position(ticker)

        if qty_owned > 0 and stop_loss and current_price <= stop_loss:
            profit_loss = self._execute_sell(ticker, trade_date, stop_loss)
            # Optional: log stop-loss trigger
            self._log_event(trade_date, ticker, f"Stop-loss triggered for {ticker} on {trade_date}")
            return True, profit_loss

        return False, None

    # Height adjustment moved to decision module; simulator no longer updates per-ticker Darvas height.

    def run_simulation(self) -> Dict:
        """
        Run the complete trading simulation using a central wallet.

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
                        continue

                    open_price, high_price, low_price, close_price = map(Decimal, price_data)

                    # Check stop-loss first (using low price for worst case)
                    triggered, pl = self._check_stop_loss(ticker, trade_date, float(low_price))
                    if triggered:
                        if pl is not None and pl < Decimal("0"):
                            self.loss_carryover_tickers.add(ticker)
                        continue  # Position was closed due to stop-loss

                    # Get decision from the decision engine
                    loss_flag = ticker in self.loss_carryover_tickers
                    decision = get_decision(
                        self.con,
                        ticker,
                        str(trade_date),
                        breakout_streak=self.breakout_streak,
                        default_height_pct=self.darvas_height_pct,
                        height_increment_pct=self.darvas_height_increment_pct,
                        loss_occurred=loss_flag,
                        leader_lookback_days=self.leader_lookback_days,
                    )
                    if loss_flag:
                        self.loss_carryover_tickers.discard(ticker)

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
                        pl = self._execute_sell(ticker, trade_date, float(open_price))
                        if pl < Decimal("0"):
                            self.loss_carryover_tickers.add(ticker)

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
        """Generate final portfolio performance report using wallet and invested cash."""
        con = self._ensure_connection()

        # Wallet cash
        wallet_row = con.execute("SELECT available_cash FROM wallet LIMIT 1").fetchone()
        wallet_cash = float(wallet_row[0]) if wallet_row else 0.0

        # Active positions and current prices
        positions = con.execute(
            """
            SELECT 
                a.ticker,
                a.qty_owned,
                a.buy_price,
                h.close AS current_price
            FROM active_trades a
            LEFT JOIN historicals h ON a.ticker = h.ticker 
                AND h.trade_date = (
                    SELECT MAX(trade_date) FROM historicals WHERE ticker = a.ticker
                )
            ORDER BY a.ticker
            """
        ).fetchall()

        total_position_value = 0.0
        ticker_details = []
        for row in positions:
            ticker = row[0]
            qty_owned = int(row[1]) if row[1] else 0
            buy_price = float(row[2]) if row[2] else 0.0
            current_price = float(row[3]) if row[3] else 0.0
            position_value = qty_owned * current_price
            total_position_value += position_value
            unrealized_pnl = (
                (current_price - buy_price) * qty_owned
                if qty_owned > 0 and buy_price > 0
                else 0.0
            )
            ticker_details.append(
                {
                    "ticker": ticker,
                    "qty_owned": qty_owned,
                    "buy_price": buy_price,
                    "current_price": current_price,
                    "position_value": position_value,
                    "unrealized_pnl": unrealized_pnl,
                }
            )

        total_portfolio_value = wallet_cash + total_position_value

        # Total cash invested (cost basis of active positions)
        total_invested_row = con.execute(
            """
            SELECT COALESCE(SUM(qty_owned * buy_price), 0.0)
            FROM active_trades
            """
        ).fetchone()
        total_invested_cash = float(total_invested_row[0]) if total_invested_row else 0.0

        # Realized gains for CGT: match each SELL to the latest prior BUY of same ticker
        realized_rows = con.execute(
            """
            SELECT 
                s.ticker,
                s.txn_date,
                s.price AS sell_price,
                s.qty AS sell_qty,
                (
                    SELECT b.price
                    FROM transactions b
                    WHERE b.ticker = s.ticker AND b.txn_type = 'BUY' AND b.txn_date <= s.txn_date
                    ORDER BY b.txn_date DESC
                    LIMIT 1
                ) AS buy_price,
                (
                    SELECT b.qty
                    FROM transactions b
                    WHERE b.ticker = s.ticker AND b.txn_type = 'BUY' AND b.txn_date <= s.txn_date
                    ORDER BY b.txn_date DESC
                    LIMIT 1
                ) AS buy_qty
            FROM transactions s
            WHERE s.txn_type = 'SELL'
            """
        ).fetchall()

        realized_gains = 0.0
        for row in realized_rows:
            sell_price = float(row[2]) if row[2] else 0.0
            sell_qty = int(row[3]) if row[3] else 0
            buy_price = float(row[4]) if row[4] else 0.0
            buy_qty = int(row[5]) if row[5] else 0
            qty = min(sell_qty, buy_qty) if buy_qty > 0 else sell_qty
            gain = (sell_price - buy_price) * qty
            realized_gains += gain

        capital_gains_tax = 0.2 * realized_gains if realized_gains > 0 else 0.0
        portfolio_value_after_tax = total_portfolio_value - capital_gains_tax
        total_return = portfolio_value_after_tax - total_invested_cash
        total_return_pct = (
            (total_return / total_invested_cash) * 100 if total_invested_cash > 0 else 0.0
        )

        # Transaction summary
        transaction_summary = con.execute(
            """
            SELECT 
                txn_type,
                COUNT(*) as count,
                SUM(price * qty) as total_value
            FROM transactions
            GROUP BY txn_type
            """
        ).fetchall()

        results = {
            "portfolio_summary": {
                "wallet_cash": wallet_cash,
                "total_position_value": total_position_value,
                "total_portfolio_value": total_portfolio_value,
                "total_invested_cash": total_invested_cash,
                "capital_gains_tax": capital_gains_tax,
                "portfolio_value_after_tax": portfolio_value_after_tax,
                "total_return": total_return,
                "total_return_pct": total_return_pct,
                "active_positions": len(positions),
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
        print(f"Wallet Cash:            ₹{wallet_cash:,.2f}")
        print(f"Positions Value:        ₹{total_position_value:,.2f}")
        print(f"Portfolio Value:        ₹{total_portfolio_value:,.2f}")
        print(f"Invested Cash:          ₹{total_invested_cash:,.2f}")
        print(f"Capital Gains Tax:      ₹{capital_gains_tax:,.2f}")
        print(f"Portfolio After Tax:    ₹{portfolio_value_after_tax:,.2f}")
        print(
            f"Total Return:           ₹{total_return:,.2f} ({total_return_pct:+.2f}%)"
        )
        print(f"Active Positions:       {len(positions)} / {len(self.tickers)}")
        print("\n" + "=" * 80)

        print('Worst performing Stocks (by position value): ')
        for ticker_detail in sorted(ticker_details, key=lambda x: x['position_value'])[:10]:
            print(f"  {ticker_detail['ticker']}: Position ₹{ticker_detail['position_value']:,.2f}")
    
        print("\n" + "=" * 80)
        print('Best performing Stocks (by position value): ')
        for ticker_detail in sorted(ticker_details, key=lambda x: x['position_value'], reverse=True)[:10]:
            print(f"  {ticker_detail['ticker']}: Position ₹{ticker_detail['position_value']:,.2f}")

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
    initial_wallet_cash: float,
    max_invest_per_stock: float,
) -> None:
    base_db_path = Path(db_path)
    base_db_path.parent.mkdir(parents=True, exist_ok=True)
    builder = TradingSimulator(str(base_db_path), initial_wallet_cash, max_invest_per_stock)
    try:
        builder.initialize_database(schema_path)
        builder.load_yfinance_data(data_dir)
        builder.initialize_wallet_cash()
    finally:
        builder.close()


def run_simulation_from_files(
    schema_path: str,
    db_path: str = ":memory:",
    initial_wallet_cash: float = 500.0,
    max_invest_per_stock: float = 10000.0,
    breakout_streak: int = 1,
    darvas_height_pct: float = 0.01,
    darvas_height_increment_pct: float = 0.0,
    leader_lookback_days: int = 20,
) -> Dict:
    source_db_path = Path(__file__).parent / "test_data.sqlite"
    if not source_db_path.exists():
        raise FileNotFoundError(f"Source test database not found: {source_db_path}")

    simulator = TradingSimulator(
        db_path,
        initial_wallet_cash,
        max_invest_per_stock,
        breakout_streak=breakout_streak,
        darvas_height_pct=darvas_height_pct,
        darvas_height_increment_pct=darvas_height_increment_pct,
        leader_lookback_days=leader_lookback_days,
    )
    try:
        simulator.initialize_database(schema_path)
        con = simulator._ensure_connection()
        con.execute(f"ATTACH DATABASE '{str(source_db_path)}' AS src")
        # Copy historicals content from source DB
        con.execute("DELETE FROM historicals")
        con.execute("INSERT INTO historicals SELECT * FROM src.historicals")

        # Derive tickers and dates
        tickers_rows = con.execute(
            "SELECT DISTINCT ticker FROM historicals ORDER BY ticker"
        ).fetchall()
        simulator.tickers = [row[0] for row in tickers_rows]

        dates_rows = con.execute(
            "SELECT DISTINCT trade_date FROM historicals ORDER BY trade_date"
        ).fetchall()
        simulator.trading_dates = [row[0] for row in dates_rows]

        # Reset simulation tables
        con.execute("DELETE FROM active_trades")
        con.execute("DELETE FROM transactions")
        con.execute("DELETE FROM simulation_log")
        simulator.initialize_wallet_cash()

        results = simulator.run_simulation()
        con.commit()
        return results
    finally:
        simulator.close()
