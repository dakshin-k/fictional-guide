#!/usr/bin/env python3
"""
Test script for the trading simulator.
This script runs a complete simulation using the existing yfinance data files.
"""

import sys
from pathlib import Path
import argparse
import logging

# Add the impl directory to the path so we can import our modules
sys.path.append(str(Path(__file__).parent / "impl"))

from test.simulator import run_simulation_from_files, initialize_db_from_files


def main():
    """Run the trading simulator test."""

    # Define paths
    project_root = Path(__file__).parent.parent
    data_dir = str(project_root)  # CSV files are in the project root
    schema_path = str(project_root / "impl" / "setup" / "schema.sql")

    parser = argparse.ArgumentParser(description="Trading Simulator Test Runner")
    parser.add_argument(
        "--init-db",
        action="store_true",
        help="Initialize the base SQLite file from CSVs",
    )
    parser.add_argument(
        '--wallet-cash',
        type=float,
        default=500.0,
        help='Initial wallet cash X (default: 500.0)',
    )
    parser.add_argument(
        '--invest-cap',
        type=float,
        default=10000.0,
        help='Max cash to invest per stock Y (default: 10000.0)',
    )
    parser.add_argument(
        '--db-path',
        type=str,
        default=':memory:',
        help='Path to the SQLite database file (default: in-memory)',
    )
    parser.add_argument(
        '--breakout-streak',
        type=int,
        default=1,
        help='Number of consecutive breakouts required before BUY (default: 1)',
    )
    parser.add_argument(
        '--darvas-height-pct',
        type=float,
        default=0.01,
        help='Darvas box height as a fraction of base close (default: 0.01)',
    )
    parser.add_argument(
        '--darvas-height-increment-pct',
        type=float,
        default=0.01,
        help='Increment fraction to add to Darvas height after a loss (default: 0.01)',
    )
    parser.add_argument(
        '--leader-lookback-days',
        type=int,
        default=20,
        help='Lookback window (days) for leader checks (default: 20)',
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug-level logging output',
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.WARN,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    )

    if args.init_db:
        db_path = str(project_root / "impl" / "test" / "test_data.sqlite")
        print(f"Initializing base DB at: {db_path}")
        initialize_db_from_files(
            data_dir=data_dir,
            schema_path=schema_path,
            db_path=db_path,
            initial_wallet_cash=args.wallet_cash,
            max_invest_per_stock=args.invest_cap,
        )
        return True

    print("Starting Trading Simulator Test")
    print("=" * 50)
    print(f"Data directory: {data_dir}")
    print(f"Schema path: {schema_path}")
    print(f"DB path: {args.db_path}")
    print(f"Wallet cash X: {args.wallet_cash}")
    print(f"Invest cap Y: {args.invest_cap}")
    print(f"Breakout streak: {args.breakout_streak}")
    print(f"Darvas height pct: {args.darvas_height_pct}")
    print(f"Darvas height increment pct: {args.darvas_height_increment_pct}")
    print(f"Leader lookback days: {args.leader_lookback_days}")
    print(f"Debug logging: {'ON' if args.debug else 'OFF'}")
    print()

    db_path = Path(args.db_path)
    if db_path.is_file():
        db_path.unlink()
        print(f"Deleted existing database file: {db_path}")

    # Run the simulation
    results = run_simulation_from_files(
        schema_path=schema_path,
        db_path=args.db_path,
        initial_wallet_cash=args.wallet_cash,
        max_invest_per_stock=args.invest_cap,
        breakout_streak=args.breakout_streak,
        darvas_height_pct=args.darvas_height_pct,
        darvas_height_increment_pct=args.darvas_height_increment_pct,
        leader_lookback_days=args.leader_lookback_days,
    )

    print("\nSimulation complete. Summary:")
    print(results.get('portfolio_summary', {}))

    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
