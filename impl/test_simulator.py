#!/usr/bin/env python3
"""
Test script for the trading simulator.
This script runs a complete simulation using the existing yfinance data files.
"""

import sys
from pathlib import Path
import argparse

# Add the impl directory to the path so we can import our modules
sys.path.append(str(Path(__file__).parent / "impl"))

from test.simulator import run_simulation_from_files, initialize_db_from_files


def main():
    """Run the trading simulator test."""

    # Define paths
    project_root = Path(__file__).parent.parent
    data_dir = str(project_root)  # CSV files are in the project root
    schema_path = str(project_root / "impl" / "setup" / "schema.sql")
    db_path = ":memory:"

    parser = argparse.ArgumentParser(description="Trading Simulator Test Runner")
    parser.add_argument(
        "--init-db",
        action="store_true",
        help="Initialize the base DuckDB file from CSVs",
    )
    args = parser.parse_args()
    initial_cash_per_ticker = 5000.0

    if args.init_db:
        db_path = str(project_root / "impl" / "test" / "test_data.duckdb")
        print(f"Initializing base DB at: {db_path}")
        initialize_db_from_files(
            data_dir=data_dir,
            schema_path=schema_path,
            db_path=db_path,
            initial_cash_per_ticker=initial_cash_per_ticker,
        )
        return True

    print("Starting Trading Simulator Test")
    print("=" * 50)
    print(f"Data directory: {data_dir}")
    print(f"Schema path: {schema_path}")
    print(f"DB path: {db_path}")
    print(f"Initial cash per ticker: {initial_cash_per_ticker}")
    print()

    try:
        # Run the simulation
        results = run_simulation_from_files(
            schema_path=schema_path,
            db_path=db_path,
            initial_cash_per_ticker=initial_cash_per_ticker,
        )

        print("\nSimulation completed successfully!")
        print(f"Simulation duration: {results['simulation_duration']:.2f} seconds")

        # Display key results
        portfolio = results["portfolio_summary"]
        print(f"\nKey Results:")
        print(
            f"- Total Return: ₹{portfolio['total_return']:,.2f} ({portfolio['total_return_pct']:+.2f}%)"
        )
        print(
            f"- Active Tickers: {portfolio['active_tickers']}/{portfolio['total_tickers']}"
        )
        print(f"- Final Portfolio Value: ₹{portfolio['total_portfolio_value']:,.2f}")

        return True

    except Exception as e:
        print(f"Simulation failed with error: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
