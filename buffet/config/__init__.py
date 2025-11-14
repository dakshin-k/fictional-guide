import os

starting_cash = 1_00_000
leader_lookback_days = 365
breakout_streak: int = 1
default_height_pct: float = 0.01
height_increment_pct: float = 0.01
max_invest_per_stock: float = 10_000

def groww_api_key() -> str:
    return os.environ['GROWW_API_KEY']

def groww_api_secret() -> str:
    return os.environ['GROWW_API_SECRET']
