from django.db import models, connection
import pyotp
from tqdm import tqdm
from models import *
from datetime import date, timedelta
import decision
import config
from decimal import Decimal
from repository import DataRepository
from utils import max_affordable_buy_qty
from growwapi import GrowwAPI

def run():
    '''
    1. Get today's date
    2. If today was a trading day, update historicals and portfolio
    3. If tomorrow is a trading, generate decision plan and send email
    '''

    today = date.today()
    tomorrow = today + timedelta(days=1)
    todays_losses = dict()

    api_key = config.groww_api_key()
    totp_gen = pyotp.TOTP(config.groww_api_secret())
    totp = totp_gen.now()
    
    access_token = GrowwAPI.get_access_token(api_key=api_key, totp=totp)
    growwapi = GrowwAPI(access_token)

    def is_trading_day(date: date) -> bool:
        # TODO
        return True

    def get_trading_price(ticker: str) -> Decimal:
        return Decimal(
            growwapi.get_quote(
                exchange=growwapi.EXCHANGE_NSE,
                segment=growwapi.SEGMENT_CASH,
                trading_symbol=ticker)['ohlc']['open']
            )


    # Distinct tickers from Django ORM
    tickers = models.Historicals.objects.values_list('ticker', flat=True).distinct()

    if is_trading_day(today):
        '''
        1. For each ticker, fetch and update historicals
        2. For each pending sell order, check status and update order and portfolio. Make note of losses for the decision engine
        '''
        pass

    repo = DataRepository()

    if is_trading_day(tomorrow):
        print("Fetching trading prices:")
        trading_prices = {ticker: get_trading_price(ticker) for ticker in tqdm(tickers)}
        print(trading_prices)

        decisions = []
        for ticker in tickers:
            order_decision = decision.get_decision(
                connection.cursor(),
                ticker,
                str(tomorrow),
                trading_prices[ticker],
                config.leader_lookback_days,
                config.breakout_streak,
                config.default_height_pct,
                config.height_increment_pct,
                todays_losses.get(ticker, False),
            )
            decisions.append(order_decision)
            # Insert TradingPlan rows for BUY or UPDATE_STOP_LOSS decisions
            if order_decision.decision == "BUY":
                wallet = models.Wallet.objects.first()
                wallet_cash = Decimal(str(wallet.available_cash)) if wallet else Decimal("0")
                invest_cap = config.max_invest_per_stock
                qty = max_affordable_buy_qty(wallet_cash, trading_prices[ticker], invest_cap)
                models.TradingPlan.objects.create(
                    ticker=ticker,
                    order_type="BUY",
                    qty=qty,
                )
            elif order_decision.decision == "UPDATE_STOP_LOSS":
                models.TradingPlan.objects.create(
                    ticker=ticker,
                    order_type="UPDATE_STOP_LOSS",
                    qty=None,
                )
            print(f'Ticker: {ticker}, Decision: {order_decision.decision}, Stop Loss: {order_decision.stop_loss}')
        
