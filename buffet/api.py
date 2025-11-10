from abc import ABC, abstractmethod
from datetime import datetime, date
from decimal import Decimal
from typing import Optional

import pyotp
from growwapi import GrowwAPI

import config


class FinanceApi(ABC):
    @abstractmethod
    def get_trading_price(self, date: datetime, ticker: str) -> Optional[Decimal]:
        pass

    @abstractmethod
    def is_trading_day(self, date: datetime) -> bool:
        pass

    @abstractmethod
    def buy(self, ticker: str, qty: int, today: date) -> Decimal:
        pass


class GrowwApi(FinanceApi):
    def __init__(self):
        api_key = config.groww_api_key()
        totp_gen = pyotp.TOTP(config.groww_api_secret())
        totp = totp_gen.now()

        access_token = GrowwAPI.get_access_token(api_key=api_key, totp=totp)
        self.growwapi = GrowwAPI(str(access_token))

    def get_trading_price(self, date: datetime, ticker: str) -> Optional[Decimal]:
        return Decimal(
            self.growwapi.get_quote(
                exchange=self.growwapi.EXCHANGE_NSE,
                segment=self.growwapi.SEGMENT_CASH,
                trading_symbol=ticker,
            )["ohlc"]["open"]
        )

    def is_trading_day(self, date: datetime) -> bool:
        raise NotImplementedError()

    def buy(self, ticker: str, qty: int, today: date) -> Decimal:
        raise NotImplementedError()
