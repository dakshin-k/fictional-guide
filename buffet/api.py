from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date as date_type
from decimal import Decimal
from typing import Optional

import pyotp
from growwapi import GrowwAPI

import config
from repository import ActiveTrade


@dataclass
class StopLossStatus:
    triggered: bool
    amount: Optional[Decimal]


class FinanceApi(ABC):
    @abstractmethod
    def get_trading_price(self, date: date_type, ticker: str) -> Optional[Decimal]:
        pass

    @abstractmethod
    def is_trading_day(self, date: date_type) -> bool:
        pass

    @abstractmethod
    def get_buy_cost(self, ticker: str, qty: int, today: date_type) -> Decimal:
        pass

    @abstractmethod
    def buy(self, ticker: str, qty: int, today: date_type) -> None:
        pass

    @abstractmethod
    def update_stop_loss(self, ticker: str, stop_loss: Decimal) -> None:
        pass

    @abstractmethod
    def get_stop_loss_status(self, trade: ActiveTrade) -> StopLossStatus:
        pass


class GrowwApi(FinanceApi):
    def __init__(self, today: date_type):
        api_key = config.groww_api_key()
        totp_gen = pyotp.TOTP(config.groww_api_secret())
        totp = totp_gen.now()

        access_token = GrowwAPI.get_access_token(api_key=api_key, totp=totp)
        self.growwapi = GrowwAPI(str(access_token))
        self.today = today

    def get_trading_price(self, date: date_type, ticker: str) -> Optional[Decimal]:
        return Decimal(
            self.growwapi.get_quote(
                exchange=self.growwapi.EXCHANGE_NSE,
                segment=self.growwapi.SEGMENT_CASH,
                trading_symbol=ticker,
            )["ohlc"]["open"]
        )

    def is_trading_day(self, date: date_type) -> bool:
        raise NotImplementedError()

    def buy(self, ticker: str, qty: int, today: date_type) -> None:
        raise NotImplementedError()

    def update_stop_loss(self, ticker: str, stop_loss: Decimal) -> None:
        raise NotImplementedError()

    def get_stop_loss_status(self, trade: ActiveTrade) -> StopLossStatus:
        raise NotImplementedError()

    def get_buy_cost(self, ticker: str, qty: int, today: date_type) -> Decimal:
        raise NotImplementedError()
