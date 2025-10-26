from decimal import Decimal
from django.db import models


class Historicals(models.Model):
    trade_date = models.DateField()
    ticker = models.CharField(max_length=50)
    close = models.DecimalField(max_digits=18, decimal_places=4, null=True, blank=True)
    high = models.DecimalField(max_digits=18, decimal_places=4, null=True, blank=True)
    low = models.DecimalField(max_digits=18, decimal_places=4, null=True, blank=True)
    open = models.DecimalField(max_digits=18, decimal_places=4, null=True, blank=True)
    volume = models.BigIntegerField(null=True, blank=True)

    class Meta:
        db_table = 'historicals'
        constraints = [
            models.UniqueConstraint(fields=['ticker', 'trade_date'], name='historicals_unique')
        ]
        indexes = [
            models.Index(fields=['ticker']),
            models.Index(fields=['trade_date']),
        ]

    def __str__(self) -> str:
        return f"{self.ticker} @ {self.trade_date}"


class ActiveTrade(models.Model):
    ticker = models.CharField(max_length=50, primary_key=True)
    qty_owned = models.BigIntegerField()
    buy_price = models.DecimalField(max_digits=18, decimal_places=4)
    stop_loss_amt = models.DecimalField(max_digits=18, decimal_places=4, null=True, blank=True)

    class Meta:
        db_table = 'active_trades'

    def __str__(self) -> str:
        return f"{self.ticker}: qty={self.qty_owned}, buy={self.buy_price}"


class Transaction(models.Model):
    BUY = 'BUY'
    SELL = 'SELL'
    TXN_TYPE_CHOICES = (
        (BUY, 'BUY'),
        (SELL, 'SELL'),
    )

    txn_date = models.DateField()
    ticker = models.CharField(max_length=50)
    txn_type = models.CharField(max_length=4, choices=TXN_TYPE_CHOICES)
    price = models.DecimalField(max_digits=18, decimal_places=4)
    qty = models.BigIntegerField()

    class Meta:
        db_table = 'transactions'
        indexes = [
            models.Index(fields=['ticker']),
            models.Index(fields=['txn_date']),
        ]

    def __str__(self) -> str:
        return f"{self.txn_type} {self.ticker} {self.qty} @ {self.price} on {self.txn_date}"


class PortfolioCash(models.Model):
    ticker = models.CharField(max_length=50, primary_key=True)
    available_cash = models.DecimalField(max_digits=18, decimal_places=4, default=Decimal('500.0'))
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = 'portfolio_cash'

    def __str__(self) -> str:
        return f"{self.ticker}: cash={self.available_cash}, active={self.is_active}"


class Wallet(models.Model):
    id = models.BigAutoField(primary_key=True)
    available_cash = models.DecimalField(max_digits=18, decimal_places=4)

    class Meta:
        db_table = 'wallet'

    def __str__(self) -> str:
        return f"wallet: cash={self.available_cash}"


class SimulationLog(models.Model):
    INFO = 'INFO'
    WARNING = 'WARNING'
    ERROR = 'ERROR'
    LOG_TYPE_CHOICES = (
        (INFO, 'INFO'),
        (WARNING, 'WARNING'),
        (ERROR, 'ERROR'),
    )

    log_date = models.DateField()
    ticker = models.CharField(max_length=50)
    message = models.TextField()
    log_type = models.CharField(max_length=10, choices=LOG_TYPE_CHOICES, default=INFO)

    class Meta:
        db_table = 'simulation_log'
        indexes = [
            models.Index(fields=['ticker']),
            models.Index(fields=['log_date']),
        ]

    def __str__(self) -> str:
        return f"[{self.log_type}] {self.ticker} {self.log_date}: {self.message[:40]}..."


class StrategyState(models.Model):
    ticker = models.CharField(max_length=50, primary_key=True)
    breakout_streak = models.IntegerField(default=0)

    class Meta:
        db_table = 'strategy_state'

    def __str__(self) -> str:
        return f"{self.ticker}: streak={self.breakout_streak}"


class DarvasBox(models.Model):
    box_id = models.AutoField(primary_key=True)
    ticker = models.CharField(max_length=50)
    start_date = models.DateField()
    end_date = models.DateField(null=True, blank=True)
    min_price = models.DecimalField(max_digits=18, decimal_places=4)
    max_price = models.DecimalField(max_digits=18, decimal_places=4)
    base_close = models.DecimalField(max_digits=18, decimal_places=4)
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = 'darvas_boxes'
        indexes = [
            models.Index(fields=['ticker']),
            models.Index(fields=['start_date']),
            models.Index(fields=['end_date']),
        ]

    def __str__(self) -> str:
        return f"{self.ticker} box({self.start_date}→{self.end_date or '...'}) [{self.min_price}, {self.max_price}]"
    

class Order(models.Model):
    PENDING = 'PENDING'
    FILLED = 'FILLED'
    CANCELLED = 'CANCELLED'
    STATUS_CHOICES = (
        (PENDING, 'PENDING'),
        (FILLED, 'FILLED'),
        (CANCELLED, 'CANCELLED'),
    )

    BUY = 'BUY'
    SELL = 'SELL'
    ORDER_TYPE_CHOICES = (
        (BUY, 'BUY'),
        (SELL, 'SELL'),
    )

    order_id = models.AutoField(primary_key=True)
    ticker = models.CharField(max_length=50)
    order_type = models.CharField(max_length=4, choices=ORDER_TYPE_CHOICES)
    qty = models.BigIntegerField()
    price = models.DecimalField(max_digits=18, decimal_places=4)
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default=PENDING)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'orders'
        indexes = [
            models.Index(fields=['ticker']),
            models.Index(fields=['status']),
            models.Index(fields=['created_at']),
        ]

    def __str__(self) -> str:
        return f"{self.order_type} {self.qty} {self.ticker} @ {self.price} ({self.status})"



class TradingPlan(models.Model):
    BUY = 'BUY'
    SELL = 'SELL'
    ORDER_TYPE_CHOICES = (
        (BUY, 'BUY'),
        (SELL, 'SELL'),
    )

    ticker = models.CharField(max_length=50)
    order_type = models.CharField(max_length=4, choices=ORDER_TYPE_CHOICES)
    qty = models.BigIntegerField(null=True, blank=True)

    class Meta:
        db_table = 'trading_plan'

    def clean(self):
        if self.order_type == self.BUY and self.qty is None:
            raise Exception("Quantity is required for BUY orders.")

    def __str__(self) -> str:
        return f"{self.order_type} {self.ticker} qty={self.qty}"
