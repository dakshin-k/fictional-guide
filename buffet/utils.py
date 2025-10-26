from decimal import Decimal


def calculate_transaction_charges(trade_value, is_buy: bool = True) -> Decimal:
    """Calculate total transaction cost for a trade value.

    Charges:
    - brokerage: 0.1% bounded between ₹5 and ₹20
    - STT: 0.1%
    - turnover: 0.0001%
    - stamp duty: 0.1% only on buy
    """
    tv = Decimal(str(trade_value))
    brokerage = max(Decimal("5"), min(Decimal("20"), Decimal("0.001") * tv))
    stt = Decimal("0.001") * tv
    turnover = Decimal("0.000001") * tv
    stamp_duty = (Decimal("0.001") * tv) if is_buy else Decimal("0")
    return brokerage + stt + turnover + stamp_duty


def max_affordable_buy_qty(
    available_cash, price, invest_cap=None
) -> int:
    """Return max integer qty such that qty*price + fees <= min(available_cash, invest_cap).

    - If invest_cap is None, only available_cash is considered.
    - Uses calculate_transaction_charges for buy-side fees.
    """
    cash = Decimal(str(available_cash))
    p = Decimal(str(price))
    budget = cash if invest_cap is None else min(cash, Decimal(str(invest_cap)))

    if p <= 0 or budget <= 0:
        return 0

    qty = int(budget / p)
    while qty > 0:
        trade_val = Decimal(qty) * p
        fees = calculate_transaction_charges(trade_val, is_buy=True)
        total_cost = trade_val + fees
        if total_cost <= budget:
            return qty
        qty -= 1
    return 0