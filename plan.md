# Interface

## Inputs

Stock name, date

## Output

- Decision: buy/sell/update SL/nop
- Stop loss amount (for sell and update decision)


# Configuration

- Box length (lookback period)
- Buffer % (% change above a box height to declare a breakout run)
- Stop loss % (todo - change to lower limit of current box)
- Stack size (no. of consecutive breakout boxes before entering trade)

# Data reqd.

### Historicals

For each ticker:  
Following Data of the last 30 days, queryable by date and ticker:
- Closing
- High
- Low

Also stored for future use:
- Open
- Volume

### Active Trades

- Ticker
- Qty owned
- Buy price
- Stop loss amt

### Transaction History

- Date
- Ticker
- Buy/sell
- Price
- Qty

# Algorithm

- Identify box limits - min and max of the last 15 days
- Check if today's opening price qualifies as a breakout
- If there is an active trade:
  - Raise the stop loss to lower of current box
- Else:
  - Increment breakout counter for ticker
  - If threshold reached, return buy decision and stop loss

# Backlog
- The box height is unbounded over the last 15 days, look for better options to model this

# Benchmarks

1. Buy on day 1, never sell: +2.32% (6707a87)
2. 3 consecutive breakout days (open > max of last 15 days): +10.29% (22c63c9)
3. Fancy boxes - +/- 3% of the first day's close with 3 day streak: -8.65%