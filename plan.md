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
3. Fancy boxes - +/- 3% of the first day's close with 3 day streak: -8.65% (e6c65fc)
4. Boxes plus buy only on higher volume + near historical high, + Rs. 10,000 budget per stock
5. Endgame - box breakout streak, plus increasing heights, plus trigger on higher volume and near historical high
   1. Lookback 180 days:
      1. Wallet 1L, cap per stock 5K -> +26.34%
      2. Wallet 2L, cap per stock 5K -> +102.16%
      3. Wallet 1L, cap per stock 10K -> -4.54%
      4. Wallet 2L, cap per stock 10K -> +15.66%
      5. Wallet 4L, cap per stock 10K -> +81.64%
    2. Lookback 365 days:
       1. Wallet 1L, cap per stock 5K -> +165.47%
       2. Wallet 2L, cap per stock 5K -> +325.21%
       3. Wallet 1L, cap per stock 10K -> +76.96%
       4. Wallet 2L, cap per stock 10K -> +148.55%
       5. Wallet 4L, cap per stock 10K -> +291.74%
       6. Wallet 1Cr, cap per stock 20K -> +3280.34% 
