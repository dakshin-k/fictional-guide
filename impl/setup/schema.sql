CREATE TABLE IF NOT EXISTS historicals (
    trade_date DATE NOT NULL,
    ticker VARCHAR NOT NULL,
    close DECIMAL(18,4),
    high DECIMAL(18,4),
    low DECIMAL(18,4),
    open DECIMAL(18,4),
    volume BIGINT,
    PRIMARY KEY (ticker, trade_date)
);

CREATE TABLE IF NOT EXISTS active_trades (
    ticker VARCHAR NOT NULL PRIMARY KEY,
    qty_owned BIGINT NOT NULL,
    buy_price DECIMAL(18,4) NOT NULL,
    stop_loss_amt DECIMAL(18,4)
);

CREATE TABLE IF NOT EXISTS transactions (
    txn_date DATE NOT NULL,
    ticker VARCHAR NOT NULL,
    txn_type VARCHAR NOT NULL, -- BUY or SELL
    price DECIMAL(18,4) NOT NULL,
    qty BIGINT NOT NULL,
    CHECK (txn_type IN ('BUY','SELL'))
);

-- Portfolio tracking tables for simulation
CREATE TABLE IF NOT EXISTS portfolio_cash (
    ticker VARCHAR NOT NULL PRIMARY KEY,
    available_cash DECIMAL(18,4) NOT NULL DEFAULT 500.0,
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS simulation_log (
    log_date DATE NOT NULL,
    ticker VARCHAR NOT NULL,
    message TEXT NOT NULL,
    log_type VARCHAR NOT NULL DEFAULT 'INFO' -- INFO, WARNING, ERROR
);
