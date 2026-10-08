-- ============================================================
-- DATA WAREHOUSE SCHEMA (Denormalized Star Schema)
-- Purpose: Analytics-optimized schema for trading operations
-- This schema should be created on a separate PostgreSQL instance
-- to mimic a data warehouse environment
-- ============================================================

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

-- DIM_CLIENTS: Client master dimension
CREATE TABLE IF NOT EXISTS dim_clients (
    client_id                UUID PRIMARY KEY,
    first_name               VARCHAR(50) NOT NULL,
    middle_name              VARCHAR(50),
    last_name                VARCHAR(50) NOT NULL,
    email                    VARCHAR(255) NOT NULL,
    date_of_birth            DATE NOT NULL,
    join_date                TIMESTAMP WITH TIME ZONE NOT NULL,
    portfolio_size_range     VARCHAR(30),
    risk_tolerance           VARCHAR(30),
    extract_timestamp        TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT uq_dim_clients_email UNIQUE (email)
);

CREATE INDEX idx_dim_clients_join_date ON dim_clients (join_date DESC);
CREATE INDEX idx_dim_clients_portfolio_size ON dim_clients (portfolio_size_range);
CREATE INDEX idx_dim_clients_risk_tolerance ON dim_clients (risk_tolerance);

-- DIM_ACCOUNTS: Account master dimension
CREATE TABLE IF NOT EXISTS dim_accounts (
    account_id              UUID PRIMARY KEY,
    client_id               UUID NOT NULL,
    account_name            VARCHAR(255) NOT NULL,
    cash_balance            NUMERIC(18,2) NOT NULL DEFAULT 0,
    status                  VARCHAR(20) NOT NULL,
    open_date               TIMESTAMP WITH TIME ZONE NOT NULL,
    extract_timestamp       TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT fk_dim_accounts_client
        FOREIGN KEY (client_id) REFERENCES dim_clients (client_id) ON DELETE CASCADE
);

CREATE INDEX idx_dim_accounts_client ON dim_accounts (client_id);
CREATE INDEX idx_dim_accounts_status ON dim_accounts (status);
CREATE INDEX idx_dim_accounts_open_date ON dim_accounts (open_date DESC);

-- DIM_INSTRUMENTS: Security/instrument master dimension
CREATE TABLE IF NOT EXISTS dim_instruments (
    instrument_id           UUID PRIMARY KEY,
    ticker                  VARCHAR(10) NOT NULL UNIQUE,
    instrument_name         VARCHAR(255) NOT NULL,
    asset_class             VARCHAR(20) NOT NULL,
    industry                VARCHAR(100),
    bid                     NUMERIC(18,4),
    ask                     NUMERIC(18,4),
    mid_price               NUMERIC(18,4),
    price_updated_at        TIMESTAMP WITH TIME ZONE,
    extract_timestamp       TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX idx_dim_instruments_ticker ON dim_instruments (ticker);
CREATE INDEX idx_dim_instruments_asset_class ON dim_instruments (asset_class);
CREATE INDEX idx_dim_instruments_industry ON dim_instruments (industry);

-- ============================================================
-- FACT TABLES
-- ============================================================

-- FACT_ORDERS: Order transaction facts
-- Grain: one row per order
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id                UUID PRIMARY KEY,
    account_id              UUID NOT NULL,
    instrument_id           UUID NOT NULL,
    order_type              VARCHAR(10) NOT NULL,
    quantity                NUMERIC(18,6) NOT NULL,
    limit_price             NUMERIC(18,4),
    filled_price            NUMERIC(18,4),
    status                  VARCHAR(20) NOT NULL,
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL,
    filled_at               TIMESTAMP WITH TIME ZONE,
    cancelled_at            TIMESTAMP WITH TIME ZONE,
    cancel_reason           TEXT,
    duration_seconds        NUMERIC(18,2),
    extract_timestamp       TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT fk_fact_orders_account
        FOREIGN KEY (account_id) REFERENCES dim_accounts (account_id) ON DELETE CASCADE,
    CONSTRAINT fk_fact_orders_instrument
        FOREIGN KEY (instrument_id) REFERENCES dim_instruments (instrument_id)
);

CREATE INDEX idx_fact_orders_account ON fact_orders (account_id);
CREATE INDEX idx_fact_orders_instrument ON fact_orders (instrument_id);
CREATE INDEX idx_fact_orders_status ON fact_orders (status);
CREATE INDEX idx_fact_orders_created_at ON fact_orders (created_at DESC);
CREATE INDEX idx_fact_orders_order_type ON fact_orders (order_type);

-- FACT_TRANSACTIONS: Cash flow transactions
-- Grain: one row per deposit/withdrawal
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id          UUID PRIMARY KEY,
    account_id              UUID NOT NULL,
    txn_type                VARCHAR(20) NOT NULL,
    amount                  NUMERIC(18,2) NOT NULL,
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL,
    extract_timestamp       TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT fk_fact_transactions_account
        FOREIGN KEY (account_id) REFERENCES dim_accounts (account_id) ON DELETE CASCADE
);

CREATE INDEX idx_fact_transactions_account ON fact_transactions (account_id);
CREATE INDEX idx_fact_transactions_txn_type ON fact_transactions (txn_type);
CREATE INDEX idx_fact_transactions_created_at ON fact_transactions (created_at DESC);

-- FACT_AUDIT_LOGS: Audit trail event stream
-- Grain: one row per auditable event
CREATE TABLE IF NOT EXISTS fact_audit_logs (
    audit_log_id            UUID PRIMARY KEY,
    client_id               UUID NOT NULL,
    account_id              UUID NOT NULL,
    order_id                UUID NOT NULL,
    event_type              VARCHAR(50) NOT NULL,
    event_time              TIMESTAMP WITH TIME ZONE NOT NULL,
    reason                  TEXT,
    details                 JSONB,
    extract_timestamp       TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT fk_fact_audit_logs_client
        FOREIGN KEY (client_id) REFERENCES dim_clients (client_id) ON DELETE CASCADE,
    CONSTRAINT fk_fact_audit_logs_account
        FOREIGN KEY (account_id) REFERENCES dim_accounts (account_id) ON DELETE CASCADE
);

CREATE INDEX idx_fact_audit_logs_client ON fact_audit_logs (client_id);
CREATE INDEX idx_fact_audit_logs_account ON fact_audit_logs (account_id);
CREATE INDEX idx_fact_audit_logs_event_type ON fact_audit_logs (event_type);
CREATE INDEX idx_fact_audit_logs_event_time ON fact_audit_logs (event_time DESC);

-- FACT_PRICE_HISTORY: Historical OHLCV price data
-- Grain: one row per instrument per date/period
CREATE TABLE IF NOT EXISTS fact_price_history (
    price_history_id        UUID PRIMARY KEY,
    instrument_id           UUID NOT NULL,
    timestamp               TIMESTAMP WITH TIME ZONE NOT NULL,
    open                    NUMERIC(18,4) NOT NULL,
    high                    NUMERIC(18,4) NOT NULL,
    low                     NUMERIC(18,4) NOT NULL,
    close                   NUMERIC(18,4) NOT NULL,
    volume                  BIGINT NOT NULL,
    extract_timestamp       TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT fk_fact_price_history_instrument
        FOREIGN KEY (instrument_id) REFERENCES dim_instruments (instrument_id) ON DELETE CASCADE,
    CONSTRAINT uq_fact_price_history
        UNIQUE (instrument_id, timestamp)
);

CREATE INDEX idx_fact_price_history_instrument ON fact_price_history (instrument_id);
CREATE INDEX idx_fact_price_history_timestamp ON fact_price_history (timestamp DESC);

-- FACT_WATCHLISTS: Client watchlist activities (behavioral signal)
-- Grain: one row per watchlist addition
CREATE TABLE IF NOT EXISTS fact_watchlists (
    watchlist_id            UUID PRIMARY KEY,
    client_id               UUID NOT NULL,
    instrument_id           UUID NOT NULL,
    added_at                TIMESTAMP WITH TIME ZONE NOT NULL,
    extract_timestamp       TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT fk_fact_watchlists_client
        FOREIGN KEY (client_id) REFERENCES dim_clients (client_id) ON DELETE CASCADE,
    CONSTRAINT fk_fact_watchlists_instrument
        FOREIGN KEY (instrument_id) REFERENCES dim_instruments (instrument_id) ON DELETE CASCADE
);

CREATE INDEX idx_fact_watchlists_client ON fact_watchlists (client_id);
CREATE INDEX idx_fact_watchlists_instrument ON fact_watchlists (instrument_id);
CREATE INDEX idx_fact_watchlists_added_at ON fact_watchlists (added_at DESC);

-- FACT_HISTORICAL_SNAPSHOTS: End-of-day portfolio snapshots
-- Grain: one row per account per business day
CREATE TABLE IF NOT EXISTS fact_historical_snapshots (
    snapshot_id             UUID PRIMARY KEY,
    account_id              UUID NOT NULL,
    snapshot_date           DATE NOT NULL,
    cash_balance            NUMERIC(18,2) NOT NULL,
    holdings_value          NUMERIC(18,2) NOT NULL,
    total_value             NUMERIC(18,2) NOT NULL,
    extract_timestamp       TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT fk_fact_historical_snapshots_account
        FOREIGN KEY (account_id) REFERENCES dim_accounts (account_id) ON DELETE CASCADE,
    CONSTRAINT uq_fact_historical_snapshots
        UNIQUE (account_id, snapshot_date)
);

CREATE INDEX idx_fact_historical_snapshots_account ON fact_historical_snapshots (account_id);
CREATE INDEX idx_fact_historical_snapshots_snapshot_date ON fact_historical_snapshots (snapshot_date DESC);

-- ============================================================
-- SNAPSHOT TABLES (Current State)
-- ============================================================

-- SNAPSHOT_HOLDINGS: Current position holdings (refreshed daily)
-- Grain: one row per account-instrument
CREATE TABLE IF NOT EXISTS snapshot_holdings (
    holding_id              UUID PRIMARY KEY,
    account_id              UUID NOT NULL,
    instrument_id           UUID NOT NULL,
    quantity                NUMERIC(18,6) NOT NULL,
    average_cost_basis      NUMERIC(18,4) NOT NULL,
    extract_timestamp       TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT fk_snapshot_holdings_account
        FOREIGN KEY (account_id) REFERENCES dim_accounts (account_id) ON DELETE CASCADE,
    CONSTRAINT fk_snapshot_holdings_instrument
        FOREIGN KEY (instrument_id) REFERENCES dim_instruments (instrument_id) ON DELETE CASCADE,
    CONSTRAINT uq_snapshot_holdings
        UNIQUE (account_id, instrument_id)
);

CREATE INDEX idx_snapshot_holdings_account ON snapshot_holdings (account_id);
CREATE INDEX idx_snapshot_holdings_instrument ON snapshot_holdings (instrument_id);

-- ============================================================
-- METADATA TABLE
-- ============================================================

-- ETL_LOAD_LOG: Track ETL run history
CREATE TABLE IF NOT EXISTS etl_load_log (
    load_id                 SERIAL PRIMARY KEY,
    load_type               VARCHAR(20) NOT NULL,  -- 'extract', 'load', 'transform'
    load_mode               VARCHAR(20),           -- 'full', 'incremental'
    started_at              TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at            TIMESTAMP WITH TIME ZONE,
    status                  VARCHAR(20) NOT NULL,  -- 'STARTED', 'SUCCESS', 'FAILED'
    rows_affected           INTEGER,
    error_message           TEXT
);

CREATE INDEX idx_etl_load_log_started_at ON etl_load_log (started_at DESC);
CREATE INDEX idx_etl_load_log_status ON etl_load_log (status);
