-- ==============================================================================
-- Kimball Dimensional Modeling: Forex Financial Data Warehouse
-- Dialect: PostgreSQL 13+
-- Author: Hoang Quoc Viet
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. Date Dimension (Conformed Dimension)
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,               -- Format: YYYYMMDD
    full_date DATE NOT NULL UNIQUE,
    day_name VARCHAR(10) NOT NULL,
    day_of_week INT NOT NULL,               -- 1 (Mon) to 7 (Sun)
    day_of_month INT NOT NULL,
    day_of_year INT NOT NULL,
    week_of_year INT NOT NULL,
    month_number INT NOT NULL,
    month_name VARCHAR(15) NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON dim_date(year, month_number);

-- ------------------------------------------------------------------------------
-- 2. Currency Dimension
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_currency (
    currency_code VARCHAR(3) PRIMARY KEY,
    currency_name VARCHAR(100) NOT NULL,
    country_or_zone VARCHAR(100),
    is_g10_major BOOLEAN DEFAULT FALSE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Seed initial master currency dimension
INSERT INTO dim_currency (currency_code, currency_name, country_or_zone, is_g10_major)
VALUES
    ('USD', 'United States Dollar', 'United States', TRUE),
    ('EUR', 'Euro', 'European Union', TRUE),
    ('GBP', 'British Pound Sterling', 'United Kingdom', TRUE),
    ('JPY', 'Japanese Yen', 'Japan', TRUE),
    ('CHF', 'Swiss Franc', 'Switzerland', TRUE),
    ('CAD', 'Canadian Dollar', 'Canada', TRUE),
    ('AUD', 'Australian Dollar', 'Australia', TRUE),
    ('NZD', 'New Zealand Dollar', 'New Zealand', TRUE),
    ('SEK', 'Swedish Krona', 'Sweden', TRUE),
    ('NOK', 'Norwegian Krone', 'Norway', TRUE),
    ('SGD', 'Singapore Dollar', 'Singapore', FALSE),
    ('VND', 'Vietnamese Dong', 'Vietnam', FALSE)
ON CONFLICT (currency_code) DO NOTHING;

-- ------------------------------------------------------------------------------
-- 3. Fact Table: Daily Exchange Rates
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_exchange_rates (
    rate_id BIGSERIAL PRIMARY KEY,
    date_key INT NOT NULL REFERENCES dim_date(date_key),
    base_currency_code VARCHAR(3) NOT NULL REFERENCES dim_currency(currency_code),
    target_currency_code VARCHAR(3) NOT NULL REFERENCES dim_currency(currency_code),
    exchange_rate NUMERIC(16, 6) NOT NULL,
    previous_day_rate NUMERIC(16, 6),
    daily_spread NUMERIC(16, 6),
    daily_percentage_change NUMERIC(8, 4),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fact_exchange_rates UNIQUE (date_key, base_currency_code, target_currency_code)
);

CREATE INDEX IF NOT EXISTS idx_fact_rates_date ON fact_exchange_rates(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_rates_pair ON fact_exchange_rates(base_currency_code, target_currency_code);

-- ------------------------------------------------------------------------------
-- 4. Fact Table: Currency Market Volatility & Technical Indicators
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_currency_volatility (
    volatility_id BIGSERIAL PRIMARY KEY,
    date_key INT NOT NULL REFERENCES dim_date(date_key),
    currency_code VARCHAR(3) NOT NULL REFERENCES dim_currency(currency_code),
    rolling_7d_volatility NUMERIC(10, 6),
    rolling_30d_volatility NUMERIC(10, 6),
    rolling_30d_max_rate NUMERIC(16, 6),
    rolling_30d_min_rate NUMERIC(16, 6),
    bollinger_upper_band NUMERIC(16, 6),
    bollinger_lower_band NUMERIC(16, 6),
    is_volatility_spike BOOLEAN DEFAULT FALSE,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fact_volatility UNIQUE (date_key, currency_code)
);

-- ------------------------------------------------------------------------------
-- 5. Analytical View: Major Currency Daily Heatmap & Momentum
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_currency_momentum AS
SELECT 
    d.full_date,
    d.year,
    d.month_name,
    c.currency_code,
    c.currency_name,
    f.exchange_rate,
    f.daily_percentage_change,
    v.rolling_7d_volatility,
    v.rolling_30d_volatility,
    v.is_volatility_spike
FROM fact_exchange_rates f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_currency c ON f.target_currency_code = c.currency_code
LEFT JOIN fact_currency_volatility v ON f.date_key = v.date_key AND f.target_currency_code = v.currency_code
WHERE c.is_g10_major = TRUE;
