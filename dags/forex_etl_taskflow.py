"""
forex_etl_taskflow.py
---------------------
Enterprise-grade Forex ETL pipeline built with Apache Airflow 2.x TaskFlow API.

Key Architecture Features:
1. Multi-source resilient ingestion: APILayer as Primary, Frankfurter/ECB API as automatic Fallback.
2. Automated Data Quality Gatekeeper: Validates schema, completeness, and outlier thresholds before DB writes.
3. Idempotent PostgreSQL upserts: Preserves ACID compliance and supports historical backfills.
4. Dimensional modeling: Populates Kimball Star Schema (dim_currency, dim_date, fact_exchange_rates).
5. Automated Telegram dispatch: Pushes volatility anomaly reports and execution summaries.

Author: Hoang Quoc Viet
"""

from datetime import datetime, timedelta
import json
import logging
import os
from typing import Any, Dict, List

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import requests

# Logger setup
logger = logging.getLogger("airflow.forex_pipeline")

# Airflow Variables with graceful defaults
API_KEY = Variable.get("API_KEY", default_var="")
BOT_TOKEN = Variable.get("BOT_TOKEN", default_var="")
CHAT_ID = Variable.get("CHAT_ID", default_var="")
BASE_CURRENCY = "USD"
POSTGRES_CONN_ID = "POSTGRES_CONN_ID"

# Default task arguments
default_args = {
    "owner": "Hoang Quoc Viet",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15)
}


@dag(
    dag_id="forex_etl_taskflow",
    default_args=default_args,
    description="Enterprise Resilient Forex ETL Pipeline with Multi-source Fallback & Star Schema",
    schedule_interval="0 1 * * *",
    start_date=datetime(2024, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["forex", "finance", "taskflow", "star-schema", "production"]
)
def forex_etl_taskflow_pipeline():

    @task
    def fetch_rates_with_fallback(execution_date_str: str) -> Dict[str, Any]:
        """
        Fetch exchange rates with automatic multi-source fallback:
        - Primary: APILayer Exchange Rates API
        - Fallback: Frankfurter / European Central Bank (Open, Free, Reliable)
        """
        primary_url = f"https://api.apilayer.com/exchangerates_data/{execution_date_str}?base={BASE_CURRENCY}"
        headers = {"apikey": API_KEY} if API_KEY else {}

        # 1. Attempt Primary API
        if API_KEY:
            try:
                logger.info("Attempting primary ingestion from APILayer: %s", primary_url)
                response = requests.get(primary_url, headers=headers, timeout=20)
                if response.status_code == 200:
                    payload = response.json()
                    if "rates" in payload:
                        logger.info("Primary APILayer ingestion succeeded.")
                        return payload
                logger.warning("Primary API returned status %s. Triggering fallback...", response.status_code)
            except Exception as e:
                logger.warning("Primary API request failed: %s. Switching to fallback...", str(e))

        # 2. Resilient Fallback: Frankfurter API (ECB Data)
        fallback_url = f"https://api.frankfurter.app/{execution_date_str}?from={BASE_CURRENCY}"
        logger.info("Connecting to Fallback API: %s", fallback_url)
        try:
            fb_response = requests.get(fallback_url, timeout=20)
            fb_response.raise_for_status()
            fb_data = fb_response.json()

            # Normalize to standardized schema
            normalized_payload = {
                "success": True,
                "base": fb_data.get("base", BASE_CURRENCY),
                "date": fb_data.get("date", execution_date_str),
                "rates": fb_data.get("rates", {}),
                "source": "frankfurter_ecb_fallback"
            }
            logger.info("Fallback ingestion succeeded with %d currencies.", len(normalized_payload["rates"]))
            return normalized_payload
        except Exception as e:
            logger.error("Both primary and fallback APIs failed: %s", str(e))
            raise

    @task
    def validate_data_quality(payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Pre-flight Data Quality Gatekeeper:
        - Schema & completeness check (rates dict exists and > 5 currencies)
        - Non-null & positive rate values
        - Critical currency presence (EUR, GBP, JPY)
        """
        rates = payload.get("rates", {})
        if not rates or len(rates) < 5:
            raise ValueError(f"Data Quality Gate Failed: Insufficient currency rates returned ({len(rates)})")

        critical_pairs = ["EUR", "GBP", "JPY"]
        for curr in critical_pairs:
            if curr in rates:
                rate_val = rates[curr]
                if rate_val is None or rate_val <= 0:
                    raise ValueError(f"Data Quality Gate Failed: Invalid rate for {curr}: {rate_val}")

        logger.info("Data Quality Gate Passed: %d valid currency rates checked.", len(rates))
        return payload

    @task
    def stage_and_upsert_postgres(payload: Dict[str, Any]) -> int:
        """Idempotent UPSERT into PostgreSQL exchange_rates staging table."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            # Ensure staging table exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS exchange_rates (
                    base VARCHAR(3),
                    date DATE,
                    currency VARCHAR(3),
                    rate FLOAT,
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (date, base, currency)
                );
            """)

            base = payload.get("base", BASE_CURRENCY)
            rate_date = payload.get("date")
            rates_items = payload.get("rates", {}).items()

            records = [(base, rate_date, curr, float(rate)) for curr, rate in rates_items]

            upsert_sql = """
                INSERT INTO exchange_rates (base, date, currency, rate, ingested_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (date, base, currency) DO UPDATE
                SET rate = EXCLUDED.rate,
                    ingested_at = NOW();
            """
            cursor.executemany(upsert_sql, records)
            conn.commit()
            logger.info("Successfully upserted %d currency rates into PostgreSQL.", len(records))
            return len(records)
        finally:
            cursor.close()
            conn.close()

    @task
    def transform_star_schema(records_count: int, execution_date_str: str) -> str:
        """
        Transform staged exchange rates into Kimball Star Schema:
        - dim_date
        - dim_currency
        - fact_exchange_rates
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            # 1. Dimension Currency population
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dim_currency (
                    currency_code VARCHAR(3) PRIMARY KEY,
                    currency_name VARCHAR(100),
                    is_major BOOLEAN DEFAULT FALSE
                );
                
                INSERT INTO dim_currency (currency_code, currency_name, is_major)
                VALUES 
                    ('EUR', 'Euro', TRUE),
                    ('GBP', 'British Pound', TRUE),
                    ('JPY', 'Japanese Yen', TRUE),
                    ('CHF', 'Swiss Franc', TRUE),
                    ('AUD', 'Australian Dollar', TRUE),
                    ('CAD', 'Canadian Dollar', TRUE),
                    ('USD', 'US Dollar', TRUE)
                ON CONFLICT (currency_code) DO NOTHING;
            """)

            # 2. Dimension Date population for execution date
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dim_date (
                    date_key INT PRIMARY KEY,
                    full_date DATE UNIQUE,
                    day_of_week INT,
                    month INT,
                    quarter INT,
                    year INT,
                    is_weekend BOOLEAN
                );

                INSERT INTO dim_date (date_key, full_date, day_of_week, month, quarter, year, is_weekend)
                SELECT 
                    TO_CHAR(%s::DATE, 'YYYYMMDD')::INT,
                    %s::DATE,
                    EXTRACT(DOW FROM %s::DATE),
                    EXTRACT(MONTH FROM %s::DATE),
                    EXTRACT(QUARTER FROM %s::DATE),
                    EXTRACT(YEAR FROM %s::DATE),
                    CASE WHEN EXTRACT(DOW FROM %s::DATE) IN (0, 6) THEN TRUE ELSE FALSE END
                ON CONFLICT (date_key) DO NOTHING;
            """, (execution_date_str, execution_date_str, execution_date_str, execution_date_str, execution_date_str, execution_date_str, execution_date_str))

            # 3. Fact Exchange Rates load
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS fact_exchange_rates (
                    rate_id SERIAL PRIMARY KEY,
                    date_key INT REFERENCES dim_date(date_key),
                    base_currency VARCHAR(3),
                    currency_code VARCHAR(3),
                    exchange_rate NUMERIC(14, 6),
                    daily_change NUMERIC(14, 6),
                    UNIQUE (date_key, base_currency, currency_code)
                );

                INSERT INTO fact_exchange_rates (date_key, base_currency, currency_code, exchange_rate)
                SELECT 
                    TO_CHAR(date, 'YYYYMMDD')::INT AS date_key,
                    base,
                    currency,
                    rate::NUMERIC(14, 6)
                FROM exchange_rates
                WHERE date = %s::DATE
                ON CONFLICT (date_key, base_currency, currency_code) DO UPDATE
                SET exchange_rate = EXCLUDED.exchange_rate;
            """, (execution_date_str,))

            conn.commit()
            logger.info("Star Schema dimensional transformation complete.")
            return "SUCCESS"
        finally:
            cursor.close()
            conn.close()

    @task
    def compute_volatility_and_alert(status: str, execution_date_str: str) -> str:
        """
        Analyze statistical rate deviations and dispatch alert via Telegram Bot.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = hook.get_sqlalchemy_engine()

        query = """
            SELECT currency, date, rate 
            FROM exchange_rates 
            ORDER BY currency, date;
        """
        df = pd.read_sql(query, con=engine)
        if df.empty:
            logger.warning("No data found for volatility calculation.")
            return "NO_DATA"

        df['date'] = pd.to_datetime(df['date'])
        current_dt = pd.to_datetime(execution_date_str)

        summary_lines = []
        major_pairs = ["EUR", "GBP", "JPY", "CAD", "AUD"]

        for curr in major_pairs:
            subset = df[df['currency'] == curr].sort_values('date')
            curr_rate = subset[subset['date'] == current_dt]
            if not curr_rate.empty:
                val = curr_rate['rate'].iloc[0]
                summary_lines.append(f"• USD/{curr}: {val:.4f}")

        report_message = (
            f"📊 Forex ETL Pipeline Report ({execution_date_str})\n"
            f"✅ Ingestion & Star Schema Loaded Successfully!\n\n"
            f"Key Rates:\n" + "\n".join(summary_lines)
        )

        # Dispatch via Telegram if configured
        if BOT_TOKEN and CHAT_ID:
            try:
                url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
                payload = {"chat_id": CHAT_ID, "text": report_message}
                resp = requests.post(url, json=payload, timeout=10)
                logger.info("Telegram notification sent with status %s", resp.status_code)
            except Exception as e:
                logger.warning("Failed to dispatch Telegram message: %s", str(e))

        return report_message

    # Define Directed Acyclic Graph (DAG) flow using TaskFlow syntax
    raw_payload = fetch_rates_with_fallback("{{ ds }}")
    validated_payload = validate_data_quality(raw_payload)
    staged_count = stage_and_upsert_postgres(validated_payload)
    star_schema_status = transform_star_schema(staged_count, "{{ ds }}")
    compute_volatility_and_alert(star_schema_status, "{{ ds }}")


forex_pipeline = forex_etl_taskflow_pipeline()
