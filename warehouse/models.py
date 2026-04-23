import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from warehouse.db import get_engine

def create_tables():
    engine = get_engine()
    with engine.connect() as conn:

        # ETF prices table with custom autovacuum tuning
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS etf_prices (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(20) NOT NULL,
                date DATE NOT NULL,
                open NUMERIC(18,6),
                high NUMERIC(18,6),
                low NUMERIC(18,6),
                close NUMERIC(18,6),
                volume BIGINT,
                adj_close NUMERIC(18,6),
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(ticker, date)
            );
        """))

        # Custom autovacuum settings - mirrors exactly what Sean built
        conn.execute(text("""
            ALTER TABLE etf_prices SET (
                autovacuum_vacuum_scale_factor = 0.01,
                autovacuum_analyze_scale_factor = 0.005,
                autovacuum_vacuum_cost_delay = 2
            );
        """))

        # Indexes for fast querying
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_etf_ticker 
            ON etf_prices(ticker);
        """))

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_etf_date 
            ON etf_prices(date);
        """))

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_etf_ticker_date 
            ON etf_prices(ticker, date DESC);
        """))

        # Technical indicators table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS technical_indicators (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(20) NOT NULL,
                date DATE NOT NULL,
                sma_20 NUMERIC(18,6),
                sma_50 NUMERIC(18,6),
                ema_20 NUMERIC(18,6),
                rsi_14 NUMERIC(10,4),
                volatility_30d NUMERIC(10,6),
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(ticker, date)
            );
        """))

        conn.execute(text("""
            ALTER TABLE technical_indicators SET (
                autovacuum_vacuum_scale_factor = 0.01,
                autovacuum_analyze_scale_factor = 0.005
            );
        """))

        # ETF metadata table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS etf_metadata (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(20) UNIQUE NOT NULL,
                name VARCHAR(255),
                category VARCHAR(100),
                aum_billions NUMERIC(10,2),
                expense_ratio NUMERIC(6,4),
                benchmark VARCHAR(255),
                inception_date DATE,
                extracted_by_llm BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """))

        # Pipeline runs log table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id SERIAL PRIMARY KEY,
                run_time TIMESTAMP DEFAULT NOW(),
                tickers_processed INTEGER,
                records_inserted INTEGER,
                records_failed INTEGER,
                schema_drift_detected BOOLEAN DEFAULT FALSE,
                status VARCHAR(50),
                notes TEXT
            );
        """))

        conn.commit()
        print("All tables created with autovacuum tuning and indexes.")

if __name__ == "__main__":
    create_tables()