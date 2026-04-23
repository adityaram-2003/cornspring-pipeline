import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import pandas as pd
from sqlalchemy import text
from warehouse.db import get_engine

@pytest.fixture
def engine():
    return get_engine()

def test_connection(engine):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1;"))
        assert result.fetchone()[0] == 1

def test_etf_prices_table_exists(engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'etf_prices'
            );
        """))
        assert result.fetchone()[0] is True

def test_etf_prices_has_records(engine):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM etf_prices;"))
        count = result.fetchone()[0]
        assert count > 100000, f"Expected >100k records, got {count}"

def test_no_null_tickers(engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM etf_prices WHERE ticker IS NULL;
        """))
        assert result.fetchone()[0] == 0

def test_no_null_dates(engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM etf_prices WHERE date IS NULL;
        """))
        assert result.fetchone()[0] == 0

def test_no_negative_close(engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM etf_prices WHERE close < 0;
        """))
        assert result.fetchone()[0] == 0

def test_high_always_gte_low(engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM etf_prices 
            WHERE high < low;
        """))
        assert result.fetchone()[0] == 0

def test_spy_exists(engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM etf_prices WHERE ticker = 'SPY';
        """))
        assert result.fetchone()[0] > 0

def test_date_range_reasonable(engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT MIN(date), MAX(date) FROM etf_prices;
        """))
        min_date, max_date = result.fetchone()
        assert min_date.year >= 2020
        assert max_date.year >= 2024

def test_unique_ticker_date(engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM (
                SELECT ticker, date, COUNT(*) as cnt
                FROM etf_prices
                GROUP BY ticker, date
                HAVING COUNT(*) > 1
            ) dupes;
        """))
        assert result.fetchone()[0] == 0