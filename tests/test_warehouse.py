import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from sqlalchemy import text
from warehouse.db import get_engine

@pytest.fixture
def engine():
    return get_engine()

def test_technical_indicators_table_exists(engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'technical_indicators'
            );
        """))
        assert result.fetchone()[0] is True

def test_indicators_has_records(engine):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM technical_indicators;"))
        count = result.fetchone()[0]
        assert count > 100000

def test_rsi_in_valid_range(engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM technical_indicators
            WHERE rsi_14 < 0 OR rsi_14 > 100;
        """))
        assert result.fetchone()[0] == 0

def test_sma_20_exists_for_spy(engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM technical_indicators
            WHERE ticker = 'SPY' AND sma_20 IS NOT NULL;
        """))
        assert result.fetchone()[0] > 0

def test_volatility_non_negative(engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM technical_indicators
            WHERE volatility_30d < 0;
        """))
        assert result.fetchone()[0] == 0

def test_pipeline_runs_logged(engine):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM pipeline_runs;"))
        assert result.fetchone()[0] > 0

def test_autovacuum_settings_applied(engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT reloptions FROM pg_class
            WHERE relname = 'etf_prices';
        """))
        options = result.fetchone()[0]
        assert options is not None
        assert any('autovacuum' in opt for opt in options)