import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import json
from sqlalchemy import text
from warehouse.db import get_engine
from datetime import datetime

# Baseline schema fingerprint we expect
BASELINE_FINGERPRINT = {
    "etf_prices": {
        "columns": ["id", "ticker", "date", "open", "high", "low", "close", "volume", "adj_close", "created_at"],
        "nullable": ["open", "high", "low", "close", "volume", "adj_close", "created_at"],
        "non_nullable": ["id", "ticker", "date"]
    },
    "technical_indicators": {
        "columns": ["id", "ticker", "date", "sma_20", "sma_50", "ema_20", "rsi_14", "volatility_30d", "created_at"],
        "nullable": ["sma_20", "sma_50", "ema_20", "rsi_14", "volatility_30d", "created_at"],
        "non_nullable": ["id", "ticker", "date"]
    }
}

def get_live_schema(table_name):
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position;
        """))
        rows = result.fetchall()
    return {
        "columns": [r[0] for r in rows],
        "nullable": [r[0] for r in rows if r[2] == 'YES'],
        "non_nullable": [r[0] for r in rows if r[2] == 'NO']
    }

def detect_schema_drift():
    drift_report = {
        'run_time': datetime.now().isoformat(),
        'drift_detected': False,
        'tables': {}
    }

    print(f"\n{'='*50}")
    print("SCHEMA DRIFT DETECTION REPORT")
    print(f"{'='*50}")

    for table_name, baseline in BASELINE_FINGERPRINT.items():
        live = get_live_schema(table_name)
        table_drift = []

        # Check for missing columns
        missing = set(baseline['columns']) - set(live['columns'])
        if missing:
            table_drift.append(f"MISSING columns: {missing}")

        # Check for new columns
        added = set(live['columns']) - set(baseline['columns'])
        if added:
            table_drift.append(f"NEW columns detected: {added}")

        # Check nullability drift
        nullable_drift = set(baseline['non_nullable']) & set(live['nullable'])
        if nullable_drift:
            table_drift.append(f"NULLABILITY changed: {nullable_drift}")

        drift_report['tables'][table_name] = {
            'drift': table_drift,
            'status': 'DRIFT DETECTED' if table_drift else 'OK'
        }

        if table_drift:
            drift_report['drift_detected'] = True
            print(f"\n❌ {table_name}: DRIFT DETECTED")
            for d in table_drift:
                print(f"   → {d}")
        else:
            print(f"\n✅ {table_name}: Schema OK")

    print(f"\nOverall Drift: {'⚠️  YES' if drift_report['drift_detected'] else '✅ NONE'}")
    return drift_report

if __name__ == "__main__":
    detect_schema_drift()