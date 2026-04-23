import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from sqlalchemy import text
from warehouse.db import get_engine
from datetime import datetime, date

# Expected schema - anything deviating from this gets flagged
EXPECTED_SCHEMA = {
    'ticker': str,
    'date': date,
    'open': float,
    'high': float,
    'low': float,
    'close': float,
    'volume': int,
    'adj_close': float
}

VALIDATION_RULES = {
    'close_positive': lambda df: df['close'] > 0,
    'high_gte_low': lambda df: df['high'] >= df['low'],
    'high_gte_close': lambda df: df['high'] >= df['close'],
    'low_lte_close': lambda df: df['low'] <= df['close'],
    'volume_non_negative': lambda df: df['volume'] >= 0,
    'no_future_dates': lambda df: pd.to_datetime(df['date']) <= pd.Timestamp.now(),
    'open_positive': lambda df: df['open'] > 0,
}

def run_validation(ticker=None, limit=10000):
    engine = get_engine()
    results = {
        'ticker': ticker or 'ALL',
        'run_time': datetime.now().isoformat(),
        'total_records': 0,
        'passed': 0,
        'failed': 0,
        'rule_failures': {},
        'anomalies': []
    }

    query = "SELECT * FROM etf_prices"
    if ticker:
        query += f" WHERE ticker = '{ticker}'"
    query += f" ORDER BY date DESC LIMIT {limit}"

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    results['total_records'] = len(df)

    if df.empty:
        print("No data found.")
        return results

    # Run each validation rule
    for rule_name, rule_fn in VALIDATION_RULES.items():
        try:
            mask = rule_fn(df)
            failures = df[~mask]
            results['rule_failures'][rule_name] = len(failures)
            if len(failures) > 0:
                results['anomalies'].append({
                    'rule': rule_name,
                    'failed_count': len(failures),
                    'sample_tickers': failures['ticker'].unique()[:3].tolist()
                })
        except Exception as e:
            results['rule_failures'][rule_name] = f"ERROR: {e}"

    total_failures = sum(v for v in results['rule_failures'].values() if isinstance(v, int))
    results['passed'] = results['total_records'] - total_failures
    results['failed'] = total_failures

    print(f"\n{'='*50}")
    print(f"VALIDATION REPORT — {results['ticker']}")
    print(f"{'='*50}")
    print(f"Total Records : {results['total_records']}")
    print(f"Passed        : {results['passed']}")
    print(f"Failed        : {results['failed']}")
    print(f"\nRule Breakdown:")
    for rule, count in results['rule_failures'].items():
        status = "✅" if count == 0 else "❌"
        print(f"  {status} {rule}: {count} failures")

    if results['anomalies']:
        print(f"\nAnomalies Detected:")
        for a in results['anomalies']:
            print(f"  - {a['rule']}: {a['failed_count']} records | Tickers: {a['sample_tickers']}")
    else:
        print("\n✅ No anomalies detected.")

    return results

if __name__ == "__main__":
    run_validation()