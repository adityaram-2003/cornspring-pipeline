import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import pandas as pd
from etl.validation import run_validation, VALIDATION_RULES

def test_validation_runs_without_error():
    results = run_validation(ticker='SPY', limit=500)
    assert results is not None

def test_validation_returns_correct_keys():
    results = run_validation(ticker='QQQ', limit=500)
    assert 'total_records' in results
    assert 'passed' in results
    assert 'failed' in results
    assert 'rule_failures' in results

def test_all_rules_pass_for_spy():
    results = run_validation(ticker='SPY', limit=1000)
    for rule, count in results['rule_failures'].items():
        assert count == 0, f"Rule {rule} failed with {count} failures"

def test_validation_rules_cover_all_expected():
    expected_rules = [
        'close_positive',
        'high_gte_low',
        'high_gte_close',
        'low_lte_close',
        'volume_non_negative',
        'no_future_dates',
        'open_positive'
    ]
    for rule in expected_rules:
        assert rule in VALIDATION_RULES

def test_validation_total_matches_limit():
    results = run_validation(ticker='IVV', limit=100)
    assert results['total_records'] <= 100