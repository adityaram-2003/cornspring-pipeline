import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
from sqlalchemy import text
from warehouse.db import get_engine
from datetime import datetime

def compute_sma(series, window):
    return series.rolling(window=window).mean()

def compute_ema(series, window):
    return series.ewm(span=window, adjust=False).mean()

def compute_rsi(series, window=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=window).mean()
    avg_loss = loss.rolling(window=window).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def compute_volatility(series, window=30):
    log_returns = np.log(series / series.shift(1))
    return log_returns.rolling(window=window).std() * np.sqrt(252)

def compute_and_store_indicators(ticker=None):
    engine = get_engine()

    query = "SELECT ticker, date, close FROM etf_prices"
    if ticker:
        query += f" WHERE ticker = '{ticker}'"
    query += " ORDER BY ticker, date ASC"

    print("Loading price data...")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    if df.empty:
        print("No data found.")
        return

    tickers = df['ticker'].unique()
    print(f"Computing indicators for {len(tickers)} tickers...")

    total_inserted = 0

    for t in tickers:
        try:
            tdf = df[df['ticker'] == t].copy()
            tdf = tdf.sort_values('date').reset_index(drop=True)

            tdf['sma_20'] = compute_sma(tdf['close'], 20)
            tdf['sma_50'] = compute_sma(tdf['close'], 50)
            tdf['ema_20'] = compute_ema(tdf['close'], 20)
            tdf['rsi_14'] = compute_rsi(tdf['close'], 14)
            tdf['volatility_30d'] = compute_volatility(tdf['close'], 30)

            tdf = tdf.dropna(subset=['sma_20', 'rsi_14'])

            with engine.connect() as conn:
                for _, row in tdf.iterrows():
                    conn.execute(text("""
                        INSERT INTO technical_indicators 
                            (ticker, date, sma_20, sma_50, ema_20, rsi_14, volatility_30d)
                        VALUES 
                            (:ticker, :date, :sma_20, :sma_50, :ema_20, :rsi_14, :volatility_30d)
                        ON CONFLICT (ticker, date) DO UPDATE SET
                            sma_20 = EXCLUDED.sma_20,
                            sma_50 = EXCLUDED.sma_50,
                            ema_20 = EXCLUDED.ema_20,
                            rsi_14 = EXCLUDED.rsi_14,
                            volatility_30d = EXCLUDED.volatility_30d;
                    """), {
                        'ticker': t,
                        'date': row['date'],
                        'sma_20': float(row['sma_20']) if pd.notna(row['sma_20']) else None,
                        'sma_50': float(row['sma_50']) if pd.notna(row['sma_50']) else None,
                        'ema_20': float(row['ema_20']) if pd.notna(row['ema_20']) else None,
                        'rsi_14': float(row['rsi_14']) if pd.notna(row['rsi_14']) else None,
                        'volatility_30d': float(row['volatility_30d']) if pd.notna(row['volatility_30d']) else None,
                    })
                conn.commit()

            total_inserted += len(tdf)
            print(f"  ✅ {t}: {len(tdf)} indicator rows stored.")

        except Exception as e:
            print(f"  ❌ {t}: FAILED — {e}")

    print(f"\nDone. {total_inserted} indicator records stored.")

if __name__ == "__main__":
    compute_and_store_indicators()