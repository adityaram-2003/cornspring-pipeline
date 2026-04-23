import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yfinance as yf
import pandas as pd
from sqlalchemy import text
from warehouse.db import get_engine
from datetime import datetime

# 150 highly relevant ETFs - equity, fixed income, ESG, alternatives
# Exactly the asset classes Cornspring's Family Office clients hold
ETF_TICKERS = [
    # Broad Market
    "SPY", "IVV", "VOO", "QQQ", "DIA", "IWM", "VTI", "ITOT", "SCHB", "VXF",
    # Sector
    "XLF", "XLK", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU", "XLB", "XLRE",
    # International
    "EFA", "EEM", "VEA", "VWO", "IEFA", "ACWI", "VXUS", "IXUS", "EWJ", "EWZ",
    # Fixed Income
    "AGG", "BND", "TLT", "IEF", "SHY", "LQD", "HYG", "MUB", "VCIT", "VCSH",
    # ESG
    "ESGU", "ESGD", "ESGE", "SUSL", "SUSA", "DSI", "CRBN", "NUMG", "NULV", "NUSC",
    # Dividend
    "VIG", "VYM", "DVY", "SDY", "HDV", "DGRO", "SCHD", "FVD", "PEY", "DLN",
    # Real Estate
    "VNQ", "IYR", "SCHH", "REM", "MORT", "KBWY", "REZ", "BBRE", "INDS", "SRVR",
    # Commodities
    "GLD", "SLV", "IAU", "PDBC", "DJP", "GSG", "COMB", "BCI", "FTGC", "COMT",
    # Multi-Asset / Alternatives
    "AOR", "AOM", "AOA", "AOK", "VPGD", "VSMV", "BTAL", "TAIL", "HDGE", "VIXM",
    # Thematic
    "ARKK", "ARKG", "ARKW", "ARKF", "ARKQ", "BOTZ", "ROBO", "IRBO", "AIQ", "THNQ",
    # Factor
    "MTUM", "VLUE", "QUAL", "SIZE", "USMV", "SPLV", "SPHQ", "SPHD", "COWZ", "CALF",
    # Leveraged / Inverse (common in family office hedging)
    "SSO", "UPRO", "SDS", "SPXU", "QLD", "TQQQ", "PSQ", "SQQQ", "DDM", "DXD",
    # Bond Alternatives
    "TIPS", "VTIP", "STIP", "RINF", "FISR", "IVOL", "LQDH", "IGBH", "HYGH", "FLOT",
    # Small/Mid Cap
    "IJH", "IJR", "MDY", "SCHA", "VB", "VO", "IWO", "IWN", "IWP", "IWS",
    # Global Macro
    "URTH", "IOO", "ONEQ", "QMOM", "GMOM", "IMOM", "DVAL", "IVAL", "GVAL", "FYLD"
]

def fetch_and_store(start_date="2020-01-01"):
    engine = get_engine()
    total_inserted = 0
    total_failed = 0
    run_start = datetime.now()

    print(f"Starting ingestion for {len(ETF_TICKERS)} ETFs from {start_date}...")

    for i, ticker in enumerate(ETF_TICKERS):
        try:
            df = yf.download(ticker, start=start_date, progress=False, auto_adjust=True)

            if df.empty:
                print(f"[{i+1}/{len(ETF_TICKERS)}] {ticker}: No data found, skipping.")
                total_failed += 1
                continue

            # Flatten columns if multi-index
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            df = df.reset_index()
            df.columns = [c.lower().replace(' ', '_') for c in df.columns]
            df['ticker'] = ticker
            df['date'] = pd.to_datetime(df['date']).dt.date

            # Rename adj close if present
            if 'adj_close' not in df.columns and 'close' in df.columns:
                df['adj_close'] = df['close']

            df = df[['ticker', 'date', 'open', 'high', 'low', 'close', 'volume', 'adj_close']]
            df = df.dropna(subset=['close'])

            # Insert with conflict handling
            with engine.connect() as conn:
                for _, row in df.iterrows():
                    conn.execute(text("""
                        INSERT INTO etf_prices (ticker, date, open, high, low, close, volume, adj_close)
                        VALUES (:ticker, :date, :open, :high, :low, :close, :volume, :adj_close)
                        ON CONFLICT (ticker, date) DO NOTHING;
                    """), {
                        'ticker': row['ticker'],
                        'date': row['date'],
                        'open': float(row['open']) if pd.notna(row['open']) else None,
                        'high': float(row['high']) if pd.notna(row['high']) else None,
                        'low': float(row['low']) if pd.notna(row['low']) else None,
                        'close': float(row['close']) if pd.notna(row['close']) else None,
                        'volume': int(row['volume']) if pd.notna(row['volume']) else None,
                        'adj_close': float(row['adj_close']) if pd.notna(row['adj_close']) else None,
                    })
                conn.commit()

            total_inserted += len(df)
            print(f"[{i+1}/{len(ETF_TICKERS)}] {ticker}: {len(df)} records inserted.")

        except Exception as e:
            print(f"[{i+1}/{len(ETF_TICKERS)}] {ticker}: FAILED — {e}")
            total_failed += 1

    # Log the pipeline run
    duration = (datetime.now() - run_start).seconds
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO pipeline_runs (tickers_processed, records_inserted, records_failed, status, notes)
            VALUES (:tickers, :inserted, :failed, :status, :notes)
        """), {
            'tickers': len(ETF_TICKERS),
            'inserted': total_inserted,
            'failed': total_failed,
            'status': 'success' if total_failed < 10 else 'partial',
            'notes': f"Completed in {duration}s"
        })
        conn.commit()

    print(f"\nIngestion complete. {total_inserted} records inserted, {total_failed} failed.")
    print(f"Duration: {duration}s")

if __name__ == "__main__":
    fetch_and_store()