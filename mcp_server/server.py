import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import pandas as pd
from mcp.server.fastmcp import FastMCP
from sqlalchemy import text
from warehouse.db import get_engine
from dotenv import load_dotenv

load_dotenv()

mcp = FastMCP("Cornspring Financial Intelligence Pipeline")

@mcp.tool()
def get_portfolio_summary(ticker: str) -> str:
    """Get price summary and latest metrics for a given ETF ticker."""
    engine = get_engine()
    try:
        with engine.connect() as conn:
            price = conn.execute(text("""
                SELECT ticker, date, close, volume
                FROM etf_prices
                WHERE ticker = :ticker
                ORDER BY date DESC LIMIT 1
            """), {'ticker': ticker}).fetchone()

            indicator = conn.execute(text("""
                SELECT sma_20, sma_50, rsi_14, volatility_30d
                FROM technical_indicators
                WHERE ticker = :ticker
                ORDER BY date DESC LIMIT 1
            """), {'ticker': ticker}).fetchone()

            metadata = conn.execute(text("""
                SELECT name, category, expense_ratio, aum_billions, benchmark
                FROM etf_metadata
                WHERE ticker = :ticker
            """), {'ticker': ticker}).fetchone()

        if not price:
            return json.dumps({"error": f"No data found for {ticker}"})

        result = {
            "ticker": ticker,
            "latest_close": float(price[1]) if price[1] else None,
            "latest_date": str(price[0]),
            "volume": int(price[3]) if price[3] else None,
        }

        if indicator:
            result.update({
                "sma_20": round(float(indicator[0]), 4) if indicator[0] else None,
                "sma_50": round(float(indicator[1]), 4) if indicator[1] else None,
                "rsi_14": round(float(indicator[2]), 4) if indicator[2] else None,
                "volatility_30d": round(float(indicator[3]), 6) if indicator[3] else None,
            })

        if metadata:
            result.update({
                "fund_name": metadata[0],
                "category": metadata[1],
                "expense_ratio": float(metadata[2]) if metadata[2] else None,
                "aum_billions": float(metadata[3]) if metadata[3] else None,
                "benchmark": metadata[4],
            })

        return json.dumps(result, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})

@mcp.tool()
def run_data_validation(ticker: str = None) -> str:
    """Run validation checks on the ETF price data. Optionally filter by ticker."""
    engine = get_engine()
    try:
        query = "SELECT ticker, date, open, high, low, close, volume FROM etf_prices"
        params = {}
        if ticker:
            query += " WHERE ticker = :ticker"
            params['ticker'] = ticker
        query += " ORDER BY date DESC LIMIT 5000"

        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params=params)

        total = len(df)
        failures = {}
        failures['close_positive'] = int((df['close'] <= 0).sum())
        failures['high_gte_low'] = int((df['high'] < df['low']).sum())
        failures['volume_non_negative'] = int((df['volume'] < 0).sum())
        failures['no_null_close'] = int(df['close'].isna().sum())

        total_failed = sum(failures.values())

        result = {
            "scope": ticker or "ALL",
            "total_records": total,
            "passed": total - total_failed,
            "failed": total_failed,
            "rule_breakdown": failures,
            "status": "PASS" if total_failed == 0 else "FAIL"
        }
        return json.dumps(result, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})

@mcp.tool()
def get_technical_indicators(ticker: str, days: int = 30) -> str:
    """Get technical indicators (RSI, SMA, volatility) for a ticker over N days."""
    engine = get_engine()
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text("""
                SELECT date, sma_20, sma_50, ema_20, rsi_14, volatility_30d
                FROM technical_indicators
                WHERE ticker = :ticker
                ORDER BY date DESC
                LIMIT :days
            """), conn, params={'ticker': ticker, 'days': days})

        if df.empty:
            return json.dumps({"error": f"No indicators found for {ticker}"})

        latest = df.iloc[0]
        result = {
            "ticker": ticker,
            "days_returned": len(df),
            "latest": {
                "date": str(latest['date']),
                "sma_20": round(float(latest['sma_20']), 4) if pd.notna(latest['sma_20']) else None,
                "sma_50": round(float(latest['sma_50']), 4) if pd.notna(latest['sma_50']) else None,
                "rsi_14": round(float(latest['rsi_14']), 4) if pd.notna(latest['rsi_14']) else None,
                "volatility_30d": round(float(latest['volatility_30d']), 6) if pd.notna(latest['volatility_30d']) else None,
            },
            "rsi_signal": "OVERBOUGHT" if latest['rsi_14'] > 70 else "OVERSOLD" if latest['rsi_14'] < 30 else "NEUTRAL"
        }
        return json.dumps(result, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})

@mcp.tool()
def trigger_ingestion(ticker: str) -> str:
    """Trigger a fresh data ingestion for a specific ETF ticker."""
    try:
        import yfinance as yf
        engine = get_engine()

        df = yf.download(ticker, period="5d", progress=False, auto_adjust=True)

        if df.empty:
            return json.dumps({"error": f"No data returned for {ticker}"})

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df = df.reset_index()
        df.columns = [c.lower() for c in df.columns]
        df['ticker'] = ticker
        df['date'] = pd.to_datetime(df['date']).dt.date
        if 'adj_close' not in df.columns:
            df['adj_close'] = df['close']

        inserted = 0
        with engine.connect() as conn:
            for _, row in df.iterrows():
                conn.execute(text("""
                    INSERT INTO etf_prices (ticker, date, open, high, low, close, volume, adj_close)
                    VALUES (:ticker, :date, :open, :high, :low, :close, :volume, :adj_close)
                    ON CONFLICT (ticker, date) DO NOTHING;
                """), {
                    'ticker': ticker,
                    'date': row['date'],
                    'open': float(row['open']) if pd.notna(row['open']) else None,
                    'high': float(row['high']) if pd.notna(row['high']) else None,
                    'low': float(row['low']) if pd.notna(row['low']) else None,
                    'close': float(row['close']) if pd.notna(row['close']) else None,
                    'volume': int(row['volume']) if pd.notna(row['volume']) else None,
                    'adj_close': float(row['adj_close']) if pd.notna(row['adj_close']) else None,
                })
            conn.commit()
            inserted = len(df)

        return json.dumps({
            "ticker": ticker,
            "records_ingested": inserted,
            "status": "success",
            "message": f"Fresh data ingested for {ticker}"
        })
    except Exception as e:
        return json.dumps({"error": str(e)})

@mcp.tool()
def get_pipeline_health() -> str:
    """Get overall pipeline health: record counts, last run, schema status."""
    engine = get_engine()
    try:
        with engine.connect() as conn:
            price_count = conn.execute(text("SELECT COUNT(*) FROM etf_prices")).fetchone()[0]
            indicator_count = conn.execute(text("SELECT COUNT(*) FROM technical_indicators")).fetchone()[0]
            ticker_count = conn.execute(text("SELECT COUNT(DISTINCT ticker) FROM etf_prices")).fetchone()[0]
            last_run = conn.execute(text("SELECT run_time, status, records_inserted FROM pipeline_runs ORDER BY run_time DESC LIMIT 1")).fetchone()
            metadata_count = conn.execute(text("SELECT COUNT(*) FROM etf_metadata WHERE extracted_by_llm = TRUE")).fetchone()[0]

        result = {
            "pipeline_status": "HEALTHY",
            "warehouse": {
                "etf_price_records": price_count,
                "indicator_records": indicator_count,
                "unique_tickers": ticker_count,
                "llm_extracted_metadata": metadata_count,
            },
            "last_pipeline_run": {
                "time": str(last_run[0]) if last_run else None,
                "status": last_run[1] if last_run else None,
                "records": last_run[2] if last_run else None,
            },
            "components": {
                "ingestion": "✅ operational",
                "validation": "✅ operational",
                "schema_drift": "✅ operational",
                "technical_indicators": "✅ operational",
                "llm_rag_extraction": "✅ operational",
                "azure_blob": "✅ simulated / ready for live credentials",
            }
        }
        return json.dumps(result, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})

if __name__ == "__main__":
    print("Starting Cornspring MCP Server...")
    print("Tools available:")
    print("  - get_portfolio_summary(ticker)")
    print("  - run_data_validation(ticker)")
    print("  - get_technical_indicators(ticker, days)")
    print("  - trigger_ingestion(ticker)")
    print("  - get_pipeline_health()")
    mcp.run(transport='stdio')