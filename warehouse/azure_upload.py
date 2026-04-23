import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import pandas as pd
from sqlalchemy import text
from warehouse.db import get_engine
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

AZURE_AVAILABLE = bool(os.getenv("AZURE_STORAGE_CONNECTION_STRING"))

if AZURE_AVAILABLE:
    from azure.storage.blob import BlobServiceClient

def get_blob_client():
    if not AZURE_AVAILABLE:
        return None
    return BlobServiceClient.from_connection_string(
        os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    )

def export_to_parquet(ticker="SPY"):
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(
            f"SELECT * FROM etf_prices WHERE ticker = '{ticker}' ORDER BY date DESC",
            conn
        )
    os.makedirs("data/processed", exist_ok=True)
    path = f"data/processed/{ticker}_prices.parquet"
    df.to_parquet(path, index=False)
    print(f"  Exported {len(df)} rows to {path}")
    return path

def upload_to_azure(local_path, blob_name):
    if not AZURE_AVAILABLE:
        print(f"  [SIMULATED] Azure upload: {blob_name}")
        print(f"  [SIMULATED] Container: cornspring-datalake/raw/{blob_name}")
        print(f"  [SIMULATED] Status: Success — {os.path.getsize(local_path)} bytes")
        return True

    try:
        client = get_blob_client()
        container = "cornspring-datalake"
        try:
            client.create_container(container)
        except:
            pass

        with open(local_path, 'rb') as f:
            client.get_blob_client(
                container=container,
                blob=f"raw/{blob_name}"
            ).upload_blob(f, overwrite=True)

        print(f"  ✅ Uploaded to Azure Blob: raw/{blob_name}")
        return True
    except Exception as e:
        print(f"  ❌ Azure upload failed: {e}")
        return False

def run_azure_pipeline():
    tickers = ["SPY", "QQQ", "AGG", "GLD", "VTI"]
    print(f"{'='*50}")
    print("AZURE BLOB STORAGE PIPELINE")
    mode = "LIVE" if AZURE_AVAILABLE else "SIMULATED"
    print(f"Mode: {mode}")
    print(f"{'='*50}\n")

    results = []
    for ticker in tickers:
        print(f"Processing {ticker}...")
        path = export_to_parquet(ticker)
        blob_name = f"{ticker}_prices_{datetime.now().strftime('%Y%m%d')}.parquet"
        success = upload_to_azure(path, blob_name)
        results.append({
            'ticker': ticker,
            'blob': blob_name,
            'status': 'success' if success else 'failed'
        })

    print(f"\n{'='*50}")
    print("PIPELINE SUMMARY")
    print(f"{'='*50}")
    for r in results:
        icon = "✅" if r['status'] == 'success' else "❌"
        print(f"  {icon} {r['ticker']} → {r['blob']}")

    print(f"\nArchitecture: PostgreSQL → Parquet → Azure Blob Storage")
    print(f"Container: cornspring-datalake/raw/")
    print(f"Format: Parquet (columnar, optimized for analytics)")

if __name__ == "__main__":
    run_azure_pipeline()