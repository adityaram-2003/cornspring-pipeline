import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import re
import json
from dotenv import load_dotenv
from sqlalchemy import text
from warehouse.db import get_engine
from groq import Groq

load_dotenv()

client = Groq(api_key=os.getenv("GROQ_API_KEY"))

def read_filing(filepath):
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        raw = f.read()

    # Remove scripts and styles first
    raw = re.sub(r'<script[^>]*>.*?</script>', ' ', raw, flags=re.DOTALL)
    raw = re.sub(r'<style[^>]*>.*?</style>', ' ', raw, flags=re.DOTALL)

    # Strip remaining HTML tags
    clean = re.sub(r'<[^>]+>', ' ', raw)

    # Normalize whitespace
    clean = re.sub(r'\s+', ' ', clean).strip()

    # Find the most financially dense section
    # Look for keywords and grab surrounding context
    keywords = ['expense ratio', 'net assets', 'benchmark', 'inception', 'annual fund']
    best_start = 0
    best_score = 0

    words = clean.split()
    chunk_size = 600

    for i in range(0, len(words) - chunk_size, 100):
        chunk = ' '.join(words[i:i+chunk_size]).lower()
        score = sum(chunk.count(kw) for kw in keywords)
        if score > best_score:
            best_score = score
            best_start = i

    best_chunk = ' '.join(words[best_start:best_start+chunk_size])

    # Also grab the first 500 words (usually has fund name)
    header = ' '.join(words[:500])

    return header + " ... " + best_chunk

def extract_metrics_with_llm(text_content, filename):
    prompt = f"""You are a financial data extraction specialist working with SEC 497K fund filings.

Extract these fields from the document. Return ONLY valid JSON, nothing else.

{{
  "fund_name": "full name of the fund or null",
  "ticker": "ticker symbol or null",
  "expense_ratio": numeric decimal like 0.0085 or null,
  "net_assets": numeric billions like 1.2 or null,
  "benchmark": "benchmark index name or null",
  "inception_date": "YYYY-MM-DD or null",
  "category": "fund category or null",
  "top_holding": "largest holding name or null"
}}

DOCUMENT:
{text_content}"""

    try:
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )
        raw_response = response.choices[0].message.content.strip()
        raw_response = re.sub(r'```json|```', '', raw_response).strip()
        metrics = json.loads(raw_response)
        metrics['source_file'] = os.path.basename(filename)
        return metrics
    except Exception as e:
        print(f"    LLM error: {e}")
        return None

def store_metrics(metrics):
    if not metrics:
        return

    # Build a safe ticker - never store None
    ticker = metrics.get('ticker')
    fund_name = metrics.get('fund_name')

    if ticker and ticker != 'null':
        safe_ticker = str(ticker)[:20]
    elif fund_name and fund_name != 'null':
        # Use first word of fund name as fallback ticker
        safe_ticker = str(fund_name).split()[0][:20]
    else:
        safe_ticker = os.path.splitext(metrics.get('source_file', 'UNKNOWN'))[0][:20]

    engine = get_engine()
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO etf_metadata
                    (ticker, name, category, aum_billions, expense_ratio, benchmark, extracted_by_llm)
                VALUES
                    (:ticker, :name, :category, :aum, :expense_ratio, :benchmark, TRUE)
                ON CONFLICT (ticker) DO UPDATE SET
                    name = EXCLUDED.name,
                    category = EXCLUDED.category,
                    aum_billions = EXCLUDED.aum_billions,
                    expense_ratio = EXCLUDED.expense_ratio,
                    benchmark = EXCLUDED.benchmark,
                    extracted_by_llm = TRUE;
            """), {
                'ticker': safe_ticker,
                'name': str(fund_name or '')[:255],
                'category': str(metrics.get('category') or '')[:100],
                'aum': float(metrics['net_assets']) if metrics.get('net_assets') and metrics['net_assets'] != 'null' else None,
                'expense_ratio': float(metrics['expense_ratio']) if metrics.get('expense_ratio') and metrics['expense_ratio'] != 'null' else None,
                'benchmark': str(metrics.get('benchmark') or '')[:255],
            })
            conn.commit()
            print(f"    💾 Stored: {safe_ticker}")
    except Exception as e:
        print(f"    DB store error: {e}")

def run_extraction():
    pdf_dir = "data/raw/pdfs"
    files = [f for f in os.listdir(pdf_dir) if f.endswith('.txt')]
    print(f"Found {len(files)} filings to process\n")

    results = []
    for i, filename in enumerate(files):
        filepath = os.path.join(pdf_dir, filename)
        print(f"[{i+1}/{len(files)}] Processing: {filename[:50]}")

        text_content = read_filing(filepath)
        metrics = extract_metrics_with_llm(text_content, filename)

        if metrics:
            print(f"    ✅ Fund: {str(metrics.get('fund_name') or 'N/A')[:50]}")
            print(f"       Ticker: {metrics.get('ticker')} | Expense: {metrics.get('expense_ratio')} | AUM: {metrics.get('net_assets')}B")
            store_metrics(metrics)
            results.append(metrics)
        else:
            print(f"    ❌ Extraction failed")

    print(f"\nDone. Extracted metrics from {len(results)}/{len(files)} filings.")
    return results

if __name__ == "__main__":
    run_extraction()