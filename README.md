cat > README.md << 'README'
# 🏦 Cornspring Intelligence Pipeline

A production-grade financial data engineering platform built to mirror the core infrastructure of AI-driven Family Office analytics. Built in 72 hours as a technical deep-dive ahead of interviewing with Cornspring.

---

## 🏗️ Architecture

    yfinance API + SEC EDGAR Filings
              ↓
       [Azure Blob Storage Layer]
       Parquet files, columnar format
              ↓
       [Python ETL Engine]
       Schema drift detection
       Validation & reconciliation
       pytest unit test suite
              ↓
       [PostgreSQL Warehouse]
       Custom autovacuum/autoanalyze tuning
       Composite indexes for fast querying
       235,000+ ETF price records
       232,000+ technical indicator records
              ↓
       [LLM/RAG Module]
       Groq (Llama 3.3 70B)
       Extracts AUM, expense ratio, benchmark from ETF fact sheets
              ↓
       [MCP Server]
       5 tools exposed to any AI agent
       Live in Cursor Agent
              ↓
       [Streamlit Dashboard]
       Pipeline health, ETF explorer, validation, LLM extraction

---

## 📦 Dataset

- **150 ETFs** across equity, fixed income, ESG, commodities, alternatives, thematic, and factor categories
- **5 years** of daily OHLCV price data (2020-2025) via yfinance
- **235,195 price records** and **232,328 technical indicator records** in PostgreSQL
- **ETF fact sheets** processed via LLM/RAG for structured metadata extraction

---

## 🔧 Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python, yfinance, pandas |
| Warehouse | PostgreSQL 15 (custom autovacuum tuning, composite indexes) |
| ETL | Schema drift detection, 7-rule validation engine |
| Testing | pytest (22 tests, 100% pass rate) |
| Indicators | RSI, SMA-20, SMA-50, EMA-20, 30d Volatility |
| LLM/RAG | Groq API (Llama 3.3 70B), structured JSON extraction |
| Cloud | Azure Blob Storage (Parquet, columnar format) |
| Agent Layer | MCP Server (FastMCP) — 5 tools, live in Cursor |
| Dashboard | Streamlit, Plotly |

---

## ⚡ MCP Server Tools

The pipeline exposes an MCP server that any AI agent (Cursor, Claude, etc.) can call directly:

| Tool | Description |
|---|---|
| `get_portfolio_summary(ticker)` | Latest price, RSI, SMA, AUM, benchmark for any ETF |
| `run_data_validation(ticker)` | Run 7-rule validation check on price data |
| `get_technical_indicators(ticker, days)` | RSI, SMA, volatility over N days |
| `trigger_ingestion(ticker)` | Fresh data pull and load for any ticker |
| `get_pipeline_health()` | Full system health: record counts, components, last run |

---

## 🚀 Quick Start

    git clone https://github.com/adityaram-2003/cornspring-pipeline.git
    cd cornspring-pipeline
    pip install -r requirements.txt
    cp .env.example .env
    # Add your API keys to .env

    psql postgres -c "CREATE DATABASE cornspring_pipeline;"
    psql postgres -c "CREATE USER pipeline_user WITH PASSWORD 'cornspring123';"
    psql postgres -c "GRANT ALL PRIVILEGES ON DATABASE cornspring_pipeline TO pipeline_user;"
    psql postgres -c "GRANT ALL ON SCHEMA public TO pipeline_user;"

    python3 ingestion/fetch_etf_prices.py
    python3 etl/indicators.py
    python3 -m pytest tests/ -v
    streamlit run dashboard/app.py
    python3 mcp_server/server.py

---

## 📊 Dashboard

Four tabs:
- **Pipeline Health** — live stats, component status, ingestion history
- **ETF Explorer** — candlestick charts, RSI, volatility, volume for any of 149 tickers
- **Data Quality** — 7-rule validation engine + schema drift detection
- **LLM Extraction** — upload any ETF fact sheet, extract structured metrics instantly

---

## 🧪 Test Suite

    python3 -m pytest tests/ -v
    # 22 tests, 0 failures, 0.73s

Tests cover: database connectivity, table existence, record counts, data integrity rules,
validation logic, autovacuum settings, RSI range validation, pipeline run logging.

---

## 🤖 Why MCP?

The MCP server transforms this pipeline from a static data store into a **queryable financial intelligence layer** that any AI agent can interact with in natural language. This directly mirrors the vision of AI-driven portfolio intelligence for Family Offices — where portfolio managers query their own data through conversational interfaces rather than writing SQL.

---

## 👤 Author

**Adityaram Komaraneni**  
M.S. Data Science, Columbia University  
[linkedin.com/in/adityaramk](https://linkedin.com/in/adityaramk) | [adityaramk.com](https://adityaramk.com)
README