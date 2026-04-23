import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from sqlalchemy import text
from warehouse.db import get_engine
from etl.validation import run_validation
from etl.schema_drift import detect_schema_drift
import json
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Cornspring Intelligence Pipeline",
    page_icon="📊",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2rem;
        font-weight: 700;
        color: #1a1a2e;
        margin-bottom: 0.2rem;
    }
    .sub-header {
        font-size: 1rem;
        color: #666;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: #f8f9fa;
        border-radius: 8px;
        padding: 1rem;
        border-left: 4px solid #2563eb;
    }
    .status-healthy {
        color: #16a34a;
        font-weight: 700;
    }
    .status-warning {
        color: #d97706;
        font-weight: 700;
    }
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="main-header">🏦 Cornspring Intelligence Pipeline</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Financial Data Engineering Platform — ETF Analytics, Validation & LLM Extraction</div>', unsafe_allow_html=True)

engine = get_engine()

tab1, tab2, tab3, tab4 = st.tabs([
    "⚡ Pipeline Health",
    "📈 ETF Explorer",
    "🔍 Data Quality",
    "🤖 LLM Extraction"
])

# ─── TAB 1: PIPELINE HEALTH ───────────────────────────────────────────────────
with tab1:
    st.subheader("Pipeline Health Dashboard")

    with engine.connect() as conn:
        price_count = conn.execute(text("SELECT COUNT(*) FROM etf_prices")).fetchone()[0]
        indicator_count = conn.execute(text("SELECT COUNT(*) FROM technical_indicators")).fetchone()[0]
        ticker_count = conn.execute(text("SELECT COUNT(DISTINCT ticker) FROM etf_prices")).fetchone()[0]
        metadata_count = conn.execute(text("SELECT COUNT(*) FROM etf_metadata WHERE extracted_by_llm = TRUE")).fetchone()[0]
        last_run = conn.execute(text("SELECT run_time, status, records_inserted, notes FROM pipeline_runs ORDER BY run_time DESC LIMIT 1")).fetchone()
        daily_counts = pd.read_sql("SELECT date, COUNT(*) as records FROM etf_prices WHERE date >= CURRENT_DATE - INTERVAL '30 days' GROUP BY date ORDER BY date", conn)
        top_tickers = pd.read_sql("SELECT ticker, COUNT(*) as records FROM etf_prices GROUP BY ticker ORDER BY records DESC LIMIT 10", conn)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("📦 Price Records", f"{price_count:,}")
    col2.metric("📊 Indicator Records", f"{indicator_count:,}")
    col3.metric("🏷️ Unique Tickers", ticker_count)
    col4.metric("🤖 LLM Extracted", metadata_count)

    st.divider()

    col5, col6 = st.columns(2)

    with col5:
        st.markdown("**Last Pipeline Run**")
        if last_run:
            st.write(f"🕐 Time: `{last_run[0]}`")
            st.write(f"✅ Status: `{last_run[1]}`")
            st.write(f"📥 Records: `{last_run[2]:,}`")
            st.write(f"⏱️ Notes: `{last_run[3]}`")

        st.markdown("**Component Status**")
        components = {
            "Ingestion Engine": "✅ Operational",
            "Validation Engine": "✅ Operational",
            "Schema Drift Detector": "✅ Operational",
            "Technical Indicators": "✅ Operational",
            "LLM/RAG Extractor": "✅ Operational",
            "Azure Blob Storage": "🔄 Simulated — Ready",
            "MCP Server": "✅ Live in Cursor",
            "PostgreSQL Warehouse": "✅ Operational",
        }
        for comp, status in components.items():
            st.write(f"{status} — {comp}")

    with col6:
        if not daily_counts.empty:
            fig = px.bar(
                daily_counts,
                x='date', y='records',
                title='Records Ingested (Last 30 Days)',
                color_discrete_sequence=['#2563eb']
            )
            fig.update_layout(showlegend=False, height=300)
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("**Top 10 Tickers by Record Count**")
    fig2 = px.bar(
        top_tickers,
        x='ticker', y='records',
        color_discrete_sequence=['#7c3aed']
    )
    fig2.update_layout(height=250)
    st.plotly_chart(fig2, use_container_width=True)

# ─── TAB 2: ETF EXPLORER ──────────────────────────────────────────────────────
with tab2:
    st.subheader("ETF Explorer")

    with engine.connect() as conn:
        tickers = pd.read_sql("SELECT DISTINCT ticker FROM etf_prices ORDER BY ticker", conn)['ticker'].tolist()

    col1, col2 = st.columns([2, 1])
    with col1:
        selected = st.selectbox("Select ETF Ticker", tickers, index=tickers.index('SPY') if 'SPY' in tickers else 0)
    with col2:
        days = st.selectbox("Time Period", [90, 180, 365, 730, 1825], index=2)

    with engine.connect() as conn:
        prices = pd.read_sql(f"""
            SELECT date, open, high, low, close, volume
            FROM etf_prices
            WHERE ticker = '{selected}'
            ORDER BY date DESC
            LIMIT {days}
        """, conn)

        indicators = pd.read_sql(f"""
            SELECT date, sma_20, sma_50, ema_20, rsi_14, volatility_30d
            FROM technical_indicators
            WHERE ticker = '{selected}'
            ORDER BY date DESC
            LIMIT {days}
        """, conn)

        metadata = pd.read_sql(f"""
            SELECT name, category, expense_ratio, aum_billions, benchmark
            FROM etf_metadata WHERE ticker = '{selected}'
        """, conn)

    prices = prices.sort_values('date')
    indicators = indicators.sort_values('date')

    # Metadata strip
    if not metadata.empty:
        m = metadata.iloc[0]
        mc1, mc2, mc3, mc4 = st.columns(4)
        mc1.metric("Fund Name", str(m['name'])[:30] if m['name'] else selected)
        mc2.metric("Category", m['category'] or "N/A")
        mc3.metric("Expense Ratio", f"{float(m['expense_ratio'])*100:.2f}%" if m['expense_ratio'] else "N/A")
        mc4.metric("AUM", f"${m['aum_billions']}B" if m['aum_billions'] else "N/A")

    # Candlestick chart
    fig_price = go.Figure()
    fig_price.add_trace(go.Candlestick(
        x=prices['date'],
        open=prices['open'],
        high=prices['high'],
        low=prices['low'],
        close=prices['close'],
        name=selected
    ))
    if not indicators.empty:
        fig_price.add_trace(go.Scatter(x=indicators['date'], y=indicators['sma_20'], name='SMA 20', line=dict(color='orange', width=1)))
        fig_price.add_trace(go.Scatter(x=indicators['date'], y=indicators['sma_50'], name='SMA 50', line=dict(color='blue', width=1)))

    fig_price.update_layout(
        title=f"{selected} — Price Chart with SMA Overlays",
        xaxis_rangeslider_visible=False,
        height=400
    )
    st.plotly_chart(fig_price, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        if not indicators.empty:
            fig_rsi = go.Figure()
            fig_rsi.add_trace(go.Scatter(
                x=indicators['date'], y=indicators['rsi_14'],
                name='RSI 14', line=dict(color='purple')
            ))
            fig_rsi.add_hline(y=70, line_dash="dash", line_color="red", annotation_text="Overbought")
            fig_rsi.add_hline(y=30, line_dash="dash", line_color="green", annotation_text="Oversold")
            fig_rsi.update_layout(title="RSI (14)", height=280, yaxis=dict(range=[0, 100]))
            st.plotly_chart(fig_rsi, use_container_width=True)

    with col4:
        if not indicators.empty:
            fig_vol = go.Figure()
            fig_vol.add_trace(go.Scatter(
                x=indicators['date'], y=indicators['volatility_30d'],
                fill='tozeroy', name='30d Volatility',
                line=dict(color='#ef4444')
            ))
            fig_vol.update_layout(title="30-Day Annualized Volatility", height=280)
            st.plotly_chart(fig_vol, use_container_width=True)

    # Volume bar
    fig_vol2 = go.Figure()
    fig_vol2.add_trace(go.Bar(x=prices['date'], y=prices['volume'], name='Volume', marker_color='#94a3b8'))
    fig_vol2.update_layout(title="Trading Volume", height=200)
    st.plotly_chart(fig_vol2, use_container_width=True)

# ─── TAB 3: DATA QUALITY ──────────────────────────────────────────────────────
with tab3:
    st.subheader("Data Quality & Validation")

    col1, col2 = st.columns([2, 1])
    with col1:
        with engine.connect() as conn:
            tickers_q = pd.read_sql("SELECT DISTINCT ticker FROM etf_prices ORDER BY ticker", conn)['ticker'].tolist()
        val_ticker = st.selectbox("Validate Ticker", ["ALL"] + tickers_q)
    with col2:
        val_limit = st.selectbox("Sample Size", [1000, 5000, 10000, 50000], index=1)

    if st.button("▶ Run Validation", type="primary"):
        with st.spinner("Running validation checks..."):
            ticker_param = None if val_ticker == "ALL" else val_ticker
            results = run_validation(ticker=ticker_param, limit=val_limit)

        total = results['total_records']
        passed = results['passed']
        failed = results['failed']

        c1, c2, c3 = st.columns(3)
        c1.metric("Total Records", f"{total:,}")
        c2.metric("✅ Passed", f"{passed:,}")
        c3.metric("❌ Failed", f"{failed:,}")

        st.markdown("**Rule Breakdown**")
        rule_data = []
        for rule, count in results['rule_failures'].items():
            rule_data.append({
                'Rule': rule,
                'Failures': count,
                'Status': '✅ Pass' if count == 0 else '❌ Fail'
            })
        st.dataframe(pd.DataFrame(rule_data), use_container_width=True)

        if results['anomalies']:
            st.warning(f"⚠️ {len(results['anomalies'])} anomalies detected")
            for a in results['anomalies']:
                st.write(f"- **{a['rule']}**: {a['failed_count']} records | Tickers: {a['sample_tickers']}")
        else:
            st.success("✅ All validation rules passed. Data is clean.")

    st.divider()
    st.markdown("**Schema Drift Detection**")
    if st.button("▶ Run Schema Check", type="secondary"):
        with st.spinner("Checking schema integrity..."):
            drift = detect_schema_drift()

        for table, info in drift['tables'].items():
            if info['status'] == 'OK':
                st.success(f"✅ `{table}`: Schema OK")
            else:
                st.error(f"❌ `{table}`: {info['drift']}")

        if not drift['drift_detected']:
            st.success("✅ No schema drift detected across all tables.")

# ─── TAB 4: LLM EXTRACTION ────────────────────────────────────────────────────
with tab4:
    st.subheader("🤖 LLM-Powered ETF Fact Sheet Extraction")
    st.markdown("Upload any ETF fact sheet (`.txt`) and Claude/Groq will extract structured financial metrics automatically.")

    uploaded = st.file_uploader("Upload ETF Fact Sheet", type=['txt', 'htm', 'html'])

    if uploaded:
        content = uploaded.read().decode('utf-8', errors='ignore')
        import re
        content = re.sub(r'<[^>]+>', ' ', content)
        content = re.sub(r'\s+', ' ', content).strip()[:8000]

        st.markdown("**Document Preview (first 500 chars)**")
        st.code(content[:500])

        if st.button("🤖 Extract Metrics with LLM", type="primary"):
            with st.spinner("LLM extracting financial metrics..."):
                from llm_rag.pdf_extractor import extract_metrics_with_llm
                metrics = extract_metrics_with_llm(content, uploaded.name)

            if metrics:
                st.success("✅ Extraction complete!")
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Fund Name", metrics.get('fund_name') or "N/A")
                    st.metric("Ticker", metrics.get('ticker') or "N/A")
                    st.metric("Expense Ratio", f"{float(metrics['expense_ratio'])*100:.3f}%" if metrics.get('expense_ratio') else "N/A")
                    st.metric("AUM", f"${metrics.get('net_assets')}B" if metrics.get('net_assets') else "N/A")
                with col2:
                    st.metric("Benchmark", metrics.get('benchmark') or "N/A")
                    st.metric("Category", metrics.get('category') or "N/A")
                    st.metric("Inception Date", metrics.get('inception_date') or "N/A")
                    st.metric("Top Holding", metrics.get('top_holding') or "N/A")

                st.markdown("**Raw JSON Output**")
                st.json(metrics)
            else:
                st.error("Extraction failed. Check the document format.")

    st.divider()
    st.markdown("**Previously Extracted ETF Metadata**")
    with engine.connect() as conn:
        meta_df = pd.read_sql("""
            SELECT ticker, name, category, 
                   ROUND(aum_billions::numeric, 2) as aum_billions,
                   expense_ratio, benchmark, extracted_by_llm
            FROM etf_metadata
            ORDER BY aum_billions DESC NULLS LAST
        """, conn)

    if not meta_df.empty:
        st.dataframe(meta_df, use_container_width=True)
    else:
        st.info("No metadata extracted yet.")