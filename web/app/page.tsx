"use client";
import { useState } from "react";
import { useApi, fmt } from "@/components/ui";
import TabOverview from "@/components/TabOverview";
import TabMarket from "@/components/TabMarket";
import TabReplication from "@/components/TabReplication";
import TabAI from "@/components/TabAI";
import TabPipeline from "@/components/TabPipeline";
import TabDocs from "@/components/TabDocs";

const TABS = [
  ["🏠  Portfolio Overview", TabOverview],
  ["📊  Market Intelligence", TabMarket],
  ["🔄  Index Replication", TabReplication],
  ["🤖  AI Data Intelligence", TabAI],
  ["🔍  Pipeline Control", TabPipeline],
  ["📄  Document Extraction", TabDocs],
] as const;

export default function Page() {
  const [active, setActive] = useState(0);
  const strip = useApi<any>("/api/strip");
  const ov = useApi<any>("/api/overview");
  const Active = TABS[active][1];

  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="cs-logo">
          <div className="cs-logo-title">🏦 Cornspring</div>
          <div className="cs-logo-sub">Intelligence Terminal v2.0</div>
        </div>
        <div className="cs-section">System Status</div>
        <span className="badge badge-green">● PIPELINE HEALTHY</span><br />
        <span className="badge badge-blue">● MCP SERVER LIVE</span><br />
        <span className="badge badge-blue">● LLM/RAG ACTIVE</span>

        <div className="cs-section">Warehouse Stats</div>
        <div style={{ fontSize: "0.8rem", color: "#8b949e", lineHeight: 2 }}>
          📦 Price Records: <b style={{ color: "#e6edf3" }}>{ov.data ? fmt(ov.data.priceCount, 0) : "…"}</b><br />
          🏷️ ETF Coverage: <b style={{ color: "#e6edf3" }}>{ov.data ? `${ov.data.tickerCount} tickers` : "…"}</b><br />
          🕐 Last Ingest: <b style={{ color: "#e6edf3" }}>{ov.data?.lastRun?.run_time || "N/A"}</b><br />
          ✅ Status: <b className="up">{ov.data?.lastRun?.status?.toUpperCase() || "N/A"}</b>
        </div>

        <div className="cs-section">Built With</div>
        <div style={{ fontSize: "0.75rem", color: "#8b949e", lineHeight: 2.2 }}>
          🐘 PostgreSQL (Neon) + autovacuum tuning<br />
          ☁️ Azure Blob Storage (Parquet)<br />
          🤖 Groq LLM (Llama 3.3 70B)<br />
          🔌 MCP Server (FastMCP)<br />
          ▲ Next.js on Vercel<br />
          📊 235K+ records, 22 pytest tests
        </div>
      </aside>

      <main className="main">
        <div style={{ marginBottom: 16 }}>
          <span style={{ fontSize: "1.6rem", fontWeight: 700 }}>Family Office Intelligence Platform</span>
          <span style={{ marginLeft: 12, fontSize: "0.8rem", color: "#8b949e" }}>Real-time AI-driven data intelligence for Family Offices &amp; Asset Owners</span>
        </div>

        <div className="strip">
          {(strip.data?.quotes || []).map((q: any) => (
            <span className="strip-item" key={q.ticker}>
              <span className="strip-sym">{q.ticker}</span>
              <span className="strip-price">${q.price}</span>
              <span className={`strip-chg ${q.chg >= 0 ? "up" : "down"}`}>{q.chg >= 0 ? "▲" : "▼"} {Math.abs(q.chg).toFixed(2)}%</span>
            </span>
          ))}
          {strip.loading && <span style={{ color: "#8b949e", fontSize: "0.75rem" }}>Loading quotes…</span>}
        </div>

        <div className="tabs">
          {TABS.map(([label], i) => (
            <button key={label} className={`tab ${i === active ? "active" : ""}`} onClick={() => setActive(i)}>{label}</button>
          ))}
        </div>
        <Active />
      </main>
    </div>
  );
}
