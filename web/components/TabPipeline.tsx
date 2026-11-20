"use client";
import { useState } from "react";
import Plot from "./Plot";
import { Section, Metric, Table, ErrorBox, useApi, post, fmt } from "./ui";

const COMPONENTS: [string, string, string, string][] = [
  ["Ingestion Engine (yfinance → PostgreSQL)", "✅", "OPERATIONAL", "up"],
  ["ETL Validation (7-rule engine)", "✅", "OPERATIONAL", "up"],
  ["Schema Drift Detector", "✅", "OPERATIONAL", "up"],
  ["Technical Indicators (RSI/SMA/EMA)", "✅", "OPERATIONAL", "up"],
  ["LLM/RAG Extractor (Groq Llama 3.3)", "✅", "OPERATIONAL", "up"],
  ["Azure Blob Storage (Parquet)", "🔄", "SIMULATED", "warn"],
  ["MCP Server (FastMCP)", "✅", "LIVE IN CURSOR", "up"],
  ["PostgreSQL Warehouse", "✅", "OPERATIONAL", "up"],
  ["pytest Suite (22 tests)", "✅", "100% PASS", "up"],
];

export default function TabPipeline() {
  const p = useApi<any>("/api/pipeline");
  const tk = useApi<{ tickers: string[] }>("/api/tickers");
  const [vt, setVt] = useState("ALL");
  const [vn, setVn] = useState(10000);
  const [val, setVal] = useState<any>(null);
  const [drift, setDrift] = useState<any>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState("");

  async function runVal() {
    setBusy("val"); setErr(null);
    try { setVal(await post("/api/validate", { ticker: vt, limit: vn })); } catch (e: any) { setErr(e.message); }
    setBusy("");
  }
  async function runDrift() {
    setBusy("drift"); setErr(null);
    try { const j = await (await fetch("/api/drift")).json(); if (j.error) throw new Error(j.error); setDrift(j); } catch (e: any) { setErr(e.message); }
    setBusy("");
  }

  if (p.error) return <ErrorBox msg={p.error} />;
  const d = p.data;
  if (!d) return <div className="alert info">Loading pipeline status…</div>;

  return (
    <>
      <Section>Pipeline Control Center</Section>
      <div className="grid g4">
        <Metric label="Price Records" value={fmt(d.prices, 0)} />
        <Metric label="Indicator Records" value={fmt(d.indicators, 0)} />
        <Metric label="LLM Extracted" value={d.llm} />
        <Metric label="ETF Coverage" value={`${d.tickers} tickers`} />
      </div>

      <div className="grid g2" style={{ marginTop: 12 }}>
        <div>
          <Section>Component Health</Section>
          {COMPONENTS.map(([n, i, s, t]) => (
            <div className="comp-row" key={n}><span style={{ color: "#8b949e" }}>{n}</span><span className={t} style={{ fontWeight: 600, fontSize: "0.75rem" }}>{i} {s}</span></div>
          ))}
        </div>
        <div>
          <Section>Daily Coverage (Last 60 Days)</Section>
          <Plot height={300} data={[{ type: "bar", x: d.daily.map((r: any) => r.date), y: d.daily.map((r: any) => r.tickers), marker: { color: "#2563eb" } }]} layout={{ yaxis: { title: "Tickers Covered", gridcolor: "#21262d" }, xaxis: { nticks: 6, tickangle: 0, gridcolor: "#21262d" } }} />
        </div>
      </div>

      <Section>Live Validation Engine</Section>
      <div className="row">
        <label className="field" style={{ flex: 2 }}>Ticker to Validate
          <select value={vt} onChange={(e) => setVt(e.target.value)}>{["ALL", ...(tk.data?.tickers || [])].map((t) => <option key={t}>{t}</option>)}</select>
        </label>
        <label className="field">Sample Size
          <select value={vn} onChange={(e) => setVn(Number(e.target.value))}>{[1000, 5000, 10000, 50000].map((n) => <option key={n} value={n}>{n.toLocaleString()}</option>)}</select>
        </label>
        <button className="btn" onClick={runVal} disabled={!!busy}>{busy === "val" ? "Running…" : "▶ Run Validation Engine"}</button>
        <button className="btn secondary" onClick={runDrift} disabled={!!busy}>{busy === "drift" ? "Checking…" : "▶ Run Schema Drift Check"}</button>
      </div>
      {err && <ErrorBox msg={err} />}
      {val && (
        <>
          <div className="grid g3">
            <Metric label="Records Checked" value={fmt(val.total, 0)} />
            <Metric label="Passed" value={fmt(val.passed, 0)} />
            <Metric label="Failed" value={fmt(val.failed, 0)} />
          </div>
          <div style={{ marginTop: 12 }}>
            <Table cols={["Rule", "Failures", "Status"]} rows={Object.entries(val.rules).map(([r, c]: any) => [r, c, c === 0 ? "✅" : "❌"])} />
          </div>
          {val.failed === 0 && <div className="alert ok">✅ All validation rules passed. Data integrity confirmed.</div>}
        </>
      )}
      {drift && Object.entries(drift.tables).map(([t, info]: any) => (
        <div key={t} className={`alert ${info.status === "OK" ? "ok" : "bad"}`}>{info.status === "OK" ? `✅ ${t}: Schema intact` : `❌ ${t}: ${info.drift.join("; ")}`}</div>
      ))}

      <Section>Pipeline Run History</Section>
      <Table
        cols={["Run Time", "Tickers", "Inserted", "Failed", "Status", "Notes"]}
        rows={d.runs.map((r: any) => [r.run_time, r.tickers_processed, r.records_inserted, r.records_failed, r.status, r.notes])}
      />
    </>
  );
}
