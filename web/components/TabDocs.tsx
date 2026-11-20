"use client";
import { useState } from "react";
import Plot from "./Plot";
import { Card, Section, Metric, Table, ErrorBox, useApi, post, fmt } from "./ui";

export default function TabDocs() {
  const list = useApi<any>("/api/metadata");
  const [file, setFile] = useState<{ name: string; content: string } | null>(null);
  const [out, setOut] = useState<any>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function onFile(f: File | undefined) {
    setOut(null); setErr(null);
    if (!f) return setFile(null);
    setFile({ name: f.name, content: await f.text() });
  }
  async function extract() {
    if (!file) return;
    setBusy(true); setErr(null);
    try { setOut(await post("/api/extract", file)); } catch (e: any) { setErr(e.message); }
    setBusy(false);
  }

  const m = out?.metrics;
  const rows: any[] = list.data?.rows || [];
  const top = rows.filter((r) => r.aum !== null).slice(0, 10);

  return (
    <>
      <Section>LLM/RAG Document Extraction Pipeline</Section>
      <Card title="Pipeline Description">
        Upload any ETF fact sheet or fund document. The LLM/RAG engine (Groq Llama 3.3 70B) extracts structured financial
        metrics — fund name, ticker, AUM, expense ratio, benchmark, inception date, category, and top holding. On this hosted
        version the result is shown here only; the batch pipeline writes to the PostgreSQL <code>etf_metadata</code> table.
      </Card>
      <label className="field" style={{ marginBottom: 12 }}>Upload ETF Fact Sheet (.txt, .htm, .html)
        <input type="file" accept=".txt,.htm,.html" onChange={(e) => onFile(e.target.files?.[0])} />
      </label>
      {file && (
        <>
          <div className="alert info">📄 {file.name} · {file.content.length.toLocaleString()} chars · {file.content.split(/\s+/).filter(Boolean).length.toLocaleString()} words</div>
          <button className="btn" onClick={extract} disabled={busy}>{busy ? "Extracting…" : "🤖 Extract Metrics"}</button>
        </>
      )}
      {err && <div style={{ marginTop: 12 }}><ErrorBox msg={err} /></div>}
      {m && (
        <div style={{ marginTop: 12 }}>
          <div className="alert ok">✅ Extraction successful</div>
          <div className="grid g4">
            <Metric label="Fund Name" value={String(m.fund_name || "N/A").slice(0, 20)} />
            <Metric label="Ticker" value={m.ticker || "N/A"} />
            <Metric label="Expense Ratio" value={m.expense_ratio ? `${fmt(Number(m.expense_ratio) * 100, 3)}%` : "N/A"} />
            <Metric label="AUM" value={m.net_assets ? `$${m.net_assets}B` : "N/A"} />
            <Metric label="Benchmark" value={String(m.benchmark || "N/A").slice(0, 20)} />
            <Metric label="Category" value={String(m.category || "N/A").slice(0, 20)} />
            <Metric label="Inception" value={m.inception_date || "N/A"} />
            <Metric label="Top Holding" value={String(m.top_holding || "N/A").slice(0, 20)} />
          </div>
          <div className="cs-section">Raw Extracted JSON</div>
          <pre>{JSON.stringify(m, null, 2)}</pre>
        </div>
      )}

      <Section>Extracted ETF Metadata — PostgreSQL Warehouse</Section>
      {list.error && <ErrorBox msg={list.error} />}
      {rows.length > 0 && (
        <>
          <Table cols={["Ticker", "Name", "Category", "AUM ($B)", "Expense (%)", "Benchmark", "LLM"]}
            rows={rows.map((r) => [r.ticker, r.name, r.category, r.aum ?? "", r.expense ?? "", r.benchmark, r.llm_extracted ? "✅" : ""])} />
          {top.length > 0 && (
            <Plot height={300}
              data={[{ type: "bar", x: top.map((r) => r.ticker), y: top.map((r) => r.aum), marker: { color: "#2563eb" }, text: top.map((r) => `$${r.aum}B`), textposition: "outside" }]}
              layout={{ title: "AUM by ETF (LLM Extracted from Fact Sheets)", yaxis: { title: "AUM ($B)", gridcolor: "#21262d" } }} />
          )}
        </>
      )}
      {!list.loading && !list.error && rows.length === 0 && <div className="alert info">No metadata extracted yet.</div>}
    </>
  );
}
