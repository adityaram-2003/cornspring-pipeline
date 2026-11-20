"use client";
import { useState } from "react";
import Plot from "./Plot";
import { Card, Section, Metric, Table, ErrorBox, fmt } from "./ui";

export default function TabReplication() {
  const [benchmark, setBenchmark] = useState("SPY");
  const [replicant, setReplicant] = useState("IVV");
  const [res, setRes] = useState<any>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function run() {
    setBusy(true); setErr(null);
    try {
      const j = await (await fetch(`/api/replication?benchmark=${benchmark}&replicant=${replicant}`)).json();
      if (j.error) throw new Error(j.error);
      setRes(j);
    } catch (e: any) { setErr(e.message); setRes(null); }
    setBusy(false);
  }

  return (
    <>
      <Section>Index Replication Engine — Tracking Error Analysis</Section>
      <Card title="About This Engine">
        This engine replicates the index replication methodology described by Cornspring&apos;s Data Engineering team on public
        profiles — calculating multi-currency hedged returns and benchmark tracking error. Cross-currency hedging algorithms
        target tracking error below <strong>0.01%</strong> against official benchmarks.
      </Card>
      <div className="row">
        <label className="field">Benchmark ETF (Index Proxy)
          <select value={benchmark} onChange={(e) => setBenchmark(e.target.value)}>{["SPY", "QQQ", "AGG", "EFA", "GLD"].map((t) => <option key={t}>{t}</option>)}</select>
        </label>
        <label className="field">Replication Portfolio ETF
          <select value={replicant} onChange={(e) => setReplicant(e.target.value)}>{["IVV", "VOO", "VTI", "ITOT", "SCHB"].map((t) => <option key={t}>{t}</option>)}</select>
        </label>
        <button className="btn" onClick={run} disabled={busy}>{busy ? "Computing…" : "▶ Run Replication Analysis"}</button>
      </div>
      {err && <ErrorBox msg={err} />}
      {res && (
        <>
          <div className="grid g4">
            <Metric label="Annualized Tracking Error" value={`${fmt(res.trackingError, 4)}%`} delta={res.trackingError < 0.5 ? "Excellent" : "Review"} tone={res.trackingError < 0.5 ? "up" : "warn"} />
            <Metric label="Return Spread (Annual)" value={`${fmt(res.meanDiff, 4)}%`} />
            <Metric label="Max Daily Deviation" value={`${fmt(res.maxDiff, 4)}%`} />
            <Metric label="Correlation" value={fmt(res.correlation, 6)} />
          </div>
          <Plot
            height={500}
            data={[
              { type: "scatter", mode: "lines", x: res.dates, y: res.bCum, name: `${res.benchmark} (Benchmark)`, line: { color: "#2563eb", width: 2 } },
              { type: "scatter", mode: "lines", x: res.dates, y: res.rCum, name: `${res.replicant} (Replicant)`, line: { color: "#10b981", width: 2, dash: "dot" } },
              { type: "bar", x: res.dates, y: res.diff, yaxis: "y2", name: "Daily Tracking Diff (%)", marker: { color: res.diff.map((v: number) => (v < 0 ? "#ef4444" : "#10b981")) } },
            ]}
            layout={{ title: `${res.replicant} vs ${res.benchmark} — Tracking Error Analysis`, yaxis: { domain: [0.37, 1], gridcolor: "#21262d" }, yaxis2: { domain: [0, 0.32], gridcolor: "#21262d" }, xaxis: { gridcolor: "#21262d" }, legend: { orientation: "h", y: 1.08 } }}
          />
          <Section>Cross-Currency Hedged Returns Simulation</Section>
          <Table
            cols={["Currency", "Base Return (USD)", "Hedged Return", "Unhedged Return", "FX Rate", "Hedge Benefit (bps)"]}
            rows={res.fx.map((f: any) => [f.ccy, `${fmt(f.base, 3)}%`, `${fmt(f.hedged, 3)}%`, `${fmt(f.unhedged, 3)}%`, f.rate, fmt(f.benefitBps, 1)])}
          />
        </>
      )}
    </>
  );
}
