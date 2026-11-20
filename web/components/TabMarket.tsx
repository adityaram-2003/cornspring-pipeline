"use client";
import { useState } from "react";
import Plot from "./Plot";
import { Section, Metric, ErrorBox, useApi, fmt } from "./ui";

const PERIODS: Record<string, number> = { "90D": 90, "180D": 180, "1Y": 365, "2Y": 730, "5Y": 1825 };

export default function TabMarket() {
  const tk = useApi<{ tickers: string[] }>("/api/tickers");
  const [ticker, setTicker] = useState("SPY");
  const [period, setPeriod] = useState("1Y");
  const [type, setType] = useState("Candlestick");
  const [comp, setComp] = useState<string[]>(["SPY", "AGG", "GLD", "EFA", "HYG"]);

  const m = useApi<any>(`/api/market?ticker=${ticker}&days=${PERIODS[period]}`);
  const c = useApi<any>(comp.length >= 2 ? `/api/correlation?tickers=${comp.join(",")}` : null);

  if (tk.error) return <ErrorBox msg={tk.error} />;
  const tickers = tk.data?.tickers || [];

  const d = m.data;
  const px = d?.prices || [];
  const ind = d?.indicators || [];
  const x = px.map((r: any) => r.date);
  const traces: any[] = [];
  if (px.length) {
    if (type === "Candlestick")
      traces.push({ type: "candlestick", x, open: px.map((r: any) => r.open), high: px.map((r: any) => r.high), low: px.map((r: any) => r.low), close: px.map((r: any) => r.close), name: ticker, increasing: { line: { color: "#10b981" } }, decreasing: { line: { color: "#ef4444" } } });
    else if (type === "OHLC")
      traces.push({ type: "ohlc", x, open: px.map((r: any) => r.open), high: px.map((r: any) => r.high), low: px.map((r: any) => r.low), close: px.map((r: any) => r.close), name: ticker });
    else traces.push({ type: "scatter", mode: "lines", x, y: px.map((r: any) => r.close), name: ticker, line: { color: "#2563eb", width: 2 } });

    if (ind.length) {
      const ix = ind.map((r: any) => r.date);
      traces.push({ type: "scatter", mode: "lines", x: ix, y: ind.map((r: any) => r.sma_20), name: "SMA 20", line: { color: "#f59e0b", width: 1.2 } });
      traces.push({ type: "scatter", mode: "lines", x: ix, y: ind.map((r: any) => r.sma_50), name: "SMA 50", line: { color: "#8b5cf6", width: 1.2 } });
      traces.push({ type: "scatter", mode: "lines", x: ix, y: ind.map((r: any) => r.rsi_14), name: "RSI 14", yaxis: "y2", line: { color: "#2563eb", width: 1.5 } });
      traces.push({ type: "scatter", mode: "lines", x: ix, y: ind.map((r: any) => r.volatility_30d), name: "Volatility", yaxis: "y3", fill: "tozeroy", line: { color: "#f59e0b", width: 1 }, fillcolor: "rgba(245,158,11,0.1)" });
    }
    traces.push({ type: "bar", x, y: px.map((r: any) => r.volume), name: "Volume", yaxis: "y4", marker: { color: "#30363d" }, opacity: 0.6 });
  }
  const mainLayout = {
    xaxis: { rangeslider: { visible: false }, gridcolor: "#21262d", anchor: "y3" },
    yaxis: { domain: [0.42, 1], gridcolor: "#21262d" },
    yaxis2: { domain: [0.22, 0.38], gridcolor: "#21262d", range: [0, 100], title: "RSI" },
    yaxis3: { domain: [0, 0.18], gridcolor: "#21262d", title: "Vol" },
    yaxis4: { overlaying: "y3", side: "right", showgrid: false, showticklabels: false },
    legend: { orientation: "h", y: 1.06, font: { size: 10 } },
    shapes: [70, 30].map((v) => ({ type: "line", xref: "paper", x0: 0, x1: 1, yref: "y2", y0: v, y1: v, line: { dash: "dash", color: v === 70 ? "#ef4444" : "#10b981", width: 1 } })),
  };

  const meta = d?.meta;
  const cd = c.data;

  return (
    <>
      <div className="row">
        <label className="field" style={{ flex: 2 }}>Select ETF
          <select value={ticker} onChange={(e) => setTicker(e.target.value)}>
            {(tickers.length ? tickers : [ticker]).map((t) => <option key={t}>{t}</option>)}
          </select>
        </label>
        <label className="field">Period
          <select value={period} onChange={(e) => setPeriod(e.target.value)}>{Object.keys(PERIODS).map((p) => <option key={p}>{p}</option>)}</select>
        </label>
        <label className="field">Chart Type
          <select value={type} onChange={(e) => setType(e.target.value)}>{["Candlestick", "Line", "OHLC"].map((p) => <option key={p}>{p}</option>)}</select>
        </label>
      </div>

      {m.error && <ErrorBox msg={m.error} />}
      {meta && (
        <div className="grid g5" style={{ marginBottom: 12 }}>
          <Metric label="Fund" value={String(meta.name || ticker).slice(0, 20)} />
          <Metric label="Category" value={meta.category || "N/A"} />
          <Metric label="Expense Ratio" value={meta.expense_ratio ? `${fmt(meta.expense_ratio * 100)}%` : "N/A"} />
          <Metric label="AUM" value={meta.aum_billions ? `$${meta.aum_billions}B` : "N/A"} />
          <Metric label="Benchmark" value={String(meta.benchmark || "N/A").slice(0, 20)} />
        </div>
      )}
      {m.loading && !d ? <div className="alert info">Loading {ticker}…</div> : px.length ? <Plot data={traces} layout={mainLayout} height={620} /> : null}

      <Section>Cross-Asset Correlation Matrix</Section>
      <label className="field" style={{ marginBottom: 12 }}>Select tickers to compare (Ctrl/Cmd-click for several)
        <select multiple value={comp} onChange={(e) => setComp(Array.from(e.target.selectedOptions).map((o) => o.value))}>
          {tickers.map((t) => <option key={t}>{t}</option>)}
        </select>
      </label>
      {c.error && <ErrorBox msg={c.error} />}
      {cd?.tickers?.length ? (
        <Plot
          height={340}
          data={[{ type: "heatmap", z: cd.matrix, x: cd.tickers, y: cd.tickers, zmin: -1, zmax: 1, colorscale: [[0, "#ef4444"], [0.5, "#21262d"], [1, "#10b981"]], text: cd.matrix.map((r: number[]) => r.map((v) => v.toFixed(2))), texttemplate: "%{text}", textfont: { size: 11, color: "white" } }]}
          layout={{ title: "Return Correlations (1Y)" }}
        />
      ) : comp.length < 2 ? <div className="alert info">Pick at least two tickers.</div> : null}
    </>
  );
}
