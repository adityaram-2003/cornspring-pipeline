"use client";
import Plot from "./Plot";
import { Section, Metric, Table, ErrorBox, useApi, fmt } from "./ui";

const AUM = 250_000_000;
const FX: Record<string, number> = { USD: 1.0, GBP: 0.786, EUR: 0.921, JPY: 149.5 };
const COLORS = ["#2563eb", "#10b981", "#f59e0b", "#8b949e", "#ef4444"];

export default function TabOverview() {
  const { data, error, loading } = useApi<any>("/api/overview");
  if (error) return <ErrorBox msg={error} />;
  if (loading || !data) return <div className="alert info">Loading portfolio…</div>;

  const h: any[] = data.holdings;
  const pnlPct = h.reduce((s, x) => s + x.dailyChg * x.weight, 0);
  const pnl = (AUM * pnlPct) / 100;

  // Asset class totals for the donut
  const byClass: Record<string, number> = {};
  h.forEach((x) => (byClass[x.assetClass] = (byClass[x.assetClass] || 0) + AUM * x.weight));

  return (
    <>
      <Section>Family Office Portfolio Snapshot — Model Portfolio</Section>
      <div className="grid g4">
        <Metric label="Total AUM" value="$250.0M" delta={`${pnlPct >= 0 ? "▲" : "▼"} ${fmt(Math.abs(pnlPct), 3)}% today`} tone={pnlPct >= 0 ? "up" : "down"} />
        <Metric label="Daily P&L" value={`${pnl >= 0 ? "+" : ""}${fmt(pnl / 1e6)}M`} delta="USD" />
        <Metric label="Positions" value={h.length} delta="ETF holdings" />
        <Metric label="Coverage" value={`${data.tickerCount} ETFs`} delta="in warehouse" />
      </div>

      <div className="grid g65" style={{ marginTop: 12 }}>
        <Plot
          height={320}
          data={[{ type: "pie", labels: Object.keys(byClass), values: Object.values(byClass), hole: 0.6, marker: { colors: COLORS } }]}
          layout={{ title: "Asset Allocation", annotations: [{ text: "$250M<br>AUM", x: 0.5, y: 0.5, showarrow: false, font: { size: 14, color: "#e6edf3" } }] }}
        />
        <Plot
          height={320}
          data={[{
            type: "bar", x: h.map((x) => x.ticker), y: h.map((x) => x.weight * 100),
            marker: { color: h.map((x) => (x.dailyChg >= 0 ? "#10b981" : "#ef4444")) },
            text: h.map((x) => `${(x.weight * 100).toFixed(1)}%`), textposition: "outside",
          }]}
          layout={{ title: "Portfolio Weights & Daily P&L", yaxis: { title: "Weight (%)", gridcolor: "#21262d" }, showlegend: false }}
        />
      </div>

      <Section>Holdings Detail</Section>
      <Table
        cols={["Ticker", "Name", "Asset Class", "Weight", "Price", "Daily Change", "Value"]}
        rows={h.map((x) => [
          x.ticker, x.name, x.assetClass, `${(x.weight * 100).toFixed(1)}%`, `$${fmt(x.price)}`,
          <span key="c" className={x.dailyChg >= 0 ? "up" : "down"}>{x.dailyChg >= 0 ? "▲" : "▼"} {fmt(Math.abs(x.dailyChg))}%</span>,
          `$${((AUM * x.weight) / 1e6).toFixed(1)}M`,
        ])}
      />

      <Section>Multi-Currency Portfolio View</Section>
      <div className="grid g4">
        {Object.entries(FX).map(([ccy, rate]) => <Metric key={ccy} label={`AUM (${ccy})`} value={`${ccy} ${((AUM * rate) / 1e6).toFixed(1)}M`} />)}
      </div>
    </>
  );
}
