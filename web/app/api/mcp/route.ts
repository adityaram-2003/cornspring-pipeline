import { NextRequest } from "next/server";
import { q } from "@/lib/db";
import { safe, cleanTicker } from "@/lib/respond";

export const dynamic = "force-dynamic";

const r4 = (v: any, d = 4) => (v === null || v === undefined ? null : Number(Number(v).toFixed(d)));

// Runs the same four read-only "MCP tool" queries as the Streamlit demo
export async function POST(req: NextRequest) {
  return safe(async () => {
    const body = await req.json().catch(() => ({}));
    const tool = String(body.tool || "");
    const t = cleanTicker(body.ticker);

    if (tool === "get_portfolio_summary") {
      const p = await q("SELECT ticker, to_char(date,'YYYY-MM-DD') AS date, close::float8 AS close FROM etf_prices WHERE ticker=$1 ORDER BY date DESC LIMIT 1", [t]);
      const i = await q("SELECT sma_20::float8 AS sma_20, rsi_14::float8 AS rsi_14 FROM technical_indicators WHERE ticker=$1 ORDER BY date DESC LIMIT 1", [t]);
      return { result: { ticker: t, latest_close: p[0]?.close ?? null, date: p[0]?.date ?? null, rsi_14: r4(i[0]?.rsi_14), sma_20: r4(i[0]?.sma_20) } };
    }
    if (tool === "run_data_validation") {
      const r = await q("SELECT COUNT(*)::int AS total, COUNT(*) FILTER (WHERE close<=0 OR high<low)::int AS bad FROM etf_prices WHERE ticker=$1", [t]);
      const { total, bad } = r[0] as any;
      return { result: { ticker: t, total, passed: total - bad, failed: bad, status: bad === 0 ? "PASS" : "FAIL" } };
    }
    if (tool === "get_technical_indicators") {
      const r = await q("SELECT to_char(date,'YYYY-MM-DD') AS date, rsi_14::float8 AS rsi_14 FROM technical_indicators WHERE ticker=$1 ORDER BY date DESC LIMIT 5", [t]);
      return { result: { ticker: t, latest_5_days: r.map((x: any) => ({ date: x.date, rsi_14: r4(x.rsi_14, 3) })) } };
    }
    if (tool === "get_pipeline_health") {
      const r = await q("SELECT (SELECT COUNT(*) FROM etf_prices)::int AS p, (SELECT COUNT(*) FROM technical_indicators)::int AS i, (SELECT COUNT(DISTINCT ticker) FROM etf_prices)::int AS t");
      return { result: { status: "HEALTHY", price_records: r[0].p, indicator_records: r[0].i, tickers: r[0].t } };
    }
    throw new Error("Unknown tool");
  });
}
