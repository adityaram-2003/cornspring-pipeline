import { q } from "@/lib/db";
import { safe } from "@/lib/respond";

export const dynamic = "force-dynamic";

const WATCH = ["SPY", "QQQ", "AGG", "GLD", "VTI", "EFA", "HYG", "VNQ"];

// Live quote from Yahoo's public chart endpoint
async function live(t: string) {
  try {
    const res = await fetch(`https://query1.finance.yahoo.com/v8/finance/chart/${t}?range=5d&interval=1d`, {
      headers: { "User-Agent": "Mozilla/5.0" },
      signal: AbortSignal.timeout(4000),
    });
    if (!res.ok) return null;
    const j = await res.json();
    const closes: number[] = (j.chart.result[0].indicators.quote[0].close || []).filter((x: any) => x !== null);
    if (closes.length < 2) return null;
    const cur = closes[closes.length - 1], prev = closes[closes.length - 2];
    return { ticker: t, price: Math.round(cur * 100) / 100, chg: Math.round(((cur - prev) / prev) * 10000) / 100, live: true };
  } catch { return null; }
}

// Ticker strip: live quotes, falling back to the warehouse's last two closes
export async function GET() {
  return safe(async () => {
    const out = await Promise.all(WATCH.map(live));
    const missing = WATCH.filter((_, i) => !out[i]);
    let fallback: any[] = [];
    if (missing.length) {
      try {
        const rows = await q(
          `SELECT ticker, rn, close::float8 AS close FROM (
             SELECT ticker, close, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
             FROM etf_prices WHERE ticker = ANY($1) AND date >= (SELECT MAX(date) FROM etf_prices) - 14
           ) t WHERE rn <= 2`, [missing]);
        fallback = missing.map((t) => {
          const c: any = rows.find((r: any) => r.ticker === t && Number(r.rn) === 1);
          const p: any = rows.find((r: any) => r.ticker === t && Number(r.rn) === 2);
          return c && p ? { ticker: t, price: Math.round(c.close * 100) / 100, chg: Math.round(((c.close - p.close) / p.close) * 10000) / 100, live: false } : null;
        });
      } catch {}
    }
    const byT: Record<string, any> = {};
    [...out, ...fallback].filter(Boolean).forEach((x: any) => (byT[x.ticker] = x));
    return { quotes: WATCH.map((t) => byT[t]).filter(Boolean) };
  });
}
