import { q, num } from "@/lib/db";
import { safe } from "@/lib/respond";

export const dynamic = "force-dynamic";

// Same model portfolio as the Streamlit version
const PORTFOLIO: Record<string, { weight: number; name: string; cls: string }> = {
  SPY: { weight: 0.22, name: "US Large Cap Equity", cls: "Equity" },
  EFA: { weight: 0.12, name: "International Developed", cls: "Equity" },
  EEM: { weight: 0.06, name: "Emerging Markets", cls: "Equity" },
  AGG: { weight: 0.18, name: "US Aggregate Bond", cls: "Fixed Income" },
  TLT: { weight: 0.08, name: "Long Duration Treasury", cls: "Fixed Income" },
  GLD: { weight: 0.08, name: "Gold", cls: "Alternatives" },
  VNQ: { weight: 0.07, name: "Real Estate", cls: "Real Assets" },
  HYG: { weight: 0.07, name: "High Yield Credit", cls: "Fixed Income" },
  QQQ: { weight: 0.07, name: "US Tech Growth", cls: "Equity" },
  ARKK: { weight: 0.05, name: "Disruptive Innovation", cls: "Equity" },
};

export async function GET() {
  return safe(async () => {
    const tickers = Object.keys(PORTFOLIO);
    // Latest two closes per ticker in one query
    const rows = await q(
      `SELECT ticker, rn, close::float8 AS close FROM (
         SELECT ticker, close, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
         FROM etf_prices WHERE ticker = ANY($1) AND date >= (SELECT MAX(date) FROM etf_prices) - 14
       ) t WHERE rn <= 2`,
      [tickers]
    );
    const counts = await q(
      `SELECT (SELECT COUNT(*) FROM etf_prices) AS price_count,
              (SELECT COUNT(DISTINCT ticker) FROM etf_prices) AS ticker_count`
    );
    const lastRun = await q(
      "SELECT to_char(run_time,'YYYY-MM-DD HH24:MI') AS run_time, status FROM pipeline_runs ORDER BY run_time DESC LIMIT 1"
    );

    const holdings = tickers
      .map((t) => {
        const cur = rows.find((r: any) => r.ticker === t && Number(r.rn) === 1);
        const prev = rows.find((r: any) => r.ticker === t && Number(r.rn) === 2);
        if (!cur) return null;
        const price = Number(cur.close);
        const prevPrice = prev ? Number(prev.close) : price;
        return {
          ticker: t,
          name: PORTFOLIO[t].name,
          assetClass: PORTFOLIO[t].cls,
          weight: PORTFOLIO[t].weight,
          price,
          dailyChg: ((price - prevPrice) / prevPrice) * 100,
        };
      })
      .filter(Boolean);

    return {
      holdings,
      priceCount: num(counts[0]?.price_count),
      tickerCount: num(counts[0]?.ticker_count),
      lastRun: lastRun[0] || null,
    };
  });
}
