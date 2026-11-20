import { NextRequest } from "next/server";
import { q } from "@/lib/db";
import { safe, cleanTicker } from "@/lib/respond";

export const dynamic = "force-dynamic";

// Prices + indicators + metadata for one ticker
export async function GET(req: NextRequest) {
  return safe(async () => {
    const p = req.nextUrl.searchParams;
    const ticker = cleanTicker(p.get("ticker"));
    const days = Math.min(Math.max(parseInt(p.get("days") || "365", 10) || 365, 10), 1825);

    const prices = await q(
      `SELECT to_char(date,'YYYY-MM-DD') AS date, open::float8 AS open, high::float8 AS high,
              low::float8 AS low, close::float8 AS close, volume::float8 AS volume
       FROM etf_prices WHERE ticker=$1 ORDER BY date DESC LIMIT $2`,
      [ticker, days]
    );
    const indicators = await q(
      `SELECT to_char(date,'YYYY-MM-DD') AS date, sma_20::float8 AS sma_20, sma_50::float8 AS sma_50,
              ema_20::float8 AS ema_20, rsi_14::float8 AS rsi_14, volatility_30d::float8 AS volatility_30d
       FROM technical_indicators WHERE ticker=$1 ORDER BY date DESC LIMIT $2`,
      [ticker, days]
    );
    const meta = await q(
      `SELECT ticker, name, category, expense_ratio::float8 AS expense_ratio,
              aum_billions::float8 AS aum_billions, benchmark FROM etf_metadata WHERE ticker=$1`,
      [ticker]
    );
    return { ticker, prices: prices.reverse(), indicators: indicators.reverse(), meta: meta[0] || null };
  });
}
