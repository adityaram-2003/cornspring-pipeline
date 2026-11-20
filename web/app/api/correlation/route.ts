import { NextRequest } from "next/server";
import { q } from "@/lib/db";
import { safe, cleanTicker } from "@/lib/respond";
import { corr, pctChange } from "@/lib/stats";

export const dynamic = "force-dynamic";

// Return-correlation matrix over the last year for the chosen tickers
export async function GET(req: NextRequest) {
  return safe(async () => {
    const raw = (req.nextUrl.searchParams.get("tickers") || "").split(",");
    const tickers = Array.from(new Set(raw.map((t) => cleanTicker(t, "")).filter(Boolean))).slice(0, 12);
    if (tickers.length < 2) return { tickers: [], matrix: [] };

    const rows = await q(
      `SELECT ticker, to_char(date,'YYYY-MM-DD') AS d, close::float8 AS close FROM etf_prices
       WHERE ticker = ANY($1) AND date >= (SELECT MAX(date) FROM etf_prices WHERE ticker = ANY($1)) - 365
       ORDER BY date`,
      [tickers]
    );
    // Keep only dates where every ticker has a price
    const byDate = new Map<string, Record<string, number>>();
    for (const r of rows as any[]) {
      if (!byDate.has(r.d)) byDate.set(r.d, {});
      byDate.get(r.d)![r.ticker] = r.close;
    }
    const dates = Array.from(byDate.keys()).filter((d) => tickers.every((t) => byDate.get(d)![t] !== undefined)).sort();
    const rets: Record<string, number[]> = {};
    for (const t of tickers) rets[t] = pctChange(dates.map((d) => byDate.get(d)![t]));
    const matrix = tickers.map((a) => tickers.map((b) => corr(rets[a], rets[b])));
    return { tickers, matrix, days: dates.length };
  });
}
