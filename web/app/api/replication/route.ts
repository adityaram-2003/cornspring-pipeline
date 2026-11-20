import { NextRequest } from "next/server";
import { q } from "@/lib/db";
import { safe, cleanTicker } from "@/lib/respond";
import { corr, mean, std, pctChange, seededNormal } from "@/lib/stats";

export const dynamic = "force-dynamic";

const CURRENCIES: Record<string, number> = { USD: 1.0, GBP: 0.786, EUR: 0.921, JPY: 149.5, CHF: 0.896 };

// Tracking-error analysis between a benchmark ETF and a replicating ETF
export async function GET(req: NextRequest) {
  return safe(async () => {
    const p = req.nextUrl.searchParams;
    const benchmark = cleanTicker(p.get("benchmark"), "SPY");
    const replicant = cleanTicker(p.get("replicant"), "IVV");

    const get = (t: string) =>
      q(
        `SELECT to_char(date,'YYYY-MM-DD') AS d, close::float8 AS close FROM etf_prices
         WHERE ticker=$1 ORDER BY date DESC LIMIT 504`,
        [t]
      );
    const [b, r] = await Promise.all([get(benchmark), get(replicant)]);
    if (!b.length) throw new Error(`No data for ${benchmark} in the warehouse.`);
    if (!r.length) throw new Error(`No data for ${replicant} in the warehouse (it may not be ingested).`);

    const rm = new Map((r as any[]).map((x) => [x.d, x.close]));
    const merged = (b as any[]).filter((x) => rm.has(x.d)).map((x) => ({ d: x.d, b: x.close, r: rm.get(x.d) as number })).reverse();
    if (merged.length < 3) throw new Error("Not enough overlapping dates to compare.");

    const bRet = pctChange(merged.map((m) => m.b));
    const rRet = pctChange(merged.map((m) => m.r));
    const diff = rRet.map((x, i) => x - bRet[i]);
    const dates = merged.slice(1).map((m) => m.d);

    const cum = (rets: number[]) => { let c = 1; return rets.map((x) => (c *= 1 + x)); };

    const trackingError = std(diff) * Math.sqrt(252) * 100;
    const meanDiff = mean(diff) * 252 * 100;
    const maxDiff = Math.max(...diff.map(Math.abs)) * 100;

    // FX hedging table (illustrative simulation, as in the original dashboard)
    const noise = seededNormal(42);
    const baseRet = mean(bRet) * 252 * 100;
    const fx = Object.entries(CURRENCIES).map(([ccy, rate]) => {
      const n = noise() * 0.3;
      const hedged = baseRet + n;
      const unhedged = baseRet + n * 8;
      return { ccy, base: baseRet, hedged, unhedged, rate, benefitBps: (hedged - unhedged) * 100 };
    });

    return {
      benchmark, replicant, dates,
      bCum: cum(bRet), rCum: cum(rRet), diff: diff.map((x) => x * 100),
      trackingError, meanDiff, maxDiff, correlation: corr(bRet, rRet), fx,
    };
  });
}
