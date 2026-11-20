import { NextRequest } from "next/server";
import { q } from "@/lib/db";
import { safe, cleanTicker } from "@/lib/respond";

export const dynamic = "force-dynamic";

// 7-rule validation, counted in SQL over the most recent N rows (null counts as a failure, like pandas)
export async function POST(req: NextRequest) {
  return safe(async () => {
    const body = await req.json().catch(() => ({}));
    const ticker = body.ticker && body.ticker !== "ALL" ? cleanTicker(body.ticker, "") : null;
    const limit = [1000, 5000, 10000, 50000].includes(Number(body.limit)) ? Number(body.limit) : 10000;

    const rows = await q(
      `SELECT COUNT(*)::int AS total,
        COUNT(*) FILTER (WHERE NOT COALESCE(close > 0, false))::int AS close_positive,
        COUNT(*) FILTER (WHERE NOT COALESCE(high >= low, false))::int AS high_gte_low,
        COUNT(*) FILTER (WHERE NOT COALESCE(high >= close, false))::int AS high_gte_close,
        COUNT(*) FILTER (WHERE NOT COALESCE(low <= close, false))::int AS low_lte_close,
        COUNT(*) FILTER (WHERE NOT COALESCE(volume >= 0, false))::int AS volume_non_negative,
        COUNT(*) FILTER (WHERE NOT COALESCE(date <= NOW(), false))::int AS no_future_dates,
        COUNT(*) FILTER (WHERE NOT COALESCE(open > 0, false))::int AS open_positive
       FROM (SELECT * FROM etf_prices WHERE ($1::text IS NULL OR ticker = $1) ORDER BY date DESC LIMIT $2) t`,
      [ticker, limit]
    );
    const { total, ...rules } = rows[0] as any;
    const failed = Object.values(rules as Record<string, number>).reduce((s, v) => s + Number(v), 0);
    return { total, passed: total - failed, failed, rules };
  });
}
