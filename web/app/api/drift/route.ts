import { q } from "@/lib/db";
import { safe } from "@/lib/respond";

export const dynamic = "force-dynamic";

// Same baseline fingerprint as etl/schema_drift.py
const BASELINE: Record<string, { columns: string[]; nonNullable: string[] }> = {
  etf_prices: {
    columns: ["id", "ticker", "date", "open", "high", "low", "close", "volume", "adj_close", "created_at"],
    nonNullable: ["id", "ticker", "date"],
  },
  technical_indicators: {
    columns: ["id", "ticker", "date", "sma_20", "sma_50", "ema_20", "rsi_14", "volatility_30d", "created_at"],
    nonNullable: ["id", "ticker", "date"],
  },
};

export async function GET() {
  return safe(async () => {
    const tables: Record<string, { status: string; drift: string[] }> = {};
    for (const [name, base] of Object.entries(BASELINE)) {
      const live = await q(
        `SELECT column_name, is_nullable FROM information_schema.columns
         WHERE table_name = $1 ORDER BY ordinal_position`,
        [name]
      );
      const cols = live.map((r: any) => r.column_name as string);
      const nullable = live.filter((r: any) => r.is_nullable === "YES").map((r: any) => r.column_name as string);
      const drift: string[] = [];
      const missing = base.columns.filter((c) => !cols.includes(c));
      const added = cols.filter((c) => !base.columns.includes(c));
      const nullDrift = base.nonNullable.filter((c) => nullable.includes(c));
      if (missing.length) drift.push(`MISSING columns: ${missing.join(", ")}`);
      if (added.length) drift.push(`NEW columns detected: ${added.join(", ")}`);
      if (nullDrift.length) drift.push(`NULLABILITY changed: ${nullDrift.join(", ")}`);
      tables[name] = { status: drift.length ? "DRIFT DETECTED" : "OK", drift };
    }
    return { tables };
  });
}
