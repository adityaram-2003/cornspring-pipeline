import { q } from "@/lib/db";
import { safe } from "@/lib/respond";

export const dynamic = "force-dynamic";

// All tickers in the warehouse (used by selectors)
export async function GET() {
  return safe(async () => {
    const rows = await q<{ ticker: string }>("SELECT DISTINCT ticker FROM etf_prices ORDER BY ticker");
    return { tickers: rows.map((r) => r.ticker) };
  });
}
