import { q } from "@/lib/db";
import { safe } from "@/lib/respond";

export const dynamic = "force-dynamic";

// Table of extracted ETF metadata, largest AUM first
export async function GET() {
  return safe(async () => {
    const rows = await q(
      `SELECT ticker, name, category, ROUND(aum_billions::numeric,2)::float8 AS aum,
              ROUND(expense_ratio::numeric*100,4)::float8 AS expense, benchmark, extracted_by_llm AS llm_extracted
       FROM etf_metadata ORDER BY aum_billions DESC NULLS LAST`
    );
    return { rows };
  });
}
