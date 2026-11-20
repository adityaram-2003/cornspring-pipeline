import { q, num } from "@/lib/db";
import { safe } from "@/lib/respond";

export const dynamic = "force-dynamic";

// Counts, recent runs and daily coverage for the Pipeline Control tab
export async function GET() {
  return safe(async () => {
    const c = await q(
      `SELECT (SELECT COUNT(*) FROM etf_prices) AS prices,
              (SELECT COUNT(*) FROM technical_indicators) AS indicators,
              (SELECT COUNT(*) FROM etf_metadata WHERE extracted_by_llm = TRUE) AS llm,
              (SELECT COUNT(DISTINCT ticker) FROM etf_prices) AS tickers`
    );
    const runs = await q(
      `SELECT to_char(run_time,'YYYY-MM-DD HH24:MI') AS run_time, tickers_processed, records_inserted,
              records_failed, status, notes FROM pipeline_runs ORDER BY run_time DESC LIMIT 10`
    );
    const daily = await q(
      `SELECT to_char(date,'YYYY-MM-DD') AS date, COUNT(DISTINCT ticker)::int AS tickers
       FROM etf_prices GROUP BY date ORDER BY date DESC LIMIT 60`
    );
    return {
      prices: num(c[0].prices), indicators: num(c[0].indicators), llm: num(c[0].llm), tickers: num(c[0].tickers),
      runs, daily: daily.reverse(),
    };
  });
}
