import { NextRequest } from "next/server";
import { q } from "@/lib/db";
import { safe } from "@/lib/respond";
import { groqChat } from "@/lib/groq";

export const dynamic = "force-dynamic";

const SYSTEM_PROMPT = `You are the Cornspring Intelligence Agent — an AI assistant embedded in a 
Family Office financial data platform. You have access to a PostgreSQL warehouse containing:
- 235,195 ETF price records across 149 tickers (2020-2025)
- 232,328 technical indicator records (RSI, SMA-20, SMA-50, EMA-20, 30d Volatility)
- ETF metadata extracted by LLM/RAG (expense ratios, AUM, benchmarks, categories)
- Pipeline health metrics and run logs

You can answer questions about portfolio data, technical indicators, data quality,
market trends, ETF comparisons, and pipeline operations. Be precise, data-driven,
and concise. Use numbers and specifics. When referencing data, cite the exact values
from the warehouse. Format key numbers in bold. Keep responses under 150 words.`;

const DEFAULTS = ["SPY", "QQQ", "AGG", "GLD", "EFA", "VTI", "HYG", "VNQ"];

// Latest snapshot for tickers mentioned in the question (plus defaults), max 8
async function buildContext(question: string) {
  const tokens = new Set(question.toUpperCase().match(/[A-Z]{2,6}/g) || []);
  const all = (await q<{ ticker: string }>("SELECT DISTINCT ticker FROM etf_prices")).map((r) => r.ticker);
  const mentioned = all.filter((t) => tokens.has(t));
  const combined = Array.from(new Set([...mentioned, ...DEFAULTS])).slice(0, 8);
  const rows = await q(
    `SELECT DISTINCT ON (p.ticker) p.ticker, p.date::text AS date, ROUND(p.close::numeric,2) AS close,
            ROUND(i.rsi_14::numeric,2) AS rsi, ROUND(i.sma_20::numeric,2) AS sma, ROUND(i.volatility_30d::numeric,4) AS vol
     FROM etf_prices p JOIN technical_indicators i ON p.ticker = i.ticker AND p.date = i.date
     WHERE p.ticker = ANY($1) ORDER BY p.ticker, p.date DESC`,
    [combined]
  );
  if (!rows.length) return "No data available.";
  return ["ticker | date | close | rsi_14 | sma_20 | volatility", "-".repeat(60),
    ...rows.map((r: any) => `${r.ticker} | ${r.date} | ${r.close} | ${r.rsi} | ${r.sma} | ${r.vol}`)].join("\n");
}

export async function POST(req: NextRequest) {
  return safe(async () => {
    const body = await req.json().catch(() => ({}));
    const message = String(body.message || "").trim().slice(0, 500);
    if (!message) throw new Error("Empty message");
    // Keep only the last few plain turns from the browser
    const history = (Array.isArray(body.history) ? body.history : [])
      .filter((m: any) => (m.role === "user" || m.role === "assistant") && typeof m.content === "string")
      .slice(-8)
      .map((m: any) => ({ role: m.role, content: String(m.content).slice(0, 1500) }));

    let context: string;
    try { context = await buildContext(message); } catch (e: any) { context = `Data fetch error: ${e.message}`; }

    const prompt = `Here is the latest data snapshot from the warehouse:\n\n${context}\n\nUser question: ${message}`;
    const answer = await groqChat([{ role: "system", content: SYSTEM_PROMPT }, ...history, { role: "user", content: prompt }], 300);
    return { answer };
  });
}
