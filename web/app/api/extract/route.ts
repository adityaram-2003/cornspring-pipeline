import { NextRequest } from "next/server";
import { safe } from "@/lib/respond";
import { groqChat } from "@/lib/groq";

export const dynamic = "force-dynamic";

// LLM extraction of fund metrics from an uploaded fact sheet (display only, nothing is written to the DB)
export async function POST(req: NextRequest) {
  return safe(async () => {
    const body = await req.json().catch(() => ({}));
    const name = String(body.name || "upload.txt").slice(0, 120);
    let content = String(body.content || "");
    if (content.length > 2_000_000) throw new Error("File too large (2 MB max).");
    content = content.replace(/<script[^>]*>[\s\S]*?<\/script>/gi, " ").replace(/<style[^>]*>[\s\S]*?<\/style>/gi, " ");
    const clean = content.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim().slice(0, 8000);
    if (!clean) throw new Error("The file has no readable text.");

    const prompt = `You are a financial data extraction specialist working with SEC 497K fund filings.

Extract these fields from the document. Return ONLY valid JSON, nothing else.

{
  "fund_name": "full name of the fund or null",
  "ticker": "ticker symbol or null",
  "expense_ratio": numeric decimal like 0.0085 or null,
  "net_assets": numeric billions like 1.2 or null,
  "benchmark": "benchmark index name or null",
  "inception_date": "YYYY-MM-DD or null",
  "category": "fund category or null",
  "top_holding": "largest holding name or null"
}

DOCUMENT:
${clean}`;
    const raw = (await groqChat([{ role: "user", content: prompt }], 500)).replace(/```json|```/g, "").trim();
    let metrics: any;
    try { metrics = JSON.parse(raw); } catch { throw new Error("Extraction failed. Try a cleaner document format."); }
    metrics.source_file = name;
    return { metrics, chars: content.length, words: content.split(/\s+/).filter(Boolean).length, preview: clean.slice(0, 600) };
  });
}
