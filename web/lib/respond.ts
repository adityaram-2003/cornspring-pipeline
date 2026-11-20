import { NextResponse } from "next/server";

// Wrap a route handler so DB / API errors come back as JSON instead of a crash
export async function safe(fn: () => Promise<any>) {
  try {
    return NextResponse.json(await fn());
  } catch (e: any) {
    return NextResponse.json({ error: String(e?.message || e) }, { status: 500 });
  }
}

// Tickers only: letters, digits, dot, dash (blocks anything odd)
export const cleanTicker = (t: string | null, fallback = "SPY") =>
  t && /^[A-Za-z0-9.\-]{1,20}$/.test(t) ? t.toUpperCase() : fallback;
