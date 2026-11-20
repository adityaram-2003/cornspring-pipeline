"use client";
import { useEffect, useState } from "react";

export function Section({ children }: { children: React.ReactNode }) {
  return <div className="cs-section">{children}</div>;
}

export function Card({ title, children }: { title?: string; children: React.ReactNode }) {
  return (
    <div className="cs-card">
      {title && <div className="cs-card-title">{title}</div>}
      <div className="cs-card-body">{children}</div>
    </div>
  );
}

export function Metric({ label, value, delta, tone }: { label: string; value: React.ReactNode; delta?: string; tone?: "up" | "down" | "warn" }) {
  return (
    <div className="metric">
      <div className="metric-label">{label}</div>
      <div className="metric-value">{value}</div>
      {delta && <div className={`metric-delta ${tone || ""}`}>{delta}</div>}
    </div>
  );
}

export function Table({ cols, rows }: { cols: string[]; rows: React.ReactNode[][] }) {
  return (
    <div className="table-wrap">
      <table>
        <thead><tr>{cols.map((c) => <th key={c}>{c}</th>)}</tr></thead>
        <tbody>{rows.map((r, i) => <tr key={i}>{r.map((c, j) => <td key={j}>{c}</td>)}</tr>)}</tbody>
      </table>
    </div>
  );
}

export function ErrorBox({ msg }: { msg: string }) {
  return <div className="alert bad">{msg}</div>;
}

// GET helper: fetches a URL, re-fetching when it changes
export function useApi<T = any>(url: string | null) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(!!url);
  useEffect(() => {
    if (!url) return;
    let alive = true;
    setLoading(true);
    setError(null);
    fetch(url)
      .then((r) => r.json())
      .then((j) => { if (!alive) return; if (j.error) setError(j.error); else setData(j); })
      .catch((e) => alive && setError(String(e)))
      .finally(() => alive && setLoading(false));
    return () => { alive = false; };
  }, [url]);
  return { data, error, loading };
}

// POST helper used by buttons
export async function post(url: string, body: any) {
  const r = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  const j = await r.json();
  if (j.error) throw new Error(j.error);
  return j;
}

export const fmt = (n: number | null | undefined, d = 2) =>
  n === null || n === undefined || Number.isNaN(n) ? "N/A" : n.toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
