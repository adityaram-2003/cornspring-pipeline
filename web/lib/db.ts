import { neon } from "@neondatabase/serverless";

// Read the connection string from Vercel env vars (NEON_DATABASE_URL, or DATABASE_URL as a fallback)
function getUrl(): string {
  const url = process.env.NEON_DATABASE_URL || process.env.DATABASE_URL;
  if (!url) {
    throw new Error("NEON_DATABASE_URL is not set. Add it in Vercel -> Project Settings -> Environment Variables.");
  }
  return url;
}

let pool: any = null;

// Run a parameterised query and return rows as plain objects
export async function q<T = Record<string, any>>(text: string, params: any[] = []): Promise<T[]> {
  const url = getUrl();
  // Local development against a plain Postgres on localhost (Neon's HTTP driver only works against Neon)
  if (/@(localhost|127\.0\.0\.1)[:/]/.test(url)) {
    if (!pool) {
      const { Pool } = await import("pg");
      pool = new Pool({ connectionString: url });
    }
    return (await pool.query(text, params)).rows as T[];
  }
  const sql = neon(url);
  const rows = await sql.query(text, params);
  return rows as T[];
}

export const num = (v: any): number | null => (v === null || v === undefined ? null : Number(v));
