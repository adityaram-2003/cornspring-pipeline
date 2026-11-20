# Cornspring Intelligence Terminal (Vercel / Next.js)

Next.js port of `dashboard/app.py`. Reads the Neon PostgreSQL warehouse and calls Groq for the chat and document extraction.
The Streamlit app in `dashboard/` still works for Streamlit Cloud; this folder is what runs on Vercel.

## Deploy
1. Vercel -> Add New Project -> import `adityaram-2003/cornspring-pipeline`.
2. Set **Root Directory** to `web`. Framework preset: Next.js (auto-detected).
3. Add environment variables: `NEON_DATABASE_URL`, `GROQ_API_KEY`.
4. Deploy. Every push to `main` redeploys.

## Run locally
    cd web
    cp .env.example .env.local   # fill in the two values
    npm install
    npm run dev

## Notes
- The browser never sees the keys; all DB and Groq calls happen in `app/api/*` routes.
- Use a **read-only** Neon role for `NEON_DATABASE_URL`. The site is public and nothing here writes to the database.
- The ticker strip uses Yahoo's public chart endpoint and falls back to the warehouse's last two closes.
- A plain `postgresql://user@localhost/...` URL works for local dev (uses `pg`); Neon URLs use the serverless HTTP driver.
