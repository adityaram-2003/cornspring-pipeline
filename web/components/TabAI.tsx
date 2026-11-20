"use client";
import { useState } from "react";
import { Card, Section, ErrorBox, post } from "./ui";

const QUICK = [
  "What is SPY's current RSI and what does it signal?",
  "Compare volatility between SPY, AGG, and GLD",
  "Which ETFs are most overbought right now?",
  "Give me a pipeline health summary",
  "What's the tracking error between IVV and SPY?",
];
const TOOLS = ["get_portfolio_summary", "run_data_validation", "get_technical_indicators", "get_pipeline_health"];

export default function TabAI() {
  const [msgs, setMsgs] = useState<{ role: string; content: string }[]>([]);
  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [mTicker, setMTicker] = useState("SPY");
  const [mTool, setMTool] = useState(TOOLS[0]);
  const [mOut, setMOut] = useState<string | null>(null);

  async function send(text: string) {
    const t = text.trim();
    if (!t || busy) return;
    setBusy(true); setErr(null); setInput("");
    const history = msgs;
    setMsgs([...history, { role: "user", content: t }]);
    try {
      const j = await post("/api/chat", { message: t, history });
      setMsgs((m) => [...m, { role: "assistant", content: j.answer }]);
    } catch (e: any) { setErr(e.message); }
    setBusy(false);
  }

  async function runTool() {
    setMOut("Running…");
    try { setMOut(JSON.stringify((await post("/api/mcp", { tool: mTool, ticker: mTicker })).result, null, 2)); }
    catch (e: any) { setMOut(`Error: ${e.message}`); }
  }

  return (
    <>
      <Section>AI Data Intelligence — Natural Language Portfolio Queries</Section>
      <Card title="How This Works">
        This AI agent is connected to the Cornspring Intelligence Pipeline&apos;s PostgreSQL warehouse. It pulls the latest
        price and indicator snapshot for the tickers in your question and answers with Groq (Llama 3.3 70B). The same
        queries are exposed as tools by the pipeline&apos;s MCP server (FastMCP) for Cursor, Claude, or any MCP client.
      </Card>
      <div className="chips">{QUICK.map((p) => <button key={p} className="chip" onClick={() => send(p)}>→ {p}</button>)}</div>
      {msgs.map((m, i) => m.role === "user" ? (
        <div key={i}><div className="chat-label-user">YOU</div><div className="chat-user">{m.content}</div></div>
      ) : (
        <div key={i}><div className="chat-label-ai">🤖 CORNSPRING AGENT</div><div className="chat-ai">{m.content}</div></div>
      ))}
      {busy && <div className="alert info" style={{ marginTop: 10 }}>Agent querying pipeline…</div>}
      {err && <ErrorBox msg={err} />}
      <form className="row" style={{ marginTop: 12 }} onSubmit={(e) => { e.preventDefault(); send(input); }}>
        <label className="field" style={{ flex: 4 }}>Ask the AI agent anything about your portfolio data
          <input type="text" value={input} maxLength={500} onChange={(e) => setInput(e.target.value)} placeholder="e.g. What is the RSI for SPY? Which ETFs have highest volatility?" />
        </label>
        <button className="btn" disabled={busy || !input.trim()}>Send →</button>
        <button type="button" className="btn secondary" onClick={() => { setMsgs([]); setErr(null); }}>Clear Chat</button>
      </form>

      <Section>MCP Server — Live Tool Execution</Section>
      <Card title="Connected MCP Tools">
        <div className="mono" style={{ lineHeight: 2.2 }}>
          {["get_portfolio_summary(ticker)", "run_data_validation(ticker)", "get_technical_indicators(ticker, days)", "trigger_ingestion(ticker)", "get_pipeline_health()"].map((t) => (
            <div key={t}><span className="up">●</span> {t}</div>
          ))}
        </div>
      </Card>
      <div className="row">
        <label className="field">Run MCP Tool
          <select value={mTicker} onChange={(e) => setMTicker(e.target.value)}>{["SPY", "QQQ", "AGG", "GLD", "VTI"].map((t) => <option key={t}>{t}</option>)}</select>
        </label>
        <label className="field">Select Tool
          <select value={mTool} onChange={(e) => setMTool(e.target.value)}>{TOOLS.map((t) => <option key={t}>{t}</option>)}</select>
        </label>
        <button className="btn" onClick={runTool}>▶ Execute MCP Tool</button>
      </div>
      {mOut && <pre>{mOut}</pre>}
    </>
  );
}
