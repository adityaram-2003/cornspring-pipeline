// Minimal Groq chat-completions client (OpenAI-compatible REST API)
export async function groqChat(messages: { role: string; content: string }[], maxTokens: number) {
  const key = process.env.GROQ_API_KEY;
  if (!key) throw new Error("GROQ_API_KEY is not set in Vercel environment variables.");
  const res = await fetch("https://api.groq.com/openai/v1/chat/completions", {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${key}` },
    body: JSON.stringify({ model: "llama-3.3-70b-versatile", max_tokens: maxTokens, messages }),
  });
  if (!res.ok) throw new Error(`Groq error ${res.status}: ${(await res.text()).slice(0, 200)}`);
  const data = await res.json();
  return (data.choices?.[0]?.message?.content ?? "") as string;
}
