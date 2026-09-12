// /api/chat.js
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const MODELS = {
  claude: process.env.ANTHROPIC_MODEL || 'claude-sonnet-4-6',
  openai: process.env.OPENAI_MODEL || 'gpt-4o-mini',
  gemini: process.env.GEMINI_MODEL || 'gemini-2.0-flash',
};

module.exports = async function handler(req, res) {
  if (req.method !== 'POST') {
    res.status(405).json({ message: 'Method not allowed' });
    return;
  }

  const authHeader = req.headers['authorization'] || '';
  const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null;
  if (!token) {
    res.status(401).json({ message: 'Missing Authorization header' });
    return;
  }
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    res.status(500).json({ message: 'Server is not configured (missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY)' });
    return;
  }
  const adminClient = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
  const { data: userData, error: userErr } = await adminClient.auth.getUser(token);
  if (userErr || !userData?.user) {
    res.status(401).json({ message: 'Your session has expired — please log in again.' });
    return;
  }
  const userId = userData.user.id;

  let body = req.body;
  if (typeof body === 'string') {
    try { body = JSON.parse(body); } catch (e) { body = {}; }
  }
  const { model, messages } = body || {};
  if (!Array.isArray(messages) || messages.length === 0) {
    res.status(400).json({ message: 'messages[] is required' });
    return;
  }
  const provider = ['claude', 'openai', 'gemini'].includes(model) ? model : 'claude';

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 55000);

  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no',
  });
  const send = (obj) => res.write(`data: ${JSON.stringify(obj)}\n\n`);

  try {
    if (provider === 'claude') await streamClaude(messages, controller.signal, send);
    else if (provider === 'openai') await streamOpenAI(messages, controller.signal, send);
    else await streamGemini(messages, controller.signal, send);
  } catch (err) {
    console.error(`[${provider}] stream error:`, err);
    send({ error: friendlyProviderError(err) });
  } finally {
    clearTimeout(timeout);
    res.write('data: [DONE]\n\n');
    res.end();
    void userId;
  }
};

function friendlyProviderError(err) {
  if (err?.name === 'AbortError') return 'The AI provider took too long to respond.';
  if (err?.status === 401 || err?.status === 403) return 'Invalid or missing API key on the server for this provider.';
  if (err?.status === 429) return 'The AI provider is rate-limiting requests. Please try again shortly.';
  if (err?.status) return `The AI provider returned an error (${err.status}).`;
  return 'Could not reach the AI provider.';
}

async function streamClaude(messages, signal, send) {
  const key = process.env.ANTHROPIC_API_KEY;
  if (!key) { send({ error: 'AI_PROVIDER_NOT_CONFIGURED: set ANTHROPIC_API_KEY on the server' }); return; }
  const resp = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    signal,
    headers: {
      'content-type': 'application/json',
      'x-api-key': key,
      'anthropic-version': '2023-06-01',
    },
    body: JSON.stringify({
      model: MODELS.claude,
      max_tokens: 2048,
      stream: true,
      messages: messages.map(m => ({ role: m.role === 'assistant' ? 'assistant' : 'user', content: m.content })),
    }),
  });
  if (!resp.ok) { const t = await resp.text().catch(()=> ''); const e = new Error(t); e.status = resp.status; throw e; }
  await pumpSSE(resp.body, (evt) => {
    if (evt.type === 'content_block_delta' && evt.delta?.type === 'text_delta') {
      send({ delta: evt.delta.text });
    }
  });
}

async function streamOpenAI(messages, signal, send) {
  const key = process.env.OPENAI_API_KEY;
  if (!key) { send({ error: 'AI_PROVIDER_NOT_CONFIGURED: set OPENAI_API_KEY on the server' }); return; }
  const resp = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    signal,
    headers: {
      'content-type': 'application/json',
      'authorization': `Bearer ${key}`,
    },
    body: JSON.stringify({
      model: MODELS.openai,
      stream: true,
      messages: messages.map(m => ({ role: m.role === 'assistant' ? 'assistant' : 'user', content: m.content })),
    }),
  });
  if (!resp.ok) { const t = await resp.text().catch(()=> ''); const e = new Error(t); e.status = resp.status; throw e; }
  await pumpSSE(resp.body, (evt) => {
    const delta = evt.choices?.[0]?.delta?.content;
    if (delta) send({ delta });
  });
}

async function streamGemini(messages, signal, send) {
  const key = process.env.GEMINI_API_KEY;
  if (!key) { send({ error: 'AI_PROVIDER_NOT_CONFIGURED: set GEMINI_API_KEY on the server' }); return; }
  const contents = messages.map(m => ({
    role: m.role === 'assistant' ? 'model' : 'user',
    parts: [{ text: m.content }],
  }));
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${MODELS.gemini}:streamGenerateContent?alt=sse&key=${key}`;
  const resp = await fetch(url, {
    method: 'POST',
    signal,
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ contents }),
  });
  if (!resp.ok) { const t = await resp.text().catch(()=> ''); const e = new Error(t); e.status = resp.status; throw e; }
  await pumpSSE(resp.body, (evt) => {
    const delta = evt.candidates?.[0]?.content?.parts?.map(p => p.text || '').join('');
    if (delta) send({ delta });
  });
}

async function pumpSSE(stream, onEvent) {
  const reader = stream.getReader ? stream.getReader() : require('stream').Readable.toWeb(stream).getReader();
  const decoder = new TextDecoder();
  let buf = '';
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += decoder.decode(value, { stream: true });
    const lines = buf.split('\n');
    buf = lines.pop();
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed.startsWith('data:')) continue;
      const payload = trimmed.slice(5).trim();
      if (!payload || payload === '[DONE]') continue;
      try { onEvent(JSON.parse(payload)); } catch (e) { /* ignore partial/malformed chunk */ }
    }
  }
}

module.exports.config = { api: { bodyParser: true } };
