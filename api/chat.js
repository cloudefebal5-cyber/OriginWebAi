// /api/chat.js
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const MODELS = {
  claude: process.env.ANTHROPIC_MODEL || 'claude-sonnet-4-6',
  openai: process.env.OPENAI_MODEL || 'gpt-4o-mini',
  gemini: process.env.GEMINI_MODEL || 'gemini-3.8-flash',
};
// Standard chat models listed as free in Gemini pricing; override with a comma-separated env value.
const GEMINI_FREE_FALLBACKS = (process.env.GEMINI_FALLBACK_MODELS || [
  'gemini-3.8-flash',
  'gemini-3.7-flash',
  'gemini-3.6-flash',
  'gemini-3.5-flash',
  'gemini-3.5-flash-lite',
  'gemini-3.1-flash-lite',
  'gemini-3-flash-preview',
  'gemini-2.5-flash',
  'gemini-2.5-flash-lite',
  'gemini-2.5-pro',
].join(','))
  .split(',')
  .map(model => model.trim())
  .filter(Boolean);

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
  const { model, messages, system } = body || {};
  if (!Array.isArray(messages) || messages.length === 0) {
    res.status(400).json({ message: 'messages[] is required' });
    return;
  }
  const provider = ['claude', 'openai', 'gemini'].includes(model) ? model : 'gemini';

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
    if (provider === 'claude') await streamClaude(messages, system, controller.signal, send);
    else if (provider === 'openai') await streamOpenAI(messages, system, controller.signal, send);
    else await streamGemini(messages, system, controller.signal, send);
  } catch (err) {
    console.error(`[${provider}] stream error:`, err);
    send({ error: friendlyProviderError(err, provider) });
  } finally {
    clearTimeout(timeout);
    res.write('data: [DONE]\n\n');
    res.end();
    void userId;
  }
};

function friendlyProviderError(err, provider) {
  if (err?.name === 'AbortError') return 'The AI provider took too long to respond.';
  if (provider === 'gemini' && err?.modelUnavailable) return 'The available Gemini models are not enabled for this API project.';
  if (err?.status === 401 || err?.status === 403) return 'Invalid or missing API key on the server for this provider.';
  if (provider === 'gemini' && err?.status === 429) return 'All configured Gemini models are at their current free-tier limits. Please try again later.';
  if (provider === 'gemini' && err?.status === 404) return 'None of the configured Gemini chat models is available to this API project.';
  if (err?.status === 429) return 'The AI provider is rate-limiting requests. Please try again shortly.';
  if (err?.status) return `The AI provider returned an error (${err.status}).`;
  return 'Could not reach the AI provider.';
}

async function streamClaude(messages, system, signal, send) {
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
      system: system || undefined,
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

async function streamOpenAI(messages, system, signal, send) {
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
      messages: [
        ...(system ? [{ role: 'system', content: system }] : []),
        ...messages.map(m => ({ role: m.role === 'assistant' ? 'assistant' : 'user', content: m.content }))
      ],
    }),
  });
  if (!resp.ok) { const t = await resp.text().catch(()=> ''); const e = new Error(t); e.status = resp.status; throw e; }
  await pumpSSE(resp.body, (evt) => {
    const delta = evt.choices?.[0]?.delta?.content;
    if (delta) send({ delta });
  });
}

async function streamGemini(messages, system, signal, send) {
  const key = process.env.GEMINI_API_KEY;
  if (!key) { send({ error: 'AI_PROVIDER_NOT_CONFIGURED: set GEMINI_API_KEY on the server' }); return; }
  const models = [...new Set([MODELS.gemini, ...GEMINI_FREE_FALLBACKS])];
  for (let index = 0; index < models.length; index++) {
    try {
      await streamGeminiModel(models[index], key, messages, system, signal, send);
      return;
    } catch (error) {
      const canFallback = ([404, 429, 503].includes(error.status) || error.modelUnavailable)
        && !error.partialResponse
        && index < models.length - 1;
      if (!canFallback) throw error;
      console.warn(`Gemini model ${models[index]} unavailable (${error.status}); trying ${models[index + 1]}`);
    }
  }
}

async function streamGeminiModel(model, key, messages, system, signal, send) {
  const contents = messages.map(m => ({
    role: m.role === 'assistant' ? 'model' : 'user',
    parts: [{ text: m.content }],
  }));
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:streamGenerateContent?alt=sse&key=${key}`;
  const resp = await fetch(url, {
    method: 'POST',
    signal,
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      contents,
      ...(system ? { systemInstruction: { parts: [{ text: system }] } } : {})
    }),
  });
  if (!resp.ok) {
    const message = await resp.text().catch(() => '');
    const error = new Error(message);
    error.status = resp.status;
    error.modelUnavailable = resp.status === 403 && isGeminiModelAccessError(message);
    throw error;
  }
  let sentText = false;
  try {
    await pumpSSE(resp.body, (evt) => {
      if (evt.error) {
        const error = new Error(evt.error.message || 'Gemini stream failed.');
        error.status = Number(evt.error.code) || ({
          RESOURCE_EXHAUSTED: 429,
          UNAVAILABLE: 503,
          NOT_FOUND: 404,
          PERMISSION_DENIED: 403
        })[evt.error.status];
        error.modelUnavailable = error.status === 403 && isGeminiModelAccessError(error.message);
        throw error;
      }
      const delta = evt.candidates?.[0]?.content?.parts?.map(p => p.text || '').join('');
      if (delta) {
        sentText = true;
        send({ delta });
      }
    });
  } catch (error) {
    error.partialResponse = sentText;
    throw error;
  }
}

function isGeminiModelAccessError(message) {
  return /(?:model.{0,50}(?:not found|not available|unavailable|not enabled|not allowed|access denied|permission)|(?:access denied|permission denied).{0,50}model)/i.test(message);
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
      let event;
      try { event = JSON.parse(payload); } catch (e) { continue; }
      onEvent(event);
    }
  }
}

module.exports.config = { api: { bodyParser: true } };
