const CF_BASE = 'https://api.cloudflare.com/client/v4/accounts';

export function chatUrl(accountId) {
  return `${CF_BASE}/${accountId}/ai/v1/chat/completions`;
}

export function embeddingsUrl(accountId) {
  return `${CF_BASE}/${accountId}/ai/v1/embeddings`;
}

export function runUrl(accountId, model) {
  return `${CF_BASE}/${accountId}/ai/run/${model}`;
}

function extractUsage(json) {
  if (!json) return null;
  if (json.usage) return json.usage;
  if (json.result?.usage) return json.result.usage;
  return null;
}

function extractError(text) {
  if (!text) return {};
  try {
    const j = JSON.parse(text);
    const e = j.errors?.[0] ?? j.error;
    if (e) return { errorCode: e.code ?? null, message: e.message ?? '' };
  } catch {}
  return {};
}

/** Scan SSE fragments for the last `usage` object (chat stream). */
function scanUsage(buffer) {
  let usage = null;
  for (const line of buffer.split('\n')) {
    const s = line.trim();
    if (!s.startsWith('data:')) continue;
    const payload = s.slice(5).trim();
    if (payload === '[DONE]' || !payload.startsWith('{')) continue;
    try {
      const obj = JSON.parse(payload);
      if (obj.usage) usage = obj.usage;
    } catch {}
  }
  return usage;
}

export async function callNormal(url, apiKey, body, { timeoutMs = 60000 } = {}) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(timeoutMs),
  });
  const status = res.status;
  if (status >= 400) {
    const text = await res.text();
    const { errorCode, message } = extractError(text);
    return { status, headers: res.headers, text, errorCode, errorMessage: message };
  }
  const json = await res.json();
  return { status, headers: res.headers, json, usage: extractUsage(json) };
}

/**
 * Open the upstream stream and return its status WITHOUT piping yet.
 * Lets the caller check for 429/4xx first before committing a 200 SSE response.
 * @returns {Promise<{status, headers, text?, errorCode?, stream?}>}
 *   On error (status>=400): {status, headers, text, errorCode}.
 *   On success: {status, headers, stream} — caller must pump via pumpStream.
 */
export async function openStream(url, apiKey, body, { timeoutMs = 90000 } = {}) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(timeoutMs),
  });
  if (res.status >= 400) {
    const text = await res.text();
    const { errorCode, message } = extractError(text);
    return { status: res.status, headers: res.headers, text, errorCode, errorMessage: message };
  }
  return { status: res.status, headers: res.headers, stream: res.body };
}

/**
 * Pipe an opened upstream stream to `write`, scanning SSE for the final usage.
 * @param {ReadableStream} stream - upstream response body
 * @param {(chunk: Uint8Array) => Promise<void>|void} write
 * @returns {Promise<{usage?:any}>}
 */
export async function pumpStream(stream, write, { chunkTimeoutMs = 60000 } = {}) {
  const decoder = new TextDecoder();
  let tail = '';
  let usage = null;
  const reader = stream.getReader();
  try {
    while (true) {
      // Race each read against a timeout to prevent infinite hangs
      // when CF stops sending data mid-stream
      const result = await Promise.race([
        reader.read(),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error(`Chunk read timeout (${chunkTimeoutMs}ms)`)), chunkTimeoutMs)
        ),
      ]);
      if (result.done) break;
      const chunk = result.value;
      await write(chunk);
      tail += decoder.decode(chunk, { stream: true });
      const nl = tail.lastIndexOf('\n');
      if (nl >= 0) {
        const found = scanUsage(tail.slice(0, nl));
        if (found) usage = found;
        tail = tail.slice(nl + 1);
      }
    }
  } finally {
    reader.releaseLock();
  }
  const found = scanUsage(tail);
  if (found) usage = found;
  return { usage };
}
