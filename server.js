import 'dotenv/config';
import express from 'express';
import { existsSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

import { initDb } from './lib/db.js';
import { AccountPool } from './lib/pool.js';
import { RequestLog, captureBody } from './lib/log.js';
import { importFrom9router } from './lib/importer.js';
import { chatUrl, embeddingsUrl, runUrl, callNormal, callStream } from './lib/cf.js';
import { NEURON_FREE_DAILY, todayUTC } from './lib/neurons.js';

const __dirname = dirname(fileURLToPath(import.meta.url));

// --- Config ---
const HOST = process.env.CF_GATEWAY_HOST || '0.0.0.0';
const PORT = parseInt(process.env.CF_GATEWAY_PORT || '8750', 10);
const NINE_DB = process.env.CF_GATEWAY_9ROUTER_DB || join(process.env.HOME || '', '.9router/db/data.sqlite');
const OWN_DB = process.env.CF_GATEWAY_DB || join(__dirname, 'data', 'accounts.db');
const API_KEY = process.env.CF_GATEWAY_API_KEY || '';
const COOLDOWN_429 = parseInt(process.env.CF_GATEWAY_COOLDOWN_429 || '90', 10);
const MAX_RETRIES = parseInt(process.env.CF_GATEWAY_MAX_RETRIES || '8', 10);
const RETRY_DELAY_MS = parseInt(process.env.CF_GATEWAY_RETRY_DELAY_MS || '3000', 10);

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// --- Concurrency limiter ---
const MAX_CONCURRENT = parseInt(process.env.CF_GATEWAY_MAX_CONCURRENT || '2', 10);
let _inFlight = 0;
const _waitQueue = [];
function acquireSlot() {
  if (_inFlight < MAX_CONCURRENT) { _inFlight++; return Promise.resolve(); }
  return new Promise(resolve => _waitQueue.push(resolve));
}
function releaseSlot() {
  if (_waitQueue.length > 0) { _waitQueue.shift()(); }
  else { _inFlight--; }
}

// --- Logger ---
const stamp = () => new Date().toISOString().slice(11, 19);
const log = {
  debug: (m) => console.log(`${stamp()} [DEBUG] ${m}`),
  info: (m) => console.log(`${stamp()} [INFO]  ${m}`),
  warn: (m) => console.warn(`${stamp()} [WARN]  ${m}`),
  error: (m) => console.error(`${stamp()} [ERROR] ${m}`),
};

// --- Init ---
const db = initDb(OWN_DB);
const pool = new AccountPool(db, { cooldown429: COOLDOWN_429, log });
const requestLog = new RequestLog({ capacity: 500 });

const app = express();
app.use(express.json({ limit: '1mb' }));

// JSON error handler
app.use((err, _req, res, next) => {
  if (err?.type === 'entity.parse.failed' || err instanceof SyntaxError) {
    return res.status(400).json({ error: 'Invalid JSON' });
  }
  if (err?.type === 'entity.too.large') {
    return res.status(413).json({ error: 'Request too large' });
  }
  return next(err);
});

// Bearer auth on API/proxy paths
const PROTECTED = ['/v1', '/api', '/health', '/ai'];
app.use((req, res, next) => {
  if (!API_KEY) return next();
  const path = req.path.toLowerCase();
  // Dashboard HTML (GET /) — no auth needed
  if (path === '/' || path === '/index.html') return next();
  const guarded = PROTECTED.some((p) => path === p || path.startsWith(p + '/'));
  if (!guarded) return next();
  if (req.headers.authorization === `Bearer ${API_KEY}`) return next();
  return res.status(401).json({ error: 'Unauthorized' });
});

// --- Model resolution (simple — no cache needed, just prefix @cf/) ---
function resolveModel(input) {
  if (!input || typeof input !== 'string') return { error: 'model required', status: 400 };
  const s = input.trim();
  if (!s) return { error: 'model required', status: 400 };
  if (s.startsWith('@cf/')) return { id: s };
  if (s.includes('/')) return { id: `@cf/${s}` };
  // Short ID — pass through (CF will reject if invalid)
  return { id: s };
}

// --- Flatten OpenAI content arrays to string (CF rejects multipart) ---
function flattenContent(messages) {
  for (const msg of messages) {
    if (Array.isArray(msg.content)) {
      msg.content = msg.content
        .map((p) => (typeof p === 'string' ? p : p?.type === 'text' ? p.text ?? '' : ''))
        .join('');
    }
  }
  return messages;
}

// --- Core: retry across pool on 429 ---
async function withPool({ res, buildUrl, body, stream, model, endpoint, clientRequest }) {
  // === FAST GATE CHECK — reject immediately if CF is overloaded ===
  const preGate = pool.checkCapacityGate();
  if (preGate.blocked) {
    log.warn(`Capacity gate active — rejecting request fast (would wait ${Math.ceil(preGate.waitMs / 1000)}s)`);
    return res.status(503).json({ error: 'Service temporarily at capacity', retry_after: Math.ceil(preGate.waitMs / 1000) });
  }

  // === CONCURRENCY LIMIT ===
  await acquireSlot();
  try {
    return await _withPoolInner({ res, buildUrl, body, stream, model, endpoint, clientRequest });
  } finally {
    releaseSlot();
  }
}

async function _withPoolInner({ res, buildUrl, body, stream, model, endpoint, clientRequest }) {
  const startedAt = Date.now();
  const clientReq = clientRequest ?? captureBody(body);

  const record = (account, status, extra = {}) => {
    requestLog.record({
      endpoint,
      model,
      account_id: account?.id ?? null,
      account_name: account?.name ?? null,
      status,
      stream,
      latency_ms: Date.now() - startedAt,
      client_request: clientReq,
      provider_request: captureBody(body),
      ...extra,
    });
  };

  let capacityBackoffMs = 0; // exponential backoff tracker for capacity errors

  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    // === GLOBAL CAPACITY GATE CHECK ===
    // If CF backend is overloaded, wait for gate to clear before trying ANY account
    const gate = pool.checkCapacityGate();
    if (gate.blocked) {
      log.info(`Capacity gate active — waiting ${Math.ceil(gate.waitMs / 1000)}s before retry (attempt ${attempt + 1}/${MAX_RETRIES})`);
      await sleep(gate.waitMs);
    }

    const account = pool.getAvailable();
    if (!account) {
      // No accounts available — check if it's temporary (all in cooldown) or permanent
      const gateInfo = pool.getCapacityGateInfo();
      if (gateInfo.gate_active) {
        // All accounts burned by capacity errors — wait and retry (don't count as attempt)
        log.warn(`No available accounts during capacity gate — waiting ${Math.ceil(gateInfo.gate_remaining_ms / 1000)}s`);
        await sleep(gateInfo.gate_remaining_ms);
        attempt--; // don't count this as a real attempt
        continue;
      }
      record(null, 503, { error: 'No available accounts', provider_request: null });
      return res.status(503).json({ error: 'No available accounts', pool: pool.stats() });
    }

    const url = buildUrl(account.account_id);
    let released = false;
    const release = () => {
      if (!released) { released = true; pool.release(account.id); }
    };

    try {
      if (stream) {
        let prepared = false;
        const result = await callStream(url, account.api_key, body, (chunk) => {
          if (!prepared) {
            res.status(200);
            res.setHeader('Content-Type', 'text/event-stream');
            res.setHeader('Cache-Control', 'no-cache');
            res.setHeader('X-CF-Gateway-Account', account.name || String(account.id));
            prepared = true;
          }
          if (!res.write(chunk)) return new Promise((r) => res.once('drain', r));
        });

        if (result.status === 429) {
          const errInfo = result.text ? result.text.slice(0, 200) : '';
          log.warn(`Account ${account.name} 429 (attempt ${attempt + 1}/${MAX_RETRIES}) errorCode=${result.errorCode} body=${errInfo}`);
          record(account, 429, { error_code: result.errorCode, error: errInfo });
          pool.mark429(account.id, result.errorCode, result.text);
          // Exponential backoff for capacity errors, flat delay for rate limits
          const isCapacity = result.errorCode === 4006 || result.errorCode === 3040;
          if (isCapacity) {
            capacityBackoffMs = capacityBackoffMs ? Math.min(capacityBackoffMs * 2, 30000) : RETRY_DELAY_MS;
            if (attempt < MAX_RETRIES - 1) await sleep(capacityBackoffMs);
          } else {
            if (attempt < MAX_RETRIES - 1) await sleep(RETRY_DELAY_MS);
          }
          continue;
        }
        if (result.status === 403) {
          // Bad API key or suspended — deactivate and try next account
          log.warn(`Account ${account.name} 403 — deactivating`);
          record(account, 403, { error: result.text?.slice(0, 200) });
          pool.deactivate(account.id);
          continue;
        }
        if (result.status >= 500) {
          // CF server error — cooldown and retry
          log.warn(`Account ${account.name} stream -> ${result.status}: ${result.text?.slice(0, 200)}`);
          record(account, result.status, { error: result.text?.slice(0, 200) });
          pool.markError(account.id);
          if (attempt < MAX_RETRIES - 1) await sleep(RETRY_DELAY_MS);
          continue;
        }
        if (result.status >= 400) {
          // Client error (400/401/404/etc) — return immediately, retrying won't help
          log.warn(`Account ${account.name} stream -> ${result.status}: ${result.text?.slice(0, 200)}`);
          record(account, result.status, { error: result.text?.slice(0, 200) });
          return res.status(result.status).type('application/json').send(result.text);
        }
        log.info(`Account ${account.name} stream -> 200 (attempt ${attempt + 1})`);
        record(account, 200, { usage: result.usage });
        pool.markSuccess(account.id, model, result.usage);
        return res.end();
      }

      // Non-streaming
      const result = await callNormal(url, account.api_key, body);
      if (result.status === 429) {
        const errInfo = result.text ? result.text.slice(0, 200) : '';
        log.warn(`Account ${account.name} 429 (attempt ${attempt + 1}/${MAX_RETRIES}) errorCode=${result.errorCode} body=${errInfo}`);
        record(account, 429, { error_code: result.errorCode, error: errInfo });
        pool.mark429(account.id, result.errorCode, result.text);
        // Exponential backoff for capacity errors, flat delay for rate limits
        const isCapacity = result.errorCode === 4006 || result.errorCode === 3040;
        if (isCapacity) {
          capacityBackoffMs = capacityBackoffMs ? Math.min(capacityBackoffMs * 2, 30000) : RETRY_DELAY_MS;
          if (attempt < MAX_RETRIES - 1) await sleep(capacityBackoffMs);
        } else {
          if (attempt < MAX_RETRIES - 1) await sleep(RETRY_DELAY_MS);
        }
        continue;
      }
      if (result.status === 403) {
        log.warn(`Account ${account.name} 403 — deactivating`);
        record(account, 403, { error: result.text?.slice(0, 200) });
        pool.deactivate(account.id);
        continue;
      }
      if (result.status >= 500) {
        log.warn(`Account ${account.name} -> ${result.status}: ${result.text?.slice(0, 200)}`);
        record(account, result.status, { error: result.text?.slice(0, 200) });
        pool.markError(account.id);
        if (attempt < MAX_RETRIES - 1) await sleep(RETRY_DELAY_MS);
        continue;
      }
      if (result.status >= 400) {
        log.warn(`Account ${account.name} -> ${result.status}: ${result.text?.slice(0, 200)}`);
        record(account, result.status, { error: result.text?.slice(0, 200) });
        return res.status(result.status).type('application/json').send(result.text);
      }
      log.info(`Account ${account.name} -> 200 (attempt ${attempt + 1})`);
      record(account, 200, { usage: result.usage });
      pool.markSuccess(account.id, model, result.usage);
      return res.json(result.json);
    } catch (e) {
      log.warn(`Account ${account.name} error: ${e.message}`);
      record(account, 'error', { error: e.message });
      if (res.headersSent) return res.destroy(e);
      continue;
    } finally {
      release();
    }
  }

  record(null, 502, { error: 'All retries failed' });
  if (res.headersSent) return res.end();
  return res.status(502).json({ error: 'All retries failed', pool: pool.stats() });
}

// === ROUTES ===

// Health
app.get('/health', (_req, res) => res.json({ status: 'ok', pool: pool.stats() }));

// OpenAI: /v1/models
app.get('/v1/models', (_req, res) => {
  const stats = pool.stats();
  // Return a basic model list (live CF list would require a working account)
  res.json({
    object: 'list',
    data: [
      { id: '@cf/meta/llama-3.3-70b-instruct-fp8-fast', object: 'model', owned_by: 'cloudflare' },
      { id: '@cf/meta/llama-3.1-8b-instruct-fp8-fast', object: 'model', owned_by: 'cloudflare' },
      { id: '@cf/moonshotai/kimi-k2.7-code', object: 'model', owned_by: 'cloudflare' },
      { id: '@cf/moonshotai/kimi-k2.6', object: 'model', owned_by: 'cloudflare' },
      { id: '@cf/zai-org/glm-5.2', object: 'model', owned_by: 'cloudflare' },
      { id: '@cf/meta/llama-3.2-1b-instruct', object: 'model', owned_by: 'cloudflare' },
      { id: '@cf/meta/llama-3.2-3b-instruct', object: 'model', owned_by: 'cloudflare' },
      { id: '@cf/mistralai/mistral-small-3.1-24b-instruct', object: 'model', owned_by: 'cloudflare' },
    ],
  });
});

// OpenAI: /v1/chat/completions
app.post('/v1/chat/completions', async (req, res) => {
  const body = req.body;
  if (!body || typeof body !== 'object') return res.status(400).json({ error: 'Invalid JSON' });
  if (!body.model) return res.status(400).json({ error: 'model required' });

  const clientRequest = captureBody(body);
  const r = resolveModel(body.model);
  if (r.error) return res.status(r.status).json({ error: r.error });
  body.model = r.id;
  if (Array.isArray(body.messages)) body.messages = flattenContent(body.messages);

  return withPool({
    res,
    buildUrl: (id) => chatUrl(id),
    body,
    model: r.id,
    stream: body.stream === true,
    endpoint: 'chat',
    clientRequest,
  });
});

// OpenAI: /v1/embeddings
app.post('/v1/embeddings', async (req, res) => {
  const body = req.body;
  if (!body || typeof body !== 'object') return res.status(400).json({ error: 'Invalid JSON' });
  if (!body.model) return res.status(400).json({ error: 'model required' });

  const clientRequest = captureBody(body);
  const r = resolveModel(body.model);
  if (r.error) return res.status(r.status).json({ error: r.error });
  body.model = r.id;

  return withPool({
    res,
    buildUrl: (id) => embeddingsUrl(id),
    body,
    model: r.id,
    stream: false,
    endpoint: 'embeddings',
    clientRequest,
  });
});

// CF passthrough: /ai/run/*
app.post('/ai/run/*', async (req, res) => {
  const model = req.params[0];
  if (!model) return res.status(400).json({ error: 'model required in path' });
  const body = req.body ?? {};
  const clientRequest = captureBody({ model, ...body });
  const r = resolveModel(model);
  if (r.error) return res.status(r.status).json({ error: r.error });

  return withPool({
    res,
    buildUrl: (id) => runUrl(id, r.id),
    body,
    model: r.id,
    stream: false,
    endpoint: 'run',
    clientRequest,
  });
});

// Admin: /api/stats
app.get('/api/stats', (_req, res) => res.json(pool.stats()));

// Admin: /api/accounts
function shapeAccount(row) {
  const today = todayUTC();
  const usedToday = row.neurons_day === today ? row.neurons_today : 0;
  const reqsToday = row.neurons_day === today ? row.requests_today : 0;
  const now = Date.now() / 1000;
  const inCooldown = row.cooldown_until > now;
  let status = 'available';
  if (!row.is_active) status = 'inactive';
  else if (usedToday >= NEURON_FREE_DAILY) status = 'exhausted';
  else if (inCooldown) status = 'cooldown';
  return {
    id: row.id,
    name: row.name,
    account_id: row.account_id.slice(0, 8),
    is_active: !!row.is_active,
    status,
    neurons_today: Math.round(usedToday),
    neurons_remaining: Math.max(0, Math.round(NEURON_FREE_DAILY - usedToday)),
    neurons_free_daily: NEURON_FREE_DAILY,
    requests_today: reqsToday,
    cooldown_seconds: inCooldown ? Math.round(row.cooldown_until - now) : 0,
  };
}

app.get('/api/accounts', (_req, res) => {
  const rows = db.prepare('SELECT * FROM accounts ORDER BY id').all();
  const accounts = rows.map(shapeAccount);
  // Sort: available first, then cooldown, then exhausted/inactive
  const order = { available: 0, cooldown: 1, exhausted: 2, inactive: 3 };
  accounts.sort((a, b) => (order[a.status] ?? 9) - (order[b.status] ?? 9) || a.id - b.id);
  res.json({ accounts, stats: pool.stats() });
});

// Admin: /api/import
app.post('/api/import', (_req, res) => {
  try {
    const result = importFrom9router(db, NINE_DB, log);
    res.json(result);
  } catch (e) {
    log.error(`import failed: ${e.message}`);
    res.status(500).json({ error: e.message });
  }
});

// Admin: POST /api/accounts — add single account
app.post('/api/accounts', (req, res) => {
  const { name, api_key, account_id } = req.body || {};
  if (!api_key || !account_id) return res.status(400).json({ error: 'api_key and account_id required' });
  try {
    const ins = db.prepare('INSERT OR IGNORE INTO accounts (name, api_key, account_id) VALUES (?, ?, ?)');
    const r = ins.run(name || `cf-${account_id.slice(0, 6)}`, api_key, account_id);
    if (r.changes === 0) return res.status(409).json({ error: 'Account already exists' });
    log.info(`Added account ${name || account_id.slice(0, 8)}`);
    res.json({ ok: true, id: r.lastInsertRowid });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Admin: DELETE /api/accounts/:id — remove account
app.delete('/api/accounts/:id', (req, res) => {
  const r = db.prepare('DELETE FROM accounts WHERE id = ?').run(req.params.id);
  if (r.changes === 0) return res.status(404).json({ error: 'Not found' });
  log.info(`Deleted account #${req.params.id}`);
  res.json({ ok: true });
});

// Admin: POST /api/accounts/bulk — bulk import from JSON array
app.post('/api/accounts/bulk', (req, res) => {
  const { accounts } = req.body || {};
  if (!Array.isArray(accounts)) return res.status(400).json({ error: 'accounts array required' });
  const ins = db.prepare('INSERT OR IGNORE INTO accounts (name, api_key, account_id) VALUES (?, ?, ?)');
  let imported = 0, skipped = 0;
  for (const a of accounts) {
    if (!a.api_key || !a.account_id) { skipped++; continue; }
    const r = ins.run(a.name || `cf-${a.account_id.slice(0, 6)}`, a.api_key, a.account_id);
    if (r.changes > 0) imported++; else skipped++;
  }
  log.info(`Bulk import: ${imported} added, ${skipped} skipped`);
  res.json({ imported, skipped, total: db.prepare('SELECT COUNT(*) AS c FROM accounts').get().c });
});

// Admin: /api/logs
app.get('/api/logs', (_req, res) => res.json({ logs: requestLog.all() }));
app.delete('/api/logs', (_req, res) => { requestLog.clear(); res.json({ cleared: true }); });

// Dashboard (static HTML)
const indexPath = join(__dirname, 'public', 'index.html');
if (existsSync(indexPath)) {
  app.use(express.static(join(__dirname, 'public'), { maxAge: 0 }));
  app.get('/', (_req, res) => res.sendFile(indexPath));
}

// --- Start ---
const stats = pool.stats();
app.listen(PORT, HOST, () => {
  log.info(`cf-gateway ready on ${HOST}:${PORT} — ${stats.total} accounts (${stats.available} available)`);
  if (stats.total === 0) log.info('pool empty — POST /api/import to load accounts from 9router DB');
});
