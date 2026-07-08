import 'dotenv/config';
import express from 'express';
import { existsSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

import { initDb } from './lib/db.js';
import { AccountPool } from './lib/pool.js';
import { RequestLog, captureBody } from './lib/log.js';
import { importFrom9router } from './lib/importer.js';
import { chatUrl, embeddingsUrl, runUrl, callNormal, openStream, pumpStream } from './lib/cf.js';
import { NEURON_FREE_DAILY, todayUTC } from './lib/neurons.js';
import { getModels, resolveModel as resolveModelLive, invalidateModels } from './lib/models.js';

const __dirname = dirname(fileURLToPath(import.meta.url));

// --- Config ---
const HOST = process.env.CF_GATEWAY_HOST || '0.0.0.0';
const PORT = parseInt(process.env.CF_GATEWAY_PORT || '8750', 10);
const NINE_DB = process.env.CF_GATEWAY_9ROUTER_DB || join(process.env.HOME || '', '.9router/db/data.sqlite');
const OWN_DB = process.env.CF_GATEWAY_DB || join(__dirname, 'data', 'accounts.db');
const API_KEY = process.env.CF_GATEWAY_API_KEY || '';
const COOLDOWN_429 = parseInt(process.env.CF_GATEWAY_COOLDOWN_429 || '90', 10);
const MAX_RETRIES = parseInt(process.env.CF_GATEWAY_MAX_RETRIES || '50', 10);
const RETRY_DELAY_MS = parseInt(process.env.CF_GATEWAY_RETRY_DELAY_MS || '3000', 10);

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const startedAt = Date.now();
let lastSuccessAt = null;

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

// --- Request Queue (priority: stream > normal) ---
const QUEUE_MAX = parseInt(process.env.CF_GATEWAY_QUEUE_MAX || '50', 10);
const QUEUE_TIMEOUT_MS = parseInt(process.env.CF_GATEWAY_QUEUE_TIMEOUT_MS || '60000', 10);
const _requestQueue = []; // { priority, enqueuedAt, resolve, reject, timer }
let _queueProcessorRunning = false;

function enqueueRequest({ stream, res, runFn }) {
  return new Promise((resolve, reject) => {
    if (_requestQueue.length >= QUEUE_MAX) {
      return reject(Object.assign(new Error('queue_full'), { code: 'QUEUE_FULL' }));
    }

    const priority = stream ? 1 : 2; // 1=high (stream), 2=normal
    const enqueuedAt = Date.now();

    const timer = setTimeout(() => {
      const idx = _requestQueue.findIndex(e => e.enqueuedAt === enqueuedAt);
      if (idx !== -1) _requestQueue.splice(idx, 1);
      reject(Object.assign(new Error('queue_timeout'), { code: 'QUEUE_TIMEOUT' }));
    }, QUEUE_TIMEOUT_MS);

    const entry = { priority, enqueuedAt, resolve, reject, timer, runFn, stream };
    // Insert sorted by priority (lower number = higher priority)
    let inserted = false;
    for (let i = 0; i < _requestQueue.length; i++) {
      if (_requestQueue[i].priority > priority) {
        _requestQueue.splice(i, 0, entry);
        inserted = true;
        break;
      }
    }
    if (!inserted) _requestQueue.push(entry);

    log.info(`Request queued (priority=${priority}, queue_size=${_requestQueue.length})`);
    _processQueue();
  });
}

async function _processQueue() {
  if (_queueProcessorRunning) return;
  _queueProcessorRunning = true;
  try {
    while (_requestQueue.length > 0) {
      const gate = pool.checkCapacityGate();
      if (gate.blocked) {
        log.info(`Queue processor: gate active, waiting ${Math.ceil(gate.waitMs / 1000)}s`);
        await sleep(gate.waitMs);
        continue;
      }
      const entry = _requestQueue.shift();
      if (!entry) break;
      clearTimeout(entry.timer);
      try {
        await entry.runFn();
        entry.resolve();
      } catch (e) {
        entry.reject(e);
      }
    }
  } finally {
    _queueProcessorRunning = false;
  }
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
const PROTECTED = ['/v1', '/api', '/ai'];
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

// --- Model resolution (live from CF API, cached 10min) ---
const pickAccount = () => pool.peekAccount();

async function resolveModel(input) {
  return resolveModelLive(input, pickAccount, log.warn);
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
  // === FAST GATE CHECK — queue instead of reject ===
  const preGate = pool.checkCapacityGate();
  if (preGate.blocked) {
    log.warn(`Capacity gate active — queuing request (would wait ${Math.ceil(preGate.waitMs / 1000)}s)`);
    try {
      await enqueueRequest({
        stream,
        res,
        runFn: () => _withPoolInner({ res, buildUrl, body, stream, model, endpoint, clientRequest }),
      });
      return; // resolved by queue processor
    } catch (e) {
      if (e.code === 'QUEUE_FULL') {
        return res.status(503).json({ error: 'Queue full — try again later', queue_size: _requestQueue.length });
      }
      if (e.code === 'QUEUE_TIMEOUT') {
        return res.status(504).json({ error: 'Queue timeout — request waited too long', timeout_ms: QUEUE_TIMEOUT_MS });
      }
      throw e;
    }
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
        // Try opening stream BEFORE sending headers — allows retry on 429
        const opened = await openStream(url, account.api_key, body);
        if (opened.status === 429) {
          const errInfo = opened.text ? opened.text.slice(0, 200) : '';
          log.warn(`Account ${account.name} stream 429 (attempt ${attempt + 1}/${MAX_RETRIES}) errorCode=${opened.errorCode}`);
          record(account, 429, { error_code: opened.errorCode, error: errInfo });
          pool.mark429(account.id, opened.errorCode, opened.text);
          release();
          // Exponential backoff for capacity errors
          const isCapacity = opened.errorCode === 4006 || opened.errorCode === 3040;
          if (isCapacity) {
            capacityBackoffMs = capacityBackoffMs ? Math.min(capacityBackoffMs * 2, 120000) : RETRY_DELAY_MS;
            if (attempt < MAX_RETRIES - 1) await sleep(capacityBackoffMs);
          } else {
            if (attempt < MAX_RETRIES - 1) await sleep(RETRY_DELAY_MS);
          }
          continue; // retry with next account
        }
        if (opened.status >= 400) {
          log.warn(`Account ${account.name} stream -> ${opened.status}: ${opened.text?.slice(0, 200)}`);
          record(account, opened.status, { error: opened.text?.slice(0, 200) });
          release();
          if (attempt < MAX_RETRIES - 1) await sleep(RETRY_DELAY_MS);
          continue; // retry
        }

        // Stream opened successfully — NOW send headers
        res.status(200);
        res.setHeader('Content-Type', 'text/event-stream');
        res.setHeader('Cache-Control', 'no-cache');
        res.setHeader('X-Accel-Buffering', 'no');
        res.setHeader('X-CF-Gateway-Account', account.name || String(account.id));
        res.flushHeaders();

        let realDataStarted = false;
        const heartbeat = setInterval(() => {
          if (!realDataStarted && !res.writableEnded) res.write(': ping\n');
        }, 15000);

        const writeChunk = (chunk) => {
          if (chunk && chunk.length) realDataStarted = true;
          if (!res.write(chunk)) return new Promise((r) => res.once('drain', r));
        };
        const { usage } = await pumpStream(opened.stream, writeChunk);
        clearInterval(heartbeat);
        log.info(`Account ${account.name} stream -> 200 (attempt ${attempt + 1})`);
        record(account, 200, { usage });
        pool.markSuccess(account.id, model, usage);
        lastSuccessAt = new Date().toISOString();
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
          capacityBackoffMs = capacityBackoffMs ? Math.min(capacityBackoffMs * 2, 120000) : RETRY_DELAY_MS;
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
      lastSuccessAt = new Date().toISOString();
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

  // Determine if failure was due to capacity (503) or other (502)
  const gateInfo = pool.getCapacityGateInfo();
  const isCapacityFailure = gateInfo.gate_active || gateInfo.gate_consecutive > 0;

  if (isCapacityFailure) {
    // 503 = "Service Unavailable" + Retry-After so clients back off properly
    const retryAfterSec = Math.ceil((gateInfo.gate_remaining_ms || gateInfo.gate_cooldown_ms) / 1000);
    res.set('Retry-After', String(retryAfterSec));
    record(null, 503, { error: 'CF capacity exhausted', retry_after: retryAfterSec });
    if (res.headersSent) return res.end();
    return res.status(503).json({
      error: 'Cloudflare capacity exhausted — retry later',
      retry_after_seconds: retryAfterSec,
      pool: pool.stats()
    });
  }

  record(null, 502, { error: 'All retries failed' });
  if (res.headersSent) return res.end();
  return res.status(502).json({ error: 'All retries failed', pool: pool.stats() });
}

// === ROUTES ===

// Health — enhanced
app.get('/health', (_req, res) => {
  const stats = pool.stats();
  const gate = stats.capacity_gate || {};
  const uptime = Math.floor((Date.now() - startedAt) / 1000);

  // Status logic
  let status = 'ok';
  if (stats.available === 0) status = 'down';
  else if (gate.gate_active) status = 'degraded';
  else if (stats.cooldown > stats.total * 0.5) status = 'degraded';

  const code = status === 'down' ? 503 : 200;
  res.status(code).json({
    status,
    uptime,
    accounts: {
      total: stats.total,
      available: stats.available,
      cooldown: stats.cooldown,
      exhausted: stats.exhausted,
      inactive: stats.inactive,
    },
    neurons: {
      used_today: stats.neurons_used_today,
      capacity_today: stats.neurons_capacity_today,
      remaining_today: stats.neurons_remaining_today,
    },
    requests_today: stats.requests_today,
    capacity_gate: {
      active: gate.gate_active || false,
      remaining_ms: gate.gate_remaining_ms || 0,
    },
    concurrency: {
      max: MAX_CONCURRENT,
      in_flight: _inFlight,
      queued: _waitQueue.length,
    },
    queue: {
      size: _requestQueue.length,
      max: QUEUE_MAX,
      timeout_ms: QUEUE_TIMEOUT_MS,
    },
    last_success: lastSuccessAt,
  });
});

// OpenAI: /v1/models — live from CF API
app.get('/v1/models', async (_req, res) => {
  try {
    const models = await getModels(pickAccount, log.warn);
    res.json({
      object: 'list',
      data: models.map((m) => {
        const i = m.id.lastIndexOf('/');
        const short = i >= 0 ? m.id.slice(i + 1) : m.id;
        return { id: m.id, short, object: 'model', owned_by: 'cloudflare' };
      }),
    });
  } catch (e) {
    log.error(`/v1/models failed: ${e.message}`);
    res.status(502).json({ error: 'Failed to fetch model list' });
  }
});

// Admin: /api/models — raw with metadata
app.get('/api/models', async (req, res) => {
  try {
    const fresh = req.query.fresh === '1';
    const models = await getModels(pickAccount, log.warn, { fresh });
    res.json({ models, count: models.length, cached: !fresh });
  } catch (e) {
    log.error(`/api/models failed: ${e.message}`);
    res.status(502).json({ error: 'Failed to fetch model list' });
  }
});

// OpenAI: /v1/chat/completions
app.post('/v1/chat/completions', async (req, res) => {
  const body = req.body;
  if (!body || typeof body !== 'object') return res.status(400).json({ error: 'Invalid JSON' });
  if (!body.model) return res.status(400).json({ error: 'model required' });

  const clientRequest = captureBody(body);
  const r = await resolveModel(body.model);
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
  const r = await resolveModel(body.model);
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
  const r = await resolveModel(model);
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
app.get('/api/stats', (_req, res) => {
  const stats = pool.stats();
  stats.queue = { size: _requestQueue.length, max: QUEUE_MAX, timeout_ms: QUEUE_TIMEOUT_MS };
  stats.concurrency = { max: MAX_CONCURRENT, in_flight: _inFlight, queued: _waitQueue.length };
  stats.last_success = lastSuccessAt;
  res.json(stats);
});

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
