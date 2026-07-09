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
const MAX_RETRIES = parseInt(process.env.CF_GATEWAY_MAX_RETRIES || '20', 10);
const RETRY_DELAY_MS = parseInt(process.env.CF_GATEWAY_RETRY_DELAY_MS || '1000', 10);
const REQUEST_DEADLINE_MS = parseInt(process.env.CF_GATEWAY_REQUEST_DEADLINE_MS || '120000', 10);

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const startedAt = Date.now();
let lastSuccessAt = null;

// --- Concurrency limiter ---
const MAX_CONCURRENT = parseInt(process.env.CF_GATEWAY_MAX_CONCURRENT || '4', 10);
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

// --- Sync modelLocks from 9router (disabled by default) ---
const SYNC_INTERVAL_MS = parseInt(process.env.CF_GATEWAY_SYNC_INTERVAL_MS || '30000', 10);
const SYNC_LOCKS = process.env.CF_GATEWAY_SYNC_LOCKS === 'true';
function doSync() {
  if (!SYNC_LOCKS) return;
  const result = pool.syncModelLocks(NINE_DB);
  if (result.synced > 0) {
    log.info(`modelLock sync: ${result.synced}/${result.total} accounts locked`);
  }
}
if (SYNC_LOCKS) {
  doSync(); // initial sync
  setInterval(doSync, SYNC_INTERVAL_MS);
} else {
  log.info('modelLock sync: DISABLED (set CF_GATEWAY_SYNC_LOCKS=true to enable)');
}

// --- Backoff decay --- decay stuck backoff levels every 5 minutes
const DECAY_INTERVAL_MS = parseInt(process.env.CF_GATEWAY_DECAY_INTERVAL_MS || '300000', 10);
const DECAY_THRESHOLD_SEC = parseInt(process.env.CF_GATEWAY_DECAY_THRESHOLD_SEC || '600', 10);
function doDecay() {
  pool.decayBackoff(DECAY_THRESHOLD_SEC);
  pool.decayLocks(DECAY_THRESHOLD_SEC);
}
doDecay(); // initial decay
setInterval(doDecay, DECAY_INTERVAL_MS);

// --- Probing --- test locked accounts every 10 minutes
const PROBE_INTERVAL_MS = parseInt(process.env.CF_GATEWAY_PROBE_INTERVAL_MS || '600000', 10);
async function doProbe() {
  const model = '@cf/zai-org/glm-5.2'; // Default model for probing
  const account = pool.getProbeAccount(model);
  if (!account) return;
  
  try {
    const url = chatUrl(account.account_id);
    const result = await callNormal(url, account.api_key, {
      model: '@cf/zai-org/glm-5.2',
      messages: [{ role: 'user', content: 'hi' }],
      max_tokens: 1,
    });
    
    if (result.status === 200) {
      pool.markProbeResult(account.id, true);
      log.info(`probe: account #${account.id} (${account.name}) recovered!`);
    } else {
      pool.markProbeResult(account.id, false);
      log.debug(`probe: account #${account.id} still failing (${result.status})`);
    }
  } catch (e) {
    pool.markProbeResult(account.id, false);
    log.debug(`probe: account #${account.id} error: ${e.message}`);
  }
}
setInterval(doProbe, PROBE_INTERVAL_MS);

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

// --- Core: retry across pool with per-model locks and capacity gate ---
async function withPool({ res, buildUrl, body, stream, model, endpoint, clientRequest }) {
  await acquireSlot();
  try {
    return await _withPoolInner({ res, buildUrl, body, stream, model, endpoint, clientRequest });
  } finally {
    releaseSlot();
  }
}

async function _withPoolInner({ res, buildUrl, body, stream, model, endpoint, clientRequest }) {
  const reqStartedAt = Date.now();
  const clientReq = clientRequest ?? captureBody(body);

  const record = (account, status, extra = {}) => {
    requestLog.record({
      endpoint,
      model,
      account_id: account?.id ?? null,
      account_name: account?.name ?? null,
      status,
      stream,
      latency_ms: Date.now() - reqStartedAt,
      client_request: clientReq,
      provider_request: captureBody(body),
      ...extra,
    });
  };

  let consecutiveCapacityErrors = 0;

  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    // Deadline check
    if (Date.now() - reqStartedAt > REQUEST_DEADLINE_MS) {
      log.warn(`Deadline exceeded (${Math.ceil((Date.now() - reqStartedAt) / 1000)}s) after ${attempt} attempts`);
      record(null, 504, { error: 'Request deadline exceeded', attempts: attempt });
      return res.status(504).json({ error: 'Request deadline exceeded', attempts: attempt });
    }

    // Soft stop: 5 consecutive capacity errors = stop retrying this request
    if (consecutiveCapacityErrors >= 5) {
      log.warn(`Soft stop: ${consecutiveCapacityErrors} consecutive capacity errors`);
      record(null, 503, { error: 'Consecutive capacity errors', count: consecutiveCapacityErrors });
      return res.status(503).json({ error: 'CF capacity exhausted', pool: pool.stats() });
    }

    // Hard capacity stop: global CF overload, wait briefly before next account
    if (consecutiveCapacityErrors >= 3 && attempt < MAX_RETRIES - 1) {
      await sleep(2000 + Math.floor(Math.random() * 1000));
    }

    const account = pool.getAvailable(model);
    if (!account) {
      record(null, 503, { error: 'No available accounts' });
      return res.status(503).json({ error: 'No available accounts', pool: pool.stats() });
    }

    const url = buildUrl(account.account_id);
    let released = false;
    const release = () => {
      if (!released) { released = true; pool.release(account.id); }
    };

    try {
      if (stream) {
        const opened = await openStream(url, account.api_key, body);
        if (opened.status === 429) {
          const classification = pool.mark429(account.id, opened.errorCode, opened.text, model);
          log.warn(`Account ${account.name} stream 429 (attempt ${attempt + 1}/${MAX_RETRIES}) errorCode=${opened.errorCode} action=${classification.action}`);
          record(account, 429, { error_code: opened.errorCode, action: classification.action });
          release();
          
          if (classification.action === 'capacity') {
            consecutiveCapacityErrors++;
          } else {
            consecutiveCapacityErrors = 0;
          }
          continue;
        }
        if (opened.status >= 400) {
          log.warn(`Account ${account.name} stream -> ${opened.status}`);
          record(account, opened.status, { error: opened.text?.slice(0, 200) });
          release();
          continue;
        }

        // Stream opened
        res.status(200);
        res.setHeader("Content-Type", "text/event-stream");
        res.setHeader("Cache-Control", "no-cache");
        res.setHeader("X-Accel-Buffering", "no");
        res.setHeader("X-CF-Gateway-Account", account.name || String(account.id));
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
        log.info(`Account ${account.name} stream -> 200`);
        record(account, 200, { usage });
        pool.markSuccess(account.id, model, usage);
        lastSuccessAt = new Date().toISOString();
        return res.end();
      }

      // Non-streaming
      const result = await callNormal(url, account.api_key, body);
      if (result.status === 429) {
        const classification = pool.mark429(account.id, result.errorCode, result.text, model);
        log.warn(`Account ${account.name} 429 (attempt ${attempt + 1}/${MAX_RETRIES}) errorCode=${result.errorCode} action=${classification.action}`);
        record(account, 429, { error_code: result.errorCode, action: classification.action });
        
        if (classification.action === 'capacity') {
          consecutiveCapacityErrors++;
          // No delay for capacity errors, immediately try next
        } else {
          consecutiveCapacityErrors = 0;
          if (attempt < MAX_RETRIES - 1) await sleep(RETRY_DELAY_MS + Math.floor(Math.random() * 300));
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
        log.warn(`Account ${account.name} -> ${result.status}`);
        record(account, result.status, { error: result.text?.slice(0, 200) });
        pool.markError(account.id);
        continue;
      }
      if (result.status >= 400) {
        log.warn(`Account ${account.name} -> ${result.status}`);
        record(account, result.status, { error: result.text?.slice(0, 200) });
        return res.status(result.status).type('application/json').send(result.text);
      }
      
      // Success!
      log.info(`Account ${account.name} -> 200 (attempt ${attempt + 1})`);
      record(account, 200, { usage: result.usage });
      pool.markSuccess(account.id, model, result.usage);
      lastSuccessAt = new Date().toISOString();
      consecutiveCapacityErrors = 0;
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
app.get('/health', (_req, res) => {
  const stats = pool.stats();
  const uptime = Math.floor((Date.now() - startedAt) / 1000);
  let status = 'ok';
  if (stats.available === 0) status = 'down';
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
      exhausted_ratio: stats.exhausted_ratio,
      used_ratio: stats.used_ratio,
    },
    neurons: {
      used_today: stats.neurons_used_today,
      used_ratio: stats.used_ratio,
      capacity_today: stats.neurons_capacity_today,
      remaining_today: stats.neurons_remaining_today,
    },
    requests_today: stats.requests_today,
    concurrency: {
      max: MAX_CONCURRENT,
      in_flight: _inFlight,
      queued: _waitQueue.length,
    },
    last_success: lastSuccessAt,
    capacity_failures: stats.capacity_failures,
    model_locks: stats.model_locks,
    probing: stats.probing,
    features: {
      model_lock_sync: SYNC_LOCKS,
      per_model_locks: true,
      backoff_decay: true,
      probing: true,
      decay_interval_ms: DECAY_INTERVAL_MS,
      decay_threshold_sec: DECAY_THRESHOLD_SEC,
    },
  });
});

// OpenAI: /v1/models
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

// Admin: /api/models
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
  return withPool({ res, buildUrl: (id) => chatUrl(id), body, model: r.id, stream: body.stream === true, endpoint: 'chat', clientRequest });
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
  return withPool({ res, buildUrl: (id) => embeddingsUrl(id), body, model: r.id, stream: false, endpoint: 'embeddings', clientRequest });
});

// Admin: /api/import
app.post('/api/import', (_req, res) => {
  try {
    const result = importFrom9router(NINE_DB, db);
    invalidateModels();
    log.info(`Import: ${result.imported} imported, ${result.skipped} skipped, ${result.total} total`);
    res.json(result);
  } catch (e) {
    log.error(`Import failed: ${e.message}`);
    res.status(500).json({ error: e.message });
  }
});

// Admin: /api/stats
app.get('/api/stats', (_req, res) => {
  res.json({ ...pool.stats(), last_success: lastSuccessAt, uptime: Math.floor((Date.now() - startedAt) / 1000) });
});

// Admin: /api/accounts
app.get('/api/accounts', (req, res) => {
  const page = Math.max(1, parseInt(req.query.page || '1', 10));
  const perPage = Math.min(100, Math.max(1, parseInt(req.query.per_page || '20', 10)));
  const offset = (page - 1) * perPage;
  const rows = db.prepare('SELECT * FROM accounts ORDER BY id LIMIT ? OFFSET ?').all(perPage, offset);
  const total = db.prepare('SELECT COUNT(*) AS c FROM accounts').get().c;
  const today = todayUTC();
  const now = Date.now() / 1000;

  // Get all model locks for these accounts
  const accountIds = rows.map(r => r.id);
  const locksByAccount = {};
  if (accountIds.length > 0) {
    const placeholders = accountIds.map(() => '?').join(',');
    const locks = db.prepare(
      `SELECT account_id, model, locked_until, error_count FROM model_locks 
       WHERE account_id IN (${placeholders}) AND locked_until > ?`
    ).all(...accountIds, now);
    for (const lock of locks) {
      if (!locksByAccount[lock.account_id]) locksByAccount[lock.account_id] = [];
      locksByAccount[lock.account_id].push({
        model: lock.model,
        locked_until: lock.locked_until,
        cooldown_seconds: Math.ceil(lock.locked_until - now),
        error_count: lock.error_count,
      });
    }
  }

  // Compute status and display fields
  const accounts = rows.map(row => {
    const sameDay = row.neurons_day === today;
    const neuronsUsed = sameDay ? row.neurons_today : 0;
    const neuronsRemaining = Math.max(0, NEURON_FREE_DAILY - neuronsUsed);
    const cooldownSeconds = row.cooldown_until > now ? Math.ceil(row.cooldown_until - now) : 0;
    const modelLocks = locksByAccount[row.id] || [];

    let status;
    if (!row.is_active) status = 'inactive';
    else if (neuronsUsed >= NEURON_FREE_DAILY) status = 'exhausted';
    else if (cooldownSeconds > 0) status = 'cooldown';
    else if (modelLocks.length > 0) status = 'locked';
    else status = 'available';

    return {
      id: row.id,
      name: row.name,
      account_id: row.account_id,
      status,
      neurons_today: neuronsUsed,
      neurons_free_daily: NEURON_FREE_DAILY,
      neurons_remaining: neuronsRemaining,
      requests_today: sameDay ? row.requests_today : 0,
      cooldown_seconds: cooldownSeconds,
      backoff_level: row.backoff_level,
      error_count: row.error_count,
      model_locks: modelLocks,
    };
  });

  res.json({ accounts, page, per_page: perPage, total, total_pages: Math.ceil(total / perPage) });
});

// Admin: /api/logs
app.get('/api/logs', (_req, res) => {
  res.json({ logs: requestLog.all(), count: requestLog.all().length });
});
app.delete('/api/logs', (_req, res) => {
  requestLog.clear();
  res.json({ ok: true });
});

// Dashboard
app.get('/', (_req, res) => {
  const htmlPath = join(__dirname, 'public', 'index.html');
  if (existsSync(htmlPath)) {
    res.type('html').send(readFileSync(htmlPath, 'utf-8'));
  } else {
    res.type('html').send('<h1>CF Gateway</h1><p>Dashboard not built. See /health</p>');
  }
});

app.listen(PORT, HOST, () => {
  log.info(`CF Gateway listening on ${HOST}:${PORT} — ${MAX_RETRIES} retries, ${MAX_CONCURRENT} concurrent, ${REQUEST_DEADLINE_MS / 1000}s deadline`);
});
