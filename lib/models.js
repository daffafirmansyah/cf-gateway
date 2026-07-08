// Live model list from Cloudflare, cached in-memory.
// Fetches from CF /ai/models/search API, filters out partner models.
// Cache TTL: 10 minutes.

const CF_BASE = 'https://api.cloudflare.com/client/v4/accounts';
const TTL_MS = 10 * 60 * 1000;
const PER_PAGE = 100;

let cache = { at: 0, models: null, byShort: null, byMid: null };

export async function getModels(pickAccount, logWarn, { fresh = false } = {}) {
  if (!fresh && cache.models && Date.now() - cache.at < TTL_MS) return cache.models;

  const account = pickAccount();
  if (!account) return cache.models || [];

  try {
    const raw = [];
    for (let page = 1; page <= 10; page++) {
      const url = `${CF_BASE}/${account.account_id}/ai/models/search?per_page=${PER_PAGE}&page=${page}`;
      const res = await fetch(url, {
        headers: { Authorization: `Bearer ${account.api_key}` },
        signal: AbortSignal.timeout(15000),
      });
      if (!res.ok) {
        logWarn?.(`model list fetch page ${page} -> ${res.status}`);
        break;
      }
      const body = await res.json();
      const rows = body.result || [];
      raw.push(...rows);
      if (rows.length < PER_PAGE) break;
    }

    const models = raw
      .filter((m) => !isPartner(m))
      .map((m) => ({
        id: m.name,
        name: m.name,
        description: m.description || '',
        task: m.task?.name || '',
        tags: m.tags || [],
        created_at: m.created_at || '',
        partner: false,
        capabilities: capabilityFlags(m),
      }))
      .sort((a, b) => (b.created_at || '').localeCompare(a.created_at || ''));

    // short = last path segment; mid = vendor/model (disambiguates collisions)
    const byShort = new Map();
    const byMid = new Map();
    for (const m of models) {
      const i = m.name.lastIndexOf('/');
      const short = i >= 0 ? m.name.slice(i + 1) : m.name;
      (byShort.get(short) ?? byShort.set(short, []).get(short)).push(m.name);
      const mid = m.name.startsWith('@cf/') ? m.name.slice(4) : m.name;
      (byMid.get(mid) ?? byMid.set(mid, []).get(mid)).push(m.name);
    }
    cache = { at: Date.now(), models, byShort, byMid };
    logWarn?.(`model list refreshed: ${models.length} models`);
    return models;
  } catch (e) {
    logWarn?.(`model list fetch error: ${e.message}; serving ${cache.models ? 'stale cache' : 'empty'}`);
    return cache.models || [];
  }
}

/**
 * Resolve client-supplied model id to full CF id (@cf/vendor/model).
 * Accepts: full id, short id (last segment), or mid (vendor/model).
 */
export async function resolveModel(input, pickAccount, logWarn) {
  if (!input || typeof input !== 'string') return { error: 'model required', status: 400 };
  const s = input.trim().replace(/^cf\//, '');
  if (!s) return { error: 'model required', status: 400 };

  // Already full id
  if (s.startsWith('@cf/')) return { id: s };
  if (s.includes('/') && !s.startsWith('@')) return { id: `@cf/${s}` };

  // Short id — try cache
  if (!cache.byShort) await getModels(pickAccount, logWarn);
  if (!cache.byShort) {
    // Fallback: pass through (CF will reject if invalid)
    return { id: s };
  }

  const hits = cache.byShort.get(s);
  if (!hits || hits.length === 0) {
    // Not in cache — pass through anyway (new model not in list yet)
    return { id: s };
  }
  if (hits.length === 1) return { id: hits[0] };
  return {
    error: `ambiguous model "${s}" — ${hits.length} matches, use vendor/model`,
    candidates: hits,
    status: 409,
  };
}

function isPartner(m) {
  return (m.properties || []).some((p) => p.property_id === 'partner' && p.value === true);
}

function capabilityFlags(m) {
  const ids = new Set((m.properties || []).map((p) => p.property_id));
  const flags = [];
  if (ids.has('vision')) flags.push('vision');
  if (ids.has('reasoning')) flags.push('reasoning');
  if (ids.has('function_calling')) flags.push('tools');
  if (ids.has('realtime')) flags.push('realtime');
  if (ids.has('lora')) flags.push('lora');
  if (ids.has('async_queue')) flags.push('async');
  return flags;
}

export function invalidateModels() {
  cache = { at: 0, models: null, byShort: null, byMid: null };
}
