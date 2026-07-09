import Database from 'better-sqlite3';
import { estimate, todayUTC, NEURON_FREE_DAILY } from './neurons.js';

// Smart cooldown: 2^level seconds, cap 3600s (1h)
// Level 0=1s, 1=2s, 2=4s, 3=8s, 4=16s, 5=32s, 6=64s, 7=128s, 8=256s, 9=512s, 10=1024s, 11=2048s, 12=3600s
const COOLDOWN_CAP_SEC = 3600;
const MAX_BACKOFF_LEVEL = 12;

// Probing: test locked accounts every 10 minutes
const PROBE_INTERVAL_SEC = 600;

export class AccountPool {
  constructor(db, { cooldown429 = 90, log = console, reserveNeurons = 250 } = {}) {
    this.db = db;
    this.cooldown429 = cooldown429;
    this.log = log;
    this._cursor = 0;
    this._nineDb = null;
    this._nineDbPath = null;

    // In-flight neuron reservations
    this._reserved = new Map();
    this._reservedDay = todayUTC();
    this.reserveNeurons = reserveNeurons;

    // Consecutive capacity failure tracking (no gate)
    this._consecutiveCapacityFailures = 0;

    // Probing state
    this._lastProbeAt = 0;
    this._probingAccounts = new Set();

    // Pre-compiled statements
    this._selAvail = db.prepare(
      `SELECT * FROM accounts
       WHERE is_active = 1 AND cooldown_until < ?
         AND (CASE WHEN neurons_day = ? THEN neurons_today ELSE 0 END) < ?
       ORDER BY backoff_level ASC, (CASE WHEN neurons_day = ? THEN neurons_today ELSE 0 END) ASC, id
       LIMIT 1 OFFSET ?`
    );
    this._countEligible = db.prepare(
      `SELECT COUNT(*) AS c FROM accounts
       WHERE is_active = 1 AND cooldown_until < ?
         AND (CASE WHEN neurons_day = ? THEN neurons_today ELSE 0 END) < ?`
    );
    this._candidates = db.prepare(
      `SELECT id, neurons_today, neurons_day FROM accounts
       WHERE is_active = 1 AND cooldown_until < ?
         AND (CASE WHEN neurons_day = ? THEN neurons_today ELSE 0 END) < ?`
    );
    this._get = db.prepare('SELECT * FROM accounts WHERE id = ?');
    this._peek = db.prepare(
      'SELECT account_id, api_key FROM accounts WHERE is_active = 1 ORDER BY id LIMIT 1'
    );
    this._markExhausted = db.prepare(
      `UPDATE accounts SET neurons_today = ?, neurons_day = ?, cooldown_until = 0 WHERE id = ?`
    );
    this._countTotal = db.prepare('SELECT COUNT(*) AS c FROM accounts WHERE is_active = 1');
    this._countCooldown = db.prepare(
      `SELECT COUNT(*) AS c FROM accounts WHERE is_active = 1 AND cooldown_until > ? AND neurons_day = ? AND neurons_today < ?`
    );
    this._countExhausted = db.prepare(
      `SELECT COUNT(*) AS c FROM accounts WHERE is_active = 1 AND neurons_day = ? AND neurons_today >= ?`
    );

    // Per-model lock statements
    this._getLock = db.prepare(
      'SELECT * FROM model_locks WHERE account_id = ? AND model = ?'
    );
    this._setLock = db.prepare(
      `INSERT INTO model_locks (account_id, model, locked_until, error_count, last_error_at)
       VALUES (?, ?, ?, 1, ?)
       ON CONFLICT(account_id, model) DO UPDATE SET
         locked_until = MAX(model_locks.locked_until, excluded.locked_until),
         error_count = model_locks.error_count + 1,
         last_error_at = excluded.last_error_at`
    );
    this._clearLock = db.prepare(
      'DELETE FROM model_locks WHERE account_id = ? AND model = ?'
    );
    this._clearExpiredLocks = db.prepare(
      'DELETE FROM model_locks WHERE locked_until < ?'
    );
    this._countLockedForModel = db.prepare(
      `SELECT COUNT(*) AS c FROM model_locks WHERE model = ? AND locked_until > ?`
    );
    this._isAccountLockedForModel = db.prepare(
      `SELECT locked_until FROM model_locks WHERE account_id = ? AND model = ? AND locked_until > ?`
    );
    this._getLockedAccounts = db.prepare(
      `SELECT account_id, model, locked_until, error_count FROM model_locks WHERE locked_until > ? ORDER BY locked_until ASC`
    );
    this._decayLocks = db.prepare(
      `UPDATE model_locks SET locked_until = 0, error_count = 0 WHERE locked_until > 0 AND last_error_at < ?`
    );
  }

  peekAccount() {
    return this._peek.get() || null;
  }

  /**
   * Sync modelLock state from 9router's DB.
   * Only syncs per-model locks, NOT global backoff.
   */
  syncModelLocks(nineDbPath) {
    try {
      if (!this._nineDb || this._nineDbPath !== nineDbPath) {
        if (this._nineDb) this._nineDb.close();
        this._nineDb = new Database(nineDbPath, { readonly: true, fileMustExist: true });
        this._nineDb.pragma('busy_timeout = 2000');
        this._nineDbPath = nineDbPath;
      }

      const rows = this._nineDb.prepare(
        "SELECT name, data FROM providerConnections WHERE provider = 'cloudflare-ai' AND isActive = 1"
      ).all();

      const now = Date.now() / 1000;
      let synced = 0;

      for (const { name, data: dataStr } of rows) {
        try {
          const data = JSON.parse(dataStr);
          const backoff = data.backoffLevel || 0;

          // Sync per-model locks
          for (const [key, val] of Object.entries(data)) {
            if (key.startsWith('modelLock_') && val) {
              const model = key.replace('modelLock_', '');
              const setTime = new Date(val).getTime() / 1000;
              if (isNaN(setTime)) continue;
              const expiry = setTime + Math.pow(2, Math.min(backoff, MAX_BACKOFF_LEVEL));
              if (expiry > now) {
                // Find account_id by name
                const account = this.db.prepare('SELECT id FROM accounts WHERE name = ?').get(name);
                if (account) {
                  this._setLock.run(account.id, model, expiry, setTime);
                  synced++;
                }
              }
            }
          }
        } catch (e) { /* skip parse errors */ }
      }

      return { synced, total: rows.length };
    } catch (e) {
      this.log.warn?.(`syncModelLocks error: ${e.message}`);
      return { synced: 0, total: 0, error: e.message };
    }
  }

  _rollReservations(today) {
    if (this._reservedDay !== today) {
      this._reserved.clear();
      this._reservedDay = today;
    }
  }

  _effectiveNeurons(row, today) {
    const committed = row.neurons_day === today ? row.neurons_today : 0;
    return committed + (this._reserved.get(row.id) || 0);
  }

  /**
   * Get available account for a specific model.
   * Skips accounts locked for this model.
   */
  getAvailable(model = null) {
    const today = todayUTC();
    this._rollReservations(today);
    const now = Date.now() / 1000;

    const count = this._countEligible.get(now, today, NEURON_FREE_DAILY).c;
    if (count === 0) return null;

    for (let probe = 0; probe < count; probe++) {
      const offset = (this._cursor + probe) % count;
      const row = this._selAvail.get(now, today, NEURON_FREE_DAILY, today, offset);
      if (!row) continue;

      // Check per-model lock
      if (model) {
        const lock = this._isAccountLockedForModel.get(row.id, model, now);
        if (lock) continue; // Account locked for this model, skip
      }

      if (this._effectiveNeurons(row, today) < NEURON_FREE_DAILY) {
        this._cursor = offset + 1;
        this._reserved.set(row.id, (this._reserved.get(row.id) || 0) + this.reserveNeurons);
        return row;
      }
    }
    return null;
  }

  /**
   * Get an account for probing (testing if it's recovered).
   * Returns accounts that are locked but might be recovered.
   */
  getProbeAccount(model) {
    const now = Date.now() / 1000;
    if (now - this._lastProbeAt < PROBE_INTERVAL_SEC) return null;

    const locked = this._getLockedAccounts.all(now);
    if (locked.length === 0) return null;

    // Pick a random locked account for probing
    const idx = Math.floor(Math.random() * locked.length);
    const lock = locked[idx];
    if (this._probingAccounts.has(lock.account_id)) return null;

    this._probingAccounts.add(lock.account_id);
    this._lastProbeAt = now;

    const account = this._get.get(lock.account_id);
    return account;
  }

  /**
   * Mark probe result.
   */
  markProbeResult(accountId, success) {
    this._probingAccounts.delete(accountId);
    if (success) {
      // Clear all locks for this account
      this.db.prepare('DELETE FROM model_locks WHERE account_id = ?').run(accountId);
      this.log.info?.(`probe: account #${accountId} recovered, cleared all locks`);
    }
  }

  release(id) {
    const cur = this._reserved.get(id);
    if (cur === undefined) return;
    const next = cur - this.reserveNeurons;
    if (next > 0) this._reserved.set(id, next);
    else this._reserved.delete(id);
  }

  _countAvailable() {
    const today = todayUTC();
    this._rollReservations(today);
    return this._candidates
      .all(Date.now() / 1000, today, NEURON_FREE_DAILY)
      .filter((r) => this._effectiveNeurons(r, today) < NEURON_FREE_DAILY).length;
  }

  /**
   * Smart error classification and handling.
   * Returns: { action: 'skip'|'retry'|'gate', message: string }
   */
  classifyError(errorCode, errorText) {
    // Daily exhaustion - permanent for today
    if (errorCode === 4006 && errorText && /daily free allocation/i.test(errorText)) {
      return { action: 'exhaust', message: 'Daily neuron budget exhausted' };
    }

    // Global capacity - CF backend overloaded
    if (errorCode === 4006 && errorText && /temporarily at capacity/i.test(errorText)) {
      return { action: 'capacity', message: 'CF backend at capacity' };
    }

    // Rate limit - per-account
    if (errorCode === 429) {
      return { action: 'ratelimit', message: 'Rate limited' };
    }

    // Account suspended
    if (errorCode === 403) {
      return { action: 'deactivate', message: 'Account suspended' };
    }

    // Unknown error
    return { action: 'error', message: `Error ${errorCode}` };
  }

  /**
   * Mark 429/error with smart cooldown and per-model locks.
   */
  mark429(id, errorCode, errorText, model = null) {
    const now = Date.now() / 1000;
    const classification = this.classifyError(errorCode, errorText);

    switch (classification.action) {
      case 'exhaust':
        // Daily exhaustion - mark as exhausted
        this._markExhausted.run(NEURON_FREE_DAILY, todayUTC(), id);
        this.log.info?.(`account #${id} DAILY EXHAUSTED`);
        break;

      case 'capacity':
        // Global capacity - track but don't gate
        this.trackCapacityError();
        this.log.info?.(`account #${id} CF CAPACITY (consecutive: ${this._consecutiveCapacityFailures})`);
        break;

      case 'ratelimit':
        // Per-account rate limit with per-model lock
        this._handleRateLimit(id, now, model);
        break;

      case 'deactivate':
        this.deactivate(id);
        break;

      default:
        // Generic error
        this._handleGenericError(id, now, model);
        break;
    }

    return classification;
  }

  _handleRateLimit(id, now, model) {
    const row = this._get.get(id);
    const currentLock = model ? this._getLock.get(id, model) : null;
    const currentLevel = currentLock?.error_count || 0;
    // 3040 lock lebih agresif: minimum level 1 (2s)
    const newLevel = Math.max(Math.min(currentLevel + 1, MAX_BACKOFF_LEVEL), 1);
    const cooldown = Math.min(Math.pow(2, newLevel), COOLDOWN_CAP_SEC);
    const until = now + cooldown;

    // Set per-model lock
    if (model) {
      this._setLock.run(id, model, until, now);
    }

    // Also set global cooldown (shorter, for general availability)
    const globalCooldown = Math.min(Math.pow(2, Math.min(row?.error_count || 0, 8)), 600);
    this.db.prepare(
      'UPDATE accounts SET cooldown_until = MAX(cooldown_until, ?), error_count = error_count + 1, last_error_at = ? WHERE id = ?'
    ).run(now + globalCooldown, now, id);

    this.log.info?.(`account #${id} rate-limited for model ${model || 'global'}: cool ${cooldown}s (level ${newLevel})`);
  }

  _handleGenericError(id, now, model) {
    const row = this._get.get(id);
    const level = Math.min(row?.error_count || 0, MAX_BACKOFF_LEVEL);
    const cooldown = Math.min(Math.pow(2, level), COOLDOWN_CAP_SEC);
    const until = now + cooldown;

    if (model) {
      this._setLock.run(id, model, until, now);
    }

    this.db.prepare(
      'UPDATE accounts SET cooldown_until = MAX(cooldown_until, ?), error_count = error_count + 1, last_error_at = ? WHERE id = ?'
    ).run(until, now, id);

    this.log.info?.(`account #${id} error for model ${model || 'global'}: cool ${cooldown}s (level ${level})`);
  }

  /**
   * Check if too many consecutive capacity errors (soft limit).
   * Returns true if we should stop retrying this request.
   */
  shouldStopRetrying() {
    return this._consecutiveCapacityFailures >= 5;
  }

  /**
   * Track capacity errors (no gate, just tracking).
   */
  trackCapacityError() {
    this._consecutiveCapacityFailures++;
  }

  /**
   * Reset capacity failure counter on success.
   */
  resetCapacityFailures() {
    this._consecutiveCapacityFailures = 0;
  }

  markError(id) {
    const now = Date.now() / 1000;
    const until = now + this.cooldown429;
    this.db.prepare(
      'UPDATE accounts SET cooldown_until = MAX(cooldown_until, ?), last_error_at = ? WHERE id = ?'
    ).run(until, now, id);
    this.log.info?.(`error -> account #${id} -> cool ${this.cooldown429}s`);
  }

  /**
   * Decay per-model locks for accounts that haven't had errors recently.
   */
  decayLocks(decayThresholdSec = 600) {
    const now = Date.now() / 1000;
    const cutoff = now - decayThresholdSec;
    const result = this._decayLocks.run(cutoff);
    if (result.changes > 0) {
      this.log.info?.(`lock decay: cleared ${result.changes} model locks (no errors in ${decayThresholdSec}s)`);
    }
    return result.changes;
  }

  /**
   * Decay global backoff_level for accounts that haven't had errors recently.
   */
  decayBackoff(decayThresholdSec = 600) {
    const now = Date.now() / 1000;
    const cutoff = now - decayThresholdSec;
    const result = this.db.prepare(
      `UPDATE accounts SET backoff_level = 0
       WHERE backoff_level > 0 AND last_error_at < ? AND last_error_at > 0`
    ).run(cutoff);
    if (result.changes > 0) {
      this.log.info?.(`backoff decay: reset ${result.changes} accounts (no errors in ${decayThresholdSec}s)`);
    }
    return result.changes;
  }

  deactivate(id) {
    this.db.prepare('UPDATE accounts SET is_active = 0 WHERE id = ?').run(id);
    this.log.warn?.(`deactivated account #${id} (403)`);
  }

  markSuccess(id, model, usage) {
    const today = todayUTC();
    const prompt = usage?.prompt_tokens ?? 0;
    const completion = usage?.completion_tokens ?? 0;
    const neurons = estimate(model, prompt, completion, (m) => this.log.warn?.(m));

    const row = this._get.get(id);
    if (!row) return;
    const sameDay = row.neurons_day === today;
    const nextNeurons = (sameDay ? row.neurons_today : 0) + neurons;
    const nextReqs = (sameDay ? row.requests_today : 0) + 1;

    // Clear per-model lock on success
    if (model) {
      this._clearLock.run(id, model);
    }

    // Reset global error state
    this.db
      .prepare(
        `UPDATE accounts
         SET last_used = ?, error_count = 0, backoff_level = 0,
             neurons_today = ?, neurons_day = ?, requests_today = ?
         WHERE id = ?`
      )
      .run(Date.now() / 1000, nextNeurons, today, nextReqs, id);

    // Reset capacity failures on success
    this.resetCapacityFailures();
  }

  stats() {
    const today = todayUTC();
    const now = Date.now() / 1000;
    const total = this._countTotal.get().c;
    const available = this._countAvailable();
    const cooldown = this._countCooldown.get(now, today, NEURON_FREE_DAILY).c;
    const exhausted = this._countExhausted.get(today, NEURON_FREE_DAILY).c;
    const agg = this.db
      .prepare(
        `SELECT COALESCE(SUM(CASE WHEN neurons_day = ? THEN neurons_today ELSE 0 END), 0) AS used,
                COALESCE(SUM(CASE WHEN neurons_day = ? THEN requests_today ELSE 0 END), 0) AS reqs
         FROM accounts WHERE is_active = 1`
      )
      .get(today, today);
    const capacity = total * NEURON_FREE_DAILY;

    // Count per-model locks
    const lockedAccounts = this._getLockedAccounts.all(now);
    const locksByModel = {};
    for (const lock of lockedAccounts) {
      locksByModel[lock.model] = (locksByModel[lock.model] || 0) + 1;
    }

    return {
      total,
      available,
      cooldown,
      exhausted,
      inactive: total - available - cooldown - exhausted,
      exhausted_ratio: total ? +(exhausted / total).toFixed(4) : 0,
      used_ratio: capacity ? +(agg.used / capacity).toFixed(4) : 0,
      neurons_used_today: Math.round(agg.used),
      neurons_capacity_today: capacity,
      neurons_remaining_today: Math.max(0, Math.round(capacity - agg.used)),
      requests_today: agg.reqs,
      capacity_failures: this._consecutiveCapacityFailures,
      model_locks: locksByModel,
      probing: this._probingAccounts.size,
    };
  }
}
