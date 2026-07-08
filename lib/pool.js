import Database from 'better-sqlite3';
import { estimate, todayUTC, NEURON_FREE_DAILY } from './neurons.js';

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
    this._mark429 = db.prepare(
      'UPDATE accounts SET cooldown_until = MAX(cooldown_until, ?), error_count = error_count + 1 WHERE id = ?'
    );
    this._markCooldown = db.prepare(
      'UPDATE accounts SET cooldown_until = MAX(cooldown_until, ?) WHERE id = ?'
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
  }

  peekAccount() {
    return this._peek.get() || null;
  }

  /**
   * Sync modelLock state from 9router's DB.
   * For each account with an active modelLock (set_time + 2^backoffLevel > now),
   * set cooldown_until to the lock expiry timestamp.
   * This prevents cf-gateway from trying accounts that 9router already knows are rate-limited.
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
      const stmtCooldown = this.db.prepare(
        'UPDATE accounts SET cooldown_until = MAX(cooldown_until, ?) WHERE name = ?'
      );

      let synced = 0;
      for (const { name, data: dataStr } of rows) {
        try {
          const data = JSON.parse(dataStr);
          const backoff = data.backoffLevel || 0;
          
          // NOTE: We do NOT sync backoff_level from 9router.
          // cf-gateway manages its own backoff independently.
          // 9router's backoffLevel may be stale (e.g. from non-existent model locks).

          let maxExpiry = 0;
          for (const [key, val] of Object.entries(data)) {
            if (key.startsWith('modelLock_') && val) {
              const setTime = new Date(val).getTime() / 1000;
              if (isNaN(setTime)) continue;
              const expiry = setTime + Math.pow(2, backoff);
              if (expiry > maxExpiry) maxExpiry = expiry;
            }
          }

          if (maxExpiry > now) {
            stmtCooldown.run(maxExpiry, name);
            synced++;
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

  getAvailable() {
    const today = todayUTC();
    this._rollReservations(today);
    const now = Date.now() / 1000;
    const count = this._countEligible.get(now, today, NEURON_FREE_DAILY).c;
    if (count === 0) return null;

    for (let probe = 0; probe < count; probe++) {
      const offset = (this._cursor + probe) % count;
      const row = this._selAvail.get(now, today, NEURON_FREE_DAILY, today, offset);
      if (row && this._effectiveNeurons(row, today) < NEURON_FREE_DAILY) {
        this._cursor = offset + 1;
        this._reserved.set(row.id, (this._reserved.get(row.id) || 0) + this.reserveNeurons);
        return row;
      }
    }
    return null;
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

  mark429(id, errorCode, errorText) {
    const now = Date.now() / 1000;
    if (errorCode === 4006) {
      if (errorText && /daily free allocation/i.test(errorText)) {
        this._markExhausted.run(NEURON_FREE_DAILY, todayUTC(), id);
        this.log.info?.(`429 -> account #${id} code=4006 DAILY EXHAUSTED`);
      } else {
        const row = this._get.get(id);
        const level = Math.min(row?.error_count || 0, 12);
        const cooldown = Math.min(60 * Math.pow(2, level), 3600);
        const until = now + cooldown;
        const newBackoff = Math.min((row?.backoff_level || 0) + 1, 15);
        this.db.prepare(
          'UPDATE accounts SET cooldown_until = MAX(cooldown_until, ?), error_count = error_count + 1, backoff_level = ?, last_error_at = ? WHERE id = ?'
        ).run(until, newBackoff, now, id);
        this.log.info?.(`429 -> account #${id} code=4006 transient -> cool ${cooldown}s (level ${level}, backoff ${newBackoff})`);
      }
    } else if (errorCode === 3040) {
      const until = now + 60;
      this.db.prepare(
        'UPDATE accounts SET cooldown_until = MAX(cooldown_until, ?), error_count = error_count + 1, last_error_at = ? WHERE id = ?'
      ).run(until, now, id);
      this.log.info?.(`429 -> account #${id} code=3040 transient -> cool 60s`);
    } else {
      const until = now + this.cooldown429;
      this.db.prepare(
        'UPDATE accounts SET cooldown_until = MAX(cooldown_until, ?), error_count = error_count + 1, last_error_at = ? WHERE id = ?'
      ).run(until, now, id);
      this.log.info?.(`429 -> account #${id} code=${errorCode} -> cool ${this.cooldown429}s`);
    }
  }

  markError(id) {
    const now = Date.now() / 1000;
    const until = now + this.cooldown429;
    this._markCooldown.run(until, id);
    this.db.prepare('UPDATE accounts SET last_error_at = ? WHERE id = ?').run(now, id);
    this.log.info?.(`error -> account #${id} -> cool ${this.cooldown429}s`);
  }

  /**
   * Decay backoff_level for accounts that haven't had errors recently.
   * Accounts with backoff_level > 0 and no errors in the last DECAY_THRESHOLD
   * get their backoff_level reset to 0.
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

    this.db
      .prepare(
        `UPDATE accounts
         SET last_used = ?, error_count = 0, backoff_level = 0,
             neurons_today = ?, neurons_day = ?, requests_today = ?
         WHERE id = ?`
      )
      .run(Date.now() / 1000, nextNeurons, today, nextReqs, id);
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
    return {
      total,
      available,
      cooldown,
      exhausted,
      inactive: total - available - cooldown - exhausted,
      neurons_used_today: Math.round(agg.used),
      neurons_capacity_today: capacity,
      neurons_remaining_today: Math.max(0, Math.round(capacity - agg.used)),
      requests_today: agg.reqs,
    };
  }
}
