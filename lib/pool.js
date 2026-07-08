import { estimate, todayUTC, NEURON_FREE_DAILY } from './neurons.js';

export class AccountPool {
  constructor(db, { cooldown429 = 90, log = console, reserveNeurons = 250 } = {}) {
    this.db = db;
    this.cooldown429 = cooldown429;
    this.log = log;
    this._cursor = 0;

    // In-flight neuron reservations
    this._reserved = new Map();
    this._reservedDay = todayUTC();
    this.reserveNeurons = reserveNeurons;

    // Pre-compiled statements
    this._selAvail = db.prepare(
      `SELECT * FROM accounts
       WHERE is_active = 1 AND cooldown_until < ?
         AND (CASE WHEN neurons_day = ? THEN neurons_today ELSE 0 END) < ?
       ORDER BY id
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
      'UPDATE accounts SET cooldown_until = ?, error_count = error_count + 1 WHERE id = ?'
    );
    this._markCooldown = db.prepare(
      'UPDATE accounts SET cooldown_until = ? WHERE id = ?'
    );
    this._countTotal = db.prepare('SELECT COUNT(*) AS c FROM accounts WHERE is_active = 1');
    this._countCooldown = db.prepare(
      `SELECT COUNT(*) AS c FROM accounts WHERE is_active = 1 AND cooldown_until > ?`
    );
    this._countExhausted = db.prepare(
      `SELECT COUNT(*) AS c FROM accounts WHERE is_active = 1 AND neurons_day = ? AND neurons_today >= ?`
    );
  }

  peekAccount() {
    return this._peek.get() || null;
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
      const row = this._selAvail.get(now, today, NEURON_FREE_DAILY, offset);
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

  mark429(id, errorCode) {
    const now = Date.now() / 1000;
    const until = now + this.cooldown429;
    if (errorCode === 4006) {
      // 4006 = "Service temporarily at capacity" — transient CF overload, NOT daily limit.
      // Cooldown only, don't increment error_count (account is healthy).
      this._markCooldown.run(until, id);
    } else {
      // Per-minute rate limit — cooldown + track error
      this._mark429.run(until, id);
    }
    const remaining = this._countAvailable();
    this.log.info?.(`429 -> account #${id} code=${errorCode} -> cool ${this.cooldown429}s, ${remaining} remaining`);
  }

  markError(id) {
    // Non-429 server error (500/502/503) — cooldown but don't blacklist
    const now = Date.now() / 1000;
    const until = now + this.cooldown429;
    this._markCooldown.run(until, id);
    const remaining = this._countAvailable();
    this.log.info?.(`error -> account #${id} -> cool ${this.cooldown429}s, ${remaining} remaining`);
  }

  deactivate(id) {
    // 403 = bad API key or suspended — disable account permanently
    this.db.prepare('UPDATE accounts SET is_active = 0 WHERE id = ?').run(id);
    const remaining = this._countAvailable();
    this.log.warn?.(`deactivated account #${id} (403), ${remaining} remaining`);
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
         SET last_used = ?, error_count = 0,
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
    const cooldown = this._countCooldown.get(now).c;
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
