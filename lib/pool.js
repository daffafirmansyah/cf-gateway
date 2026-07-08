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

    // Global capacity gate — pauses ALL retries when CF backend is overloaded
    this._capacityHits = [];        // timestamps of capacity errors
    this._capacityGateUntil = 0;    // when gate unlocks (epoch ms)
    this.CAPACITY_THRESHOLD = 2;    // hits within window to trigger gate (lower = faster protection)
    this.CAPACITY_WINDOW_MS = 60000; // 60s sliding window
    this.CAPACITY_GATE_BASE_MS = parseInt(process.env.CF_GATEWAY_GATE_BASE_MS || '90000', 10); // 90s base
    this.CAPACITY_GATE_MAX_MS = parseInt(process.env.CF_GATEWAY_GATE_MAX_MS || '600000', 10); // 10min max
    this.CAPACITY_GATE_MS = this.CAPACITY_GATE_BASE_MS; // current gate duration (escalates)
    this._gateConsecutive = 0;      // consecutive gate activations (for exponential backoff)
    this._gateExpiresAt = 0;        // when current gate expires (for reset detection)

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

  // --- Global capacity gate ---
  checkCapacityGate() {
    const now = Date.now();
    if (now < this._capacityGateUntil) {
      const waitMs = this._capacityGateUntil - now;
      return { blocked: true, waitMs, reason: `global capacity gate active (${Math.ceil(waitMs / 1000)}s remaining)`, consecutive: this._gateConsecutive };
    }
    // Gate expired naturally — reset escalation if it wasn't re-triggered
    if (this._gateConsecutive > 0 && now >= this._capacityGateUntil) {
      this._gateConsecutive = 0;
      this.CAPACITY_GATE_MS = this.CAPACITY_GATE_BASE_MS;
    }
    return { blocked: false };
  }

  markCapacityHit() {
    const now = Date.now();
    this._capacityHits.push(now);
    // Prune old entries outside sliding window
    const cutoff = now - this.CAPACITY_WINDOW_MS;
    this._capacityHits = this._capacityHits.filter(t => t > cutoff);

    if (this._capacityHits.length >= this.CAPACITY_THRESHOLD) {
      // Exponential gate backoff: 90s → 180s → 360s → 600s (max)
      this._gateConsecutive++;
      const escalated = Math.min(
        this.CAPACITY_GATE_BASE_MS * Math.pow(2, this._gateConsecutive - 1),
        this.CAPACITY_GATE_MAX_MS
      );
      this.CAPACITY_GATE_MS = escalated;
      this._capacityGateUntil = now + this.CAPACITY_GATE_MS;
      this._gateExpiresAt = this._capacityGateUntil;
      this.log.warn?.(`GLOBAL CAPACITY GATE ACTIVATED — ${this._capacityHits.length} capacity errors in ${this.CAPACITY_WINDOW_MS / 1000}s — pausing for ${this.CAPACITY_GATE_MS / 1000}s (consecutive #${this._gateConsecutive})`);
      // Clear hits so gate doesn't re-trigger immediately on resume
      this._capacityHits = [];
    }
  }

  getCapacityGateInfo() {
    const now = Date.now();
    const active = now < this._capacityGateUntil;
    return {
      gate_active: active,
      gate_remaining_ms: active ? this._capacityGateUntil - now : 0,
      recent_capacity_hits: this._capacityHits.filter(t => t > now - this.CAPACITY_WINDOW_MS).length,
      threshold: this.CAPACITY_THRESHOLD,
      window_ms: this.CAPACITY_WINDOW_MS,
      gate_cooldown_ms: this.CAPACITY_GATE_MS,
      gate_base_ms: this.CAPACITY_GATE_BASE_MS,
      gate_max_ms: this.CAPACITY_GATE_MAX_MS,
      gate_consecutive: this._gateConsecutive,
    };
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

  mark429(id, errorCode, errorText) {
    const now = Date.now() / 1000;
    const until = now + this.cooldown429;
    if (errorCode === 4006) {
      // 4006 has dual meaning — check message body to distinguish:
      // "used up your daily free allocation" = exhausted for the day (permanent)
      // "Service temporarily at capacity" = transient CF overload (global)
      if (errorText && /daily free allocation/i.test(errorText)) {
        this._markExhausted.run(NEURON_FREE_DAILY, todayUTC(), id);
        this.log.info?.(`429 -> account #${id} code=4006 DAILY EXHAUSTED — marked for rest of day`);
      } else {
        // Transient capacity error — NO account cooldown (error is global, not per-account)
        // Gate handles the pause to protect IP from bans
        this.markCapacityHit();
        const gate = this.checkCapacityGate();
        const remaining = this._countAvailable();
        this.log.info?.(`429 -> account #${id} code=4006 transient -> no cooldown (global), ${remaining} available, gate=${gate.blocked ? 'ACTIVE' : 'off'}`);
      }
    } else if (errorCode === 3040) {
      // 3040 = "Capacity temporarily exceeded" — NO cooldown, gate handles pause
      this.markCapacityHit();
      const gate = this.checkCapacityGate();
      const remaining = this._countAvailable();
      this.log.info?.(`429 -> account #${id} code=3040 -> no cooldown (global), ${remaining} available, gate=${gate.blocked ? 'ACTIVE' : 'off'}`);
    } else {
      // Per-minute rate limit — cooldown + track error
      this._mark429.run(until, id);
      const remaining = this._countAvailable();
      this.log.info?.(`429 -> account #${id} code=${errorCode} -> cool ${this.cooldown429}s, ${remaining} remaining`);
    }
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
      capacity_gate: this.getCapacityGateInfo(),
    };
  }
}
