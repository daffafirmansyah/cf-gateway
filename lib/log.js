// In-memory request log ring buffer
export function captureBody(obj, cap = 2048) {
  if (obj == null) return null;
  try {
    const s = typeof obj === 'string' ? obj : JSON.stringify(obj);
    if (s.length <= cap) return s;
    return s.slice(0, cap) + `\n… (truncated, ${s.length} bytes total)`;
  } catch {
    return String(obj).slice(0, cap);
  }
}

export class RequestLog {
  constructor({ capacity = 500 } = {}) {
    this.capacity = capacity;
    this._buf = [];
    this._seq = 0;
  }

  record(entry) {
    this._seq++;
    const rec = { seq: this._seq, ts: Date.now(), ...entry };
    this._buf.push(rec);
    if (this._buf.length > this.capacity) this._buf.shift();
    return rec;
  }

  all() {
    return [...this._buf].reverse();
  }

  clear() {
    this._buf = [];
  }
}
