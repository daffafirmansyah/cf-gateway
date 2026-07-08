import Database from 'better-sqlite3';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

export function initDb(dbPath) {
  mkdirSync(dirname(dbPath), { recursive: true });
  const db = new Database(dbPath);
  db.pragma('journal_mode = WAL');
  db.pragma('busy_timeout = 5000');

  db.exec(`
    CREATE TABLE IF NOT EXISTS accounts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT,
      api_key TEXT NOT NULL UNIQUE,
      account_id TEXT NOT NULL,
      is_active INTEGER DEFAULT 1,
      cooldown_until REAL DEFAULT 0,
      last_used REAL DEFAULT 0,
      error_count INTEGER DEFAULT 0,
      neurons_today REAL DEFAULT 0,
      neurons_day TEXT DEFAULT '',
      requests_today INTEGER DEFAULT 0,
      created_at REAL DEFAULT (strftime('%s','now')),
      backoff_level INTEGER DEFAULT 0,
      last_error_at REAL DEFAULT 0
    )
  `);

  db.exec('CREATE INDEX IF NOT EXISTS idx_active ON accounts(is_active, cooldown_until)');
  db.exec('CREATE UNIQUE INDEX IF NOT EXISTS idx_account_id ON accounts(account_id)');

  return db;
}
