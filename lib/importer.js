import Database from 'better-sqlite3';
import { existsSync } from 'node:fs';

export function importFrom9router(ownDb, ninePath, log = console) {
  const total = () => ownDb.prepare('SELECT COUNT(*) AS c FROM accounts').get().c;

  if (!existsSync(ninePath)) {
    log.warn?.(`9router DB not found: ${ninePath}`);
    return { imported: 0, skipped: 0, total: total(), error: '9router DB not found' };
  }

  const src = new Database(ninePath, { readonly: true });
  let rows;
  try {
    rows = src
      .prepare("SELECT name, data FROM providerConnections WHERE provider = 'cloudflare-ai'")
      .all();
  } finally {
    src.close();
  }

  const insert = ownDb.prepare(
    'INSERT OR IGNORE INTO accounts (name, api_key, account_id) VALUES (?, ?, ?)'
  );

  let imported = 0;
  let skipped = 0;
  for (const row of rows) {
    let data;
    try {
      data = JSON.parse(row.data);
    } catch {
      skipped++;
      continue;
    }
    const apiKey = data?.apiKey || '';
    const accountId = data?.providerSpecificData?.accountId || '';
    if (!apiKey || !accountId) {
      skipped++;
      continue;
    }
    const res = insert.run(row.name, apiKey, accountId);
    if (res.changes > 0) imported++;
    else skipped++;
  }

  log.info?.(`Imported ${imported} accounts from 9router (${skipped} skipped)`);
  return { imported, skipped, total: total() };
}
