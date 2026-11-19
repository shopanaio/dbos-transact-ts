import { createHash } from 'node:crypto';
import { escapeIdentifier, type DatabaseError, type Pool, type PoolClient } from 'pg';

export function historyMutationLockKey(schema: string): string {
  return createHash('sha256').update(`dbos.history-mutation.${schema}`).digest().readBigInt64BE(0).toString();
}

export function historyRetentionLockKey(schema: string): string {
  return createHash('sha256').update(`dbos.retention.${schema}`).digest().readBigInt64BE(0).toString();
}

const terminal = ['SUCCESS', 'ERROR', 'CANCELLED', 'RETRIES_EXCEEDED', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED'];
const phases = [
  'status',
  'workflow_input',
  'workflow_output',
  'operation_outputs',
  'transaction_completion',
  'pending',
] as const;
type Phase = (typeof phases)[number];
type PayloadTable = 'workflow_input' | 'workflow_output' | 'operation_outputs';
const payloadTables: readonly PayloadTable[] = ['workflow_input', 'workflow_output', 'operation_outputs'];
interface Cursor {
  phase: Phase;
  key: string;
  number: number;
  /** When the last full orphan walk finished. */
  orphansScannedAt?: number;
}
interface Page {
  key: string;
  number: number;
  done: boolean;
  deleted: number;
}
const BUSY = Symbol('busy');

export interface HistoryRetentionPolicy {
  applicationName: string;
  cutoffEpochMs: number | null;
}

export interface HistoryStatusOptions {
  policies?: readonly HistoryRetentionPolicy[];
  /** Explicit operator-wide cutoff; never derived from one application's policy. */
  systemCutoffEpochMs?: number;
  batchSize?: number;
}

export interface HistoryRetentionOptions extends HistoryStatusOptions {
  budgetMs?: number;
  orphanCutoffEpochMs: number;
  /** How often the full payload/checkpoint walk looks for unfenced orphans. Fenced IDs are collected every pass. */
  orphanScanIntervalMs?: number;
  transactionPool: Pool;
  transactionSchema?: string;
}

export interface HistoryRetentionResult {
  busy: boolean;
  /** `round`: another GC holds the schema. `writers`: live writers kept the mutation lock past lock_timeout. */
  blockedBy?: 'round' | 'writers';
  pages: number;
  deleted: number;
  phase?: Phase;
}

/** Indexed reverse traversal, including child references copied by fork. UNION breaks cycles. */
function ancestors(schema: string, seed: string): string {
  return `WITH RECURSIVE ancestors(candidate, workflow_uuid) AS (
    SELECT id, id FROM unnest(${seed}::text[]) id
    UNION
    SELECT a.candidate, edge.id FROM ancestors a
    CROSS JOIN LATERAL (
      SELECT parent_workflow_id id FROM ${schema}.workflow_status
      WHERE workflow_uuid=a.workflow_uuid AND parent_workflow_id IS NOT NULL
      UNION
      SELECT workflow_uuid id FROM ${schema}.operation_outputs
      WHERE child_workflow_id=a.workflow_uuid
    ) edge
  )`;
}

/** IDs with no status of their own and no status among the owners that still reference them. */
async function unownedWorkflows(client: PoolClient, schema: string, ids: string[]): Promise<string[]> {
  if (!ids.length) return [];
  const { rows } = await client.query<{ id: string }>(
    `${ancestors(schema, '$1')}
     SELECT c.id FROM unnest($1::text[]) c(id)
     WHERE NOT EXISTS (SELECT 1 FROM ancestors dependency JOIN ${schema}.workflow_status owner
       ON owner.workflow_uuid=dependency.workflow_uuid WHERE dependency.candidate=c.id)`,
    [ids],
  );
  return rows.map((r) => r.id);
}

/** Close the IDs for start, import and child references until the pending phase has emptied them. */
async function fence(client: PoolClient, schema: string, ids: string[]): Promise<number> {
  if (!ids.length) return 0;
  const result = await client.query(
    `INSERT INTO ${schema}.history_gc_pending(workflow_uuid)
     SELECT DISTINCT unnest($1::text[]) ON CONFLICT DO NOTHING`,
    [ids],
  );
  return result.rowCount ?? 0;
}

/**
 * One status page. Caller holds the mutation lock and a transaction. `fence` is the PostgreSQL
 * control-plane mode; without it metadata goes by FK cascade and nothing waits for the pending phase.
 */
export async function collectHistoryStatusBatch(
  client: PoolClient,
  schemaName: string,
  options: HistoryStatusOptions,
  cursor: { key: string; number: number },
  fenced = true,
): Promise<Page> {
  const schema = escapeIdentifier(schemaName);
  const cutoffs = Object.fromEntries((options.policies ?? []).map((p) => [p.applicationName, p.cutoffEpochMs]));
  const maxCutoff = options.systemCutoffEpochMs ?? Math.max(0, ...Object.values(cutoffs).map((v) => v ?? 0));
  const { rows } = await client.query<{ workflow_uuid: string; completed_at: string }>(
    `SELECT workflow_uuid, completed_at FROM ${schema}.workflow_status
     WHERE (completed_at, workflow_uuid) > ($2::bigint, $3::text)
       AND ((completed_at < $1 AND status=ANY($4::text[]))
         OR EXISTS (SELECT 1 FROM ${schema}.history_gc_pending p WHERE p.workflow_uuid=workflow_status.workflow_uuid))
     ORDER BY completed_at, workflow_uuid LIMIT $5`,
    [maxCutoff, cursor.number, cursor.key, terminal, options.batchSize ?? 500],
  );
  if (!rows.length) return { ...cursor, done: true, deleted: 0 };
  const ids = rows.map((r) => r.workflow_uuid);
  // An unfenced ancestor outside this page is still resumable, even if its TTL has elapsed.
  // Keep its children until it is fenced/deleted, or mark the whole eligible family in this page.
  const expired = `EXISTS (SELECT 1 FROM ${schema}.history_gc_pending p WHERE p.workflow_uuid=w.workflow_uuid)
    OR (w.workflow_uuid=ANY($1::text[]) AND w.status=ANY($2::text[]) AND w.completed_at <
      COALESCE($4::bigint, ($3::jsonb->>w.application_name)::bigint))`;
  const eligible = await client.query<{ candidate: string }>(
    `${ancestors(schema, '$1')}
     SELECT DISTINCT a.candidate FROM ancestors a
     WHERE NOT EXISTS (
       SELECT 1 FROM ancestors dependency JOIN ${schema}.workflow_status w
         ON w.workflow_uuid=dependency.workflow_uuid
       WHERE dependency.candidate=a.candidate AND (${expired}) IS NOT TRUE
     )`,
    [ids, terminal, JSON.stringify(cutoffs), options.systemCutoffEpochMs ?? null],
  );
  const expiredIds = eligible.rows.map((r) => r.candidate);
  if (expiredIds.length) {
    const metadata = [
      ['notifications', 'destination_uuid'],
      ['workflow_events', 'workflow_uuid'],
      ['workflow_events_history', 'workflow_uuid'],
      ['streams', 'workflow_uuid'],
    ];
    if (fenced) {
      // Keep the ID fenced until all payloads and business checkpoints have gone. Otherwise a new
      // start/import with the same ID could replay orphan checkpoints between the bounded sweeps.
      await fence(client, schema, expiredIds);
      // These FK cascades can also contain millions of rows. Fence the expired status first,
      // then empty each FK table in bounded pages before deleting the status itself.
      for (const [table, key] of metadata) {
        await client.query(
          `DELETE FROM ${schema}.${table} t WHERE ctid IN (
          SELECT ctid FROM ${schema}.${table} WHERE ${key}=ANY($1::text[])
          LIMIT $2 FOR UPDATE SKIP LOCKED)`,
          [expiredIds, options.batchSize ?? 500],
        );
      }
    }
    const empty = fenced
      ? metadata
          .map(([table, key]) => `NOT EXISTS (SELECT 1 FROM ${schema}.${table} t WHERE t.${key}=w.workflow_uuid)`)
          .join(' AND ')
      : 'true';
    const removed = await client.query(
      `DELETE FROM ${schema}.workflow_status w
      WHERE workflow_uuid=ANY($1::text[]) AND ${empty}`,
      [expiredIds],
    );
    if (removed.rowCount !== expiredIds.length) {
      // Keep the page until its fenced metadata has drained, including after process restart.
      return { ...cursor, done: false, deleted: removed.rowCount ?? 0 };
    }
  }
  const last = rows[rows.length - 1];
  return { key: last.workflow_uuid, number: Number(last.completed_at), done: false, deleted: expiredIds.length };
}

/** One PK page of a payload table and the expired IDs in it that nothing live still owns. */
async function payloadOrphans(
  client: PoolClient,
  schema: string,
  table: PayloadTable,
  cutoff: number,
  batchSize: number,
  cursor: { key: string; number: number },
) {
  const step = table === 'operation_outputs';
  const { rows } = await client.query<{ workflow_uuid: string; function_id: number; retention_timestamp: string }>(
    `SELECT workflow_uuid, ${step ? 'function_id' : '0 AS function_id'}, retention_timestamp FROM ${schema}.${table}
     WHERE ${step ? '(workflow_uuid,function_id)>($1::text,$2::int)' : 'workflow_uuid>$1::text'}
     ORDER BY workflow_uuid${step ? ',function_id' : ''} LIMIT $${step ? 3 : 2}`,
    step ? [cursor.key, cursor.number, batchSize] : [cursor.key, batchSize],
  );
  const expired = rows.filter((r) => Number(r.retention_timestamp) < cutoff);
  const orphans = new Set(await unownedWorkflows(client, schema, [...new Set(expired.map((r) => r.workflow_uuid))]));
  return { rows, expired: expired.filter((r) => orphans.has(r.workflow_uuid)), orphans: [...orphans] };
}

/** One native page: delete unowned payload rows below the cutoff. No fences; nothing collects them later. */
export async function collectHistoryPayloadBatch(
  client: PoolClient,
  schemaName: string,
  table: PayloadTable,
  cutoff: number,
  batchSize: number,
  cursor: { key: string; number: number },
): Promise<Page> {
  const schema = escapeIdentifier(schemaName);
  const { rows, expired } = await payloadOrphans(client, schema, table, cutoff, batchSize, cursor);
  if (!rows.length) return { ...cursor, done: true, deleted: 0 };
  let deleted = 0;
  if (expired.length) {
    const result = await client.query(
      `DELETE FROM ${schema}.${table} t USING unnest($1::text[], $2::int[]) candidate(id, num)
       WHERE t.workflow_uuid=candidate.id ${table === 'operation_outputs' ? 'AND t.function_id=candidate.num' : ''}
         AND t.retention_timestamp < $3`,
      [expired.map((r) => r.workflow_uuid), expired.map((r) => r.function_id), cutoff],
    );
    deleted = result.rowCount ?? 0;
  }
  const last = rows[rows.length - 1];
  return { key: last.workflow_uuid, number: last.function_id, done: false, deleted };
}

/** PostgreSQL online retention. One connection, one schema-wide round, bounded committed pages.
 * Status pages fence and delete expired statuses; the pending phase then empties every fenced ID
 * by index, without holding the mutation lock. The full orphan walk runs once per interval.
 * Cursor is durable, including pages containing only protected rows. No VACUUM or queue polling.
 */
export async function retainWorkflowHistory(
  pool: Pool,
  schemaName: string,
  options: HistoryRetentionOptions,
): Promise<HistoryRetentionResult> {
  const batchSize = options.batchSize ?? 500;
  const budgetMs = options.budgetMs ?? 5_000;
  const orphanScanIntervalMs = options.orphanScanIntervalMs ?? 86_400_000;
  if (
    !Number.isInteger(batchSize) ||
    batchSize < 1 ||
    batchSize > 10_000 ||
    !Number.isFinite(budgetMs) ||
    budgetMs < 1 ||
    !Number.isFinite(orphanScanIntervalMs) ||
    orphanScanIntervalMs < 0 ||
    !Number.isFinite(options.orphanCutoffEpochMs) ||
    (options.systemCutoffEpochMs !== undefined && !Number.isFinite(options.systemCutoffEpochMs)) ||
    options.policies?.some((p) => p.cutoffEpochMs !== null && !Number.isFinite(p.cutoffEpochMs))
  ) {
    throw new Error('Invalid history retention limits');
  }
  const schema = escapeIdentifier(schemaName);
  const txSchema = escapeIdentifier(options.transactionSchema ?? 'dbos');
  const mutationKey = historyMutationLockKey(schemaName);
  const settings = { ...options, batchSize };
  const system = await pool.connect();
  let locked = false;
  let broken = false;
  let businessBroken = false;
  let business: PoolClient | undefined;
  const systemError = () => {
    broken = true;
  };
  const businessError = () => {
    businessBroken = true;
  };
  system.on('error', systemError);
  let pages = 0;
  let deleted = 0;
  let phase: Phase = 'status';

  /** Bounded system transaction. Only the mutation lock may report busy; any other timeout is an error. */
  const systemTx = async <T>(mutation: boolean, fn: () => Promise<T>): Promise<T | typeof BUSY> => {
    await system.query('BEGIN');
    try {
      await system.query(`SET LOCAL statement_timeout='2s'`);
      await system.query(`SET LOCAL lock_timeout='100ms'`);
      if (mutation) {
        // Blocking with lock_timeout, not try-lock: a queued exclusive request stops new shared
        // holders from overtaking it, so steady writer traffic cannot starve the round.
        const acquired = await system.query('SELECT pg_advisory_xact_lock($1)', [mutationKey]).then(
          () => true,
          (e: DatabaseError) => {
            if (e.code === '55P03') return false;
            throw e;
          },
        );
        if (!acquired) {
          await system.query('ROLLBACK');
          return BUSY;
        }
      }
      const result = await fn();
      await system.query('COMMIT');
      return result;
    } catch (e) {
      await system.query('ROLLBACK').catch(() => {
        broken = true;
      });
      throw e;
    }
  };
  /** Bounded business transaction. Never opened while a system transaction is open. */
  const businessTx = async <T>(fn: (client: PoolClient) => Promise<T>): Promise<T> => {
    const client = business!;
    await client.query('BEGIN');
    try {
      await client.query(`SET LOCAL statement_timeout='2s'`);
      await client.query(`SET LOCAL lock_timeout='100ms'`);
      const result = await fn(client);
      await client.query('COMMIT');
      return result;
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {
        businessBroken = true;
      });
      throw e;
    }
  };
  const saveCursor = async (cursor: Cursor) => {
    await system.query(
      `INSERT INTO ${schema}.history_gc_state(singleton,cursor) VALUES (true,$1::jsonb)
      ON CONFLICT(singleton) DO UPDATE SET cursor=excluded.cursor`,
      [JSON.stringify(cursor)],
    );
  };
  const nextCursor = (cursor: Cursor): Cursor => {
    const base = { key: '', number: -1, orphansScannedAt: cursor.orphansScannedAt };
    switch (cursor.phase) {
      case 'status': {
        const due =
          cursor.orphansScannedAt === undefined || Date.now() - cursor.orphansScannedAt >= orphanScanIntervalMs;
        return { ...base, phase: due ? 'workflow_input' : 'pending' };
      }
      case 'transaction_completion':
        return { ...base, phase: 'pending', orphansScannedAt: Date.now() };
      case 'pending':
        return { ...base, phase: 'status' };
      default:
        return { ...base, phase: phases[phases.indexOf(cursor.phase) + 1] };
    }
  };
  const advance = (cursor: Cursor, page: Page): Cursor =>
    page.done ? nextCursor(cursor) : { ...cursor, key: page.key, number: page.number };

  /** Delete what fenced IDs still hold, page by page, then lift the fences of the emptied ones. */
  const pendingPage = async (cursor: Cursor): Promise<Page> => {
    const drained = await systemTx(false, async () => {
      const { rows } = await system.query<{ workflow_uuid: string; live: boolean }>(
        `SELECT p.workflow_uuid,
           EXISTS (SELECT 1 FROM ${schema}.workflow_status s WHERE s.workflow_uuid=p.workflow_uuid) AS live
         FROM ${schema}.history_gc_pending p WHERE p.workflow_uuid>$1
         ORDER BY p.workflow_uuid LIMIT $2`,
        [cursor.key, batchSize],
      );
      if (!rows.length) {
        await saveCursor(nextCursor(cursor));
        return undefined;
      }
      // A fenced status still draining its metadata keeps its rows until the status page deletes it.
      const ids = rows.filter((r) => !r.live).map((r) => r.workflow_uuid);
      let full = false;
      let removed = 0;
      // Fences block every insert for these IDs, so nothing new can appear while this runs unlocked.
      for (const table of payloadTables) {
        const result = await system.query(
          `DELETE FROM ${schema}.${table} WHERE ctid = ANY(ARRAY(
             SELECT ctid FROM ${schema}.${table} WHERE workflow_uuid=ANY($1::text[]) LIMIT $2))`,
          [ids, batchSize],
        );
        removed += result.rowCount ?? 0;
        full ||= result.rowCount === batchSize;
      }
      return { last: rows[rows.length - 1].workflow_uuid, ids, full, removed };
    });
    if (drained === undefined) return { ...cursor, done: true, deleted: 0 };
    if (drained === BUSY) throw new Error('unreachable: the pending phase takes no mutation lock');
    const checkpoints = await businessTx((client) =>
      client.query(
        `DELETE FROM ${txSchema}.transaction_completion WHERE ctid = ANY(ARRAY(
           SELECT ctid FROM ${txSchema}.transaction_completion WHERE workflow_id=ANY($1::text[]) LIMIT $2))`,
        [drained.ids, batchSize],
      ),
    );
    const full = drained.full || checkpoints.rowCount === batchSize;
    const removed = drained.removed + (checkpoints.rowCount ?? 0);
    if (full) return { ...cursor, done: false, deleted: removed };
    // Both databases are empty for these IDs: lift the fences and move past the page atomically.
    await systemTx(false, async () => {
      await system.query(`DELETE FROM ${schema}.history_gc_pending WHERE workflow_uuid=ANY($1::text[])`, [drained.ids]);
      await saveCursor({ ...cursor, key: drained.last, number: 0 });
    });
    return { key: drained.last, number: 0, done: false, deleted: removed };
  };

  try {
    locked = (
      await system.query<{ locked: boolean }>('SELECT pg_try_advisory_lock($1) locked', [
        historyRetentionLockKey(schemaName),
      ])
    ).rows[0].locked;
    if (!locked) return { busy: true, blockedBy: 'round', pages, deleted };
    // Separate pools even when both URLs address one DB; no pool-of-one waits on itself.
    business = await options.transactionPool.connect();
    business.on('error', businessError);
    const deadline = Date.now() + budgetMs;
    do {
      const state = await system.query<{ cursor: Cursor }>(
        `SELECT cursor FROM ${schema}.history_gc_state WHERE singleton`,
      );
      const cursor: Cursor = state.rows[0]?.cursor ?? { phase: 'status', key: '', number: -1 };
      phase = cursor.phase;
      let page: Page | typeof BUSY;
      if (phase === 'status') {
        page = await systemTx(true, async () => {
          const batch = await collectHistoryStatusBatch(system, schemaName, settings, cursor);
          await saveCursor(advance(cursor, batch));
          return batch;
        });
      } else if (phase === 'transaction_completion') {
        // Read the business page first; the system lock covers only the ownership check and fences.
        const { rows } = await businessTx((client) =>
          client.query<{ workflow_id: string; function_num: number; created_at: string }>(
            `SELECT workflow_id,function_num,created_at FROM ${txSchema}.transaction_completion
             WHERE (workflow_id,function_num)>($1::text,$2::int)
             ORDER BY workflow_id,function_num LIMIT $3`,
            [cursor.key, cursor.number, batchSize],
          ),
        );
        page = await systemTx(true, async () => {
          let batch: Page = { ...cursor, done: true, deleted: 0 };
          if (rows.length) {
            const expired = rows.filter((r) => Number(r.created_at) < options.orphanCutoffEpochMs);
            const orphans = await unownedWorkflows(system, schema, [...new Set(expired.map((r) => r.workflow_id))]);
            const last = rows[rows.length - 1];
            batch = {
              key: last.workflow_id,
              number: last.function_num,
              done: false,
              deleted: await fence(system, schema, orphans),
            };
          }
          await saveCursor(advance(cursor, batch));
          return batch;
        });
      } else if (phase === 'pending') {
        page = await pendingPage(cursor);
      } else {
        const table = phase;
        page = await systemTx(true, async () => {
          const { rows, orphans } = await payloadOrphans(
            system,
            schema,
            table,
            options.orphanCutoffEpochMs,
            batchSize,
            cursor,
          );
          let batch: Page = { ...cursor, done: true, deleted: 0 };
          if (rows.length) {
            const last = rows[rows.length - 1];
            batch = {
              key: last.workflow_uuid,
              number: last.function_id,
              done: false,
              deleted: await fence(system, schema, orphans),
            };
          }
          await saveCursor(advance(cursor, batch));
          return batch;
        });
      }
      if (page === BUSY) return { busy: true, blockedBy: 'writers', pages, deleted, phase };
      pages++;
      deleted += page.deleted;
      if (phase === 'pending' && page.done) break;
    } while (Date.now() < deadline);
    return { busy: false, pages, deleted, phase };
  } finally {
    if (business) {
      business.removeListener('error', businessError);
      business.release(businessBroken);
    }
    if (locked) {
      try {
        const released = await system.query<{ released: boolean }>('SELECT pg_advisory_unlock($1) released', [
          historyRetentionLockKey(schemaName),
        ]);
        broken ||= released.rows[0]?.released !== true;
      } catch {
        broken = true;
      }
    }
    // An uncertain session lock must never return to the pool.
    system.removeListener('error', systemError);
    system.release(broken);
  }
}
