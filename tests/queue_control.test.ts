import { randomUUID } from 'node:crypto';
import { Client, Notification } from 'pg';
import { DBOS, DBOSClient, QueueControlError } from '../src';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import { SystemDatabase, QueueUnavailableError, QueueReadinessRequest } from '../src/system_database';
import { WorkflowQueue, QueueParameters } from '../src/wfqueue';
import { DBOSJSON } from '../src/serialization';
import { globalParams, INTERNAL_QUEUE_NAME } from '../src/utils';
import { Event, generateDBOSTestConfig, setUpDBOSTestSysDb, retryUntilSuccess } from './helpers';
import {
  clearDebugTriggers,
  setDebugTrigger,
  DEBUG_TRIGGER_FIND_AND_MARK_AFTER_SELECT,
  DEBUG_TRIGGER_PARTITIONED_DEQUEUE_AFTER_CANDIDATES,
} from '../src/debugpoint';

const APP = 'queue-control-tests';
const routeWorkflow = DBOS.registerWorkflow(async () => DBOS.workflowQueueName, { name: 'queueControlRoute' });

describe('persisted queue control and readiness', () => {
  let config: DBOSConfig;
  let db: SystemDatabase;
  let observer: Client;
  let notifications: Notification[];
  let clients: DBOSClient[];
  let releases: Array<() => void>;
  const version = () => globalParams.appVersion;
  const request = (q: WorkflowQueue, generation = 7): QueueReadinessRequest => ({
    name: q.name,
    queueId: q.queueId,
    generation,
    internal: !q.databaseBacked,
  });
  const queue = (params: QueueParameters = {}) => DBOS.registerQueue(`q-${randomUUID()}`, params);
  async function client(applicationName?: string) {
    const c = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl!, applicationName });
    clients.push(c);
    return c;
  }
  async function insert(
    q: string | null,
    options: {
      status?: string;
      partition?: string;
      app?: string | null;
      version?: string | null;
      priority?: number;
      recovery?: number;
    } = {},
  ) {
    const id = randomUUID();
    await db.pool.query(
      `INSERT INTO dbos.workflow_status
      (workflow_uuid, name, status, queue_name, queue_partition_key, application_name,
       application_version, executor_id, priority, recovery_attempts, started_at_epoch_ms, deduplication_id)
      VALUES ($1, 'queueControlFixture', $2, $3, $4, $5, $6, 'dead-executor', $7, $8, $9, $1)`,
      [
        id,
        options.status ?? 'ENQUEUED',
        q,
        options.partition ?? null,
        options.app === undefined ? APP : options.app,
        options.version === undefined ? version() : options.version,
        options.priority ?? 0,
        options.recovery ?? 0,
        options.status === 'PENDING' ? Date.now() : null,
      ],
    );
    return id;
  }
  const dequeue = (q: WorkflowQueue, partition?: string) =>
    db.findAndMarkStartableWorkflows(q, 'worker', version(), partition);
  const batchDequeue = (q: WorkflowQueue) => db.findAndMarkStartablePartitionedWorkflows(q, 'worker', version(), 10);
  async function row(id: string) {
    return (await db.pool.query('SELECT * FROM dbos.workflow_status WHERE workflow_uuid = $1', [id])).rows[0];
  }
  async function blockedControl() {
    await retryUntilSuccess(async () => {
      const { rows } = await observer.query(`SELECT count(*)::int AS n FROM pg_stat_activity
        WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE '%queues%'`);
      expect(rows[0].n).toBeGreaterThan(0);
    }, 3000);
  }
  beforeAll(async () => {
    config = { ...generateDBOSTestConfig(), name: APP, listenQueues: [] };
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
    await DBOS.launch();
    db = DBOSExecutor.globalInstance!.systemDatabase;
    observer = new Client({ connectionString: config.systemDatabaseUrl });
    await observer.connect();
    await observer.query('LISTEN dbos_queue_control_channel');
    observer.on('notification', (message) => notifications.push(message));
  });
  beforeEach(async () => {
    clients = [];
    releases = [];
    notifications = [];
    await db.pool.query(
      'TRUNCATE dbos.workflow_status, dbos.workflow_input, dbos.workflow_output, dbos.operation_outputs, dbos.queues CASCADE',
    );
    await db.pool.query('DELETE FROM dbos.application_versions');
    await db.createApplicationVersion(version());
  });
  afterEach(async () => {
    releases.forEach((fn) => fn());
    clearDebugTriggers();
    jest.restoreAllMocks();
    await Promise.all(clients.map((c) => c.destroy()));
  });
  afterAll(async () => {
    await observer?.end();
    await DBOS.shutdown();
  });

  test('SC-qp02/04: clean migration defaults, control reads, upsert, setters and recreation', async () => {
    const q = await queue();
    expect(q.queueId).toBeTruthy();
    expect(q.paused).toBe(false);
    const paused = await q.pause();
    expect(paused).toEqual({ name: q.name, queueId: q.queueId, paused: true });
    expect(await DBOS.getQueueControlState(q.name)).toEqual(paused);
    expect(await q.getControlState()).toEqual(paused);
    const registered = await DBOS.registerQueue(q.name, { onConflict: 'always_update', workerConcurrency: 3 });
    await registered.setWorkerConcurrency(4);
    expect(await registered.getControlState()).toEqual(paused);
    expect((await DBOS.listQueues()).find((r) => r.name === q.name)).toMatchObject(paused);
    await DBOS.deleteQueue(q.name);
    await expect(DBOS.getQueueControlState(q.name)).rejects.toMatchObject({ code: 'QUEUE_NOT_FOUND' });
    const replacement = await DBOS.registerQueue(q.name);
    expect(replacement.queueId).not.toBe(q.queueId);
    expect(replacement.paused).toBe(false);
    await expect(db.pool.query('UPDATE dbos.queues SET paused = NULL WHERE name = $1', [q.name])).rejects.toMatchObject(
      { code: '23502' },
    );
  });

  test('SC-qp03/09: DBOSClient without listener notifies once per actual state change', async () => {
    const q = await queue();
    const c = await client(APP);
    const bound = (await c.retrieveQueue(q.name))!;
    await c.pauseQueue(q.name);
    await c.pauseQueue(q.name);
    await retryUntilSuccess(() => expect(notifications).toHaveLength(1));
    expect(JSON.parse(notifications[0].payload!)).toEqual({ schema: 'dbos', queueId: q.queueId, name: q.name });
    expect(await bound.getControlState()).toMatchObject({ paused: true });
    expect(await bound.wake()).toMatchObject({ paused: false });
    await c.wakeQueue(q.name);
    await retryUntilSuccess(() => expect(notifications).toHaveLength(2));
    expect(await c.getQueueControlState(q.name)).toMatchObject({ paused: false });
  });

  test.each(['missing', '_dbos_internal'])(
    'SC-qp05: typed errors for %s from all control entry points',
    async (name) => {
      const c = await client(APP);
      const code = name.startsWith('_dbos_') ? 'INTERNAL_QUEUE' : 'QUEUE_NOT_FOUND';
      for (const api of [DBOS, c]) {
        for (const op of ['pauseQueue', 'wakeQueue', 'getQueueControlState'] as const) {
          await expect(api[op](name)).rejects.toBeInstanceOf(QueueControlError);
          await expect(api[op](name)).rejects.toMatchObject({ code });
        }
      }
    },
  );

  test('SC-qp05: foreign and nameless clients cannot control owned queues; shared queues are accessible', async () => {
    const q = await queue();
    for (const c of [await client('foreign'), await client()]) {
      for (const op of ['pauseQueue', 'wakeQueue', 'getQueueControlState'] as const) {
        await expect(c[op](q.name)).rejects.toMatchObject({ code: 'QUEUE_OWNER_MISMATCH' });
      }
    }
    expect(await q.getControlState()).toMatchObject({ paused: false });
    const shared = await (await client()).registerQueue(`shared-${randomUUID()}`);
    expect(await (await client('foreign')).pauseQueue(shared.name)).toMatchObject({ paused: true });
    expect((await db.getQueue(shared.name))?.applicationName).toBeUndefined();
  });

  test('SC-qp05: ownership assignment and control write serialize on the same row', async () => {
    const c = await client();
    const q = await c.registerQueue(`shared-${randomUUID()}`);
    const lock = await db.pool.connect();
    releases.push(() => {
      void lock.query('ROLLBACK').finally(() => lock.release());
    });
    await lock.query('BEGIN');
    await lock.query('UPDATE dbos.queues SET application_name = $2 WHERE name = $1', [q.name, 'foreign']);
    const pause = c.pauseQueue(q.name);
    const rejected = expect(pause).rejects.toMatchObject({ code: 'QUEUE_OWNER_MISMATCH' });
    await blockedControl();
    await lock.query('COMMIT');
    await rejected;
    expect((await db.getQueue(q.name))?.paused).toBe(false);
  });

  test.each(['ordinary', 'partition'])(
    'SC-qp01: %s claim commits before a waiting pause, then new claims are barred',
    async (kind) => {
      const q = await queue(kind === 'partition' ? { partitionConcurrency: 1 } : {});
      const id = await insert(q.name, { partition: kind === 'partition' ? 'p' : undefined });
      const entered = new Event();
      const release = new Event();
      releases.push(() => release.set());
      const point =
        kind === 'partition'
          ? DEBUG_TRIGGER_PARTITIONED_DEQUEUE_AFTER_CANDIDATES
          : DEBUG_TRIGGER_FIND_AND_MARK_AFTER_SELECT;
      setDebugTrigger(point, {
        asyncCallback: async () => {
          entered.set();
          await release.wait();
        },
      });
      const claim = kind === 'partition' ? batchDequeue(q) : dequeue(q);
      await entered.wait();
      const pause = DBOS.pauseQueue(q.name);
      await blockedControl();
      release.set();
      expect(await claim).toEqual([id]);
      expect(await pause).toMatchObject({ paused: true });
      clearDebugTriggers();
      await insert(q.name, { partition: 'another' });
      await expect(kind === 'partition' ? batchDequeue(q) : dequeue(q)).rejects.toBeInstanceOf(QueueUnavailableError);
      expect((await row(id)).status).toBe('PENDING');
    },
  );

  test.each(['ordinary', 'partition', 'fallback'])(
    'SC-qp01: %s rejects pause between readiness and claim',
    async (kind) => {
      const q = await queue(kind === 'ordinary' ? {} : { partitionConcurrency: kind === 'partition' ? 1 : 2 });
      await insert(q.name, { partition: kind === 'ordinary' ? undefined : 'p' });
      expect(await db.findQueuesWithEnqueuedWorkflows([request(q)], version())).toEqual([request(q)]);
      await q.pause();
      await expect(
        kind === 'partition' ? batchDequeue(q) : dequeue(q, kind === 'fallback' ? 'p' : undefined),
      ).rejects.toBeInstanceOf(QueueUnavailableError);
    },
  );

  test('SC-qp04: absent or replaced identity cannot claim; the new identity can consume the backlog', async () => {
    const q = await queue();
    const id = await insert(q.name);
    await q.pause();
    await DBOS.deleteQueue(q.name);
    await expect(dequeue(q)).rejects.toBeInstanceOf(QueueUnavailableError);
    const replacement = await DBOS.registerQueue(q.name);
    await expect(dequeue(q)).rejects.toBeInstanceOf(QueueUnavailableError);
    expect(await db.findQueuesWithEnqueuedWorkflows([request(q)], version())).toEqual([]);
    expect(await dequeue(replacement)).toEqual([id]);
  });

  test('SC-qp09: rollback suppresses persisted pause and notifications', async () => {
    const q = await queue();
    await observer.query(`CREATE FUNCTION dbos.reject_pause_test() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN RAISE EXCEPTION 'control rollback'; END $$`);
    await observer.query(`CREATE CONSTRAINT TRIGGER reject_pause_test AFTER UPDATE ON dbos.queues
      DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION dbos.reject_pause_test()`);
    try {
      await expect(q.pause()).rejects.toThrow('control rollback');
      expect(await q.getControlState()).toMatchObject({ paused: false });
      // Query on the listener connection provides a protocol turn after the rollback.
      await observer.query('SELECT 1');
      expect(notifications).toEqual([]);
    } finally {
      await observer.query('DROP TRIGGER reject_pause_test ON dbos.queues');
      await observer.query('DROP FUNCTION dbos.reject_pause_test()');
    }
  });

  test('SC-qp09: long names use compact hints; disabled notifications preserve state', async () => {
    const q = await DBOS.registerQueue('q'.repeat(7200));
    await q.pause();
    await retryUntilSuccess(() => expect(notifications).toHaveLength(1));
    expect(JSON.parse(notifications[0].payload!)).toEqual({ schema: 'dbos', queueId: q.queueId });
    db.queueControlNotificationsEnabled = false;
    try {
      await q.wake();
      expect(await q.getControlState()).toMatchObject({ paused: false });
      await observer.query('SELECT 1');
      expect(notifications).toHaveLength(1);
    } finally {
      db.queueControlNotificationsEnabled = true;
    }
  });

  test('SC-qp06: 1000 readiness requests use one statement and return only eligible identities', async () => {
    const q = await queue();
    const empty = await queue();
    const paused = await queue();
    await insert(q.name);
    await insert(paused.name);
    await paused.pause();
    const requests = [
      request(q),
      request(empty),
      request(paused),
      ...Array.from({ length: 997 }, (_, i) => ({
        name: `missing-${i}`,
        queueId: randomUUID(),
        generation: i,
        internal: false,
      })),
    ];
    const query = jest.spyOn(db.pool, 'query');
    expect(await db.findQueuesWithEnqueuedWorkflows(requests, version())).toEqual([request(q)]);
    expect(query.mock.calls.filter((c) => typeof c[0] === 'string' && c[0].includes('WITH latest'))).toHaveLength(1);
    query.mockClear();
    expect(await db.findQueuesWithEnqueuedWorkflows([], version())).toEqual([]);
    expect(query).not.toHaveBeenCalled();
  });

  test.each(['latest', 'old', 'unregistered', 'nameless'])(
    'SC-qp06: readiness preserves %s version and ownership semantics',
    async (mode) => {
      const q = await queue();
      const own = await insert(q.name);
      const unversioned = await insert(q.name, { version: null, app: null });
      await insert(q.name, { version: 'foreign-version', app: 'foreign' });
      if (mode === 'old') {
        await db.createApplicationVersion('newer');
        await db.updateApplicationVersionTimestamp('newer', Date.now() + 10000);
      }
      // Only the NULL-version candidate can make readiness positive here.
      await db.pool.query('DELETE FROM dbos.workflow_status WHERE workflow_uuid = $1', [own]);
      if (mode === 'unregistered') await db.pool.query('DELETE FROM dbos.application_versions');
      const reader =
        mode === 'nameless' ? new SystemDatabase(config.systemDatabaseUrl!, db.logger, DBOSJSON, 10, db.pool) : db;
      const found = await reader.findQueuesWithEnqueuedWorkflows([request(q)], version());
      expect(found).toEqual(mode === 'old' ? [] : [request(q)]);
      expect((await row(unversioned)).status).toBe('ENQUEUED');
    },
  );

  test('SC-qp06: a foreign latest version does not demote this app; internal trust is explicit', async () => {
    const q = await queue();
    await insert(q.name, { version: null });
    await db.createApplicationVersion('foreign', 'peer');
    await db.updateApplicationVersionTimestamp('foreign', Date.now() + 10000, 'peer');
    expect(await db.findQueuesWithEnqueuedWorkflows([request(q)], version())).toEqual([request(q)]);
    await insert('_dbos_private');
    const internal = { name: '_dbos_private', queueId: '', generation: 1, internal: false };
    expect(await db.findQueuesWithEnqueuedWorkflows([internal], version())).toEqual([]);
    expect(await db.findQueuesWithEnqueuedWorkflows([{ ...internal, internal: true }], version())).toHaveLength(1);
    await db.pool.query('UPDATE dbos.workflow_status SET application_name = $1 WHERE queue_name = $2', [
      'foreign',
      q.name,
    ]);
    expect(await db.findQueuesWithEnqueuedWorkflows([request(q)], version())).toEqual([]);
  });

  test.each(['cancel', 'dead-letter', 'recovery'])(
    'SC-qp10: %s preserves route and resume waits for wake',
    async (action) => {
      const q = await queue({ partitionConcurrency: 1 });
      const id = await insert(q.name, { partition: 'tenant', status: 'PENDING', recovery: 5 });
      await q.pause();
      if (action === 'cancel') await db.cancelWorkflows([id]);
      if (action === 'dead-letter') await db.deadLetterWorkflows([id], 5);
      if (action === 'recovery')
        await db.reenqueueWorkflowsForRecovery('dead-executor', version(), INTERNAL_QUEUE_NAME);
      const saved = await row(id);
      expect(saved).toMatchObject({
        queue_name: q.name,
        queue_partition_key: 'tenant',
        application_name: APP,
        started_at_epoch_ms: null,
      });
      if (action !== 'recovery') expect(saved.deduplication_id).toBeNull();
      await (await client(APP)).resumeWorkflow(id);
      expect(await row(id)).toMatchObject({
        status: 'ENQUEUED',
        queue_name: q.name,
        queue_partition_key: 'tenant',
        application_name: APP,
      });
      await expect(batchDequeue(q)).rejects.toBeInstanceOf(QueueUnavailableError);
      await q.wake();
      expect(await batchDequeue(q)).toEqual([id]);
    },
  );

  test('SC-qp10: resume preserves missing routes, falls back only for direct work, and accepts explicit override', async () => {
    const queued = await insert('deleted', { status: 'CANCELLED', partition: 'tenant' });
    const direct = await insert(null, { status: 'CANCELLED' });
    await DBOS.resumeWorkflows([queued, direct]);
    expect(await row(queued)).toMatchObject({ queue_name: 'deleted', queue_partition_key: 'tenant' });
    expect(await row(direct)).toMatchObject({ queue_name: INTERNAL_QUEUE_NAME });
    await db.resumeWorkflows([queued], 'explicit');
    expect(await row(queued)).toMatchObject({
      queue_name: 'explicit',
      queue_partition_key: 'tenant',
      application_name: APP,
    });
  });

  test.each(['cancel', 'dead-letter'])(
    'SC-qp10: terminal route retained after %s does not consume concurrency or rate limits',
    async (action) => {
      const q = await queue({ globalConcurrency: 1, rateLimit: { limitPerPeriod: 1, periodSec: 60 } });
      const terminal = await insert(q.name, { status: 'PENDING', recovery: 5 });
      await db.pool.query('UPDATE dbos.workflow_status SET rate_limited = true WHERE workflow_uuid = $1', [terminal]);
      if (action === 'cancel') await db.cancelWorkflows([terminal]);
      else await db.deadLetterWorkflows([terminal], 5);
      const next = await insert(q.name);
      expect(await dequeue(q)).toEqual([next]);
      expect(await row(terminal)).toMatchObject({ queue_name: q.name, started_at_epoch_ms: null });
      expect((await db.listWorkflows({ workflowIDs: [terminal] }))[0]).toMatchObject({
        queueName: q.name,
        status: action === 'cancel' ? 'CANCELLED' : 'MAX_RECOVERY_ATTEMPTS_EXCEEDED',
      });
    },
  );

  test('SC-qp09: SDK listener filters namespaces and rereads hints from an external client', async () => {
    const q = await queue();
    const hint = jest.fn();
    db.queueControlListeners.add(hint);
    try {
      await observer.query('SELECT pg_notify($1, $2)', [
        'dbos_queue_control_channel',
        JSON.stringify({ schema: 'other', name: q.name }),
      ]);
      await (await client(APP)).pauseQueue(q.name);
      await retryUntilSuccess(() => expect(hint).toHaveBeenCalledWith(q.name));
      expect(hint).toHaveBeenCalledTimes(1);
      expect(await q.getControlState()).toMatchObject({ paused: true });
    } finally {
      db.queueControlListeners.delete(hint);
    }
  });

  test('SC-qp10: execution context exposes the persisted route after queued resume', async () => {
    const q = await queue();
    const handle = await DBOS.startWorkflow(routeWorkflow, { queueName: q.name })();
    await DBOS.cancelWorkflow(handle.workflowID);
    await DBOS.resumeWorkflow(handle.workflowID);
    const accepted = await dequeue(q);
    await DBOSExecutor.globalInstance!.dispatchDequeuedWorkflows(accepted);
    expect(await handle.getResult()).toBe(q.name);
    expect(await routeWorkflow()).toBeUndefined();
  });

  test.each(['ordinary', 'partition'])(
    'SC-qp01: %s rollback releases the pause barrier without accepting work',
    async (kind) => {
      const q = await queue(kind === 'partition' ? { partitionConcurrency: 1 } : {});
      const id = await insert(q.name, { partition: kind === 'partition' ? 'p' : undefined });
      const entered = new Event();
      const release = new Event();
      releases.push(() => release.set());
      const point =
        kind === 'partition'
          ? DEBUG_TRIGGER_PARTITIONED_DEQUEUE_AFTER_CANDIDATES
          : DEBUG_TRIGGER_FIND_AND_MARK_AFTER_SELECT;
      setDebugTrigger(point, {
        asyncCallback: async () => {
          entered.set();
          await release.wait();
          throw new Error('claim rollback');
        },
      });
      const claim = kind === 'partition' ? batchDequeue(q) : dequeue(q);
      const rejected = expect(claim).rejects.toThrow('claim rollback');
      await entered.wait();
      const pause = q.pause();
      await blockedControl();
      release.set();
      await rejected;
      expect(await pause).toMatchObject({ paused: true });
      expect((await row(id)).status).toBe('ENQUEUED');
    },
  );

  test('SC-qp03: concurrent control commands serialize and wake never changes queue ownership', async () => {
    const q = await queue();
    const lock = await db.pool.connect();
    releases.push(() => {
      void lock.query('ROLLBACK').finally(() => lock.release());
    });
    await lock.query('BEGIN');
    await lock.query('SELECT 1 FROM dbos.queues WHERE name = $1 FOR UPDATE', [q.name]);
    const pause = q.pause();
    await blockedControl();
    await lock.query('COMMIT');
    expect(await pause).toMatchObject({ paused: true });
    const [wake, again] = await Promise.all([q.wake(), q.wake()]);
    expect(wake).toEqual(again);
    expect(await q.getControlState()).toMatchObject({ paused: false });
    expect((await db.getQueue(q.name))?.applicationName).toBe(APP);
  });

  test('SC-qp06: persisted queue ownership independently restricts readiness and claims', async () => {
    const q = await queue();
    await insert(q.name);
    await db.pool.query('UPDATE dbos.queues SET application_name = $1 WHERE name = $2', ['foreign', q.name]);
    expect(await db.findQueuesWithEnqueuedWorkflows([request(q)], version())).toEqual([]);
    await expect(dequeue(q)).rejects.toBeInstanceOf(QueueUnavailableError);
  });

  test('SC-qp03: wake preserves priority and global concurrency instead of forcing claims', async () => {
    const q = await queue({ globalConcurrency: 1 });
    const active = await insert(q.name, { status: 'PENDING' });
    const low = await insert(q.name, { priority: 10 });
    const high = await insert(q.name, { priority: 1 });
    await q.pause();
    await q.wake();
    expect(await dequeue(q)).toEqual([]);
    await db.cancelWorkflows([active]);
    expect(await dequeue(q)).toEqual([high]);
    expect((await row(low)).status).toBe('ENQUEUED');
  });

  test('SC-qp08: clearing the local running registry signals only after releasing the budget', async () => {
    const q = await queue();
    const event = new Event();
    releases.push(() => event.set());
    const id = randomUUID();
    const hint = jest.fn(() => expect(db.countRunningWorkflowsForQueue(q.name)).toBe(0));
    db.queueBudgetListeners.add(hint);
    try {
      db.registerRunningWorkflow(id, event.wait(), () => db.clearRunningWorkflow(id), q.name, 'partition');
      expect(db.countRunningWorkflowsForQueue(q.name)).toBe(1);
      const finished = db.runningWorkflowMap.get(id)!.promise;
      event.set();
      await finished;
      expect(hint).toHaveBeenCalledWith(q.name);
      expect(hint).toHaveBeenCalledTimes(1);
      db.clearRunningWorkflow(id);
      expect(hint).toHaveBeenCalledTimes(1);
    } finally {
      db.queueBudgetListeners.delete(hint);
    }
  });

  test('SC-qp02/09: pause and delayed backlog survive restart without LISTEN/NOTIFY', async () => {
    const q = await queue({ minPollingIntervalMs: 10 });
    await q.pause();
    const h = await DBOS.startWorkflow(routeWorkflow, { queueName: q.name, enqueueOptions: { delaySeconds: 3600 } })();
    await DBOS.shutdown();
    DBOS.setConfig({ ...config, listenQueues: [q.name], useListenNotify: false });
    await DBOS.launch();
    db = DBOSExecutor.globalInstance!.systemDatabase;
    expect(await DBOS.getQueueControlState(q.name)).toMatchObject({ queueId: q.queueId, paused: true });
    await db.pool.query('UPDATE dbos.workflow_status SET delay_until_epoch_ms = 0 WHERE workflow_uuid = $1', [
      h.workflowID,
    ]);
    await db.transitionDelayedWorkflows();
    expect((await row(h.workflowID)).status).toBe('ENQUEUED');
    await expect(dequeue((await DBOS.retrieveQueue(q.name))!)).rejects.toBeInstanceOf(QueueUnavailableError);
    // No local hint: only periodic reconcile on the executor can observe this client's wake.
    await (await client(APP)).wakeQueue(q.name);
    expect(await h.getResult()).toBe(q.name);
  });
});
