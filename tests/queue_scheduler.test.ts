import { DBOSExecutor } from '../src/dbos-executor';
import { QueueReadinessRequest, QueueRecord, QueueUnavailableError } from '../src/system_database';
import { WorkflowQueue, wfQueueRunner, registerInternalQueue } from '../src/wfqueue';

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}

describe('batch queue scheduler', () => {
  let records: QueueRecord[];
  let loop: Promise<void> | undefined;
  let cleanup: Array<() => void>;
  let running: number;
  let db: ReturnType<typeof database>;
  let exec: DBOSExecutor;
  const record = (name = 'a', overrides: Partial<QueueRecord> = {}): QueueRecord => ({
    ...WorkflowQueue.recordFromParams(name, { minPollingIntervalMs: 100 }),
    queueId: `${name}-identity`,
    ...overrides,
  });
  function database() {
    return {
      appName: 'app',
      queueControlListeners: new Set<(name?: string) => void>(),
      queueBudgetListeners: new Set<(name: string) => void>(),
      listQueues: jest.fn(async (_app?: string, names?: string[]) =>
        records.filter((r) => !names || names.includes(r.name)).map((r) => ({ ...r })),
      ),
      transitionDelayedWorkflows: jest.fn(async () => undefined),
      findQueuesWithEnqueuedWorkflows: jest.fn(
        async (_requests: QueueReadinessRequest[], _version: string): Promise<QueueReadinessRequest[]> => [],
      ),
      findAndMarkStartableWorkflows: jest.fn(async (_queue: WorkflowQueue): Promise<string[]> => []),
      findAndMarkStartablePartitionedWorkflows: jest.fn(async (_queue: WorkflowQueue): Promise<string[]> => []),
      countRunningWorkflowsForQueue: jest.fn(() => running),
      countRunningWorkflowsForPartition: jest.fn(() => 0),
      getQueuePartitions: jest.fn(async () => ['p1', 'p2', 'p3']),
    };
  }
  const advance = (ms = 0) => jest.advanceTimersByTimeAsync(ms);
  const hint = (name?: string) => {
    for (const cb of db.queueControlListeners) cb(name);
  };
  const budgetHint = () => {
    for (const cb of db.queueBudgetListeners) cb('a');
  };
  const ready = () => db.findQueuesWithEnqueuedWorkflows.mockImplementation(async (requests) => requests);
  async function start(lanes = 2, batch = 1000, listen: string[] | null = null) {
    loop = wfQueueRunner.dispatchLoop(exec, listen, lanes, batch, 50);
    await advance();
  }
  function gate<T>(value: T) {
    const result = deferred<T>();
    cleanup.push(() => result.resolve(value));
    return result;
  }
  beforeEach(() => {
    jest.useFakeTimers({ now: 0 });
    jest.spyOn(Math, 'random').mockReturnValue(0.5);
    wfQueueRunner.clearRegistrations();
    records = [record()];
    running = 0;
    cleanup = [];
    db = database();
    exec = {
      executorID: 'executor',
      systemDatabase: db,
      logger: { warn: jest.fn(), info: jest.fn(), debug: jest.fn() },
      dispatchDequeuedWorkflows: jest.fn(async () => undefined),
    } as unknown as DBOSExecutor;
  });
  afterEach(async () => {
    wfQueueRunner.stop();
    cleanup.forEach((fn) => fn());
    await advance();
    await loop;
    loop = undefined;
    wfQueueRunner.clearRegistrations();
    jest.restoreAllMocks();
    jest.useRealTimers();
  });

  test('SC-qp06: coalesces 1000 empty queues into one statement without dequeue', async () => {
    records = Array.from({ length: 1000 }, (_, i) => record(`q${i}`, { pollingIntervalSec: 1 }));
    await start();
    await advance(999);
    expect(db.findQueuesWithEnqueuedWorkflows).not.toHaveBeenCalled();
    await advance(1);
    expect(db.findQueuesWithEnqueuedWorkflows).toHaveBeenCalledTimes(1);
    expect(db.findQueuesWithEnqueuedWorkflows.mock.calls[0][0]).toHaveLength(1000);
    expect(db.findAndMarkStartableWorkflows).not.toHaveBeenCalled();
  });

  test('SC-qp06: chunks due queues, never sends an empty batch or overlaps probes', async () => {
    records = Array.from({ length: 5 }, (_, i) => record(`q${i}`));
    const first = gate<QueueReadinessRequest[]>([]);
    db.findQueuesWithEnqueuedWorkflows.mockReturnValueOnce(first.promise);
    await start(2, 2);
    await advance(100);
    expect(db.findQueuesWithEnqueuedWorkflows).toHaveBeenCalledTimes(1);
    await advance(1000);
    expect(db.findQueuesWithEnqueuedWorkflows).toHaveBeenCalledTimes(1);
    expect(db.transitionDelayedWorkflows.mock.calls.length).toBeGreaterThan(1);
    first.resolve([]);
    await advance();
    expect(db.findQueuesWithEnqueuedWorkflows.mock.calls.map((c) => c[0].length)).toEqual([2, 2, 1]);
  });

  test('SC-qp06: preserves deadlines and bounds coalescing delay for mixed intervals', async () => {
    records = [record('a', { pollingIntervalSec: 0.051 }), record('b', { pollingIntervalSec: 0.101 })];
    await start();
    await advance(99);
    expect(db.findQueuesWithEnqueuedWorkflows).not.toHaveBeenCalled();
    await advance(1);
    expect(db.findQueuesWithEnqueuedWorkflows.mock.calls[0][0].map((r) => r.name)).toEqual(['a']);
    await advance(50);
    expect(db.findQueuesWithEnqueuedWorkflows.mock.calls[1][0].map((r) => r.name)).toEqual(['b']);
  });

  test('SC-qp02: paused queues have no readiness timers while maintenance continues', async () => {
    records = Array.from({ length: 1000 }, (_, i) => record(`paused-${i}`, { paused: true }));
    await start();
    await advance(5000);
    expect(db.findQueuesWithEnqueuedWorkflows).not.toHaveBeenCalled();
    expect(db.listQueues).toHaveBeenCalledTimes(6);
    expect(db.transitionDelayedWorkflows).toHaveBeenCalledTimes(6);
  });

  test('SC-qp02: Kafka bypasses listen filtering but never pause; internal queues still run', async () => {
    records = [record('ordinary'), record('kafka', { paused: true })];
    wfQueueRunner.pollerQueueNames.add('kafka');
    registerInternalQueue('_dbos_private', { minPollingIntervalMs: 100 });
    await start(2, 1000, []);
    await advance(100);
    expect(db.findQueuesWithEnqueuedWorkflows.mock.calls[0][0]).toEqual([
      expect.objectContaining({ name: '_dbos_private', internal: true }),
    ]);
    records[1].paused = false;
    hint('kafka');
    await advance(50);
    expect(db.findQueuesWithEnqueuedWorkflows.mock.calls.flatMap((c) => c[0]).map((r) => r.name)).toContain('kafka');
    expect(db.findQueuesWithEnqueuedWorkflows.mock.calls.flatMap((c) => c[0]).map((r) => r.name)).not.toContain(
      'ordinary',
    );
  });

  test.each(['notification', 'reconcile'])('SC-qp03/09: wakes a long-interval queue via %s', async (mode) => {
    records[0] = record('a', { paused: true, pollingIntervalSec: 120 });
    await start();
    records[0].paused = false;
    if (mode === 'notification') hint('a');
    await advance(mode === 'notification' ? 50 : 1050);
    expect(db.findQueuesWithEnqueuedWorkflows).toHaveBeenCalledTimes(1);
    expect(db.transitionDelayedWorkflows).toHaveBeenCalledTimes(mode === 'notification' ? 1 : 2);
  });

  test.each(['hint', 'fallback', 'config'])('SC-qp08: waits without SQL and resumes budget on %s', async (mode) => {
    records[0].workerConcurrency = 1;
    running = 1;
    ready();
    await start();
    await advance(100);
    expect(db.findQueuesWithEnqueuedWorkflows).not.toHaveBeenCalled();
    await advance(200);
    running = 0;
    if (mode === 'hint') budgetHint();
    if (mode === 'config') {
      records[0].workerConcurrency = 2;
      hint('a');
    }
    await advance(mode === 'fallback' ? 850 : 50);
    expect(db.findQueuesWithEnqueuedWorkflows).toHaveBeenCalledTimes(1);
    expect(db.findAndMarkStartableWorkflows).toHaveBeenCalledTimes(1);
  });

  test('SC-qp08: wake retains budget wait; partition-only limits do not block the queue', async () => {
    records = [record('a', { paused: true, workerConcurrency: 1 }), record('b', { partitionWorkerConcurrency: 1 })];
    running = 1;
    await start();
    records[0].paused = false;
    hint('a');
    await advance(100);
    expect(db.findQueuesWithEnqueuedWorkflows.mock.calls.flatMap((c) => c[0]).map((r) => r.name)).toEqual(['b']);
    running = 0;
    budgetHint();
    await advance(50);
    expect(db.findQueuesWithEnqueuedWorkflows.mock.calls.flatMap((c) => c[0]).map((r) => r.name)).toContain('a');
  });

  test('SC-qp07: ready queues preserve fairness and occupy lanes until dispatch finishes', async () => {
    records = [record('a'), record('b'), record('c')];
    ready();
    db.findAndMarkStartableWorkflows.mockImplementation(async (q) => [q.name]);
    const dispatch = gate<void>(undefined);
    jest.mocked(exec.dispatchDequeuedWorkflows).mockReturnValueOnce(dispatch.promise);
    await start(1);
    await advance(100);
    expect(db.findAndMarkStartableWorkflows.mock.calls.map((c) => c[0].name)).toEqual(['a']);
    await advance(200);
    expect(db.findQueuesWithEnqueuedWorkflows).toHaveBeenCalledTimes(1);
    dispatch.resolve();
    await advance();
    expect(db.findAndMarkStartableWorkflows.mock.calls.map((c) => c[0].name)).toEqual(['a', 'b', 'c']);
  });

  test.each(['pause', 'identity', 'config', 'error'])(
    'SC-qp07: stale probe after %s releases only its reservation',
    async (change) => {
      ready();
      const probe = gate<QueueReadinessRequest[]>([]);
      db.findQueuesWithEnqueuedWorkflows.mockReturnValueOnce(probe.promise);
      await start();
      await advance(100);
      const old = db.findQueuesWithEnqueuedWorkflows.mock.calls[0][0];
      if (change === 'pause') records[0].paused = true;
      else if (change === 'identity') records[0].queueId = 'replacement';
      else records[0].workerConcurrency = 2;
      hint('a');
      await advance(100);
      expect(db.findQueuesWithEnqueuedWorkflows).toHaveBeenCalledTimes(1);
      if (change === 'error') probe.reject(new Error('probe failed'));
      else probe.resolve(old);
      await advance(change === 'error' ? 100 : 0);
      if (change === 'pause') expect(db.findAndMarkStartableWorkflows).not.toHaveBeenCalled();
      else {
        expect(db.findQueuesWithEnqueuedWorkflows).toHaveBeenCalledTimes(2);
        expect(db.findAndMarkStartableWorkflows).toHaveBeenCalledTimes(1);
        expect(db.findAndMarkStartableWorkflows.mock.calls[0][0].queueId).toBe(records[0].queueId);
      }
    },
  );

  test.each(['pause', 'identity', 'config', 'stop'])(
    'SC-qp07: committed claim survives %s and shutdown drains dispatch',
    async (change) => {
      ready();
      const claim = gate<string[]>([]);
      const dispatch = gate<void>(undefined);
      db.findAndMarkStartableWorkflows.mockReturnValueOnce(claim.promise);
      jest.mocked(exec.dispatchDequeuedWorkflows).mockReturnValueOnce(dispatch.promise);
      await start(1);
      await advance(100);
      if (change === 'pause') records[0].paused = true;
      if (change === 'identity') records[0].queueId = 'replacement';
      if (change === 'config') records[0].workerConcurrency = 2;
      hint('a');
      await advance(50);
      expect(db.findAndMarkStartableWorkflows).toHaveBeenCalledTimes(1);
      wfQueueRunner.stop();
      let ended = false;
      void loop!.then(() => {
        ended = true;
      });
      claim.resolve(['accepted']);
      await advance();
      expect(exec.dispatchDequeuedWorkflows).toHaveBeenCalledWith(['accepted']);
      expect(ended).toBe(false);
      dispatch.resolve();
      await advance();
      expect(ended).toBe(true);
      hint('a');
      budgetHint();
      await advance(2000);
      expect(db.findAndMarkStartableWorkflows).toHaveBeenCalledTimes(1);
      expect(db.queueControlListeners.size).toBe(0);
      expect(db.queueBudgetListeners.size).toBe(0);
    },
  );

  test('SC-qp09: readiness failure backs off once per batch and never reports queues empty', async () => {
    records = Array.from({ length: 5 }, (_, i) => record(`q${i}`));
    db.findQueuesWithEnqueuedWorkflows.mockRejectedValue(new Error('offline'));
    await start();
    await advance(100);
    for (const delay of [100, 200, 400, 800, 1000]) {
      const count = db.findQueuesWithEnqueuedWorkflows.mock.calls.length;
      await advance(delay - 1);
      expect(db.findQueuesWithEnqueuedWorkflows).toHaveBeenCalledTimes(count);
      await advance(1);
      expect(db.findQueuesWithEnqueuedWorkflows).toHaveBeenCalledTimes(count + 1);
    }
    expect(exec.logger.warn).toHaveBeenCalledTimes(6);
    ready();
    await advance(1000);
    expect(db.findAndMarkStartableWorkflows).toHaveBeenCalledTimes(5);
  });

  test('SC-qp09: serializes metadata and retains same-name hints received during await', async () => {
    const first = gate<QueueRecord[]>([]);
    db.listQueues.mockReturnValueOnce(first.promise);
    await start();
    records[0].queueId = 'new';
    hint('a');
    await advance(500);
    expect(db.listQueues).toHaveBeenCalledTimes(1);
    first.resolve([record('a', { paused: true })]);
    await advance(50);
    expect(db.listQueues).toHaveBeenCalledTimes(2);
    expect(db.listQueues.mock.calls[1][1]).toEqual(['a']);
    expect(db.findQueuesWithEnqueuedWorkflows.mock.calls[0][0][0].queueId).toBe('new');
  });

  test.each(['partial', 'full'])(
    'SC-qp09: %s metadata retry survives signals, overflow, and restoration',
    async (mode) => {
      records[0].paused = true;
      await start(2, 2);
      db.listQueues.mockRejectedValue(new Error('metadata offline'));
      records[0].paused = false;
      if (mode === 'partial') hint('a');
      else await advance(1000);
      await advance();
      for (const delay of [100, 200, 400, 800, 1000]) {
        const count = db.listQueues.mock.calls.length;
        hint('a');
        hint('b');
        hint('c');
        await advance(delay - 1);
        expect(db.listQueues).toHaveBeenCalledTimes(count);
        await advance(1);
        expect(db.listQueues).toHaveBeenCalledTimes(count + 1);
      }
      expect(db.findQueuesWithEnqueuedWorkflows).not.toHaveBeenCalled();
      db.listQueues.mockImplementation(async (_app, names) => records.filter((r) => !names || names.includes(r.name)));
      await advance(1050);
      expect(db.listQueues.mock.calls.at(-1)![1]).toBeUndefined();
      expect(db.findQueuesWithEnqueuedWorkflows).toHaveBeenCalledTimes(1);
    },
  );

  test('SC-qp09: partial removal does not remove other queues and full reconcile cannot starve', async () => {
    records = [record('a', { paused: true }), record('b', { paused: true })];
    await start();
    records = [record('b', { paused: true })];
    hint('a');
    await advance();
    records = [record('b', { paused: false }), record('new')];
    for (let i = 0; i < 10; i++) {
      hint('b');
      await advance(100);
    }
    expect(db.listQueues.mock.calls.some((c, i) => i > 0 && c[1] === undefined)).toBe(true);
    await advance(150);
    const names = db.findQueuesWithEnqueuedWorkflows.mock.calls.flatMap((c) => c[0]).map((r) => r.name);
    expect(names).toContain('new');
    expect(names).not.toContain('a');
  });

  test('SC-qp01: pause guard stops fallback sweep without contention backoff', async () => {
    records[0] = record('a', { partitionConcurrency: 2 });
    ready();
    db.findAndMarkStartableWorkflows.mockRejectedValue(new QueueUnavailableError('a'));
    await start();
    await advance(100);
    expect(db.findAndMarkStartableWorkflows).toHaveBeenCalledTimes(1);
    expect(exec.logger.warn).not.toHaveBeenCalled();
  });

  test('SC-qp09: stop drains pending metadata and prevents retries or late state application', async () => {
    const pending = gate<QueueRecord[]>([]);
    db.listQueues.mockReturnValueOnce(pending.promise);
    await start();
    hint('a');
    wfQueueRunner.stop();
    pending.resolve([record()]);
    await advance(2000);
    await loop;
    expect(db.listQueues).toHaveBeenCalledTimes(1);
    expect(db.findQueuesWithEnqueuedWorkflows).not.toHaveBeenCalled();
  });

  test('SC-qp06: scattered deadlines stay within twenty readiness slots per second', async () => {
    records = Array.from({ length: 1000 }, (_, i) => record(`q${i}`, { pollingIntervalSec: (i + 1) / 1000 }));
    await start();
    await advance(1000);
    expect(db.findQueuesWithEnqueuedWorkflows.mock.calls.length).toBeLessThanOrEqual(20);
    const observed = new Set(db.findQueuesWithEnqueuedWorkflows.mock.calls.flatMap((c) => c[0]).map((r) => r.name));
    expect(observed.size).toBe(1000);
  });

  test('SC-qp07: wake during claim preserves immediate poll and resets contention backoff once', async () => {
    ready();
    db.findAndMarkStartableWorkflows.mockRejectedValueOnce(Object.assign(new Error('contention'), { code: '40001' }));
    const claim = gate<string[]>([]);
    db.findAndMarkStartableWorkflows.mockReturnValueOnce(claim.promise);
    await start();
    await advance(300);
    expect(db.findAndMarkStartableWorkflows).toHaveBeenCalledTimes(2);
    records[0].paused = true;
    hint('a');
    await advance();
    records[0].paused = false;
    hint('a');
    await advance();
    claim.resolve([]);
    await advance();
    expect(db.findAndMarkStartableWorkflows).toHaveBeenCalledTimes(3);
    // Positive readiness must not scale the interval down a second time.
    await advance(99);
    expect(db.findAndMarkStartableWorkflows).toHaveBeenCalledTimes(3);
    await advance(1);
    expect(db.findAndMarkStartableWorkflows).toHaveBeenCalledTimes(4);
  });

  test('SC-qp08: budget is rechecked after positive readiness and the ready phase survives waiting', async () => {
    records[0].workerConcurrency = 1;
    const probe = gate<QueueReadinessRequest[]>([]);
    db.findQueuesWithEnqueuedWorkflows.mockReturnValueOnce(probe.promise);
    await start();
    await advance(100);
    running = 1;
    probe.resolve(db.findQueuesWithEnqueuedWorkflows.mock.calls[0][0]);
    await advance(200);
    expect(db.findAndMarkStartableWorkflows).not.toHaveBeenCalled();
    expect(db.findQueuesWithEnqueuedWorkflows).toHaveBeenCalledTimes(1);
    running = 0;
    budgetHint();
    await advance();
    expect(db.findAndMarkStartableWorkflows).toHaveBeenCalledTimes(1);
    expect(db.findQueuesWithEnqueuedWorkflows).toHaveBeenCalledTimes(1);
  });

  test.each(['rollback', 'dispatch'])(
    'SC-qp07: %s failure releases an obsolete claim reservation and lane',
    async (failure) => {
      ready();
      const claim = gate<string[]>([]);
      db.findAndMarkStartableWorkflows.mockReturnValueOnce(claim.promise);
      if (failure === 'dispatch')
        jest.mocked(exec.dispatchDequeuedWorkflows).mockRejectedValueOnce(new Error('dispatch failed'));
      await start(1);
      await advance(100);
      records[0].queueId = 'replacement';
      hint('a');
      await advance();
      if (failure === 'rollback') claim.reject(new Error('rolled back'));
      else claim.resolve(['committed']);
      await advance();
      expect(db.findAndMarkStartableWorkflows).toHaveBeenCalledTimes(2);
      expect(exec.logger.warn).toHaveBeenCalledTimes(1);
      if (failure === 'dispatch') expect(exec.dispatchDequeuedWorkflows).toHaveBeenCalledWith(['committed']);
    },
  );

  test('SC-qp09: failure preserves existing active metadata and successful metadata resets retry delay', async () => {
    records[0].pollingIntervalSec = 0.05;
    await start();
    db.listQueues.mockRejectedValueOnce(new Error('offline'));
    hint('a');
    await advance();
    await advance(50);
    // Existing active metadata still schedules work before the failed refresh can retry.
    expect(db.findQueuesWithEnqueuedWorkflows).toHaveBeenCalledTimes(1);
    await advance(50);
    db.listQueues.mockRejectedValueOnce(new Error('offline again'));
    hint('a');
    await advance();
    const count = db.listQueues.mock.calls.length;
    await advance(99);
    expect(db.listQueues).toHaveBeenCalledTimes(count);
    await advance(1);
    expect(db.listQueues).toHaveBeenCalledTimes(count + 1);
  });

  test('SC-qp09: reordered pause/wake hints never apply payload state', async () => {
    records[0].paused = true;
    await start();
    hint('a');
    hint('a');
    hint('a');
    await advance(1000);
    expect(db.findQueuesWithEnqueuedWorkflows).not.toHaveBeenCalled();
  });

  test('SC-qp07: stop during probe drains it but never starts a claim', async () => {
    const probe = gate<QueueReadinessRequest[]>([]);
    db.findQueuesWithEnqueuedWorkflows.mockReturnValueOnce(probe.promise);
    await start();
    await advance(100);
    wfQueueRunner.stop();
    probe.resolve(db.findQueuesWithEnqueuedWorkflows.mock.calls[0][0]);
    await advance();
    await loop;
    expect(db.findAndMarkStartableWorkflows).not.toHaveBeenCalled();
  });

  test.each([0, 1, 10, 100])('SC-qp06: only the %i percent nonempty queues enter detailed dequeue', async (percent) => {
    records = Array.from({ length: 1000 }, (_, i) => record(`q${i}`));
    db.findQueuesWithEnqueuedWorkflows.mockImplementation(async (requests) =>
      requests.filter((r) => Number(r.name.slice(1)) < percent * 10),
    );
    await start(3, 250);
    await advance(100);
    expect(db.findQueuesWithEnqueuedWorkflows).toHaveBeenCalledTimes(4);
    expect(db.findQueuesWithEnqueuedWorkflows.mock.calls.every((c) => c[0].length === 250)).toBe(true);
    expect(db.findAndMarkStartableWorkflows).toHaveBeenCalledTimes(percent * 10);
  });

  test('SC-qp08: budget hints preserve a future deadline and never remove pause', async () => {
    records[0].workerConcurrency = 1;
    ready();
    await start();
    await advance(100);
    expect(db.findAndMarkStartableWorkflows).toHaveBeenCalledTimes(1);
    await advance(50);
    budgetHint();
    await advance(49);
    expect(db.findAndMarkStartableWorkflows).toHaveBeenCalledTimes(1);
    await advance(1);
    expect(db.findAndMarkStartableWorkflows).toHaveBeenCalledTimes(2);
    records[0].paused = true;
    hint('a');
    await advance();
    budgetHint();
    await advance(1000);
    expect(db.findAndMarkStartableWorkflows).toHaveBeenCalledTimes(2);
  });

  test.each([0, 1])('SC-qp09: metadata retry respects deterministic jitter at random=%s', async (random) => {
    jest.mocked(Math.random).mockReturnValue(random);
    db.listQueues.mockRejectedValue(new Error('offline'));
    await start();
    const delay = random === 0 ? 95 : 105;
    await advance(delay - 1);
    expect(db.listQueues).toHaveBeenCalledTimes(1);
    await advance(1);
    expect(db.listQueues).toHaveBeenCalledTimes(2);
  });

  test('SC-qp09: stop cancels a pending metadata retry even under notification load', async () => {
    db.listQueues.mockRejectedValue(new Error('offline'));
    await start();
    hint('a');
    wfQueueRunner.stop();
    await advance(2000);
    await loop;
    expect(db.listQueues).toHaveBeenCalledTimes(1);
  });

  test.each(['absent', 'old'])(
    'SC-qp09: a slow partial %s snapshot is applied before the due full reconcile',
    async (snapshot) => {
      records[0].paused = true;
      await start();
      const partial = gate<QueueRecord[]>([]);
      db.listQueues.mockReturnValueOnce(partial.promise);
      hint('a');
      await advance();
      records[0] = record('a', { queueId: 'replacement' });
      await advance(1000);
      expect(db.listQueues).toHaveBeenCalledTimes(2);
      partial.resolve(snapshot === 'absent' ? [] : [record('a', { paused: true })]);
      await advance(100);
      expect(db.listQueues.mock.calls[2][1]).toBeUndefined();
      expect(db.findQueuesWithEnqueuedWorkflows.mock.calls[0][0][0].queueId).toBe('replacement');
    },
  );
});
