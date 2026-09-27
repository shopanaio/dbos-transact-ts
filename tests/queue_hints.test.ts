import { wfQueueRunner, WorkflowQueue } from '../src/wfqueue';
import type { DBOSExecutor } from '../src/dbos-executor';
import type { ClaimResult, ClaimScope, HintOutcome, QueueHint, QueueWakeupSource } from '../src/system_database';

describe('hint queue dispatch', () => {
  afterEach(() => {
    wfQueueRunner.stop();
    jest.useRealTimers();
  });
  function fixture(concurrency = 2, running = () => 0) {
    let listener!: (hint: QueueHint) => Promise<HintOutcome>;
    let subscribed!: () => void;
    const ready = new Promise<void>((resolve) => {
      subscribed = resolve;
    });
    const publish = jest.fn(async (_hint: QueueHint) => {});
    const source: QueueWakeupSource = {
      mode: 'hint',
      metadataRefreshMs: 5000,
      blockedProbeMs: 1000,
      subscribe: (callback) => {
        listener = callback;
        subscribed();
        return () => {};
      },
      setBudgetProbe: () => {},
      publish,
    };
    const claim = jest.fn(
      async (
        _queue: WorkflowQueue,
        _executor: string,
        _version: string,
        _part?: string,
        _running?: number,
        _partitionRunning?: number,
        _scope?: ClaimScope,
      ): Promise<ClaimResult> => ({ workflowIds: [] }),
    );
    const db = {
      queueWakeupSource: source,
      queueControlListeners: new Set(),
      queueBudgetListeners: new Set(),
      listQueues: async () => [WorkflowQueue.recordFromParams('hint-test', { workerConcurrency: concurrency })],
      countRunningWorkflowsForQueue: () => running(),
      countRunningWorkflowsForPartition: () => 0,
      findAndMarkStartableWorkflows: claim,
      transitionDelayedWorkflows: async () => {},
    };
    const exec = {
      systemDatabase: db,
      executorID: 'worker',
      logger: { warn: jest.fn() },
      dispatchDequeuedWorkflows: jest.fn(async () => {}),
    } as unknown as DBOSExecutor;
    const loop = wfQueueRunner.dispatchLoop(exec, ['hint-test'], 3);
    return {
      ready,
      loop,
      claim,
      publish,
      exec,
      source,
      db,
      hint: (shard: number) => listener({ queueName: 'hint-test', scope: 'app', shard }),
    };
  }
  test('idle hint mode performs no claim without a hint', async () => {
    jest.useFakeTimers();
    const f = fixture();
    await f.ready;
    await jest.advanceTimersByTimeAsync(10000);
    expect(f.claim).not.toHaveBeenCalled();
    wfQueueRunner.stop();
    await f.loop;
  });
  test('parallel shards reserve the queue budget before I/O', async () => {
    const f = fixture(1);
    await f.ready;
    let release!: (value: ClaimResult) => void;
    const gate = new Promise<ClaimResult>((resolve) => {
      release = resolve;
    });
    f.claim.mockImplementationOnce(async () => gate);
    const first = f.hint(0);
    await f.hint(1);
    expect(f.claim.mock.calls[0][6]?.reservedBudget).toBe(1);
    expect(f.claim.mock.calls[1][6]?.reservedBudget).toBe(0);
    release({ workflowIds: ['accepted'] });
    await first;
    expect(f.exec.dispatchDequeuedWorkflows).toHaveBeenCalledWith(['accepted']);
    wfQueueRunner.stop();
    await f.loop;
  });
  test('rate and partition window deadlines publish hints rather than claim', async () => {
    jest.useFakeTimers();
    const f = fixture();
    await f.ready;
    f.claim.mockResolvedValueOnce({
      workflowIds: [],
      limitedBy: 'rate',
      retryAt: Date.now() + 1000,
      wakeAt: Date.now() + 2000,
    });
    await f.hint(7);
    await jest.advanceTimersByTimeAsync(1100);
    expect(f.publish).toHaveBeenCalledWith({ queueName: 'hint-test', scope: 'app', shard: 7 });
    expect(f.claim).toHaveBeenCalledTimes(1);
    wfQueueRunner.stop();
    await f.loop;
  });
  test('global limit parks the hint until the blocked probe reports new quota', async () => {
    jest.useFakeTimers();
    const f = fixture();
    const probe = jest.fn(async (_name: string, hints: QueueHint[]) => hints);
    (f.source as { probeBlocked?: typeof probe }).probeBlocked = probe;
    await f.ready;
    f.claim.mockResolvedValueOnce({ workflowIds: [], limitedBy: 'global' });
    await f.hint(3);
    await jest.advanceTimersByTimeAsync(500);
    expect(f.publish).not.toHaveBeenCalled();
    await jest.advanceTimersByTimeAsync(600);
    expect(probe).toHaveBeenCalledWith('hint-test', [{ queueName: 'hint-test', scope: 'app', shard: 3 }]);
    expect(f.publish).toHaveBeenCalledWith({ queueName: 'hint-test', scope: 'app', shard: 3 });
    wfQueueRunner.stop();
    await f.loop;
  });
  test('contention retries by an exponential timer and resets after progress', async () => {
    jest.useFakeTimers();
    const f = fixture();
    await f.ready;
    f.claim.mockResolvedValueOnce({ workflowIds: [], limitedBy: 'contention' });
    await f.hint(1);
    await jest.advanceTimersByTimeAsync(60);
    expect(f.publish).not.toHaveBeenCalled();
    await jest.advanceTimersByTimeAsync(100);
    expect(f.publish).toHaveBeenCalledTimes(1);
    f.claim.mockResolvedValueOnce({ workflowIds: [], limitedBy: 'contention' });
    await f.hint(1);
    await jest.advanceTimersByTimeAsync(160);
    expect(f.publish).toHaveBeenCalledTimes(1);
    await jest.advanceTimersByTimeAsync(150);
    expect(f.publish).toHaveBeenCalledTimes(2);
    wfQueueRunner.stop();
    await f.loop;
  });
  test('a hint without budget waits for the budget listener instead of polling', async () => {
    let running = 1;
    const f = fixture(1, () => running);
    await f.ready;
    await f.hint(2);
    expect(f.claim.mock.calls[0][6]?.reservedBudget).toBe(0);
    expect(f.publish).not.toHaveBeenCalled();
    running = 0;
    for (const listener of f.db.queueBudgetListeners as Set<(name: string) => void>) listener('hint-test');
    expect(f.publish).toHaveBeenCalledWith({ queueName: 'hint-test', scope: 'app', shard: 2 });
    wfQueueRunner.stop();
    await f.loop;
  });
  test('a full batch reports drainedFull only when the budget is spent and repolls the shard', async () => {
    jest.useFakeTimers();
    const f = fixture(2);
    await f.ready;
    f.claim.mockResolvedValueOnce({ workflowIds: ['a', 'b'] });
    expect(await f.hint(4)).toMatchObject({ drainedFull: true });
    await jest.advanceTimersByTimeAsync(100);
    expect(f.publish).toHaveBeenCalledWith({ queueName: 'hint-test', scope: 'app', shard: 4 });
    wfQueueRunner.stop();
    await f.loop;
  });
});
