import { wfQueueRunner, WorkflowQueue } from '../src/wfqueue';
import type { DBOSExecutor } from '../src/dbos-executor';
import type { ClaimResult, ClaimScope, HintOutcome, QueueHint, QueueWakeupSource } from '../src/system_database';

describe('hint queue dispatch', () => {
  afterEach(() => {
    wfQueueRunner.stop();
    jest.useRealTimers();
  });
  function fixture(concurrency = 2) {
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
      countRunningWorkflowsForQueue: () => 0,
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
});
