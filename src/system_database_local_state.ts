import type { GlobalLogger } from './telemetry/logs';

export class NotificationMap<T> {
  map: Map<string, Map<number, (event?: T) => void>> = new Map();
  curCK: number = 0;

  registerCallback(key: string, cb: (event?: T) => void) {
    if (!this.map.has(key)) {
      this.map.set(key, new Map());
    }
    const ck = this.curCK++;
    this.map.get(key)!.set(ck, cb);
    return { key, ck };
  }

  deregisterCallback(k: { key: string; ck: number }) {
    if (!this.map.has(k.key)) return;
    const sm = this.map.get(k.key)!;
    if (!sm.has(k.ck)) return;
    sm.delete(k.ck);
    if (sm.size === 0) {
      this.map.delete(k.key);
    }
  }

  callCallbacks(key: string, event?: T) {
    if (!this.map.has(key)) return;
    const sm = this.map.get(key)!;
    for (const cb of sm.values()) {
      cb(event);
    }
  }
}

/** Общее состояние процесса для всех реализаций системной базы. */
export class SystemDatabaseLocalState {
  constructor(
    private readonly localLogger: GlobalLogger,
    private readonly isDestroyed: () => boolean = () => false,
  ) {}
  readonly resultsMap = new NotificationMap<void>();
  readonly notificationsMap: NotificationMap<void> = new NotificationMap();

  readonly workflowEventsMap: NotificationMap<void> = new NotificationMap();

  readonly streamsMap: NotificationMap<void> = new NotificationMap();

  readonly runningWorkflowMap: Map<
    string,
    { promise: Promise<unknown>; queueName?: string; queuePartitionKey?: string }
  > = new Map();

  registerRunningWorkflow(
    workflowID: string,
    workflowPromise: Promise<unknown>,
    onSettled: () => void,
    queueName?: string,
    queuePartitionKey?: string,
  ) {
    // Need to await for the workflow and capture errors.
    const awaitWorkflowPromise = workflowPromise
      .catch((error) => {
        const outcome = this.isDestroyed() ? 'was abandoned by shutdown' : 'failed';
        this.localLogger.debug(`Workflow ${workflowID} ${outcome}: ${error}`);
      })
      .finally(() => {
        onSettled();
      });
    this.runningWorkflowMap.set(workflowID, {
      promise: awaitWorkflowPromise,
      queueName,
      queuePartitionKey,
    });
  }

  checkForRunningWorkflow(workflowID: string): boolean {
    return this.runningWorkflowMap.has(workflowID);
  }

  clearRunningWorkflow(workflowID: string): void {
    const queueName = this.runningWorkflowMap.get(workflowID)?.queueName;
    this.runningWorkflowMap.delete(workflowID);
    if (queueName) for (const listener of this.queueBudgetListeners) listener(queueName);
  }

  countRunningWorkflowsForQueue(queueName: string): number {
    let count = 0;
    for (const entry of this.runningWorkflowMap.values()) {
      if (entry.queueName === queueName) count++;
    }
    return count;
  }

  countRunningWorkflowsForPartition(queueName: string, queuePartitionKey?: string): number {
    let count = 0;
    for (const entry of this.runningWorkflowMap.values()) {
      if (entry.queueName === queueName && entry.queuePartitionKey === queuePartitionKey) count++;
    }
    return count;
  }

  async awaitRunningWorkflows(timeoutMS?: number): Promise<void> {
    if (timeoutMS !== undefined && timeoutMS > 0) {
      const deadline = Date.now() + timeoutMS;
      if (this.runningWorkflowMap.size > 0) {
        this.localLogger.info('Waiting for pending workflows to finish.');
      }
      // Each pass picks up workflows a draining workflow started, and awaits any given run only once.
      const awaited = new Set<Promise<unknown>>();
      for (;;) {
        const pending = Array.from(this.runningWorkflowMap.values(), (entry) => entry.promise).filter(
          (promise) => !awaited.has(promise),
        );
        if (pending.length === 0) break;
        for (const promise of pending) awaited.add(promise);
        let timer: ReturnType<typeof setTimeout> | undefined;
        const timedOut = await Promise.race([
          Promise.allSettled(pending).then(() => false),
          new Promise<boolean>((resolve) => {
            timer = setTimeout(() => resolve(true), Math.max(0, deadline - Date.now()));
          }),
        ]);
        clearTimeout(timer);
        if (timedOut) break;
      }
    }
    if (this.runningWorkflowMap.size > 0) {
      this.localLogger.warn(
        `Shutting down while ${this.runningWorkflowMap.size} workflows are still running: ${Array.from(this.runningWorkflowMap.keys()).join(', ')}`,
      );
    }
    if (this.workflowEventsMap.map.size > 0) {
      this.localLogger.warn('Workflow events map is not empty - shutdown is not clean.');
    }
    if (this.notificationsMap.map.size > 0) {
      this.localLogger.warn('Message notification map is not empty - shutdown is not clean.');
    }
  }

  readonly queueControlListeners = new Set<(name?: string) => void>();

  readonly queueBudgetListeners = new Set<(name: string) => void>();
}
