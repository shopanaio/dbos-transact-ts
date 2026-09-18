import { DBOSExecutor } from './dbos-executor';
import {
  DEBUG_TRIGGER_WORKFLOW_QUEUE_START,
  DEBUG_TRIGGER_BETWEEN_PARTITION_DISPATCHES,
  debugTriggerPoint,
} from './debugpoint';
import { QueueUnavailableError, type QueueControlState, type QueueRecord, type SystemDatabase } from './system_database';
import type { GlobalLogger } from './telemetry/logs';
import { globalParams, RESERVED_QUEUE_NAME_PREFIX } from './utils';

/**
 * Log a single queue's name and its set parameters. Unset parameters are
 * omitted, matching `Queue: <name> (concurrency=…, worker_concurrency=…,
 * limit=N/Ts, partition_concurrency=…)`.
 */
export function logQueue(logger: GlobalLogger, q: WorkflowQueue): void {
  const opts: string[] = [];
  if (q.concurrency !== undefined) {
    // On a partitioned queue the queue-wide scope is worth naming explicitly.
    opts.push(`${isPartitionedQueue(q) ? 'global_concurrency' : 'concurrency'}=${q.concurrency}`);
  }
  if (q.workerConcurrency !== undefined) opts.push(`worker_concurrency=${q.workerConcurrency}`);
  if (q.rateLimit !== undefined) opts.push(`limit=${q.rateLimit.limitPerPeriod}/${q.rateLimit.periodSec}s`);
  if (q.partitionConcurrency !== undefined) opts.push(`partition_concurrency=${q.partitionConcurrency}`);
  if (q.partitionWorkerConcurrency !== undefined) {
    opts.push(`partition_worker_concurrency=${q.partitionWorkerConcurrency}`);
  }
  if (q.partitionRateLimit !== undefined) {
    opts.push(`partition_limit=${q.partitionRateLimit.limitPerPeriod}/${q.partitionRateLimit.periodSec}s`);
  }
  const optsStr = opts.length > 0 ? ` (${opts.join(', ')})` : '';
  logger.info(`Queue: ${q.name}${optsStr}`);
}

/**
 * Limit the maximum number of functions started from a `WorkflowQueue`
 *   per given time period.
 * If the limit is 5 and the period is 10, no more than 5 functions can be
 *   started per 10 seconds.
 */
export interface QueueRateLimit {
  /** Number of queue dispateches per `periodSec` */
  limitPerPeriod: number;
  /** Period of time during which `limitPerPeriod` queued workflows may be dispatched */
  periodSec: number;
}

/**
 * Limit the number of concurrent workflows running for a queue.
 *
 * Queue-wide limits bound the queue as a whole. Setting any `partition` limit
 * additionally partitions the queue, so every enqueue must supply a partition
 * key, and that limit is then enforced separately within each partition.
 */
export interface QueueParameters {
  /** If defined, this limits the number of running workflows for a single DBOS process */
  workerConcurrency?: number;
  /** If defined, this limits the number of running workflows globally in the app */
  globalConcurrency?: number;
  /** If set, this limits the rate at which queued workflows are started */
  rateLimit?: QueueRateLimit;
  /** If defined, this limits the number of running workflows globally within each partition */
  partitionConcurrency?: number;
  /** If defined, this limits the number of running workflows on a single DBOS process within each partition */
  partitionWorkerConcurrency?: number;
  /** If set, this limits the rate at which queued workflows are started within each partition */
  partitionRateLimit?: QueueRateLimit;
  /** Base (minimum) polling interval in ms for this queue's dispatch loop (default 1000) */
  minPollingIntervalMs?: number;
  /** @deprecated Use `globalConcurrency`. */
  concurrency?: number;
}

/**
 * Behavior of `DBOS.registerQueue` / `DBOSClient.registerQueue` when a queue
 * with the same name already has a row in the `queues` table.
 *
 * - `update_if_latest_version`: overwrite the existing row only when the
 *   running application version is the latest registered version. Older
 *   versions in a rolling deploy will not overwrite a newer config.
 * - `always_update`: always overwrite the existing row.
 * - `never_update`: leave the existing row unchanged. The returned queue
 *   reflects the persisted config, not the supplied parameters.
 */
export type QueueConflictResolution = 'update_if_latest_version' | 'always_update' | 'never_update';

export interface RegisterQueueOptions extends QueueParameters {
  /** How to behave when a queue with the same name already exists. */
  onConflict?: QueueConflictResolution;
}

/** The per-partition limits, any of which partitions a queue. */
type PartitionLimits = Pick<
  QueueParameters,
  'partitionConcurrency' | 'partitionWorkerConcurrency' | 'partitionRateLimit'
>;

/**
 * Options removed in 5.0, and what replaces each.
 */
const REMOVED_QUEUE_PARAMS: Record<string, string> = {
  priorityEnabled: 'every queue dispatches in priority order, so the option can be deleted',
  partitionQueue:
    'set partitionConcurrency, partitionWorkerConcurrency, or partitionRateLimit instead, any of which partitions the queue',
};

/** True when any per-partition limit is set, which is what partitions a queue. */
function hasPartitionLimits(limits: PartitionLimits): boolean {
  return (
    limits.partitionConcurrency !== undefined ||
    limits.partitionWorkerConcurrency !== undefined ||
    limits.partitionRateLimit !== undefined
  );
}

/** True when a queue is partitioned, which is to say any per-partition limit is set on it. */
export function isPartitionedQueue(queue: WorkflowQueue): boolean {
  return hasPartitionLimits(queue);
}

/**
 * Room left under this worker's queue-wide concurrency limit, given how many of
 * its workflows are already running or claimed.
 */
function workerBudget(queue: WorkflowQueue, running: number): number {
  if (queue.workerConcurrency === undefined) {
    // A per-partition worker limit is enforced per partition instead.
    return Infinity;
  }
  return Math.max(0, queue.workerConcurrency - running);
}

/** 40001 serialization_failure or 55P03 lock_not_available: a peer is claiming the same rows. */
function isContentionError(e: unknown): boolean {
  const code = (e as NodeJS.ErrnoException).code;
  return code === '40001' || code === '55P03';
}

/** Fisher-Yates copy, so a sweep visits partitions in a different order each poll. */
function shuffled<T>(items: T[]): T[] {
  const result = [...items];
  for (let i = result.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [result[i], result[j]] = [result[j], result[i]];
  }
  return result;
}

/**
 * Per-instance association of a client-bound queue to its `SystemDatabase`.
 * Stored off-class because any class member — including TS `private` — gives
 * the class a nominal brand, so the type-only members below all live as
 * module-level helpers to keep `WorkflowQueue` structurally compatible across
 * separate compiled copies of this package.
 */
const clientSystemDatabases = new WeakMap<WorkflowQueue, SystemDatabase>();

function requireDatabaseBacked(q: WorkflowQueue): void {
  if (!q.databaseBacked) {
    throw new Error(
      `Cannot configure queue ${q.name}: dynamic configuration is only supported for queues registered via DBOS.registerQueue.`,
    );
  }
}

function sysDBFor(q: WorkflowQueue): SystemDatabase {
  const clientDb = clientSystemDatabases.get(q);
  if (clientDb) return clientDb;
  const exec = DBOSExecutor.globalInstance;
  if (!exec) {
    throw new Error(`Cannot access system database for queue ${q.name}: DBOS has not been launched.`);
  }
  return exec.systemDatabase;
}

/** Validate a new queue-wide concurrency against the queue's other cached limits. */
function checkConcurrencyBounds(q: WorkflowQueue, value: number | undefined): void {
  if (value === undefined) return;
  if (q.workerConcurrency !== undefined && q.workerConcurrency > value) {
    throw new Error('workerConcurrency must be less than or equal to concurrency');
  }
  if (q.partitionConcurrency !== undefined && q.partitionConcurrency > value) {
    throw new Error('partitionConcurrency must be less than or equal to globalConcurrency');
  }
  if (q.partitionWorkerConcurrency !== undefined && q.partitionWorkerConcurrency > value) {
    throw new Error('partitionWorkerConcurrency must be less than or equal to concurrency');
  }
}

/** Whether the queue is still partitioned once these limits take these values. */
function partitionedAfter(q: WorkflowQueue, overrides: PartitionLimits): boolean {
  return hasPartitionLimits({
    partitionConcurrency: q.partitionConcurrency,
    partitionWorkerConcurrency: q.partitionWorkerConcurrency,
    partitionRateLimit: q.partitionRateLimit,
    ...overrides,
  });
}

function rateLimitFromRecord(max: number | null, periodSec: number | null): QueueRateLimit | undefined {
  return max !== null && periodSec !== null ? { limitPerPeriod: max, periodSec } : undefined;
}

/** Copy a persisted row's configuration onto a queue instance. */
function applyRecord(q: WorkflowQueue, record: QueueRecord): void {
  q.queueId = record.queueId;
  q.paused = record.paused;
  q.concurrency = record.concurrency ?? undefined;
  q.workerConcurrency = record.workerConcurrency ?? undefined;
  q.rateLimit = rateLimitFromRecord(record.rateLimitMax, record.rateLimitPeriodSec);
  q.partitionConcurrency = record.partitionConcurrency ?? undefined;
  q.partitionWorkerConcurrency = record.partitionWorkerConcurrency ?? undefined;
  q.partitionRateLimit = rateLimitFromRecord(record.partitionRateLimitMax, record.partitionRateLimitPeriodSec);
  q.minPollingIntervalMs = record.pollingIntervalSec * 1000;
  q.applicationName = record.applicationName;
}

/**
 * Re-read the queue's row from the database and update the cached fields on
 * `q` in place. No-op for internal queues. Throws if the row has been
 * deleted.
 */
async function refreshFromDb(q: WorkflowQueue): Promise<void> {
  if (!q.databaseBacked) return;
  const record = await sysDBFor(q).getQueue(q.name);
  if (record === null) {
    throw new Error(`Queue '${q.name}' was not found in the database.`);
  }
  applyRecord(q, record);
}

/**
 * Settings structure for a named workflow queue.
 * Workflow queues limit the rate and concurrency at which DBOS executes workflows.
 * Queue policies apply to workflows started by `DBOS.startWorkflow`,
 *   `DBOS.withWorkflowQueue`, etc.
 */
export class WorkflowQueue {
  readonly name: string;
  queueId!: string;
  paused!: boolean;

  pause(): Promise<QueueControlState> { return sysDBFor(this).setQueuePaused(this.name, true); }
  wake(): Promise<QueueControlState> { return sysDBFor(this).setQueuePaused(this.name, false); }
  getControlState(): Promise<QueueControlState> { return sysDBFor(this).getQueueControlState(this.name); }
  /**
   * Last-known cached values. May be stale for database-backed queues if
   * another process has modified the row. Use getters instead.
   * `concurrency` is the queue-wide limit, across every worker and partition.
   */
  concurrency?: number;
  rateLimit?: QueueRateLimit;
  workerConcurrency?: number;
  partitionConcurrency?: number;
  partitionWorkerConcurrency?: number;
  partitionRateLimit?: QueueRateLimit;
  minPollingIntervalMs?: number;
  /** Owner from the queues table; undefined for internal and unclaimed queues. */
  applicationName?: string;

  /**
   * When true, this queue's configuration is persisted in the `queues` system
   * table and may be mutated at runtime via the `setX` methods. False only for
   * the process-local queues DBOS registers for its own use.
   */
  readonly databaseBacked: boolean;

  /**
   * True when configuration reads/writes target a `DBOSClient`-supplied
   * SystemDatabase rather than the global executor's. The actual handle is
   * kept off this class's public type — see the module-level WeakMap below —
   * so that `WorkflowQueue` does not transitively depend on `SystemDatabase`.
   */
  readonly clientBound: boolean;

  /**
   * Not reachable from the published package, which exports `WorkflowQueue` as a
   * type only: applications obtain queues from `DBOS.registerQueue`,
   * `DBOS.retrieveQueue`, or `DBOS.listQueues`.
   *
   * @param record - The queue's configuration, persisted or not.
   * @param databaseBacked - False only for the process-local queues DBOS registers for its own use.
   * @param clientSystemDatabase - When set, config reads and writes target this client's database.
   */
  constructor(record: QueueRecord, databaseBacked: boolean = true, clientSystemDatabase?: SystemDatabase) {
    this.name = record.name;
    this.databaseBacked = databaseBacked;
    this.clientBound = clientSystemDatabase !== undefined;
    applyRecord(this, record);
    if (clientSystemDatabase !== undefined) {
      clientSystemDatabases.set(this, clientSystemDatabase);
    }
  }

  /**
   * Throws if a user-supplied queue registration is invalid. Internal queues bypass the name
   * check: the reserved prefix is theirs, and they validate their parameters directly.
   */
  static validateQueueRegistration(name: string, params: QueueParameters): void {
    if (name.startsWith(RESERVED_QUEUE_NAME_PREFIX)) {
      throw new Error(
        `Queue name ${name} is reserved: names starting with '${RESERVED_QUEUE_NAME_PREFIX}' belong to DBOS's internal queues.`,
      );
    }
    WorkflowQueue.validateQueueParams(params);
  }

  /** Throws if any combination of queue parameters is invalid. */
  static validateQueueParams(params: QueueParameters): void {
    for (const [option, replacement] of Object.entries(REMOVED_QUEUE_PARAMS)) {
      if (Object.hasOwn(params, option)) {
        throw new Error(`${option} was removed: ${replacement}.`);
      }
    }
    const {
      concurrency,
      globalConcurrency,
      workerConcurrency,
      rateLimit,
      partitionConcurrency,
      partitionWorkerConcurrency,
      partitionRateLimit,
      minPollingIntervalMs,
    } = params;
    if (concurrency !== undefined && globalConcurrency !== undefined) {
      throw new Error('concurrency is deprecated in favor of globalConcurrency; set only one of them');
    }
    if (partitionConcurrency !== undefined && partitionConcurrency < 1) {
      throw new Error('partitionConcurrency must be at least 1');
    }
    if (partitionWorkerConcurrency !== undefined && partitionWorkerConcurrency < 1) {
      throw new Error('partitionWorkerConcurrency must be at least 1');
    }
    if (
      partitionRateLimit !== undefined &&
      (partitionRateLimit.limitPerPeriod === undefined || partitionRateLimit.periodSec === undefined)
    ) {
      throw new Error('partitionRateLimit must specify both limitPerPeriod and periodSec');
    }
    if (
      partitionWorkerConcurrency !== undefined &&
      partitionConcurrency !== undefined &&
      partitionWorkerConcurrency > partitionConcurrency
    ) {
      throw new Error('partitionConcurrency must be greater than or equal to partitionWorkerConcurrency');
    }
    if (
      partitionWorkerConcurrency !== undefined &&
      workerConcurrency !== undefined &&
      partitionWorkerConcurrency > workerConcurrency
    ) {
      throw new Error('workerConcurrency must be greater than or equal to partitionWorkerConcurrency');
    }
    const queueConcurrency = globalConcurrency ?? concurrency;
    if (workerConcurrency !== undefined && queueConcurrency !== undefined && workerConcurrency > queueConcurrency) {
      throw new Error('concurrency must be greater than or equal to workerConcurrency');
    }
    if (
      partitionConcurrency !== undefined &&
      queueConcurrency !== undefined &&
      partitionConcurrency > queueConcurrency
    ) {
      throw new Error('globalConcurrency must be greater than or equal to partitionConcurrency');
    }
    if (
      partitionWorkerConcurrency !== undefined &&
      queueConcurrency !== undefined &&
      partitionWorkerConcurrency > queueConcurrency
    ) {
      throw new Error('concurrency must be greater than or equal to partitionWorkerConcurrency');
    }
    if (minPollingIntervalMs !== undefined && minPollingIntervalMs <= 0) {
      throw new Error('minPollingIntervalMs must be positive');
    }
    if (rateLimit !== undefined && (rateLimit.limitPerPeriod === undefined || rateLimit.periodSec === undefined)) {
      throw new Error('rateLimit must specify both limitPerPeriod and periodSec');
    }
  }

  /** Build a persistable record from user-supplied registration parameters. */
  static recordFromParams(name: string, params: QueueParameters): QueueRecord {
    return {
      name,
      queueId: '',
      paused: false,
      concurrency: params.globalConcurrency ?? params.concurrency ?? null,
      workerConcurrency: params.workerConcurrency ?? null,
      rateLimitMax: params.rateLimit ? params.rateLimit.limitPerPeriod : null,
      rateLimitPeriodSec: params.rateLimit ? params.rateLimit.periodSec : null,
      partitionQueue: hasPartitionLimits(params),
      partitionConcurrency: params.partitionConcurrency ?? null,
      partitionWorkerConcurrency: params.partitionWorkerConcurrency ?? null,
      partitionRateLimitMax: params.partitionRateLimit ? params.partitionRateLimit.limitPerPeriod : null,
      partitionRateLimitPeriodSec: params.partitionRateLimit ? params.partitionRateLimit.periodSec : null,
      pollingIntervalSec: (params.minPollingIntervalMs ?? 1000) / 1000,
    };
  }

  /** @deprecated Use `setGlobalConcurrency`. */
  async setConcurrency(value: number | undefined): Promise<void> {
    return this.setGlobalConcurrency(value);
  }

  async setGlobalConcurrency(value: number | undefined): Promise<void> {
    requireDatabaseBacked(this);
    // Refresh so the cross-field checks see the limits currently stored in the database.
    await refreshFromDb(this);
    checkConcurrencyBounds(this, value);
    await sysDBFor(this).updateQueue(this.name, { concurrency: value ?? null });
    this.concurrency = value;
  }

  async setWorkerConcurrency(value: number | undefined): Promise<void> {
    requireDatabaseBacked(this);
    await refreshFromDb(this);
    if (value !== undefined) {
      if (this.concurrency !== undefined && value > this.concurrency) {
        throw new Error('workerConcurrency must be less than or equal to concurrency');
      }
      if (this.partitionWorkerConcurrency !== undefined && this.partitionWorkerConcurrency > value) {
        throw new Error('partitionWorkerConcurrency must be less than or equal to workerConcurrency');
      }
    }
    await sysDBFor(this).updateQueue(this.name, { workerConcurrency: value ?? null });
    this.workerConcurrency = value;
  }

  async setRateLimit(value: QueueRateLimit | undefined): Promise<void> {
    requireDatabaseBacked(this);
    if (value !== undefined && (value.limitPerPeriod === undefined || value.periodSec === undefined)) {
      throw new Error('rateLimit must specify both limitPerPeriod and periodSec');
    }
    await sysDBFor(this).updateQueue(this.name, {
      rateLimitMax: value ? value.limitPerPeriod : null,
      rateLimitPeriodSec: value ? value.periodSec : null,
    });
    this.rateLimit = value;
  }

  async setPartitionConcurrency(value: number | undefined): Promise<void> {
    requireDatabaseBacked(this);
    if (value !== undefined && value < 1) {
      throw new Error('partitionConcurrency must be at least 1');
    }
    await refreshFromDb(this);
    if (value !== undefined) {
      if (this.concurrency !== undefined && value > this.concurrency) {
        throw new Error('partitionConcurrency must be less than or equal to globalConcurrency');
      }
      if (this.partitionWorkerConcurrency !== undefined && this.partitionWorkerConcurrency > value) {
        throw new Error('partitionConcurrency must be greater than or equal to partitionWorkerConcurrency');
      }
    }
    // Partitioning is inferred from the limits, so the stored flag follows them.
    const partitioned = partitionedAfter(this, { partitionConcurrency: value });
    await sysDBFor(this).updateQueue(this.name, {
      partitionConcurrency: value ?? null,
      partitionQueue: partitioned,
    });
    this.partitionConcurrency = value;
  }

  async setPartitionWorkerConcurrency(value: number | undefined): Promise<void> {
    requireDatabaseBacked(this);
    if (value !== undefined && value < 1) {
      throw new Error('partitionWorkerConcurrency must be at least 1');
    }
    await refreshFromDb(this);
    if (value !== undefined) {
      if (this.partitionConcurrency !== undefined && value > this.partitionConcurrency) {
        throw new Error('partitionWorkerConcurrency must be less than or equal to partitionConcurrency');
      }
      if (this.workerConcurrency !== undefined && value > this.workerConcurrency) {
        throw new Error('partitionWorkerConcurrency must be less than or equal to workerConcurrency');
      }
      if (this.concurrency !== undefined && value > this.concurrency) {
        throw new Error('partitionWorkerConcurrency must be less than or equal to concurrency');
      }
    }
    const partitioned = partitionedAfter(this, { partitionWorkerConcurrency: value });
    await sysDBFor(this).updateQueue(this.name, {
      partitionWorkerConcurrency: value ?? null,
      partitionQueue: partitioned,
    });
    this.partitionWorkerConcurrency = value;
  }

  async setPartitionRateLimit(value: QueueRateLimit | undefined): Promise<void> {
    requireDatabaseBacked(this);
    if (value !== undefined && (value.limitPerPeriod === undefined || value.periodSec === undefined)) {
      throw new Error('partitionRateLimit must specify both limitPerPeriod and periodSec');
    }
    await refreshFromDb(this);
    const partitioned = partitionedAfter(this, { partitionRateLimit: value });
    await sysDBFor(this).updateQueue(this.name, {
      partitionRateLimitMax: value ? value.limitPerPeriod : null,
      partitionRateLimitPeriodSec: value ? value.periodSec : null,
      partitionQueue: partitioned,
    });
    this.partitionRateLimit = value;
  }

  async setMinPollingIntervalMs(value: number): Promise<void> {
    requireDatabaseBacked(this);
    if (value <= 0) {
      throw new Error('minPollingIntervalMs must be positive');
    }
    await sysDBFor(this).updateQueue(this.name, { pollingIntervalSec: value / 1000 });
    this.minPollingIntervalMs = value;
  }

  /** @deprecated Use `getGlobalConcurrency`. */
  async getConcurrency(): Promise<number | undefined> {
    return this.getGlobalConcurrency();
  }

  async getGlobalConcurrency(): Promise<number | undefined> {
    await refreshFromDb(this);
    return this.concurrency;
  }

  async getWorkerConcurrency(): Promise<number | undefined> {
    await refreshFromDb(this);
    return this.workerConcurrency;
  }

  async getRateLimit(): Promise<QueueRateLimit | undefined> {
    await refreshFromDb(this);
    return this.rateLimit;
  }

  async getPartitionConcurrency(): Promise<number | undefined> {
    await refreshFromDb(this);
    return this.partitionConcurrency;
  }

  async getPartitionWorkerConcurrency(): Promise<number | undefined> {
    await refreshFromDb(this);
    return this.partitionWorkerConcurrency;
  }

  async getPartitionRateLimit(): Promise<QueueRateLimit | undefined> {
    await refreshFromDb(this);
    return this.partitionRateLimit;
  }

  async getMinPollingIntervalMs(): Promise<number | undefined> {
    await refreshFromDb(this);
    return this.minPollingIntervalMs;
  }
}

/**
 * Register a queue that DBOS itself needs — the internal queue, a Kafka receiver's
 * queue — or return the one already registered under `name`.
 *
 * Internal queues are not persisted in the `queues` table: their configuration is
 * fixed here and lives only in this process's memory. They are always dispatched,
 * bypassing any `listenQueues` filter, since this process is the only one that
 * enqueues onto them.
 *
 * Callers must resolve their queues through this rather than caching them: a registry
 * clear (`DBOS.shutdown({ deregister: true })`) drops the registration, and a cached
 * queue would silently stop being dispatched, leaving its workflows ENQUEUED forever.
 *
 * Not a user-facing API: applications register queues with `DBOS.registerQueue`.
 * @internal
 */
export function registerInternalQueue(name: string, params: QueueParameters = {}): WorkflowQueue {
  const existing = wfQueueRunner.getInternalQueue(name);
  if (existing) return existing;

  WorkflowQueue.validateQueueParams(params);
  const queue = new WorkflowQueue(WorkflowQueue.recordFromParams(name, params), false);
  wfQueueRunner.addInternalQueue(queue);
  return queue;
}

/** State is replaceable; reservations outlive every metadata generation. */
interface QueueRuntimeState {
  queue: WorkflowQueue;
  generation: number;
  phase: 'idle' | 'probing' | 'ready' | 'claiming';
  currentPollingMs: number;
  nextPollAt: number;
  forcePollPending: boolean;
  waitingForWorkerBudget: boolean;
  nextBudgetCheckAt: number;
  token?: symbol;
}

class WFQueueRunner {
  private readonly internalQueues = new Map<string, WorkflowQueue>();
  readonly pollerQueueNames = new Set<string>();
  private isRunning = false;
  private abortController?: AbortController;
  private listenQueueNames: Set<string> | null = null;
  private readonly states = new Map<string, QueueRuntimeState>();
  private generation = 0;
  private wakePending = false;
  private wakeScheduler?: () => void;
  private static readonly defaultMinPollingIntervalMs = 1000;
  private static readonly defaultMaxPollingIntervalMs = 120000;
  private readonly backoffFactor = 2;
  private readonly scalebackFactor = 0.9;
  private readonly jitterMin = 0.95;
  private readonly jitterMax = 1.05;

  addInternalQueue(queue: WorkflowQueue): void { this.internalQueues.set(queue.name, queue); }
  getInternalQueue(name: string): WorkflowQueue | undefined { return this.internalQueues.get(name); }
  private wake(): void {
    if (!this.isRunning) return;
    if (this.wakeScheduler) this.wakeScheduler();
    else this.wakePending = true;
  }
  stop(): void {
    this.isRunning = false;
    this.abortController?.abort();
  }
  clearRegistrations(): void {
    this.internalQueues.clear();
    this.pollerQueueNames.clear();
  }

  private ensureState(queue: WorkflowQueue, now: number): void {
    const existing = this.states.get(queue.name);
    if (existing && existing.queue.queueId === queue.queueId) {
      if (JSON.stringify(existing.queue) === JSON.stringify(queue)) return;
      const waking = existing.queue.paused && !queue.paused;
      existing.queue = queue;
      existing.generation = ++this.generation;
      existing.phase = 'idle';
      existing.waitingForWorkerBudget = false;
      existing.nextBudgetCheckAt = now;
      if (queue.paused) existing.forcePollPending = false;
      if (waking) {
        existing.currentPollingMs = queue.minPollingIntervalMs ?? WFQueueRunner.defaultMinPollingIntervalMs;
        existing.nextPollAt = now;
        existing.forcePollPending = true;
      }
      return;
    }
    const interval = queue.minPollingIntervalMs ?? WFQueueRunner.defaultMinPollingIntervalMs;
    this.states.set(queue.name, {
      queue, generation: ++this.generation, phase: 'idle', currentPollingMs: interval,
      nextPollAt: now + interval, forcePollPending: false,
      waitingForWorkerBudget: false, nextBudgetCheckAt: now,
    });
  }

  private applyMetadata(records: QueueRecord[], names?: string[]): void {
    const present = new Set<string>();
    for (const record of records) {
      if (this.internalQueues.has(record.name)) continue;
      if (this.listenQueueNames && !this.listenQueueNames.has(record.name) && !this.pollerQueueNames.has(record.name)) continue;
      present.add(record.name);
      this.ensureState(new WorkflowQueue(record), Date.now());
    }
    const selected = names ? new Set(names) : undefined;
    for (const [name, state] of this.states) {
      if (state.queue.databaseBacked && (!selected || selected.has(name)) && !present.has(name)) this.states.delete(name);
    }
  }

  async dispatchLoop(exec: DBOSExecutor, listenQueuesArg: string[] | null,
    maxConcurrentQueueDispatches = 3, batchSize = 1000, coalesceMs = 50): Promise<void> {
    this.isRunning = true;
    this.states.clear();
    this.wakePending = false;
    this.abortController = new AbortController();
    this.listenQueueNames = listenQueuesArg ? new Set(listenQueuesArg) : null;
    const signal = this.abortController.signal;
    const db = exec.systemDatabase;
    const reservations = new Map<string, symbol>();
    const claims = new Set<Promise<void>>();
    let probe: Promise<void> | undefined;
    let metadata: Promise<void> | undefined;
    let transition: Promise<void> | undefined;
    let dirty = new Set<string>();
    let fullPending = true;
    let forceAllPending = false;
    let reconcileAt = 0;
    let metadataRetryAt = 0;
    let metadataBackoff = 0;
    let probeRetryAt = 0;
    let probeBackoff = 0;
    let transitionAt = 0;
    const slot = (deadline: number) => Math.ceil(deadline / coalesceMs) * coalesceMs;
    const jitter = (ms: number) => ms * (0.95 + Math.random() * 0.1);
    const hint = (name?: string) => {
      if (!this.isRunning) return;
      if (!name || dirty.size >= batchSize) { fullPending = true; forceAllPending = true; dirty.clear(); }
      else if (!forceAllPending) dirty.add(name);
      this.wake();
    };
    const budgetHint = (name: string) => {
      if (!this.isRunning) return;
      const state = this.states.get(name);
      if (state && !state.queue.paused) state.nextBudgetCheckAt = 0;
      this.wake();
    };
    db.queueControlListeners.add(hint);
    db.queueBudgetListeners.add(budgetHint);
    for (const q of this.internalQueues.values()) this.ensureState(q, Date.now());

    const current = (state: QueueRuntimeState, generation: number) =>
      this.states.get(state.queue.name) === state && state.generation === generation;
    const release = (name: string, token: symbol) => {
      if (reservations.get(name) === token) reservations.delete(name);
      const state = this.states.get(name);
      if (state?.token === token) {
        state.token = undefined;
        if (state.phase === 'probing' || state.phase === 'claiming') state.phase = 'idle';
      }
    };
    const budgetAvailable = (state: QueueRuntimeState, now: number) => {
      if (workerBudget(state.queue, db.countRunningWorkflowsForQueue(state.queue.name)) > 0) {
        state.waitingForWorkerBudget = false;
        return true;
      }
      state.waitingForWorkerBudget = true;
      state.nextBudgetCheckAt = now + 1000;
      return false;
    };
    const wait = async (ms: number) => {
      if (signal.aborted) return;
      if (this.wakePending) { this.wakePending = false; return; }
      await new Promise<void>(resolve => {
        const finish = () => {
          clearTimeout(timer);
          signal.removeEventListener('abort', finish);
          if (this.wakeScheduler === finish) this.wakeScheduler = undefined;
          resolve();
        };
        const timer = setTimeout(finish, ms);
        this.wakeScheduler = finish;
        signal.addEventListener('abort', finish, { once: true });
      });
    };

    try {
      while (this.isRunning) {
        const now = Date.now();
        if (!metadata && now >= metadataRetryAt && (fullPending || now >= reconcileAt || dirty.size > 0)) {
          const full = fullPending || now >= reconcileAt;
          const names = full ? undefined : [...dirty];
          const forceNames = dirty;
          const forceAll = forceAllPending;
          forceAllPending = false;
          // New hints belong to the next snapshot, including hints for the same name.
          dirty = new Set();
          fullPending = false;
          metadata = (async () => {
            try {
              const records = await db.listQueues(db.appName, names);
              if (!this.isRunning || signal.aborted) return;
              this.applyMetadata(records, names);
              for (const record of records) {
                const state = this.states.get(record.name);
                if (!state || state.queue.paused || (!forceAll && !forceNames.has(record.name))) continue;
                state.generation = ++this.generation;
                state.phase = 'idle';
                state.currentPollingMs = state.queue.minPollingIntervalMs ?? WFQueueRunner.defaultMinPollingIntervalMs;
                state.nextPollAt = Date.now();
                state.forcePollPending = true;
                state.nextBudgetCheckAt = 0;
              }
              metadataBackoff = 0;
              metadataRetryAt = 0;
              if (full) reconcileAt = Date.now() + 1000;
            } catch (error) {
              if (full) fullPending = true;
              if (forceAll) hint();
              else for (const name of forceNames) hint(name);
              metadataBackoff = Math.min(1000, metadataBackoff ? metadataBackoff * 2 : 100);
              metadataRetryAt = Date.now() + jitter(metadataBackoff);
              exec.logger.warn(`Error refreshing queue metadata: ${String(error)}`);
            } finally { metadata = undefined; this.wake(); }
          })();
        }
        if (!transition && now >= transitionAt) {
          transition = (async () => {
            try { await db.transitionDelayedWorkflows(); }
            catch (error) { exec.logger.warn(`Error transitioning delayed workflows: ${String(error)}`); }
            finally { transitionAt = Date.now() + 1000; transition = undefined; this.wake(); }
          })();
        }
        for (const state of this.states.values()) {
          if (!state.queue.paused && state.waitingForWorkerBudget && now >= state.nextBudgetCheckAt) budgetAvailable(state, now);
        }
        const available = () => [...this.states.values()]
          .filter(s => !s.queue.paused && !s.waitingForWorkerBudget && !reservations.has(s.queue.name))
          .sort((a, b) => a.nextPollAt - b.nextPollAt);

        for (const state of available()) {
          if (claims.size >= maxConcurrentQueueDispatches) break;
          if (state.phase !== 'ready' || !budgetAvailable(state, now)) continue;
          const name = state.queue.name;
          const queue = state.queue;
          const generation = state.generation;
          const token = Symbol(name);
          reservations.set(name, token);
          state.token = token;
          state.phase = 'claiming';
          let task!: Promise<void>;
          task = (async () => {
            try {
              // Dispatch committed claims even if metadata changed or stop was requested.
              const contention = await this.pollQueue(exec, queue);
              if (current(state, generation)) this.adjustInterval(exec, state, contention);
            } catch (error) {
              exec.logger.warn(`Error dispatching queue ${name}: ${String(error)}`);
              if (current(state, generation)) this.adjustInterval(exec, state, true);
            } finally {
              release(name, token);
              claims.delete(task);
              this.wake();
            }
          })();
          claims.add(task);
        }
        if (!probe && now >= probeRetryAt) {
          const batch = available().filter(s => s.phase === 'idle' && slot(s.nextPollAt) <= now)
            .filter(s => budgetAvailable(s, now)).slice(0, batchSize);
          if (batch.length) {
            const snapshots = batch.map(state => {
              const token = Symbol(state.queue.name);
              reservations.set(state.queue.name, token);
              state.token = token;
              state.phase = 'probing';
              state.forcePollPending = false;
              return { state, generation: state.generation, token, request: {
                name: state.queue.name, queueId: state.queue.queueId, generation: state.generation,
                internal: !state.queue.databaseBacked,
              }};
            });
            probe = (async () => {
              try {
                const ready = new Set((await db.findQueuesWithEnqueuedWorkflows(snapshots.map(s => s.request), globalParams.appVersion)).map(r => r.name));
                probeBackoff = 0;
                probeRetryAt = 0;
                for (const { state, generation } of snapshots) {
                  if (!this.isRunning || !current(state, generation)) continue;
                  if (ready.has(state.queue.name)) state.phase = 'ready';
                  else this.adjustInterval(exec, state, false);
                }
              } catch (error) {
                probeBackoff = Math.min(1000, probeBackoff ? probeBackoff * 2 : 100);
                probeRetryAt = Date.now() + jitter(probeBackoff);
                exec.logger.warn(`Error probing queue batch: ${String(error)}`);
              } finally {
                for (const { state, token } of snapshots) release(state.queue.name, token);
                probe = undefined;
                this.wake();
              }
            })();
          }
        }
        if (!this.isRunning) break;
        let next = Infinity;
        if (!metadata) next = Math.min(next, Math.max(metadataRetryAt, fullPending || dirty.size ? now : reconcileAt));
        if (!transition) next = Math.min(next, transitionAt);
        for (const state of this.states.values()) {
          if (state.queue.paused || reservations.has(state.queue.name)) continue;
          if (state.waitingForWorkerBudget) next = Math.min(next, state.nextBudgetCheckAt);
          else if (state.phase === 'ready' && claims.size < maxConcurrentQueueDispatches) next = Math.min(next, now);
          else if (state.phase === 'idle' && !probe) next = Math.min(next, Math.max(probeRetryAt, slot(state.nextPollAt)));
        }
        await wait(Number.isFinite(next) ? Math.max(0, next - Date.now()) : 1000);
      }
    } finally {
      this.isRunning = false;
      db.queueControlListeners.delete(hint);
      db.queueBudgetListeners.delete(budgetHint);
      await Promise.allSettled([...claims, ...[probe, metadata, transition].filter((p): p is Promise<void> => p !== undefined)]);
      this.wakeScheduler = undefined;
      this.wakePending = false;
    }
  }

  /** Poll one queue once, starting ready workflows; returns true if DB contention was detected. */
  private async pollQueue(exec: DBOSExecutor, queue: WorkflowQueue): Promise<boolean> {
    let contentionDetected = false;
    // Helper function that starts dequeued workflows
    const dispatch = async (wfids: string[]) => {
      if (wfids.length > 0) {
        await debugTriggerPoint(DEBUG_TRIGGER_WORKFLOW_QUEUE_START);
      }
      await exec.dispatchDequeuedWorkflows(wfids);
    };
    const sysdb = exec.systemDatabase;
    // Dequeue workflows for this queue, either in one batched sweep across partitions or one partition at a time.
    try {
      if (!isPartitionedQueue(queue)) {
        const wfids = await sysdb.findAndMarkStartableWorkflows(
          queue,
          exec.executorID,
          globalParams.appVersion,
          undefined,
          sysdb.countRunningWorkflowsForQueue(queue.name),
        );
        await dispatch(wfids);
      } else if (
        queue.partitionConcurrency === 1 &&
        queue.concurrency === undefined &&
        queue.rateLimit === undefined &&
        queue.partitionRateLimit === undefined
      ) {
        // Batched path: one transaction claims every partition's head (see findAndMarkStartablePartitionedWorkflows).
        const maxTasks = workerBudget(queue, sysdb.countRunningWorkflowsForQueue(queue.name));
        if (maxTasks > 0) {
          const wfids = await sysdb.findAndMarkStartablePartitionedWorkflows(
            queue,
            exec.executorID,
            globalParams.appVersion,
            maxTasks,
          );
          await dispatch(wfids);
        }
      } else {
        // Every other partitioned config sweeps one partition at a time, in random order to prevent starvation.
        const partitionKeys = shuffled(await sysdb.getQueuePartitions(queue.name));
        // Snapshot once: dispatch is asynchronous, so re-reading would count this sweep's own claims twice.
        const running = sysdb.countRunningWorkflowsForQueue(queue.name);
        let claimed = 0;
        for (const partitionKey of partitionKeys) {
          if (!this.isRunning || workerBudget(queue, running + claimed) <= 0) break;
          let partitionWfids: string[];
          try {
            partitionWfids = await sysdb.findAndMarkStartableWorkflows(
              queue,
              exec.executorID,
              globalParams.appVersion,
              partitionKey,
              running + claimed,
              sysdb.countRunningWorkflowsForPartition(queue.name, partitionKey),
            );
          } catch (e) {
            // Lock held or claim raced by another worker: skip just this partition, no queue-wide backoff.
            if (isContentionError(e)) continue;
            throw e;
          }
          claimed += partitionWfids.length;
          await dispatch(partitionWfids);
          await debugTriggerPoint(DEBUG_TRIGGER_BETWEEN_PARTITION_DISPATCHES);
        }
      }
    } catch (e) {
      if (e instanceof QueueUnavailableError) return false;
      const err = e as Error;
      // Handle serialization errors and lock contention with backoff
      if (isContentionError(err)) {
        contentionDetected = true;
        exec.logger.warn(`Contention detected in queue ${queue.name}.`);
      } else {
        exec.logger.warn(`Error getting startable workflows for queue ${queue.name}: ${err.message}`);
      }
    }
    return contentionDetected;
  }

  /** After a poll, grow the interval on contention or shrink it toward the minimum, then schedule the next poll with jitter. */
  private adjustInterval(exec: DBOSExecutor, state: QueueRuntimeState, contentionDetected: boolean): void {
    const minPollingMs = state.queue.minPollingIntervalMs ?? WFQueueRunner.defaultMinPollingIntervalMs;
    const maxPollingMs = WFQueueRunner.defaultMaxPollingIntervalMs;
    if (contentionDetected) {
      state.currentPollingMs = Math.min(maxPollingMs, state.currentPollingMs * this.backoffFactor);
      exec.logger.warn(
        `Increasing polling interval for queue ${state.queue.name} to ${(state.currentPollingMs / 1000).toFixed(2)}s due to contention.`,
      );
    } else {
      state.currentPollingMs = Math.max(minPollingMs, state.currentPollingMs * this.scalebackFactor);
    }
    // Clamp into the current [min, max] range in case config changed under us.
    state.currentPollingMs = Math.max(minPollingMs, Math.min(state.currentPollingMs, maxPollingMs));
    const jitter = this.jitterMin + Math.random() * (this.jitterMax - this.jitterMin);
    state.nextPollAt = Date.now() + state.currentPollingMs * jitter;
  }
}

export const wfQueueRunner = new WFQueueRunner();
