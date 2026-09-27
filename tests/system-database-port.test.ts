import { DBOSClient, SystemDatabaseLocalState } from '../src';
import type { SystemDatabase, SystemDatabaseContext, SystemDatabaseFactory } from '../src';
import { DBOSExecutor } from '../src/dbos-executor';
import { translateDbosConfig } from '../src/config';
import { DBOSInitializationError, DBOSError, DBOSWorkflowCancelledError } from '../src/error';
import { runWithTopContext } from '../src/context';
import { GlobalLogger } from '../src/telemetry/logs';
import { DBOSJSON } from '../src/serialization';
import { PostgresSystemDatabase } from '../src/system_database';
import { generateDBOSTestConfig } from './helpers';

function fakeDatabase(ctx: SystemDatabaseContext) {
  const local = new SystemDatabaseLocalState(ctx.logger);
  const methods = {
    init: jest.fn(async () => {}),
    destroy: jest.fn(async () => {}),
    checkIfCanceledLimited: jest.fn(async (_id: string) => {}),
    getOperationResultAndThrowIfCancelled: jest.fn((_id: string, _fid: number) => Promise.resolve(undefined)),
    serializer: ctx.serializer,
    appName: ctx.appName,
    logger: ctx.logger,
    resolveWaitPollingIntervalMs: (_kind: string, requested?: number) => Math.min(requested ?? Infinity, 30_000),
  };
  const database = new Proxy(Object.assign(local, methods), {
    get(target, key, receiver) {
      if (key in target) return Reflect.get(target, key, receiver) as unknown;
      throw new Error(`Unexpected port access: ${String(key)}`);
    },
  }) as unknown as SystemDatabase;
  return { database, methods };
}

const context: SystemDatabaseContext = {
  logger: new GlobalLogger(),
  serializer: DBOSJSON,
  appName: 'port-test',
  executorId: 'executor',
  appVersion: 'version',
  role: 'executor',
};

test.each([
  'systemDatabaseUrl',
  'systemDatabasePool',
  'systemDatabasePoolSize',
  'systemDatabaseSchemaName',
  'useListenNotify',
  'notificationCoalesceMs',
] as const)('rejects explicit PostgreSQL option %s with a factory', (key) => {
  const factory: SystemDatabaseFactory = () => fakeDatabase(context).database;
  expect(() => translateDbosConfig({ name: 'port-test', systemDatabase: factory, [key]: false })).toThrow(
    DBOSInitializationError,
  );
});

test('client initializes and destroys the injected backend, with no SQL access', async () => {
  const { database, methods } = fakeDatabase(context);
  const factory = jest.fn(() => database);
  const client = await DBOSClient.create({ systemDatabase: factory, applicationName: 'port-test' });
  expect(factory).toHaveBeenCalledWith(expect.objectContaining({ role: 'client', appName: 'port-test' }));
  expect(methods.init).toHaveBeenCalledWith(false);
  await expect(client.sendInTransaction({} as never, 'target', 'message')).rejects.toThrow(DBOSError);
  await client.destroy();
  expect(methods.destroy).toHaveBeenCalledTimes(1);
});

test('prefetch handles gaps and isolates child workflows while retaining cancellation checks', async () => {
  const { database, methods } = fakeDatabase(context);
  const executor = Object.create(DBOSExecutor.prototype) as DBOSExecutor;
  Object.defineProperty(executor, 'systemDatabase', { value: database });
  const checkpoint = { output: 'saved', functionName: 'step' };
  await runWithTopContext(
    {
      workflowId: 'wf',
      creationOpId: 'generation',
      prefetchedOperationResults: Promise.resolve({ results: new Map([[2, checkpoint]]), maxFunctionId: 2 }),
    },
    async () => {
      expect(await executor.getOperationResult('wf', 2)).toBe(checkpoint);
      expect(await executor.getOperationResult('wf', 1)).toBeUndefined();
      expect(methods.getOperationResultAndThrowIfCancelled).not.toHaveBeenCalled();
      await executor.getOperationResult('wf', 3);
      await executor.getOperationResult('child', 2);
      expect(methods.getOperationResultAndThrowIfCancelled).toHaveBeenCalledTimes(2);
      methods.checkIfCanceledLimited.mockRejectedValueOnce(new DBOSWorkflowCancelledError('wf'));
      await expect(executor.getOperationResult('wf', 2)).rejects.toThrow(DBOSWorkflowCancelledError);
    },
  );
});

test('shared local state signals result waiters and restores queue budget', () => {
  const local = new SystemDatabaseLocalState(context.logger);
  const result = jest.fn();
  const budget = jest.fn();
  const subscription = local.resultsMap.registerCallback('wf', result);
  local.queueBudgetListeners.add(budget);
  local.registerRunningWorkflow('wf', Promise.resolve(), () => {}, 'queue', 'partition');
  expect(local.countRunningWorkflowsForPartition('queue', 'partition')).toBe(1);
  local.resultsMap.callCallbacks('wf');
  expect(result).toHaveBeenCalledTimes(1);
  local.clearRunningWorkflow('wf');
  expect(budget).toHaveBeenCalledWith('queue');
  local.resultsMap.deregisterCallback(subscription);
  expect(local.resultsMap.map.size).toBe(0);
});

test('PostgreSQL keeps an explicit polling interval and falls back to its defaults', async () => {
  const sysdb = new PostgresSystemDatabase(generateDBOSTestConfig().systemDatabaseUrl!, context.logger, DBOSJSON);
  try {
    expect(sysdb.resolveWaitPollingIntervalMs('result', 5_000)).toBe(5_000);
    expect(sysdb.resolveWaitPollingIntervalMs('event', 30_000)).toBe(30_000);
    expect(sysdb.resolveWaitPollingIntervalMs('result')).toBe(sysdb.dbPollingIntervalResultMs);
    expect(sysdb.resolveWaitPollingIntervalMs('event')).toBe(sysdb.dbPollingIntervalEventMs);
    expect(sysdb.resolveWaitPollingIntervalMs('stream')).toBe(sysdb.dbPollingIntervalStreamMs);
  } finally {
    await sysdb.destroy();
  }
});
