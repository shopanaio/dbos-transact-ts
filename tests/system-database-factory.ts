import type { DBOSConfig } from '../src/dbos-executor';
import type { SystemDatabaseFactory } from '../src/system_database';
import type { DBOSClient } from '../src/client';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

type TestBackend = {
  default?: SystemDatabaseFactory;
  systemDatabase?: SystemDatabaseFactory;
  reset?: () => Promise<void>;
};
let loaded: Promise<TestBackend> | undefined;

export function loadTestSystemDatabase(): Promise<TestBackend | undefined> {
  const specifier = process.env.DBOS_TEST_SYSTEM_DATABASE_FACTORY;
  if (!specifier) return Promise.resolve(undefined);
  loaded ??= (async () => {
    const name = specifier.startsWith('.') ? resolve(specifier) : specifier;
    try {
      // ts-jest handles TypeScript fixtures; require also supports CommonJS factories.
      // eslint-disable-next-line @typescript-eslint/no-require-imports -- CommonJS-фабрики и TS через ts-jest
      const module = require(name) as TestBackend | SystemDatabaseFactory;
      return typeof module === 'function' ? { default: module } : module;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ERR_REQUIRE_ESM') throw error;
      // ts-jest компилирует import() в require, поэтому ESM-фабрика загружается настоящим import().
      // eslint-disable-next-line @typescript-eslint/no-implied-eval
      const dynamicImport = new Function('name', 'return import(name)') as (name: string) => Promise<TestBackend>;
      return dynamicImport(name.startsWith('/') ? pathToFileURL(name).href : name);
    }
  })();
  return loaded;
}

export async function withTestSystemDatabase<T extends Partial<DBOSConfig>>(options: T): Promise<T> {
  const backend = await loadTestSystemDatabase();
  if (!backend) return options;
  const factory = backend.systemDatabase ?? backend.default;
  if (typeof factory !== 'function') throw new Error('Test backend must export default or systemDatabase factory');
  const copy = { ...options };
  for (const key of [
    'systemDatabaseUrl',
    'systemDatabasePool',
    'systemDatabasePoolSize',
    'systemDatabaseSchemaName',
    'useListenNotify',
    'notificationCoalesceMs',
  ] as const)
    delete copy[key];
  return { ...copy, systemDatabase: factory };
}

export async function withTestClientDatabase(options: Parameters<typeof DBOSClient.create>[0]) {
  return withTestSystemDatabase(options);
}
