import { DBOSInitializationError } from './error';
import type { SystemDatabaseFactory } from './system_database';

/** Проверяется до подстановки PostgreSQL-значений по умолчанию. */
export function validateSystemDatabaseFactory(options: {
  systemDatabase?: SystemDatabaseFactory;
  systemDatabaseUrl?: unknown;
  systemDatabasePool?: unknown;
  systemDatabasePoolSize?: unknown;
  systemDatabaseSchemaName?: unknown;
  useListenNotify?: unknown;
  notificationCoalesceMs?: unknown;
}): void {
  if (!options.systemDatabase) return;
  for (const key of [
    'systemDatabaseUrl',
    'systemDatabasePool',
    'systemDatabasePoolSize',
    'systemDatabaseSchemaName',
    'useListenNotify',
    'notificationCoalesceMs',
  ] as const) {
    if (options[key] !== undefined) throw new DBOSInitializationError(`systemDatabase is incompatible with ${key}`);
  }
}
