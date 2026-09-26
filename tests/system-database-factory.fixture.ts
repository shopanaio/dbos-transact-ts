import { PostgresSystemDatabase } from '../src/system_database';
import type { SystemDatabaseFactory } from '../src/system_database';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';

/** Фикстура для проверки загрузки внешней фабрики тем же conformance-набором. */
export const systemDatabase: SystemDatabaseFactory = (context) =>
  new PostgresSystemDatabase(
    generateDBOSTestConfig().systemDatabaseUrl!,
    context.logger,
    context.serializer,
    undefined,
    undefined,
    undefined,
    context.role === 'executor',
    context.pollingConcurrency,
    undefined,
    context.appName,
    context.observabilityQueryTimeoutMs,
  );

export async function reset() {
  const value = process.env.DBOS_TEST_SYSTEM_DATABASE_FACTORY;
  delete process.env.DBOS_TEST_SYSTEM_DATABASE_FACTORY;
  try {
    await setUpDBOSTestSysDb(generateDBOSTestConfig());
  } finally {
    if (value === undefined) delete process.env.DBOS_TEST_SYSTEM_DATABASE_FACTORY;
    else process.env.DBOS_TEST_SYSTEM_DATABASE_FACTORY = value;
  }
}
