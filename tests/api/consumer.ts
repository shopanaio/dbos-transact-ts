import { DBOS, DBOSClient, WorkflowQueue } from '../../src';
import type { SystemDatabaseFactory, SystemTransaction, DBOSConfig } from '../../src';

export async function consumer(factory: SystemDatabaseFactory) {
  DBOS.setConfig({ name: 'port-consumer', systemDatabase: factory });
  const client = await DBOSClient.create({ systemDatabase: factory, applicationName: 'port-consumer' });
  await client.destroy();
}

export function opaqueTransaction(tx: SystemTransaction) {
  // @ts-expect-error Системная транзакция не предоставляет SQL-клиент.
  // eslint-disable-next-line @typescript-eslint/no-unsafe-call
  tx.query('SELECT 1');
}

export async function postgresConsumer(url: string) {
  const config: DBOSConfig = { name: 'postgres-consumer', systemDatabaseUrl: url };
  DBOS.setConfig(config);
  const client = await DBOSClient.create({ systemDatabaseUrl: url });
  const queue: WorkflowQueue = await DBOS.registerQueue('consumer');
  void queue;
  await client.send('workflow', { value: 1 });
  await client.destroy();
}
