import { loadTestSystemDatabase } from './system-database-factory';

beforeEach(async () => {
  const backend = await loadTestSystemDatabase();
  if (backend) {
    if (!backend.reset) throw new Error('Test backend must export reset() for isolated datasource tests');
    await backend.reset();
  }
});
