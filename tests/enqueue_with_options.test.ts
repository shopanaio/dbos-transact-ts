import { openWorkflowState, type WorkflowStateAccess } from './system-database-state';
import { DBOS, StatusString } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';

const QUEUE = 'enqueue-with-options-queue';

// Reports the auth the workflow body itself observes, so a test can check that it agrees
// with what was recorded on the status row.
const authWorkflow = DBOS.registerWorkflow(
  () => Promise.resolve(`${DBOS.authenticatedUser}|${DBOS.authenticatedRoles.join(',')}`),
  { name: 'authWorkflow' },
);

describe('enqueue-workflow-with-options', () => {
  let config: DBOSConfig;
  let state: WorkflowStateAccess;

  beforeEach(async () => {
    config = generateDBOSTestConfig();
    config.name = 'enqueue-options-app';
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
    state = await openWorkflowState(config);
  });

  afterEach(async () => {
    await DBOS.shutdown();
    await state.close();
  });

  test('enqueues a workflow this process does not implement', async () => {
    await DBOS.launch();
    await DBOS.registerQueue(QUEUE);

    const handle = await DBOS.enqueueWorkflowWithOptions(
      { queueName: QUEUE, workflowName: 'aWorkflowElsewhere', workflowClassName: 'RemoteClass' },
      'arg-one',
      2,
    );

    const rows = [
      await state.readStatusColumns(handle.workflowID, [
        'status',
        'name',
        'class_name',
        'queue_name',
        'application_version',
        'application_name',
      ]),
    ].filter((row) => row !== undefined);
    expect(rows).toHaveLength(1);
    expect(rows[0]!.status).toBe(StatusString.ENQUEUED);
    expect(rows[0]!.name).toBe('aWorkflowElsewhere');
    expect(rows[0]!.class_name).toBe('RemoteClass');
    expect(rows[0]!.queue_name).toBe(QUEUE);
    // Left unset unless given: the target may belong to another executor.
    expect(rows[0]!.application_version).toBeNull();
    expect(rows[0]!.application_name).toBe('enqueue-options-app');
  });

  test('portable form carries named arguments', async () => {
    await DBOS.launch();
    await DBOS.registerQueue(QUEUE);

    const handle = await DBOS.enqueueWorkflowWithOptionsPortable(
      { queueName: QUEUE, workflowName: 'aPythonWorkflow' },
      ['positional'],
      { keyword: 42 },
    );

    const row = await state.readWorkflowRow(handle.workflowID);
    expect(row?.serialization).toBe('portable_json');
    expect(JSON.parse(row!.inputs)).toEqual({ positionalArgs: ['positional'], namedArgs: { keyword: 42 } });
  });

  test('honours explicit options over the ambient defaults', async () => {
    await DBOS.launch();
    await DBOS.registerQueue(QUEUE);

    const handle = await DBOS.enqueueWorkflowWithOptions({
      queueName: QUEUE,
      workflowName: 'aWorkflowElsewhere',
      workflowID: 'enqueue-options-preset-id',
      appVersion: 'v-explicit',
      applicationName: 'another-app',
      priority: 5,
      deduplicationID: 'dedup-key',
    });
    expect(handle.workflowID).toBe('enqueue-options-preset-id');

    const row = (await state.readStatusColumns('enqueue-options-preset-id', [
      'application_version',
      'application_name',
      'priority',
      'deduplication_id',
    ]))!;
    expect(row.application_version).toBe('v-explicit');
    expect(row.application_name).toBe('another-app');
    expect(row.priority).toBe(5);
    expect(row.deduplication_id).toBe('dedup-key');
  });

  test('takes the workflow ID from the ambient context', async () => {
    await DBOS.launch();
    await DBOS.registerQueue(QUEUE);

    const handle = await DBOS.withNextWorkflowID('enqueue-options-ambient-id', async () => {
      return await DBOS.enqueueWorkflowWithOptions({ queueName: QUEUE, workflowName: 'aWorkflowElsewhere' });
    });
    expect(handle.workflowID).toBe('enqueue-options-ambient-id');
  });

  test('records the enqueue as a child of the calling workflow, replay-safe', async () => {
    let parentRuns = 0;

    class ParentTest {
      @DBOS.workflow()
      static async parent(): Promise<string> {
        parentRuns += 1;
        const child = await DBOS.enqueueWorkflowWithOptions({
          queueName: QUEUE,
          workflowName: 'aWorkflowElsewhere',
        });
        return child.workflowID;
      }
    }

    await DBOS.launch();
    await DBOS.registerQueue(QUEUE);

    const handle = await DBOS.startWorkflow(ParentTest).parent();
    const childID = await handle.getResult();
    expect(parentRuns).toBe(1);

    // The ID derives from the caller and its function ID, so a replay collides instead of enqueueing a second workflow.
    const childStepID = Number(childID.slice(handle.workflowID.length + 1));
    expect(childID.startsWith(`${handle.workflowID}-`)).toBe(true);
    expect(Number.isInteger(childStepID)).toBe(true);

    // The child is linked to its parent, so it is reachable as a child workflow.
    const childStatus = await DBOS.getWorkflowStatus(childID);
    expect(childStatus?.parentWorkflowID).toBe(handle.workflowID);
    const children = await DBOS.listWorkflows({ parentWorkflowID: handle.workflowID });
    expect(children.map((w) => w.workflowID)).toContain(childID);

    // The enqueue is checkpointed as a child step of the parent.
    const steps = await DBOS.listWorkflowSteps(handle.workflowID);
    const childStep = steps?.find((s) => s.childWorkflowID === childID);
    expect(childStep).toBeDefined();
    expect(childStep!.name).toBe('aWorkflowElsewhere');

    // A fork replaying from step 0 returns the recorded child rather than enqueueing a second workflow.
    const forkHandle = await DBOS.forkWorkflow<string>(handle.workflowID, 1, { queueName: QUEUE });
    expect(await forkHandle.getResult()).toBe(childID);

    expect(await DBOS.listWorkflows({ workflowName: 'aWorkflowElsewhere' })).toHaveLength(1);
  });

  test('records authentication from explicit options or ambient context', async () => {
    await DBOS.launch();
    await DBOS.registerQueue(QUEUE);

    // Explicit params beat the ambient authenticated context.
    const explicit = await (
      await DBOS.withAuthedContext('ambient', ['ambient-role'], () =>
        DBOS.startWorkflow(authWorkflow, {
          workflowID: 'auth-explicit',
          authenticatedUser: 'alice',
          authenticatedRoles: ['admin', 'user'],
        })(),
      )
    ).getResult();

    // Explicit params also apply to a queued workflow.
    const enqueued = await (
      await DBOS.startWorkflow(authWorkflow, {
        workflowID: 'auth-enqueued',
        queueName: QUEUE,
        authenticatedUser: 'bob',
        authenticatedRoles: ['reader'],
      })()
    ).getResult();

    // With nothing explicit, the ambient context still applies.
    const ambient = await (
      await DBOS.withAuthedContext('carol', ['ops'], () =>
        DBOS.startWorkflow(authWorkflow, { workflowID: 'auth-ambient' })(),
      )
    ).getResult();

    // The running workflow must see the same auth its status row records, on every path.
    expect(explicit).toBe('alice|admin,user');
    expect(enqueued).toBe('bob|reader');
    expect(ambient).toBe('carol|ops');

    const rows: Record<string, unknown>[] = [];
    for (const id of ['auth-ambient', 'auth-enqueued', 'auth-explicit'])
      rows.push({
        workflow_uuid: id,
        ...(await state.readStatusColumns(id, ['authenticated_user', 'authenticated_roles'])),
      });
    expect(rows.map((r) => [r.workflow_uuid, r.authenticated_user, r.authenticated_roles])).toEqual([
      ['auth-ambient', 'carol', '["ops"]'],
      ['auth-enqueued', 'bob', '["reader"]'],
      ['auth-explicit', 'alice', '["admin","user"]'],
    ]);
  });
});
