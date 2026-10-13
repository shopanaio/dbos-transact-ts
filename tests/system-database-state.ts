import { Client } from 'pg';
import type { DBOSConfig } from '../src/dbos-executor';
import { loadTestSystemDatabase } from './system-database-factory';

/**
 * Out-of-band access to the system database's workflow state, for tests that stand in for a
 * concurrent execution (a resume, a recovery, a cancel) by rewriting what that execution would
 * have written. The PostgreSQL implementation issues the SQL directly; an injected backend
 * exports its own as `workflowState(config)`.
 */
export interface WorkflowStateAccess {
  /** Set the status and the recorded outcome, as the execution that owned the workflow would have. */
  rewriteOutcome(
    workflowID: string,
    status: string,
    fields?: { output?: string | null; error?: string | null; serialization?: string | null },
  ): Promise<void>;
  /** The status and the recorded outcome, read without any reader fallback. */
  readOutcome(workflowID: string): Promise<{ status: string; output: string | null; error: string | null }>;
  /** Plant the checkpoint of a concurrent execution at `functionID`. */
  plantStepCheckpoint(workflowID: string, functionID: number, functionName: string): Promise<void>;
  /** Stamp the executor marker (and the status, if given) as another executor would have. */
  setExecutor(workflowID: string, executorID: string, status?: string): Promise<void>;
  /** The executor marker of the workflow. */
  readExecutor(workflowID: string): Promise<string | null>;
  setRecoveryAttempts(workflowID: string, attempts: number): Promise<void>;
  readRecoveryAttempts(workflowID: string): Promise<number>;
  /** Remove the workflow's status, as a concurrent delete would. */
  deleteWorkflowStatus(workflowID: string): Promise<void>;
  /** workflow_status columns by their PostgreSQL names; an absent value reads as null. */
  readStatusColumns(workflowID: string, columns: StatusColumn[]): Promise<Record<string, unknown> | undefined>;
  /** The status with its payloads resolved, the shape every reader in the SDK sees. */
  readWorkflowRow(workflowID: string): Promise<WorkflowRow | undefined>;
  /** Stored values and their serialization, in write order (stream offset order for streams). */
  readStoredValues(kind: StoredKind, workflowID: string, key: string): Promise<StoredValue[]>;
  /** A finished workflow written by another process, e.g. one using a different serializer. */
  insertFinishedWorkflow(row: {
    workflowID: string;
    name: string;
    className: string;
    status: string;
    output: string | null;
    serialization: string;
  }): Promise<void>;
  insertEvent(workflowID: string, key: string, value: string, serialization: string): Promise<void>;
  /** The owning application of a row, or null when unclaimed. */
  ownerOf(table: OwnedTable, key: string): Promise<string | null>;
  /** The owners stamped on every step checkpoint of a workflow. */
  stepOwners(workflowID: string): Promise<(string | null)[]>;
  /** A version registered by another application, e.g. with a timestamp ahead of ours. */
  insertVersion(row: {
    versionId: string;
    versionName: string;
    timestamp: number;
    applicationName: string | null;
  }): Promise<void>;
  /** A workflow row written by another application (null: unclaimed), with its input when given. */
  insertPeerWorkflow(row: PeerWorkflow): Promise<void>;
  /** A step checkpoint written by another application. */
  insertPeerStep(row: {
    workflowID: string;
    functionID: number;
    functionName: string;
    output: string | null;
    completedAt: number;
    applicationName: string | null;
  }): Promise<void>;
  /** A schedule registered by another application, or by a release that predates ownership. */
  insertPeerSchedule(row: {
    scheduleID: string;
    scheduleName: string;
    workflowName: string;
    schedule: string;
    context: string;
    applicationName: string | null;
  }): Promise<void>;
  /** A queue registered by another application, or by a release that predates ownership. */
  insertPeerQueue(name: string, applicationName: string | null): Promise<void>;
  readScheduleId(scheduleName: string): Promise<string | undefined>;
  /** Stored payload rows the retention sweeps count: inputs, outputs, step checkpoints. */
  payloadCounts(): Promise<[number, number, number]>;
  /** Status rows whose payload a reader could still ask for but is gone: [no input, SUCCESS/ERROR with no output]. */
  orphanedStatusRows(): Promise<[number, number]>;
  /** Whether the workflow still has its status row and its input row. */
  workflowRows(workflowID: string): Promise<{ status: boolean; input: boolean }>;
  /** Raw SQL for scenarios that only exist on PostgreSQL (rows written by pl/pgSQL or other languages). */
  sql(): Client;
  close(): Promise<void>;
}

export type PeerWorkflow = {
  workflowID: string;
  status: string;
  name: string;
  queueName: string | null;
  createdAt: number;
  completedAt: number | null;
  applicationName: string | null;
  /** Serialized input; null writes no input row. */
  inputs: string | null;
};

export type StatusColumn =
  | 'status'
  | 'name'
  | 'class_name'
  | 'queue_name'
  | 'application_version'
  | 'application_name'
  | 'priority'
  | 'deduplication_id'
  | 'authenticated_user'
  | 'authenticated_roles'
  | 'rate_limited'
  | 'serialization';

export type WorkflowRow = {
  status: string;
  serialization: string | null;
  inputs: string;
  output: string | null;
  error: string | null;
};
/** notification: key is the topic; event_history: every write of the key. */
export type StoredKind = 'notification' | 'event' | 'event_history' | 'stream';
export type StoredValue = { value: string | null; serialization: string | null };
export type OwnedTable = 'workflow_status' | 'application_versions' | 'queues' | 'workflow_schedules';
const OWNED_KEY: Record<OwnedTable, string> = {
  workflow_status: 'workflow_uuid',
  application_versions: 'version_name',
  queues: 'name',
  workflow_schedules: 'schedule_name',
};

class PostgresWorkflowState implements WorkflowStateAccess {
  constructor(private readonly client: Client) {}

  async rewriteOutcome(
    workflowID: string,
    status: string,
    fields: { output?: string | null; error?: string | null; serialization?: string | null } = {},
  ) {
    await this.client.query(
      `UPDATE dbos.workflow_status
       SET status=$1, output=$2, error=$3, serialization=COALESCE($4, serialization)
       WHERE workflow_uuid=$5`,
      [status, fields.output ?? null, fields.error ?? null, fields.serialization ?? null, workflowID],
    );
    // Both destinations, as the execution that really recorded this outcome would have.
    await this.client.query(
      `INSERT INTO dbos.workflow_output (workflow_uuid, output, error) VALUES ($1, $2, $3)
       ON CONFLICT (workflow_uuid) DO UPDATE SET output = EXCLUDED.output, error = EXCLUDED.error`,
      [workflowID, fields.output ?? null, fields.error ?? null],
    );
  }

  async readOutcome(workflowID: string) {
    const { rows } = await this.client.query<{ status: string; output: string | null; error: string | null }>(
      `SELECT ws.status, wo.output, wo.error
       FROM dbos.workflow_status ws
       LEFT JOIN dbos.workflow_output wo ON wo.workflow_uuid = ws.workflow_uuid
       WHERE ws.workflow_uuid=$1`,
      [workflowID],
    );
    return rows[0];
  }

  async plantStepCheckpoint(workflowID: string, functionID: number, functionName: string) {
    await this.client.query(
      `INSERT INTO dbos.operation_outputs
         (workflow_uuid, function_id, function_name, started_at_epoch_ms, completed_at_epoch_ms)
       VALUES ($1, $2, $3, 1, 1)`,
      [workflowID, functionID, functionName],
    );
  }

  async setExecutor(workflowID: string, executorID: string, status?: string) {
    await this.client.query(
      `UPDATE dbos.workflow_status SET executor_id=$1, status=COALESCE($2, status) WHERE workflow_uuid=$3`,
      [executorID, status ?? null, workflowID],
    );
  }

  async readExecutor(workflowID: string) {
    const { rows } = await this.client.query<{ executor_id: string | null }>(
      `SELECT executor_id FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [workflowID],
    );
    return rows[0]?.executor_id ?? null;
  }

  async setRecoveryAttempts(workflowID: string, attempts: number) {
    await this.client.query(`UPDATE dbos.workflow_status SET recovery_attempts=$1 WHERE workflow_uuid=$2`, [
      attempts,
      workflowID,
    ]);
  }

  async readRecoveryAttempts(workflowID: string) {
    const { rows } = await this.client.query<{ attempts: string }>(
      `SELECT recovery_attempts AS attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [workflowID],
    );
    return Number(rows[0].attempts);
  }

  async deleteWorkflowStatus(workflowID: string) {
    await this.client.query(`DELETE FROM dbos.workflow_status WHERE workflow_uuid=$1`, [workflowID]);
  }

  async readStatusColumns(workflowID: string, columns: StatusColumn[]) {
    const { rows } = await this.client.query<Record<string, unknown>>(
      `SELECT ${columns.join(', ')} FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [workflowID],
    );
    return rows[0];
  }

  async readWorkflowRow(workflowID: string) {
    const { rows } = await this.client.query<WorkflowRow>(
      `SELECT ws.status, ws.serialization,
              COALESCE(wi.inputs, ws.inputs) AS inputs,
              COALESCE(wo.output, ws.output) AS output,
              COALESCE(wo.error, ws.error) AS error
       FROM dbos.workflow_status ws
       LEFT JOIN dbos.workflow_input wi ON wi.workflow_uuid = ws.workflow_uuid
       LEFT JOIN dbos.workflow_output wo ON wo.workflow_uuid = ws.workflow_uuid
       WHERE ws.workflow_uuid = $1`,
      [workflowID],
    );
    return rows[0];
  }

  async readStoredValues(kind: StoredKind, workflowID: string, key: string) {
    const query = {
      notification: `SELECT message AS value, serialization FROM dbos.notifications
                     WHERE destination_uuid = $1 AND topic = $2 ORDER BY created_at_epoch_ms`,
      event: `SELECT value, serialization FROM dbos.workflow_events WHERE workflow_uuid = $1 AND key = $2`,
      event_history: `SELECT value, serialization FROM dbos.workflow_events_history
                      WHERE workflow_uuid = $1 AND key = $2 ORDER BY function_id`,
      stream: `SELECT value, serialization FROM dbos.streams WHERE workflow_uuid = $1 AND key = $2 ORDER BY "offset"`,
    }[kind];
    const { rows } = await this.client.query<StoredValue>(query, [workflowID, key]);
    return rows;
  }

  async insertFinishedWorkflow(row: {
    workflowID: string;
    name: string;
    className: string;
    status: string;
    output: string | null;
    serialization: string;
  }) {
    await this.client.query(
      `INSERT INTO dbos.workflow_status(
        workflow_uuid, name, class_name, status, output, created_at, serialization
      ) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [row.workflowID, row.name, row.className, row.status, row.output, Date.now(), row.serialization],
    );
  }

  async insertEvent(workflowID: string, key: string, value: string, serialization: string) {
    await this.client.query(
      `INSERT INTO dbos.workflow_events(
        workflow_uuid, key, value, serialization
      ) VALUES ($1, $2, $3, $4)`,
      [workflowID, key, value, serialization],
    );
  }

  async ownerOf(table: OwnedTable, key: string) {
    const { rows } = await this.client.query<{ application_name: string | null }>(
      `SELECT application_name FROM dbos.${table} WHERE ${OWNED_KEY[table]} = $1`,
      [key],
    );
    return rows[0]?.application_name ?? null;
  }

  async stepOwners(workflowID: string) {
    const { rows } = await this.client.query<{ application_name: string | null }>(
      `SELECT application_name FROM dbos.operation_outputs WHERE workflow_uuid = $1`,
      [workflowID],
    );
    return rows.map((row) => row.application_name);
  }

  async insertVersion(row: {
    versionId: string;
    versionName: string;
    timestamp: number;
    applicationName: string | null;
  }) {
    await this.client.query(
      `INSERT INTO dbos.application_versions (version_id, version_name, version_timestamp, application_name)
       VALUES ($1, $2, $3, $4)`,
      [row.versionId, row.versionName, row.timestamp, row.applicationName],
    );
  }

  async insertPeerWorkflow(row: PeerWorkflow) {
    await this.client.query(
      `INSERT INTO dbos.workflow_status
         (workflow_uuid, status, name, queue_name, executor_id, application_id, created_at, updated_at,
          completed_at, recovery_attempts, priority, serialization, application_name)
       VALUES ($1, $2, $3, $4, 'local', '', $5, $5, $6, 0, 0, 'portable_json', $7)`,
      [row.workflowID, row.status, row.name, row.queueName, row.createdAt, row.completedAt, row.applicationName],
    );
    if (row.inputs !== null) {
      await this.client.query(
        `INSERT INTO dbos.workflow_input (workflow_uuid, inputs, retention_timestamp) VALUES ($1, $2, $3)`,
        [row.workflowID, row.inputs, row.createdAt],
      );
    }
  }

  async insertPeerStep(row: {
    workflowID: string;
    functionID: number;
    functionName: string;
    output: string | null;
    completedAt: number;
    applicationName: string | null;
  }) {
    await this.client.query(
      `INSERT INTO dbos.operation_outputs
         (workflow_uuid, function_id, function_name, output, serialization, completed_at_epoch_ms, application_name)
       VALUES ($1, $2, $3, $4, 'portable_json', $5, $6)`,
      [row.workflowID, row.functionID, row.functionName, row.output, row.completedAt, row.applicationName],
    );
  }

  async insertPeerSchedule(row: {
    scheduleID: string;
    scheduleName: string;
    workflowName: string;
    schedule: string;
    context: string;
    applicationName: string | null;
  }) {
    await this.client.query(
      `INSERT INTO dbos.workflow_schedules (schedule_id, schedule_name, workflow_name, schedule, status, context, application_name)
       VALUES ($1, $2, $3, $4, 'ACTIVE', $5, $6)`,
      [row.scheduleID, row.scheduleName, row.workflowName, row.schedule, row.context, row.applicationName],
    );
  }

  async insertPeerQueue(name: string, applicationName: string | null) {
    await this.client.query(`INSERT INTO dbos.queues (name, application_name) VALUES ($1, $2)`, [name, applicationName]);
  }

  async readScheduleId(scheduleName: string) {
    const { rows } = await this.client.query<{ schedule_id: string }>(
      `SELECT schedule_id FROM dbos.workflow_schedules WHERE schedule_name = $1`,
      [scheduleName],
    );
    return rows[0]?.schedule_id;
  }

  async payloadCounts(): Promise<[number, number, number]> {
    const countOf = async (table: string) => {
      const { rows } = await this.client.query<{ count: string }>(`SELECT COUNT(*) AS count FROM dbos.${table}`);
      return Number(rows[0].count);
    };
    return [await countOf('workflow_input'), await countOf('workflow_output'), await countOf('operation_outputs')];
  }

  async orphanedStatusRows(): Promise<[number, number]> {
    const { rows } = await this.client.query<{ no_input: string; no_output: string }>(
      `SELECT
         COUNT(*) FILTER (WHERE wi.workflow_uuid IS NULL) AS no_input,
         COUNT(*) FILTER (WHERE ws.status IN ('SUCCESS', 'ERROR') AND wo.workflow_uuid IS NULL) AS no_output
       FROM dbos.workflow_status ws
       LEFT JOIN dbos.workflow_input wi ON wi.workflow_uuid = ws.workflow_uuid
       LEFT JOIN dbos.workflow_output wo ON wo.workflow_uuid = ws.workflow_uuid`,
    );
    return [Number(rows[0].no_input), Number(rows[0].no_output)];
  }

  async workflowRows(workflowID: string) {
    const exists = async (table: string) =>
      (await this.client.query(`SELECT 1 FROM dbos.${table} WHERE workflow_uuid = $1`, [workflowID])).rows.length > 0;
    return { status: await exists('workflow_status'), input: await exists('workflow_input') };
  }

  sql() {
    return this.client;
  }

  async close() {
    await this.client.end();
  }
}

type StateBackend = { workflowState?: (config: DBOSConfig) => Promise<WorkflowStateAccess> };

export async function openWorkflowState(config: DBOSConfig): Promise<WorkflowStateAccess> {
  const backend = (await loadTestSystemDatabase()) as StateBackend | undefined;
  if (backend) {
    if (!backend.workflowState) throw new Error('Test backend must export workflowState() for out-of-band state tests');
    return backend.workflowState(config);
  }
  const client = new Client({ connectionString: config.systemDatabaseUrl });
  await client.connect();
  return new PostgresWorkflowState(client);
}
