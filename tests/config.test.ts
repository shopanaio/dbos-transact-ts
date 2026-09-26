import * as utils from '../src/utils';
import {
  ConfigFile,
  getDbosConfig,
  getSystemDatabaseUrl,
  overwriteConfigForDBOSCloud,
  readConfigFile,
  translateDbosConfig,
} from '../src/config';
import { AssertionError } from 'assert';
import { DBOSConfig } from '../src/dbos-executor';
import { DBOSJSON } from '../src/serialization';
import { DBOS } from '../src';

describe('[postgres-only] dbos-config', () => {
  beforeEach(() => {
    jest.resetModules();
    process.env = {};
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  describe('[postgres-only] readConfigFile', () => {
    test('[postgres-only] reads package name if not specified in config file', async () => {
      const mockConfigFile = `system_database_url: 'postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable'`;
      jest.spyOn(utils, 'readFile').mockResolvedValueOnce(mockConfigFile);
      const mockPackageJson = `{ "name": "test-app-from-package-json" }`;
      jest.spyOn(utils, 'readFile').mockResolvedValueOnce(mockPackageJson);

      const cfg: ConfigFile = await readConfigFile();
      expect(cfg).toEqual({
        name: 'test-app-from-package-json',
        system_database_url: 'postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable',
      });
    });

    test('[postgres-only] handles env substitution', async () => {
      const database_url = `test-value-${Date.now()}`;
      process.env.MY_DATABASE_URL = database_url;

      const mockConfigFile = `
        name: 'test-app'
        system_database_url: \${MY_DATABASE_URL}`;
      jest.spyOn(utils, 'readFile').mockResolvedValue(mockConfigFile);

      const cfg: ConfigFile = await readConfigFile();
      expect(cfg).toEqual({
        name: 'test-app',
        system_database_url: database_url,
      });
    });

    test('[postgres-only] handles missing env substitution', async () => {
      process.env = {};

      const mockConfigFile = `
        name: 'test-app'
        system_database_url: \${MY_DATABASE_URL}`;
      jest.spyOn(utils, 'readFile').mockResolvedValue(mockConfigFile);

      const cfg: ConfigFile = await readConfigFile();
      expect(cfg).toEqual({
        name: 'test-app',
        system_database_url: '',
      });
    });

    class FakeNotFoundError extends Error {
      readonly code = 'ENOENT';
    }

    test('[postgres-only] config and package files not found', async () => {
      jest.spyOn(utils, 'readFile').mockImplementation(() => {
        throw new FakeNotFoundError('not found');
      });
      const configFile = await readConfigFile();
      expect(configFile).toEqual({});
    });

    test('[postgres-only] config file not found', async () => {
      jest.spyOn(utils, 'readFile').mockImplementationOnce(() => {
        throw new FakeNotFoundError('not found');
      });
      jest.spyOn(utils, 'readFile').mockResolvedValueOnce(`{ "name": "test-app-from-package-json" }`);
      const configFile = await readConfigFile();
      expect(configFile).toEqual({ name: 'test-app-from-package-json' });
    });

    test('[postgres-only] package file not found', async () => {
      jest
        .spyOn(utils, 'readFile')
        .mockResolvedValueOnce(
          `system_database_url: 'postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable'`,
        );
      jest.spyOn(utils, 'readFile').mockImplementationOnce(() => {
        throw new FakeNotFoundError('not found');
      });
      const configFile = await readConfigFile();
      expect(configFile).toEqual({
        system_database_url: 'postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable',
      });
    });

    test('[postgres-only] does not read package if config file has name', async () => {
      jest.spyOn(utils, 'readFile').mockResolvedValueOnce(`name: 'test-app'`);
      jest.spyOn(utils, 'readFile').mockImplementationOnce(() => {
        throw new Error('Should not be called');
      });
      const configFile = await readConfigFile();
      expect(configFile).toEqual({ name: 'test-app' });
    });

    test('[postgres-only] throws on non-ENOENT config read error', async () => {
      jest.spyOn(utils, 'readFile').mockImplementation(() => {
        throw new Error('Some other error');
      });
      await expect(readConfigFile()).rejects.toThrow(new Error('Some other error'));
    });

    test('[postgres-only] throws on non-ENOENT package read error', async () => {
      jest.spyOn(utils, 'readFile').mockImplementationOnce(() => {
        throw new FakeNotFoundError('not found');
      });
      jest.spyOn(utils, 'readFile').mockImplementationOnce(() => {
        throw new Error('Some other error');
      });
      await expect(readConfigFile()).rejects.toThrow(new Error('Some other error'));
    });
  });

  describe('[postgres-only] getDbosConfig', () => {
    test('[postgres-only] logLevel default', () => {
      const configFile: ConfigFile = {
        name: 'test-app',
      };
      const config = getDbosConfig(configFile);
      expect(config.telemetry.logs.logLevel).toEqual('info');
    });

    test('[postgres-only] logLevel specified', () => {
      const configFile: ConfigFile = {
        name: 'test-app',
        telemetry: {
          logs: {
            logLevel: 'debug',
          },
        },
      };
      const config = getDbosConfig(configFile);
      expect(config.telemetry.logs.logLevel).toEqual('debug');
    });

    test('[postgres-only] logLevel override', () => {
      const configFile: ConfigFile = {
        name: 'test-app',
        telemetry: {
          logs: {
            logLevel: 'debug',
          },
        },
      };
      const config = getDbosConfig(configFile, { logLevel: 'error' });
      expect(config.telemetry.logs.logLevel).toEqual('error');
    });

    test('[postgres-only] returns correct database url', () => {
      const configFile: ConfigFile = {
        name: 'test-app',
        system_database_url: 'postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable',
      };
      const config = getDbosConfig(configFile);
      expect(config.systemDatabaseUrl).toBe('postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable');
    });

    test('[postgres-only] handles node language', () => {
      const configFile: ConfigFile = {
        name: 'test-app',
        language: 'node',
        system_database_url: 'postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable',
      };
      expect(() => getDbosConfig(configFile)).not.toThrow();
    });

    test('[postgres-only] handles missing language', () => {
      const configFile: ConfigFile = {
        name: 'test-app',
        system_database_url: 'postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable',
      };
      expect(() => getDbosConfig(configFile)).not.toThrow();
    });

    test('[postgres-only] throws on non node language', () => {
      const configFile: ConfigFile = {
        name: 'test-app',
        language: 'not-node',
        system_database_url: 'postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable',
      };
      expect(() => getDbosConfig(configFile)).toThrow();
    });
  });

  describe('[postgres-only] getSystemDatabaseUrl', () => {
    test('[postgres-only] uses system_database_url from config when provided', () => {
      const databaseUrl = getSystemDatabaseUrl({
        name: 'Test App',
        system_database_url: 'postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable',
      });
      expect(databaseUrl).toBe('postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable');
    });

    test('[postgres-only] uses default values when config is empty', () => {
      const databaseUrl = getSystemDatabaseUrl({
        name: 'Test App',
      });
      expect(databaseUrl).toBe(
        'postgresql://postgres:dbos@localhost:5432/test_app_dbos_sys?connect_timeout=10&sslmode=disable',
      );
    });

    test('[postgres-only] throws when db url not set and app name is missing', () => {
      expect(() => getSystemDatabaseUrl({})).toThrow(AssertionError);
    });

    test('[postgres-only] uses PG env values when config is empty', () => {
      process.env.PGHOST = 'envhost';
      process.env.PGPORT = '7777';
      process.env.PGUSER = 'envuser';
      process.env.PGPASSWORD = 'envpass';

      const databaseUrl = getSystemDatabaseUrl({
        name: 'Test App',
      });
      expect(databaseUrl).toBe(
        'postgresql://envuser:envpass@envhost:7777/test_app_dbos_sys?connect_timeout=10&sslmode=allow',
      );
    });

    test('[postgres-only] correctly handles app names with spaces', () => {
      const url = getSystemDatabaseUrl({
        name: 'app name with spaces',
      });
      expect(url).toBe(
        'postgresql://postgres:dbos@localhost:5432/app_name_with_spaces_dbos_sys?connect_timeout=10&sslmode=disable',
      );
    });

    test('[postgres-only] correctly handles db url w/o password', () => {
      const url = getSystemDatabaseUrl({
        system_database_url: 'postgresql://postgres@localhost:5432/dbostest?sslmode=disable',
      });
      expect(url).toBe('postgresql://postgres@localhost:5432/dbostest?sslmode=disable');
    });

    test('[postgres-only] throws with invalid system_database_url format', () => {
      expect(() => getSystemDatabaseUrl({ system_database_url: 'not-a-valid-url' })).toThrow();
    });

    test.each(['postgres://host:5432/db', 'postgres://user:pass@:5432/db', 'postgres://user:pass@host:5432/'])(
      'throws when system_database_url is missing required fields %s',
      (system_database_url) => {
        expect(() => getSystemDatabaseUrl({ system_database_url })).toThrow();
      },
    );

    test.each(['some_DB', '123db', 'very_very_very_long_very_very_very_long_very_very__database_name', 'largeDB'])(
      'throws on invalid database name %s',
      (name) => {
        expect(() => getSystemDatabaseUrl({ system_database_url: `postgres://host:5432/${name}` })).toThrow();
      },
    );
  });

  describe('[postgres-only] translateDbosConfig', () => {
    test('[postgres-only] translate with name', () => {
      const internalConfig = translateDbosConfig({
        name: 'dbostest',
      });
      expect(internalConfig).toEqual({
        name: 'dbostest',
        systemDatabaseUrl:
          'postgresql://postgres:dbos@localhost:5432/dbostest_dbos_sys?connect_timeout=10&sslmode=disable',
        sysDbPoolSize: undefined,
        maxConcurrentQueueDispatches: undefined,
        notificationCoalesceMs: undefined,
        observabilityQueryTimeoutMs: undefined,
        systemDatabasePollingConcurrency: undefined,
        queuePollingBatchSize: 1000,
        queuePollingCoalesceMs: 50,
        systemDatabasePool: undefined,
        systemDatabaseSchemaName: 'dbos',
        schedulerPollingIntervalMs: undefined,
        serializer: DBOSJSON,
        useListenNotify: true,
        runMigrations: true,
        telemetry: {
          logs: {
            logLevel: 'info',
            logger: undefined,
            addContextMetadata: undefined,
          },
          OTLPExporter: {
            tracesEndpoint: undefined,
            logsEndpoint: undefined,
          },
          otelAttributeFormat: 'legacy',
        },
      });
    });

    test('[postgres-only] translate passes through systemDatabasePollingConcurrency', () => {
      let internalConfig = translateDbosConfig({
        name: 'dbostest',
        systemDatabasePoolSize: 50,
        systemDatabasePollingConcurrency: 8,
      });
      expect(internalConfig.sysDbPoolSize).toBe(50);
      expect(internalConfig.systemDatabasePollingConcurrency).toBe(8);
      // When unset, translation leaves it undefined; the default (half the pool)
      // is materialized later in the PostgresSystemDatabase constructor.
      internalConfig = translateDbosConfig({
        name: 'dbostest',
        systemDatabasePoolSize: 50,
      });
      expect(internalConfig.sysDbPoolSize).toBe(50);
      expect(internalConfig.systemDatabasePollingConcurrency).toBeUndefined();
    });

    test('[postgres-only] translate validates maxConcurrentQueueDispatches', () => {
      const internalConfig = translateDbosConfig({
        name: 'dbostest',
        maxConcurrentQueueDispatches: 4,
      });
      expect(internalConfig.maxConcurrentQueueDispatches).toBe(4);
      // Unset is allowed (dispatcher defaults it to 3).
      expect(translateDbosConfig({ name: 'dbostest' }).maxConcurrentQueueDispatches).toBeUndefined();
      // Every non-positive-integer value is rejected.
      for (const bad of [0, -1, 2.5, NaN, Infinity]) {
        expect(() => translateDbosConfig({ name: 'dbostest', maxConcurrentQueueDispatches: bad })).toThrow(
          'maxConcurrentQueueDispatches must be a positive integer',
        );
      }
    });

    test('[postgres-only] translate validates notificationCoalesceMs', () => {
      // A valid value is threaded through.
      expect(translateDbosConfig({ name: 'dbostest', notificationCoalesceMs: 1 }).notificationCoalesceMs).toBe(1);
      // Unset is allowed (the system database defaults it).
      expect(translateDbosConfig({ name: 'dbostest' }).notificationCoalesceMs).toBeUndefined();
      // Invalid values are rejected, including NaN/inf which slip past a bare < 1 check.
      for (const bad of [0.5, 0, -1, NaN, Infinity]) {
        expect(() => translateDbosConfig({ name: 'dbostest', notificationCoalesceMs: bad })).toThrow(
          'notificationCoalesceMs must be a finite number at least 1 millisecond',
        );
      }
    });

    test('[postgres-only] translate passes through a custom logger', () => {
      const myLogger = { info: () => {}, debug: () => {}, warn: () => {}, error: () => {} };
      let internalConfig = translateDbosConfig({ name: 'dbostest', logger: myLogger });
      expect(internalConfig.telemetry.logs.logger).toBe(myLogger);
      internalConfig = translateDbosConfig({ name: 'dbostest' });
      expect(internalConfig.telemetry.logs.logger).toBeUndefined();
    });

    test('[postgres-only] translate with db url', () => {
      const internalConfig = translateDbosConfig({
        systemDatabaseUrl:
          'postgres://jon:doe@mother:2345/dbostest?sslmode=require&sslrootcert=my_cert&connect_timeout=7',
      });
      expect(internalConfig).toEqual({
        name: undefined,
        systemDatabaseUrl:
          'postgres://jon:doe@mother:2345/dbostest?sslmode=require&sslrootcert=my_cert&connect_timeout=7',
        sysDbPoolSize: undefined,
        maxConcurrentQueueDispatches: undefined,
        notificationCoalesceMs: undefined,
        observabilityQueryTimeoutMs: undefined,
        systemDatabasePollingConcurrency: undefined,
        queuePollingBatchSize: 1000,
        queuePollingCoalesceMs: 50,
        systemDatabasePool: undefined,
        systemDatabaseSchemaName: 'dbos',
        schedulerPollingIntervalMs: undefined,
        serializer: DBOSJSON,
        useListenNotify: true,
        runMigrations: true,
        telemetry: {
          logs: {
            logLevel: 'info',
            logger: undefined,
            addContextMetadata: undefined,
          },
          OTLPExporter: {
            tracesEndpoint: undefined,
            logsEndpoint: undefined,
          },
          otelAttributeFormat: 'legacy',
        },
      });
    });

    test('[postgres-only] translate with db & sysdb urls', () => {
      const internalConfig = translateDbosConfig({
        systemDatabaseUrl: 'postgres://foo:bar@father:1234/blahblahblah',
      });
      expect(internalConfig).toEqual({
        name: undefined,
        systemDatabaseUrl: 'postgres://foo:bar@father:1234/blahblahblah',
        sysDbPoolSize: undefined,
        maxConcurrentQueueDispatches: undefined,
        notificationCoalesceMs: undefined,
        observabilityQueryTimeoutMs: undefined,
        systemDatabasePollingConcurrency: undefined,
        queuePollingBatchSize: 1000,
        queuePollingCoalesceMs: 50,
        systemDatabasePool: undefined,
        systemDatabaseSchemaName: 'dbos',
        schedulerPollingIntervalMs: undefined,
        serializer: DBOSJSON,
        useListenNotify: true,
        runMigrations: true,
        telemetry: {
          logs: {
            logLevel: 'info',
            logger: undefined,
            addContextMetadata: undefined,
          },
          OTLPExporter: {
            tracesEndpoint: undefined,
            logsEndpoint: undefined,
          },
          otelAttributeFormat: 'legacy',
        },
      });
    });
  });

  describe('[postgres-only] overwriteConfigForDBOSCloud', () => {
    const config: DBOSConfig = {
      name: 'my-app',
      systemDatabaseUrl: 'postgres://foo:bar@father:1234/blahblahblah',
      systemDatabaseSchemaName: 'my_schema',
      otlpTracesEndpoints: ['http://otel-collector:4317/traces'],
      otlpLogsEndpoints: ['http://otel-collector:4317/logs'],
    };

    test('[postgres-only] throws when cloud db url is missing', () => {
      expect(() => overwriteConfigForDBOSCloud(config)).toThrow(AssertionError);
    });

    test('[postgres-only] uses cloud app name', () => {
      process.env.DBOS_SYSTEM_DATABASE_URL = 'fake://db/url';
      process.env.DBOS_APP_NAME = 'cloud-app-name';
      expect(overwriteConfigForDBOSCloud(config).name).toBe('cloud-app-name');
    });

    test('[postgres-only] keeps configured app name when cloud app name is unset', () => {
      process.env.DBOS_SYSTEM_DATABASE_URL = 'fake://db/url';
      expect(overwriteConfigForDBOSCloud(config).name).toBe('my-app');
    });

    test('[postgres-only] uses cloud sys db url', () => {
      process.env.DBOS_SYSTEM_DATABASE_URL = 'postgres://a:b@c:2345/cloud_sys_db';
      const newConfig = overwriteConfigForDBOSCloud(config);
      expect(newConfig.systemDatabaseUrl).toBe('postgres://a:b@c:2345/cloud_sys_db');
      expect(newConfig.systemDatabaseSchemaName).toBe('my_schema');
    });

    test('[postgres-only] combine otel endpoints', () => {
      process.env.DBOS_SYSTEM_DATABASE_URL = 'fake://db/url';
      process.env.DBOS__OTLP_TRACES_ENDPOINT = 'http://otel-collector:4318/v1/traces';
      process.env.DBOS__OTLP_LOGS_ENDPOINT = 'http://otel-collector:4318/v1/logs';
      const newConfig = overwriteConfigForDBOSCloud(config);
      expect(newConfig.otlpLogsEndpoints).toEqual([
        'http://otel-collector:4317/logs',
        'http://otel-collector:4318/v1/logs',
      ]);
      expect(newConfig.otlpTracesEndpoints).toEqual([
        'http://otel-collector:4317/traces',
        'http://otel-collector:4318/v1/traces',
      ]);
    });

    test('[postgres-only] combine otel endpoints no duplicates', () => {
      process.env.DBOS_SYSTEM_DATABASE_URL = 'fake://db/url';
      process.env.DBOS__OTLP_TRACES_ENDPOINT = 'http://otel-collector:4317/traces';
      process.env.DBOS__OTLP_LOGS_ENDPOINT = 'http://otel-collector:4317/logs';
      const newConfig = overwriteConfigForDBOSCloud(config);
      expect(newConfig.otlpLogsEndpoints).toEqual(['http://otel-collector:4317/logs']);
      expect(newConfig.otlpTracesEndpoints).toEqual(['http://otel-collector:4317/traces']);
    });

    test('[postgres-only] works without any provided config', () => {
      process.env.DBOS_SYSTEM_DATABASE_URL = 'postgres://a:b@c:2345/cloud_sys_db';
      process.env.DBOS_APP_NAME = 'cloud-app-name';
      process.env.DBOS__OTLP_LOGS_ENDPOINT = 'http://otel-collector:4318/v1/logs';
      const internalConfig = translateDbosConfig(overwriteConfigForDBOSCloud({}));
      expect(internalConfig.name).toBe('cloud-app-name');
      expect(internalConfig.systemDatabaseUrl).toBe('postgres://a:b@c:2345/cloud_sys_db');
      expect(internalConfig.telemetry.OTLPExporter.logsEndpoint).toEqual(['http://otel-collector:4318/v1/logs']);
      expect(internalConfig.telemetry.OTLPExporter.tracesEndpoint).toEqual([]);
    });

    test('[postgres-only] getDbosConfig applies the cloud environment', () => {
      // DBOS__CLOUD is read once at import, so set the flag it produces.
      const wasCloud = utils.globalParams.dbosCloud;
      utils.globalParams.dbosCloud = true;
      process.env.DBOS_SYSTEM_DATABASE_URL = 'postgres://a:b@c:2345/cloud_sys_db';
      process.env.DBOS_APP_NAME = 'cloud-app-name';
      process.env.DBOS__OTLP_TRACES_ENDPOINT = 'http://otel-collector:4318/v1/traces';
      try {
        const internalConfig = getDbosConfig({ name: 'file-app-name' });
        expect(internalConfig.name).toBe('cloud-app-name');
        expect(internalConfig.systemDatabaseUrl).toBe('postgres://a:b@c:2345/cloud_sys_db');
        expect(internalConfig.telemetry.OTLPExporter.tracesEndpoint).toEqual(['http://otel-collector:4318/v1/traces']);
      } finally {
        utils.globalParams.dbosCloud = wasCloud;
      }
    });
  });

  describe('[postgres-only] DBOS.launch', () => {
    test('[postgres-only] requires setConfig outside DBOS Cloud', async () => {
      await expect(DBOS.launch()).rejects.toThrow('call DBOS.setConfig before DBOS.launch');
    });

    test('[postgres-only] requires an application name', async () => {
      // JavaScript callers can omit the name the type requires.
      DBOS.setConfig({ systemDatabaseUrl: 'postgres://postgres:dbos@localhost:5432/unnamed_sys' } as DBOSConfig);
      await expect(DBOS.launch()).rejects.toThrow('No application name was provided');
      expect(DBOS.isInitialized()).toBe(false);
    });
  });
});

describe('[postgres-only] batch polling configuration (SC-qp06)', () => {
  test('[postgres-only] defaults and explicit overrides survive translation', () => {
    expect(translateDbosConfig({ name: 'test' })).toMatchObject({
      queuePollingBatchSize: 1000,
      queuePollingCoalesceMs: 50,
    });
    expect(translateDbosConfig({ name: 'test', queuePollingBatchSize: 7, queuePollingCoalesceMs: 1 })).toMatchObject({
      queuePollingBatchSize: 7,
      queuePollingCoalesceMs: 1,
    });
  });
  test.each(['queuePollingBatchSize', 'queuePollingCoalesceMs'] as const)('rejects invalid %s', (key) => {
    for (const value of [0, -1, 1.5, NaN, Infinity, -Infinity, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() => translateDbosConfig({ [key]: value })).toThrow(key);
    }
  });
});
