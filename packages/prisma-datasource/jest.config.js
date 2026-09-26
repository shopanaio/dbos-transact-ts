/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
  preset: 'ts-jest',
  setupFilesAfterEnv: ['<rootDir>/../../tests/datasource-system-database.setup.ts'],
  ...(process.env.DBOS_TEST_SYSTEM_DATABASE_FACTORY ? { testNamePattern: '^(?!.*\\[postgres-only\\])' } : {}),
  testEnvironment: 'node',
  testRegex: '((\\.|/)(test|spec))\\.ts?$',
  moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx', 'json', 'node'],
  modulePaths: ['./'],
};
