const fs = require('node:fs');
const path = require('node:path');
const ts = require('typescript');
const baseline = require('./public-declarations.json');
const root = path.resolve(__dirname, '../..');
const printer = ts.createPrinter({ removeComments: true });
const files = {
  DBOS: 'dbos',
  DBOSClient: 'client',
  WorkflowQueue: 'wfqueue',
  Debouncer: 'debouncer',
  DebouncerClient: 'debouncer',
};
const failures = [];
for (const [name, expected] of Object.entries(baseline)) {
  const filename = path.join(root, 'dist/src', `${files[name]}.d.ts`);
  const sf = ts.createSourceFile(filename, fs.readFileSync(filename, 'utf8'), ts.ScriptTarget.Latest, true);
  const declaration = sf.statements.find((node) => ts.isClassDeclaration(node) && node.name.text === name);
  if (!declaration) {
    failures.push(`${name}: missing class`);
    continue;
  }
  const actual = {};
  for (const member of declaration.members) {
    if (!member.name) continue;
    const key =
      (member.modifiers?.some((node) => node.kind === ts.SyntaxKind.StaticKeyword) ? 'static ' : '') +
      member.name.getText(sf);
    (actual[key] ??= []).push(printer.printNode(ts.EmitHint.Unspecified, member, sf).replace(/\s+/g, ' ').trim());
  }
  for (const [key, signature] of Object.entries(expected)) {
    // P0.7 intentionally widens create to accept a factory; consumer.ts retains the PostgreSQL call.
    if (name === 'DBOSClient' && key === 'static create') continue;
    if (JSON.stringify(actual[key]) !== JSON.stringify(signature))
      failures.push(`${name}.${key}: public declaration changed`);
  }
}
if (failures.length) throw new Error(failures.join('\n'));
