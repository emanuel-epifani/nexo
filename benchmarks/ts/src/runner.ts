import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { Row, SCENARIOS_DIR, Spec, loadSpec, printResults, saveResults } from './report';

// Fan-out workloads open one connection per subscriber; each client installs signal handlers.
process.setMaxListeners(512);

const name = process.argv[2];
if (!name || !existsSync(join(SCENARIOS_DIR, `${name}.json`))) {
  console.error('usage: runner.ts <store|queue|stream|pubsub>');
  process.exit(1);
}

const spec: Spec = loadSpec(name);
const mod = (await import(`./scenarios/${name}.ts`)) as { run: (s: Spec) => Promise<Row[]> };
const rows = await mod.run(spec);
printResults(spec, rows);
console.log(`saved: ${saveResults(spec, rows, 'ts')}`);
