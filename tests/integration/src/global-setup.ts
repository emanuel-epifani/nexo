import { execSync } from 'node:child_process';
import path from 'node:path';
import { runNexoServer, killServer, isServerRunning } from './utils/server';

export default async function setup() {
  const host = process.env.SERVER_HOST!;
  const port = parseInt(process.env.SERVER_PORT!, 10);
  const ROOT_DIR = path.resolve(__dirname, '../../../');


  // If Nexo is already running (e.g. Debugging in IDE), skip build and spawn
  if (await isServerRunning(host, port)) {
    console.log(`[GlobalSetup] Nexo is already running on ${host}:${port}. Reusing instance (Debug Mode).`);
    return;
  }

  // --- FULL INTEGRATION FLOW ---
  // 1. Build the Rust binary to ensure we're testing the latest code
  console.log('--- 🛠️  Building Nexo (Rust) ---');
  execSync('cargo build', { cwd: ROOT_DIR, stdio: 'inherit' });

  // 2. Start the server once for the entire suite
  await runNexoServer(host, port);

  // 3. Return the teardown function
  return async () => {
    console.log('--- 🛑 Shutting down Nexo Server ---');
    killServer();
  };
}
