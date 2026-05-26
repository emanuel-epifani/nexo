import { execSync, spawn, ChildProcess } from 'node:child_process';
import { Socket } from 'node:net';
import path from 'node:path';

// ============================================================
// Single source of truth: change here to switch debug/release.
// ============================================================
const BUILD_MODE: 'debug' | 'release' = 'release';
const ROOT_DIR = path.resolve(__dirname, '../../../');
const BINARY_PATH = path.join(ROOT_DIR, `target/${BUILD_MODE}/nexo`);
const CARGO_BUILD_CMD = BUILD_MODE === 'release' ? 'cargo build --release' : 'cargo build';

// ============================================================
// Server lifecycle helpers
// ============================================================
let serverProcess: ChildProcess | null = null;

async function isServerRunning(host: string, port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = new Socket();
    socket.setTimeout(100);
    socket.on('connect', () => { socket.destroy(); resolve(true); });
    socket.on('error', () => { socket.destroy(); resolve(false); });
    socket.on('timeout', () => { socket.destroy(); resolve(false); });
    socket.connect(port, host);
  });
}

async function waitForPort(host: string, port: number, retries = 20): Promise<void> {
  for (let i = 0; i < retries; i++) {
    if (await isServerRunning(host, port)) return;
    await new Promise(r => setTimeout(r, 200));
  }
  throw new Error(`Timeout waiting for port ${port} on ${host}`);
}

async function runNexoServer(host: string, port: number): Promise<void> {
  console.log(`[TestSetup] Spawning Nexo server from: ${BINARY_PATH}`);
  serverProcess = spawn(BINARY_PATH, [], {
    stdio: 'inherit',
    cwd: ROOT_DIR,
    env: { ...process.env, SERVER_HOST: host, SERVER_SOCKET_TCP_PORT: port.toString() },
  });
  await waitForPort(host, port);
  console.log('[TestSetup] Server is ready.');
}

function killServer(): void {
  if (serverProcess) {
    serverProcess.kill();
    serverProcess = null;
  }
}

// ============================================================
// Vitest globalSetup entry point
// ============================================================
export default async function setup() {
  const host = process.env.SERVER_HOST!;
  const port = parseInt(process.env.SERVER_SOCKET_TCP_PORT!, 10);

  // If Nexo is already running (e.g. Debugging in IDE), skip build and spawn
  if (await isServerRunning(host, port)) {
    console.log(`[GlobalSetup] Nexo is already running on ${host}:${port}. Reusing instance (Debug Mode).`);
    return;
  }

  // --- FULL INTEGRATION FLOW ---
  // 1. Build the Rust binary to ensure we're testing the latest code
  console.log(`--- 🛠️  Building Nexo Server in "${BUILD_MODE}" mode ---`);
  execSync(CARGO_BUILD_CMD, { cwd: ROOT_DIR, stdio: 'inherit' });

  // 2. Start the server once for the entire suite
  await runNexoServer(host, port);

  // 3. Return the teardown function
  return async () => {
    console.log('--- 🛑 Shutting down Nexo Server ---');
    killServer();
  };
}