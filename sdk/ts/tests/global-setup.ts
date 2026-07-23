import { execSync, spawn, ChildProcess } from 'node:child_process';
import { Socket } from 'node:net';
import fs from 'node:fs';
import path from 'node:path';
import { DEFAULT_HOST, DEFAULT_PORT } from '../src/config';

// ============================================================
// Single source of truth: change here to switch debug/release.
// ============================================================
const BUILD_MODE: 'debug' | 'release' = 'release';
const ROOT_DIR = path.resolve(__dirname, '../../../');
const BINARY_PATH = path.join(ROOT_DIR, `target/${BUILD_MODE}/nexo`);
const CARGO_BUILD_CMD = BUILD_MODE === 'release' ? 'cargo build --release' : 'cargo build';
const DATA_DIR = path.join(ROOT_DIR, 'data');

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

function killExistingServer(host: string, port: number): void {
  try {
    // Kill any process listening on the Nexo port (leftover from crashed runs)
    execSync(`lsof -ti tcp:${port} | xargs kill -9 2>/dev/null || true`, { stdio: 'ignore' });
  } catch { /* ignore */ }
}

function cleanDataDir(): void {
  if (fs.existsSync(DATA_DIR)) {
    fs.rmSync(DATA_DIR, { recursive: true, force: true });
  }
  fs.mkdirSync(DATA_DIR, { recursive: true });
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
  });
  await waitForPort(host, port);
  console.log('[TestSetup] Server is ready.');
}

async function killServer(): Promise<void> {
  if (!serverProcess) return;
  const proc = serverProcess;
  serverProcess = null;
  proc.kill('SIGTERM');
  await new Promise<void>((resolve) => {
    proc.once('exit', () => resolve());
    setTimeout(() => { proc.kill('SIGKILL'); resolve(); }, 5000);
  });
}

// ============================================================
// Vitest globalSetup entry point
// ============================================================
export default async function setup() {
  const host = DEFAULT_HOST;
  const port = DEFAULT_PORT;

  // 1. Kill any leftover server from previous/crashed runs
  killExistingServer(host, port);

  // 2. Clean data dir (queues, streams, retained messages)
  cleanDataDir();

  // 3. Build the Rust binary to ensure we're testing the latest code
  console.log(`--- 🛠️  Building Nexo Server in "${BUILD_MODE}" mode ---`);
  execSync(CARGO_BUILD_CMD, { cwd: ROOT_DIR, stdio: 'inherit' });

  // 4. Start the server
  await runNexoServer(host, port);

  // 5. Teardown: kill server and clean data dir
  return async () => {
    console.log('--- 🛑 Shutting down Nexo Server ---');
    await killServer();
    cleanDataDir();
  };
}