import { execSync, spawn, ChildProcess } from 'node:child_process';
import { Socket } from 'node:net';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { DEFAULT_HOST, DEFAULT_PORT } from '../src/config';

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
let dataDir: string | null = null;

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

function createDataDir(): string {
  dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'nexo-sdk-ts-'));
  return dataDir;
}

function cleanDataDir(): void {
  if (dataDir && fs.existsSync(dataDir)) {
    fs.rmSync(dataDir, { recursive: true, force: true });
  }
  dataDir = null;
}

async function waitForPort(host: string, port: number, retries = 20): Promise<void> {
  for (let i = 0; i < retries; i++) {
    if (await isServerRunning(host, port)) return;
    await new Promise(r => setTimeout(r, 200));
  }
  throw new Error(`Timeout waiting for port ${port} on ${host}`);
}

async function runNexoServer(host: string, port: number): Promise<void> {
  const dir = dataDir!;
  console.log(`[TestSetup] Spawning Nexo server from: ${BINARY_PATH}`);
  serverProcess = spawn(BINARY_PATH, [], {
    stdio: 'inherit',
    cwd: dir,
    env: {
      ...process.env,
      SERVER_HOST: host,
      SERVER_SOCKET_TCP_PORT: String(port),
      QUEUE_ROOT_PERSISTENCE_PATH: path.join(dir, 'queues'),
      STREAM_ROOT_PERSISTENCE_PATH: path.join(dir, 'streams'),
      PUBSUB_ROOT_PERSISTENCE_PATH: path.join(dir, 'pubsub'),
    },
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

  // 1. Fail fast if the test port is already bound: whatever is listening
  //    belongs to someone else and is never killed by this suite.
  if (await isServerRunning(host, port)) {
    throw new Error(`[TestSetup] Port ${port} on ${host} is already in use`);
  }

  // 2. Create the isolated data dir (queues, streams, retained messages)
  createDataDir();

  try {
    // 3. Build the Rust binary to ensure we're testing the latest code
    console.log(`--- 🛠️  Building Nexo Server in "${BUILD_MODE}" mode ---`);
    execSync(CARGO_BUILD_CMD, { cwd: ROOT_DIR, stdio: 'inherit' });

    // 4. Start the server
    await runNexoServer(host, port);
  } catch (error) {
    await killServer();
    cleanDataDir();
    throw error;
  }

  // 5. Teardown: kill server and clean data dir
  return async () => {
    console.log('--- 🛑 Shutting down Nexo Server ---');
    await killServer();
    cleanDataDir();
  };
}
