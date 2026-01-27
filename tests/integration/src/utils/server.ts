import { spawn, ChildProcess } from 'node:child_process';
import { Socket } from 'node:net';
import path from 'node:path';

let serverProcess: ChildProcess | null = null;

const ROOT_DIR = path.resolve(__dirname, '../../../../');
const BINARY_PATH = path.join(ROOT_DIR, 'target/debug/nexo');

/**
 * Checks if the Nexo server is already running on the given host and port.
 */
export async function isServerRunning(host: string, port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = new Socket();
    socket.setTimeout(100);
    socket.on('connect', () => { socket.destroy(); resolve(true); });
    socket.on('error', () => { socket.destroy(); resolve(false); });
    socket.on('timeout', () => { socket.destroy(); resolve(false); });
    socket.connect(port, host);
  });
}

export async function runNexoServer(host: string, port: number): Promise<void> {
  console.log(`[TestSetup] Spawning Nexo server from: ${BINARY_PATH}`);

  serverProcess = spawn(BINARY_PATH, [], {
    stdio: 'inherit',
    cwd: ROOT_DIR,
    env: { ...process.env, SERVER_HOST: host, SERVER_PORT: port.toString() }
  });

  await waitForPort(host, port);
  console.log('[TestSetup] Server is ready.');
}

export function killServer(): void {
  if (serverProcess) {
    serverProcess.kill();
    serverProcess = null;
  }
}

async function waitForPort(host: string, port: number, retries = 20): Promise<void> {
  for (let i = 0; i < retries; i++) {
    if (await isServerRunning(host, port)) return;
    await new Promise(r => setTimeout(r, 200));
  }
  throw new Error(`Timeout waiting for port ${port} on ${host}`);
}
