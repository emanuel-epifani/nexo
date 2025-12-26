import { defineConfig } from 'vitest/config';
import { loadEnvFile } from 'node:process';
import { resolve } from 'node:path';

// Load .env from the root monorepo
loadEnvFile(resolve(__dirname, '../../.env'));

export default defineConfig({
  test: {
    globals: true,
    testTimeout: 10000,
    hookTimeout: 10000,
    include: ['src/test-features/**/*.test.ts'],
    globalSetup: ['./src/global-setup.ts'],
  },
});
