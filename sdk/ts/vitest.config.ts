import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    testTimeout: 10000000,
    hookTimeout: 10000000,
    include: ['tests/**/*.test.ts'],
    setupFiles: ['./tests/file-setup.ts'],
    globalSetup: ['./tests/global-setup.ts'],
    fileParallelism: false,
    sequence: {
      concurrent: false,
    },
  },
});
