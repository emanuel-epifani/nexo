import { defineConfig } from 'vitest/config';
import { loadEnvFile } from 'node:process';
import { resolve } from 'node:path';
import { z } from 'zod';



// 1. Load .env from the root project
loadEnvFile(resolve(__dirname, '../../.env'));


// 2. Define Env Schema
const envSchema = z.object({
  SERVER_HOST: z.string().min(1, "SERVER_HOST is required"),
  SERVER_PORT: z.string().min(4, "SENTINEL_PORT è richiesta"),
  SERVER_DASHBOARD_PORT: z.string().min(4, "SERVER_DASHBOARD_PORT is required"),
});

// 3. Validate
const result = envSchema.safeParse(process.env);

if (!result.success) {
  console.error("❌ Environment validation failed:");
  result.error.issues.forEach((issue) => {
    console.error(`  - ${issue.path.join('.')}: ${issue.message}`);
  });
  process.exit(1);
}

// 4. Log validated envs
console.log("✅ Environment validated successfully:");
Object.entries(result.data).forEach(([key, value]) => {
  console.log(`  - ${key}: ${value}`);
});


export default defineConfig({
  test: {
    globals: true,
    testTimeout: 10000000,
    hookTimeout: 10000000,
    include: ['tests/**/*.test.ts'],
    setupFiles: ["./tests/file-setup.ts"],
    globalSetup: ['./tests/global-setup.ts'],
    // 1. Disabilita parallelismo TRA file
    fileParallelism: false,
    // 2. Disabilita parallelismo tra test DENTRO lo stesso file
    sequence: {
      concurrent: false,
    },
  },
});
