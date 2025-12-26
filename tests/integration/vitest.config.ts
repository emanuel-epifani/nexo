import { defineConfig } from 'vitest/config';
import { loadEnvFile } from 'node:process';
import { resolve } from 'node:path';
import { z } from 'zod';



// 1. Load .env from the root monorepo
loadEnvFile(resolve(__dirname, '../../.env'));


// 2. Define Env Schema
const envSchema = z.object({
  NEXO_HOST: z.string().min(1, "NEXO_HOST is required"),
  NEXO_PORT: z.string().min(4, "SENTINEL_PORT è richiesta"),
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
    testTimeout: 10000,
    hookTimeout: 10000,
    include: ['src/test-features/**/*.test.ts'],
    globalSetup: ['./src/global-setup.ts'],
  },
});
