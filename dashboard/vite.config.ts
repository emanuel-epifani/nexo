import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'
import path from "path"

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, path.resolve(__dirname, '..'), '')

  const host = env.SERVER_HOST || '127.0.0.1'
  const port = env.SERVER_DASHBOARD_HTTP_PORT || '8080'
  const target = `http://${host}:${port}`

  return {
    plugins: [react()],
    resolve: {
      alias: {
        "@": path.resolve(__dirname, "./src"),
      },
    },
    server: mode === 'development'
      ? {
          proxy: {
            '/api': {
              target,
              changeOrigin: true,
              secure: false,
            }
          }
        }
      : undefined,
  }
})

