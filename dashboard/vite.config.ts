import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'
import path from "path"

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, path.resolve(__dirname, '..'), '')
  
  const host = env.SERVER_HOST
  const port = env.SERVER_DASHBOARD_PORT

  if (!host || !port) {
    throw new Error("Missing required environment variables: SERVER_HOST, SERVER_DASHBOARD_PORT")
  }

  const target = `http://${host}:${port}`
  console.log(`Using Proxy Target: ${target}`)

  return {
    plugins: [react()],
    resolve: {
      alias: {
        "@": path.resolve(__dirname, "./src"),
      },
    },
    server: {
      proxy: {
        '/api': {
          target: target,
          changeOrigin: true,
          secure: false,
        }
      }
    }
  }
})

