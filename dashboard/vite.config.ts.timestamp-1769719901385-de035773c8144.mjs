// vite.config.ts
import { defineConfig, loadEnv } from "file:///Users/emanuelepifani/Desktop/coding/nexo/dashboard/node_modules/vite/dist/node/index.js";
import react from "file:///Users/emanuelepifani/Desktop/coding/nexo/dashboard/node_modules/@vitejs/plugin-react/dist/index.js";
import path from "path";
var __vite_injected_original_dirname = "/Users/emanuelepifani/Desktop/coding/nexo/dashboard";
var vite_config_default = defineConfig(({ mode }) => {
  const env = loadEnv(mode, path.resolve(__vite_injected_original_dirname, ".."), "");
  const host = env.SERVER_HOST;
  const port = env.SERVER_DASHBOARD_PORT;
  if (!host || !port) {
    throw new Error("Missing required environment variables: SERVER_HOST, SERVER_DASHBOARD_PORT");
  }
  const target = `http://${host}:${port}`;
  console.log(`Using Proxy Target: ${target}`);
  return {
    plugins: [react()],
    resolve: {
      alias: {
        "@": path.resolve(__vite_injected_original_dirname, "./src")
      }
    },
    server: {
      proxy: {
        "/api": {
          target,
          changeOrigin: true,
          secure: false
        }
      }
    }
  };
});
export {
  vite_config_default as default
};
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsidml0ZS5jb25maWcudHMiXSwKICAic291cmNlc0NvbnRlbnQiOiBbImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvVXNlcnMvZW1hbnVlbGVwaWZhbmkvRGVza3RvcC9jb2RpbmcvbmV4by9kYXNoYm9hcmQtd2ViXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvVXNlcnMvZW1hbnVlbGVwaWZhbmkvRGVza3RvcC9jb2RpbmcvbmV4by9kYXNoYm9hcmQtd2ViL3ZpdGUuY29uZmlnLnRzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9Vc2Vycy9lbWFudWVsZXBpZmFuaS9EZXNrdG9wL2NvZGluZy9uZXhvL2Rhc2hib2FyZC13ZWIvdml0ZS5jb25maWcudHNcIjtpbXBvcnQgeyBkZWZpbmVDb25maWcsIGxvYWRFbnYgfSBmcm9tICd2aXRlJ1xuaW1wb3J0IHJlYWN0IGZyb20gJ0B2aXRlanMvcGx1Z2luLXJlYWN0J1xuaW1wb3J0IHBhdGggZnJvbSBcInBhdGhcIlxuXG4vLyBodHRwczovL3ZpdGVqcy5kZXYvY29uZmlnL1xuZXhwb3J0IGRlZmF1bHQgZGVmaW5lQ29uZmlnKCh7IG1vZGUgfSkgPT4ge1xuICBjb25zdCBlbnYgPSBsb2FkRW52KG1vZGUsIHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUsICcuLicpLCAnJylcbiAgXG4gIGNvbnN0IGhvc3QgPSBlbnYuU0VSVkVSX0hPU1RcbiAgY29uc3QgcG9ydCA9IGVudi5TRVJWRVJfREFTSEJPQVJEX1BPUlRcblxuICBpZiAoIWhvc3QgfHwgIXBvcnQpIHtcbiAgICB0aHJvdyBuZXcgRXJyb3IoXCJNaXNzaW5nIHJlcXVpcmVkIGVudmlyb25tZW50IHZhcmlhYmxlczogU0VSVkVSX0hPU1QsIFNFUlZFUl9EQVNIQk9BUkRfUE9SVFwiKVxuICB9XG5cbiAgY29uc3QgdGFyZ2V0ID0gYGh0dHA6Ly8ke2hvc3R9OiR7cG9ydH1gXG4gIGNvbnNvbGUubG9nKGBVc2luZyBQcm94eSBUYXJnZXQ6ICR7dGFyZ2V0fWApXG5cbiAgcmV0dXJuIHtcbiAgICBwbHVnaW5zOiBbcmVhY3QoKV0sXG4gICAgcmVzb2x2ZToge1xuICAgICAgYWxpYXM6IHtcbiAgICAgICAgXCJAXCI6IHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUsIFwiLi9zcmNcIiksXG4gICAgICB9LFxuICAgIH0sXG4gICAgc2VydmVyOiB7XG4gICAgICBwcm94eToge1xuICAgICAgICAnL2FwaSc6IHtcbiAgICAgICAgICB0YXJnZXQ6IHRhcmdldCxcbiAgICAgICAgICBjaGFuZ2VPcmlnaW46IHRydWUsXG4gICAgICAgICAgc2VjdXJlOiBmYWxzZSxcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cbiAgfVxufSlcblxuIl0sCiAgIm1hcHBpbmdzIjogIjtBQUF1VixTQUFTLGNBQWMsZUFBZTtBQUM3WCxPQUFPLFdBQVc7QUFDbEIsT0FBTyxVQUFVO0FBRmpCLElBQU0sbUNBQW1DO0FBS3pDLElBQU8sc0JBQVEsYUFBYSxDQUFDLEVBQUUsS0FBSyxNQUFNO0FBQ3hDLFFBQU0sTUFBTSxRQUFRLE1BQU0sS0FBSyxRQUFRLGtDQUFXLElBQUksR0FBRyxFQUFFO0FBRTNELFFBQU0sT0FBTyxJQUFJO0FBQ2pCLFFBQU0sT0FBTyxJQUFJO0FBRWpCLE1BQUksQ0FBQyxRQUFRLENBQUMsTUFBTTtBQUNsQixVQUFNLElBQUksTUFBTSw0RUFBNEU7QUFBQSxFQUM5RjtBQUVBLFFBQU0sU0FBUyxVQUFVLElBQUksSUFBSSxJQUFJO0FBQ3JDLFVBQVEsSUFBSSx1QkFBdUIsTUFBTSxFQUFFO0FBRTNDLFNBQU87QUFBQSxJQUNMLFNBQVMsQ0FBQyxNQUFNLENBQUM7QUFBQSxJQUNqQixTQUFTO0FBQUEsTUFDUCxPQUFPO0FBQUEsUUFDTCxLQUFLLEtBQUssUUFBUSxrQ0FBVyxPQUFPO0FBQUEsTUFDdEM7QUFBQSxJQUNGO0FBQUEsSUFDQSxRQUFRO0FBQUEsTUFDTixPQUFPO0FBQUEsUUFDTCxRQUFRO0FBQUEsVUFDTjtBQUFBLFVBQ0EsY0FBYztBQUFBLFVBQ2QsUUFBUTtBQUFBLFFBQ1Y7QUFBQSxNQUNGO0FBQUEsSUFDRjtBQUFBLEVBQ0Y7QUFDRixDQUFDOyIsCiAgIm5hbWVzIjogW10KfQo=
