import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// dev server proxies API calls to FastAPI so the browser can use same-origin paths
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/recommend": "http://localhost:8000",
      "/funnel-stats": "http://localhost:8000",
      "/top-items": "http://localhost:8000",
      "/predict-abandon": "http://localhost:8000",
      "/pipeline-health": "http://localhost:8000",
    },
  },
});
