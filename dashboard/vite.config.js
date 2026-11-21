import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// En dev (npm run dev), /api est redirige vers l'API locale, comme le fait nginx en prod.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      "/api": {
        target: "http://localhost:8000",
        ws: true,
        rewrite: (path) => path.replace(/^\/api/, ""),
      },
    },
  },
});
