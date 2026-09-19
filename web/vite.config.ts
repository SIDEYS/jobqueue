import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    // The API is a separate process (cmd/api, :8080) - proxying keeps the
    // dashboard on same-origin relative fetches (/api/v1/...) in dev, so
    // there's nothing CORS-specific to configure on either side, in dev or
    // once the built assets are eventually served by cmd/api itself.
    proxy: {
      '/api': 'http://localhost:8080',
      '/healthz': 'http://localhost:8080',
      '/readyz': 'http://localhost:8080',
    },
  },
})
