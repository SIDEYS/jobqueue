import { defineConfig } from 'vitest/config'

// Separate from vite.config.ts on purpose: the only thing under test right
// now is sseReducer, a plain function with no DOM or React dependency, so
// there's no need to pull the React/Tailwind plugins or jsdom into the test
// run.
export default defineConfig({
  test: {
    environment: 'node',
  },
})
