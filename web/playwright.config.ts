import { defineConfig } from '@playwright/test';

const API = 'http://localhost:8200';
const WEB = 'http://localhost:3200';

export default defineConfig({
  testDir: 'e2e',
  timeout: 90_000,
  use: { baseURL: WEB, video: process.env.VIDEO ? 'on' : 'off' },
  webServer: [
    {
      command: 'go run ./cmd/streamguard',
      cwd: '../api',
      url: `${API}/readyz`,
      reuseExistingServer: false,
      timeout: 120_000,
      env: {
        DATABASE_URL: 'postgres://streamguard:streamguard@localhost:5435/streamguard_e2e?sslmode=disable',
        REDIS_URL: 'redis://localhost:6381/2',
        KEY_PREFIX: 'sge2e:',
        WEB_ORIGIN: WEB,
        PORT: '8200',
        RUN_SECONDS: '60',
        TX_PER_SECOND: '10',
        DAILY_RUN_CAP: '1000',
      },
    },
    {
      command: 'npm run dev',
      url: WEB,
      reuseExistingServer: false,
      timeout: 120_000,
      env: { NEXT_PUBLIC_API_URL: API },
    },
  ],
});
