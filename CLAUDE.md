# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies and Playwright browsers
npm install
npx playwright install

# Run the server (production)
node server.js

# Run with auto-reload (development)
npm run dev
```

Server runs on `http://localhost:3000` by default (configurable via `PORT` in `.env`).

## Architecture

This is a two-process system: an Express HTTP server and a Playwright browser worker.

### Request Flow

1. User uploads a CSV via the web UI (`public/index.html` + `public/app.js`)
2. `server.js` stores the file in `uploads/`, creates a job entry in an in-memory `jobs` map, and queues the job ID
3. `server.js` spawns `worker.js` as a child process via `child_process.spawn`, passing `jobId`, `inputPath`, `outputPath` as args and selectors/cookies via environment variables (`JOB_SELECTORS`, `JOB_COOKIES`, `JOB_MANUAL`)
4. `worker.js` launches a persistent Chromium browser (profile stored in `playwright_profile/`) and processes each CSV row by performing UI actions on contactout.com
5. Progress is communicated via a JSON status file at `outputs/<jobId>.json`; manual-login sync is done via a signal file at `outputs/<jobId>.start`
6. Enriched CSV rows are written incrementally to `outputs/<jobId>.csv`

### Key Design Decisions

- **Single worker at a time**: `workerRunning` flag in `server.js` ensures only one worker subprocess runs at a time
- **Persistent browser profile**: `playwright_profile/` stores login sessions between runs to avoid repeated logins
- **Manual login flow**: When `JOB_MANUAL=1`, `worker.js` polls for `outputs/<jobId>.start` file (written by `POST /signal-start/:jobId`) before processing begins
- **Incremental CSV writing**: Each row is written immediately after processing via `csvWriter.writeRecords([row])`, so partial results are available even if the job stops mid-way
- **Hardcoded selectors**: `HARDCODED_SELECTORS` in `server.js:16-27` are pre-configured for contactout.com; these are passed to the worker and not configurable via the UI

### CSV Column Mapping (0-indexed)

| Index | Column |
|-------|--------|
| 1     | Full Name (`FULL_NAME_COLUMN_INDEX`) |
| 4     | Company Name (`COMPANY_NAME_COLUMN_INDEX`) |

The output CSV preserves all original columns and overwrites these specific fields if they exist in the input: `Personal Email`, `Other Personal Emails`, `Work Email`, `Other Work Emails`, `Work Email Status`, `Phone Number`, `Other Phone Numbers`, `Notes`.

### Debugging

- Set `IS_HEADLESS = false` in `worker.js:23` (already the default) to watch the browser
- Pre-flight failure saves a page snapshot to `outputs/<jobId>-pre-flight-error.html`
- Row-level errors are caught and written to the `Notes` column of that row
- For screenshot debugging inside catch blocks: `await page.screenshot({ path: 'debug.png', fullPage: true })`
