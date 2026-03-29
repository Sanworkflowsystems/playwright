# Parallel 5-Tab ContactOut Enrichment — Technical Documentation

## Overview

This document describes the parallel batch enrichment feature added to the ContactOut Playwright tool. The core capability: upload up to 5 CSV files and have them processed simultaneously in 5 browser tabs — all sharing one login session — with a live per-slot dashboard showing real-time progress.

---

## Problem Statement

The original tool processed one CSV at a time using a single browser tab. Each job had to wait for the previous one to finish before starting. For users with multiple lead lists, this was a significant bottleneck.

**Key constraint to work around:** ContactOut detects and kicks out sessions when the same account is logged in from multiple browser instances. This means spawning 5 separate browsers would cause repeated session invalidation.

**Solution:** Open one persistent browser context and run all 5 CSVs in 5 tabs within it. All tabs share the same authenticated session automatically — no re-login required.

---

## Architecture

### Before (Single-file flow)

```
User uploads CSV
      |
server.js  →  spawns worker.js  →  1 browser, 1 tab, 1 CSV
      |
Single anyWorkerRunning flag
```

### After (Parallel batch flow)

```
User uploads up to 5 CSVs
      |
server.js  →  spawns parallel-worker.js  →  1 browser, N tabs (1-5), N CSVs via Promise.allSettled
      |
Unified anyWorkerRunning mutex (covers both worker.js and parallel-worker.js)
```

The single-file flow (`worker.js`) is preserved untouched for backward compatibility.

---

## Files Changed

| File | Action | Purpose |
|------|--------|---------|
| `parallel-worker.js` | Created | Batch processing engine — one browser context, N pages, concurrent job execution |
| `server.js` | Modified | Added batch API endpoints and unified queue mutex |
| `public/index.html` | Rewritten | 5-slot upload UI + live batch status dashboard |
| `public/app.js` | Rewritten | Batch upload, per-slot polling, start/stop controls |
| `worker.js` | Untouched | Original single-file worker preserved for backward compat |

---

## File-by-File Technical Details

---

### `parallel-worker.js` (New File)

**Invocation:**
```
node parallel-worker.js <batchId>
```

**Environment variables:**

| Variable | Description |
|----------|-------------|
| `BATCH_JOBS` | JSON array of job spec objects |
| `JOB_SELECTORS` | JSON object of ContactOut CSS selectors |
| `JOB_COOKIES` | Optional cookie string for session injection |
| `JOB_MANUAL` | `"1"` if manual login is required, `"0"` otherwise |

**`BATCH_JOBS` shape:**
```json
[
  {
    "jobId": "uuid",
    "slotIndex": 0,
    "inputPath": "uploads/abc.csv",
    "outputPath": "outputs/uuid.csv",
    "statusPath": "outputs/uuid.json",
    "originalFilename": "leads.csv"
  }
]
```

**Execution flow:**

1. Parses `BATCH_JOBS` from env, reads all selectors and config
2. Opens `chromium.launchPersistentContext('playwright_profile/')` — one browser, shared session
3. Creates N pages with a 1-second stagger between tab opens (human-like pacing)
4. Navigates all tabs to `https://contactout.com/dashboard/search`
5. If `JOB_MANUAL=1`, polls for `outputs/<batchId>.start` signal file before proceeding
6. Verifies login on `pages[0]` via `waitForSelector(NAME_INPUT_SELECTOR, { timeout: 15000 })`
   - On failure: writes `error` status to all slot JSON files, writes aggregate batch JSON, closes browser, exits with code 1
7. Runs all `processJob()` calls concurrently with staggered starts:
   - Tab 1 starts at 0s
   - Tab 2 starts at 5s
   - Tab 3 starts at 10s
   - Tab 4 starts at 15s
   - Tab 5 starts at 20s
8. `await Promise.allSettled(jobPromises)` — waits for all to finish or error independently
9. Writes final `outputs/<batchId>.json` with aggregate status, closes browser, exits 0

**`processJob(page, spec, batchId, allJobStates)`:**

Direct port of the `worker.js` row loop, adapted to accept an existing `page` object instead of creating its own. For each row:
- Clears previous search inputs
- Fills Name + Company fields
- Clicks Submit, waits for results
- Clicks "View email" and "Find phone" buttons
- Extracts emails and phones from result cards
- Classifies emails as personal vs work by domain
- Writes the enriched row immediately to the output CSV (incremental — partial results survive a stop)
- Updates both `outputs/<jobId>.json` (per-slot) and `outputs/<batchId>.json` (aggregate) after every row

**Status file schemas:**

Per-slot (`outputs/<jobId>.json`):
```json
{ "status": "running", "progress": 12, "total": 50 }
```

Aggregate batch (`outputs/<batchId>.json`):
```json
{
  "batchId": "uuid",
  "status": "running",
  "jobs": [
    { "jobId": "uuid", "slotIndex": 0, "originalFilename": "leads.csv", "status": "running", "progress": 12, "total": 50 },
    { "jobId": "uuid", "slotIndex": 1, "originalFilename": "prospects.csv", "status": "running", "progress": 4, "total": 30 }
  ]
}
```

**SIGTERM handler:**

On receiving SIGTERM:
1. Marks any `running`/`pending`/`queued` jobs as `stopped`
2. Updates all per-slot JSON status files
3. Updates aggregate batch JSON
4. Closes browser context
5. Exits with code 0

Partial CSVs are fully downloadable at any point because rows are written incrementally after each row is processed.

**Note on tab isolation:** ContactOut's search is in-place AJAX — there is no `page.goto()` between rows. Each tab's DOM is completely independent; Tab 1's results never pollute Tab 2's DOM.

---

### `server.js` (Modified)

**New in-memory data structures:**
```javascript
const batches = {};     // batchId -> batch object
const batchQueue = [];  // queue of pending batchIds
```

**Unified mutex:**

The original `workerRunning` flag was replaced with a single `anyWorkerRunning` flag shared by both the single-file queue runner and the batch queue runner. This is necessary because both `worker.js` and `parallel-worker.js` open the same `playwright_profile/` directory — Chromium holds a file lock on it, so only one process can use it at a time.

```
anyWorkerRunning = true  →  blocks both runQueue() and runBatchQueue()
```

If a batch is submitted while a single job is running (or vice versa), it is queued and starts automatically when the running process finishes.

**New API endpoints:**

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/upload-batch` | Accepts up to 5 files via `multipart/form-data`. Creates a batch + per-slot job specs. Queues the batch. Returns `{ batchId, jobSpecs[] }`. Also registers each slot's `jobId` in the `jobs` map so `/download/:jobId` works unchanged. |
| `GET` | `/batch-status/:batchId` | Returns aggregate batch status merged with fresh per-slot data from individual `<jobId>.json` files. |
| `POST` | `/signal-start-batch/:batchId` | Writes `outputs/<batchId>.start` signal file. The worker polls for this before beginning processing. |
| `POST` | `/stop-batch/:batchId` | Sends `SIGTERM` to the running `parallel-worker.js` child process. |

**Existing endpoints preserved (unchanged):**

| Method | Path |
|--------|------|
| `POST` | `/upload` |
| `GET` | `/status/:jobId` |
| `GET` | `/download/:jobId` |
| `POST` | `/signal-start/:jobId` |
| `POST` | `/stop/:jobId` |

**`runBatchWorker(batch)` function:**

Spawns `node parallel-worker.js <batchId>` with all required env vars. Stores the child process handle on `batch.process` so stop signals can be sent. Resolves on exit code 0 or SIGTERM, rejects on any other exit code.

---

### `public/index.html` (Rewritten)

Two phases that swap visibility via `display: none / block`.

**Phase 1 — Upload Section:**
- 5 file slot inputs (Slot 1 implicitly required by app logic, Slots 2–5 labelled optional)
- Each slot shows the selected filename inline when a file is chosen
- Manual login checkbox (checked by default)
- Cookies textarea for optional session injection
- "Upload Files & Open Browser" button

**Phase 2 — Batch Status Dashboard:**
- "I've logged in — Start Processing All Tabs" button (shown only if manual login is selected; hidden once clicked)
- "Stop All Jobs" button (shown after start signal is sent; hidden when all jobs reach terminal state)
- Batch status text line
- 5 job panels in a CSS `auto-fill` grid (responsive, min 280px per panel)
  - Each panel: filename header, status text, progress bar, download link
  - Panels for empty slots stay `display: none` — only uploaded slot panels are shown

**Progress bar color coding:**

| Color | State |
|-------|-------|
| Green | running |
| Blue | finished |
| Orange | stopped |
| Red | error |

---

### `public/app.js` (Rewritten)

**State object:**
```javascript
let batchState = {
  batchId: null,
  jobSpecs: [],
  pollingInterval: null,
  isManual: false
};
```

**Upload flow:**
1. Collects all selected files from the 5 slot inputs (skips empty slots)
2. Builds `FormData` with `fd.append('files', file)` for each selected file
3. `POST /upload-batch` → receives `{ batchId, jobSpecs[] }`
4. Calls `switchToDashboard(jobSpecs, isManual)` which shows only the relevant panels and sets download links immediately

**Manual login flow:**
- "Start Processing" button calls `sendStartSignal()` → `POST /signal-start-batch/<batchId>`
- On success: hides start button, shows stop button, calls `startPolling()`

**Auto login flow (no manual checkbox):**
- `switchToDashboard()` skips the start button and calls `startPolling()` directly

**`pollBatchStatus()` — runs every 2.5 seconds:**
1. `GET /batch-status/<batchId>`
2. Iterates `data.jobs[]`, finds the panel by `slotIndex`
3. Updates bar width, bar color, and status text for each slot
4. Checks if all slots are in a terminal state (`finished`, `stopped`, `error`)
5. If all terminal: clears the interval, hides the Stop button, updates batch status text

**Stop flow:**
- "Stop All Jobs" → `POST /stop-batch/<batchId>` → SIGTERM sent to worker
- Polling detects the `stopped` status on next tick and updates panels

---

## Data Flow Diagram

```
Browser (user)
    |
    | POST /upload-batch (5 files)
    v
server.js
    |-- creates batchId, 5 jobIds
    |-- writes outputs/<batchId>.json  (initial)
    |-- writes outputs/<jobId>.json    (x5, initial)
    |-- stores in batches{} and batchQueue[]
    |-- calls runBatchQueue()
    |
    | spawns: node parallel-worker.js <batchId>
    v
parallel-worker.js
    |-- opens 1 browser, 5 tabs
    |-- (waits for <batchId>.start if manual)
    |-- verifies login on tab 1
    |-- starts 5 processJob() promises with staggered delay
    |
    |   Tab 1 (0s)    Tab 2 (5s)    Tab 3 (10s)   Tab 4 (15s)   Tab 5 (20s)
    |   row loop      row loop      row loop       row loop      row loop
    |      |              |              |               |              |
    |      v              v              v               v              v
    |   <jobId0>.csv   <jobId1>.csv  <jobId2>.csv  <jobId3>.csv  <jobId4>.csv
    |   <jobId0>.json  <jobId1>.json <jobId2>.json <jobId3>.json <jobId4>.json
    |                                <batchId>.json (aggregate, updated after every row)
    |
    v
server.js /batch-status/:batchId
    |-- reads <batchId>.json + all <jobId>.json files
    |-- merges and returns
    v
Browser (polling every 2.5s)
    |-- updates 5 progress panels in real time
```

---

## Edge Cases and How They Are Handled

| Scenario | Handling |
|----------|---------|
| Login verification fails | `pages[0].waitForSelector` times out → error status written to all slot JSON files → browser closes → UI shows red error panels |
| Some jobs finish before others | Finished panels show "Complete!" in blue; others continue updating; `allTerminal` check in polling handles mixed states gracefully |
| Stop mid-batch | SIGTERM → handler marks `running`/`pending` slots as `stopped` → partial CSVs are fully downloadable (rows are written after each row) |
| 1–4 files uploaded | Only N panels shown in UI; parallel-worker creates exactly N pages; stagger still applies; works identically |
| Single-file job + batch job submitted together | Unified `anyWorkerRunning` mutex queues them sequentially — the second waits for the first to release the `playwright_profile/` lock |
| Row-level error (network timeout, selector miss) | Caught in the per-row try/catch; error message written to `Notes` column of that row; processing continues to next row |
| Company field fill timeout | Detected by `e.name === 'TimeoutError'`; attempts to click the clear button and retry; re-throws only if clear button is not found |

---

## How to Run

```bash
cd enrich-hosted
npm install
npx playwright install
node server.js
```

Open `http://localhost:3000` in your browser.

**Test with 2 files:**
1. Select CSVs in Slot 1 and Slot 2
2. Click "Upload Files & Open Browser"
3. Browser opens with 2 tabs on contactout.com
4. Log in on either tab (session is shared)
5. Click "I've logged in — Start Processing All Tabs"
6. Watch 2 panels update in parallel — Tab 2 begins ~5s after Tab 1
7. Download links are available at any point for partial results

**Test stop:**
1. Click "Stop All Jobs" mid-run
2. Both panels turn orange ("Stopped")
3. Download partial CSVs immediately

---

## Output Files

For a batch with `batchId = B` and jobs `J1..J5`:

| File | Written by | Contents |
|------|-----------|---------|
| `outputs/B.json` | parallel-worker.js | Aggregate batch status + all slot states |
| `outputs/J1.json` — `J5.json` | parallel-worker.js | Per-slot progress (status, progress, total) |
| `outputs/J1.csv` — `J5.csv` | parallel-worker.js | Enriched CSV rows (written incrementally) |
| `outputs/B.start` | server.js (on POST /signal-start-batch) | Empty signal file; deleted by worker on receipt |
| `outputs/B-pre-flight-error.html` | parallel-worker.js (on login failure) | Full page HTML snapshot for debugging |
