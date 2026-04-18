# Email Enrichment Pipeline - Implementation Plan

## Context

The existing codebase (`enrich-hosted/`) is a **Node.js + Express + Playwright** app on `localhost:3000` that scrapes ContactOut via browser automation. It already has:
- 5-slot parallel CSV/Google Sheets processing
- Full Google Sheets OAuth2 integration (read/write/mark processed)
- Frontend dashboard with progress bars, verification code flow
- Batch worker spawning via child processes

**The spec requests Python + FastAPI, but the entire codebase is Node.js.** We will build the pipeline in Node.js to avoid running two servers, duplicating Google Sheets auth, and adding deployment complexity. All patterns (file-based status polling, Google Sheets helpers, Express routes) are reused as-is.

**Goal:** Add a pre-enrichment pipeline (Prospeo API -> Bouncer verify -> Hunter API -> Bouncer verify -> "ContactOut Needed") that resolves emails via APIs before falling back to Playwright scraping. Support multiple API keys per service with automatic rotation, cooldown, and daily pause.

---

## New Files to Create

```
enrich-hosted/
  pipeline/                        <-- NEW directory
    key-pool.js                    <-- Multi-key rotation, cooldown, daily limits
    prospeo-client.js              <-- Prospeo API wrapper
    hunter-client.js               <-- Hunter.io API wrapper
    bouncer-client.js              <-- Bouncer API wrapper
    orchestrator.js                <-- Waterfall logic + batch processing
    pipeline-config.js             <-- Config loader
  pipeline-config.json             <-- API keys + settings (gitignored)
  pipeline-config.example.json     <-- Committed template with placeholders
```

## Existing Files to Modify

- `server.js` - Add ~6 new routes for pipeline (after line 441)
- `google-sheets-helper.js` - Add 2 new functions for pipeline columns
- `public/index.html` - Add pipeline toggle, key management panel, two-phase dashboard
- `public/app.js` - Add pipeline start/poll/transition logic
- `package.json` - Add `p-limit` dependency
- `.gitignore` - Add `pipeline-config.json`

---

## Step-by-Step Implementation

### Step 1: Create `pipeline/key-pool.js`

The most critical component. Manages multiple API keys per service.

**KeyPool class:**
- Constructor: `(serviceName, keys[], options: { cooldownMs, dailyLimit })`
- Internal state per key: `{ key, available, cooldownUntil, dailyCount, dailyPausedUntil, lastUsed }`
- `acquireKey()` - Returns least-used available key. If all on cooldown, waits for shortest cooldown via `setTimeout` Promise. Throws `AllKeysExhausted` if all daily-paused.
- `releaseKey(keyStr, statusCode)` - On 429: set `cooldownUntil = now + cooldownMs`. On 402: set `dailyPausedUntil = next midnight UTC`. Increment `dailyCount`.
- `getStats()` - Returns key health for dashboard display
- `resetDaily()` - Scheduled via `setTimeout` to next midnight UTC. Resets all daily counters and clears daily pauses.

Round-robin with least-loaded preference. No external dependencies.

### Step 2: Create `pipeline/bouncer-client.js`

Simplest API client - single endpoint.

- `verifyEmail(email, apiKey)` -> `POST https://api.usebouncer.com/v1/email/verify`
- Headers: `{ 'Content-Type': 'application/json', 'x-api-key': apiKey }`
- Returns: `{ status: "deliverable"|"undeliverable"|"risky"|"unknown", reason, raw }`
- "deliverable" or "risky" = valid

### Step 3: Create `pipeline/prospeo-client.js`

- `enrichSingle(linkedinUrl, apiKey)` -> `POST https://api.prospeo.io/enrich-person`
- `enrichByName(firstName, lastName, company, apiKey)` -> same endpoint, different body
- `enrichBulk(urls[], apiKey)` -> `POST https://api.prospeo.io/bulk-enrich-person` (max 50)
- Headers: `{ 'Content-Type': 'application/json', 'X-KEY': apiKey }`
- Returns `{ email, confidence }` or `null`

### Step 4: Create `pipeline/hunter-client.js`

- `findEmail(firstName, lastName, domain, apiKey)` -> `GET https://api.hunter.io/v2/email-finder?first_name=...&last_name=...&domain=...&api_key=...`
- Returns `{ email, confidence }` or `null`
- Includes `companyToDomain(companyName)` helper: lowercase, strip "Inc.", "LLC", "Ltd.", etc., remove spaces, append ".com"

### Step 5: Create `pipeline/pipeline-config.js`

Loads `pipeline-config.json`, provides defaults for missing values.

```json
{
  "prospeo": { "keys": [], "cooldownMs": 60000, "dailyLimit": 1000, "enabled": true },
  "hunter": { "keys": [], "cooldownMs": 60000, "dailyLimit": 500, "enabled": true },
  "bouncer": { "keys": [], "cooldownMs": 10000, "dailyLimit": 5000, "enabled": true },
  "pipeline": { "concurrency": 5, "maxRetries": 3, "skipIfEmailExists": true }
}
```

### Step 6: Create `pipeline/orchestrator.js`

The waterfall engine. Processes rows through: Prospeo -> Bouncer -> Hunter -> Bouncer -> "ContactOut Needed".

**`processRows(rows, options)` method:**
- Uses `p-limit` for concurrency control (default 5 parallel rows)
- For each row:
  1. Extract `linkedinUrl`, `firstName`, `lastName`, `companyName` from row columns
  2. **Prospeo**: acquire key -> call enrichSingle/enrichByName -> release key
  3. If email found: **Bouncer verify** -> if deliverable/risky -> mark "Prospeo+Bouncer", DONE
  4. **Hunter**: derive domain from company -> acquire key -> findEmail -> release key
  5. If email found: **Bouncer verify** -> if deliverable/risky -> mark "Hunter+Bouncer", DONE
  6. Mark "ContactOut Needed"
- Progress callback: `onProgress(current, total, rowResult)`
- Writes `outputs/<pipelineJobId>-pipeline.json` for status polling
- Retry logic: 3 attempts per API call with 1s/2s/4s exponential backoff
- If all keys exhausted for a service: skip that service for remaining rows, don't fail the job

**Adaptive concurrency based on list size:**
- <= 100 rows: concurrency 10 (aggressive)
- 101-2000 rows: concurrency 5
- 2000+ rows: concurrency 3 + chunk processing (500 rows/chunk)

### Step 7: Add `p-limit` dependency

```bash
cd enrich-hosted && npm install p-limit
```

Note: `p-limit` v5+ is ESM-only. Since the project uses CommonJS (`require`), we'll use `p-limit@4.0.0` which supports CJS, or use a simple inline semaphore implementation to avoid the ESM/CJS issue entirely.

### Step 8: Add pipeline routes to `server.js`

Insert after line 441 (after `/stop-batch` route):

- **`POST /start-pipeline`** - Accepts `{ sheetJobs }` (sheets mode) or multipart CSV. Reads rows from Google Sheets or CSV. Creates pipelineJobId. Runs orchestrator in-process (async, not child process - no Playwright = no crash risk). Returns `{ pipelineJobId, totalRows }`.

- **`GET /pipeline-status/:id`** - Reads `outputs/<id>-pipeline.json`. Returns progress + per-service stats.

- **`POST /pipeline-keys`** - Saves updated keys to `pipeline-config.json`. Reinitializes key pools.

- **`GET /pipeline-keys`** - Returns current key pool stats (available, cooldown, daily usage per service).

- **`POST /stop-pipeline/:id`** - Uses AbortController to cancel the running pipeline.

- **`POST /pipeline-then-contactout/:id`** - After pipeline completes, filters rows to "ContactOut Needed" only, creates a batch job via existing batch system.

**Key integration:** The pipeline runs in the main process (not a child process) because it's pure HTTP calls. It shares `sheetsClient` directly - no need to serialize Google credentials via env vars.

The pipeline does NOT use the `anyWorkerRunning` mutex since it doesn't touch Playwright. It can run concurrently with ContactOut jobs.

### Step 9: Extend `google-sheets-helper.js`

Add 2 functions following the existing `ensureProcessedColumn` pattern:

- `ensurePipelineColumns(sheets, sheetId, sheetName, headers, totalDataRows)` - Adds columns: `enrichment_source`, `enrichment_email`, `enrichment_status`
- `updatePipelineResult(sheets, sheetId, sheetName, rowNumber, headers, result)` - Writes pipeline result for a single row

### Step 10: Extend `public/index.html`

Add to the existing UI:

1. **Pipeline toggle** (checkbox under mode selector): "Enable Pre-Enrichment Pipeline (Prospeo -> Hunter -> Bouncer, then ContactOut fallback)"
2. **Pipeline settings panel** (collapsible, below upload options):
   - API key textareas (Prospeo keys, Hunter keys, Bouncer key - one per line)
   - Save Keys button
   - Concurrency slider (1-20)
   - "Skip rows with existing email" checkbox
3. **Two-phase dashboard** (replaces single dashboard when pipeline enabled):
   - Phase 1: API Enrichment - progress bar + stats (Prospeo resolved, Hunter resolved, ContactOut needed, errors)
   - Phase 2: ContactOut - existing 5-slot panel (only shown for unresolved rows)
   - "Continue to ContactOut" button between phases

### Step 11: Extend `public/app.js`

Add pipeline state management:

- `startPipeline()` - Posts to `/start-pipeline`, starts polling
- `pollPipelineStatus()` - Polls `/pipeline-status/:id` every 2s, updates progress bar + stats
- `onPipelineComplete()` - Shows summary, offers "Continue to ContactOut" for remaining rows
- `savePipelineKeys()` - Posts to `/pipeline-keys`
- `loadKeyHealth()` - Polls `/pipeline-keys` for dashboard display

### Step 12: Config files

- Create `pipeline-config.example.json` with placeholder values
- Add `pipeline-config.json` to `.gitignore`

---

## Architecture Decisions

| Decision | Choice | Why |
|----------|--------|-----|
| Language | Node.js (not Python) | Entire codebase is Node.js. Avoids 2 servers, duplicate Sheets auth, deployment complexity |
| Pipeline execution | In-process async (not child process) | Pure HTTP calls, no crash risk. Shares sheetsClient directly |
| Status updates | File-based JSON polling (not SSE/WebSocket) | Consistent with existing pattern. Works fine at 2s poll interval |
| Config format | JSON (not YAML) | Native to Node.js. No new dependency needed |
| Concurrency control | Inline semaphore or p-limit@4 | p-limit v5 is ESM-only, project uses CommonJS |
| Key storage | JSON file on disk | Simple, editable, consistent with existing credential pattern |

---

## Verification Plan

1. **Unit test KeyPool**: Simulate 429/402 responses, verify rotation and cooldown timers
2. **Test with 5 rows (dry run)**: Use real API keys, verify waterfall executes correctly
3. **Test with 50 rows**: Verify concurrency, key rotation, and Google Sheets live updates
4. **Test key exhaustion**: Use 1 key with low daily limit, verify graceful fallback to "ContactOut Needed"
5. **Test stop/resume**: Start pipeline, stop mid-run, verify processed rows kept and unprocessed rows resumable
6. **Test full flow**: Pipeline -> ContactOut handoff for remaining rows
7. **UI verification**: Start dev server, test pipeline toggle, key management, two-phase dashboard
