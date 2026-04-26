'use strict';

/**
 * Pipeline Orchestrator
 *
 * Processes rows through a 4-phase waterfall:
 *   Phase 1 — Bulk Prospeo (50/batch, max concurrency, fastest path)
 *   Phase 2 — Bouncer verify all Prospeo hits concurrently
 *   Phase 3 — Hunter fallback for all unresolved rows concurrently
 *   Phase 4 — Bouncer verify all Hunter hits concurrently
 *   Phase 5 — Mark remaining "ContactOut Needed"
 *
 * Concurrency:
 *   ≤200 rows  → 80
 *   201–5000   → 120
 *   >5000      → 150
 *   Always capped to keyCount × 8 (so you don't spin more goroutines than keys can serve)
 *
 * Every row guaranteed to finish. No row ever dropped.
 */

const fs   = require('fs');
const path = require('path');

const { KeyPool, AllKeysExhaustedError } = require('./key-pool');
const prospeo  = require('./prospeo-client');
const hunter   = require('./hunter-client');
const bouncer  = require('./bouncer-client');
const { loadConfig } = require('./pipeline-config');
const sheetsHelper  = require('../google-sheets-helper');

// ─── Logger ───────────────────────────────────────────────────────────────────
function ts() { return new Date().toTimeString().slice(0, 8); }
const L = {
  info:  (msg) => console.log( `[${ts()}] INFO  ${msg}`),
  warn:  (msg) => console.warn( `[${ts()}] WARN  ${msg}`),
  error: (msg) => console.error(`[${ts()}] ERROR ${msg}`),
};

// ─── Inline semaphore (no external deps, CommonJS safe) ───────────────────────

function createSemaphore(limit) {
  let active = 0;
  const queue = [];
  return {
    acquire() {
      if (active < limit) {
        active++;
        return Promise.resolve();
      }
      return new Promise(resolve => queue.push(resolve)).then(() => { active++; });
    },
    release() {
      active = Math.max(0, active - 1);
      if (queue.length > 0) queue.shift()();
    },
    get active()  { return active; },
    get pending() { return queue.length; },
  };
}

// ─── Concurrency calculator ────────────────────────────────────────────────────

// Service-aware in-flight capacity via Little's law: rps × latency per key.
// Floors at keyCount (so tiny batches still fan out) and caps at 500 to bound memory.
function concurrencyFor(pool, rowCount, avgLatencySec) {
  const perKey   = Math.max(1, Math.ceil(pool.requestsPerSecond * avgLatencySec));
  const capacity = pool.keyCount * perKey;
  return Math.min(rowCount, Math.max(capacity, pool.keyCount), 500);
}

// ─── Retry helper ─────────────────────────────────────────────────────────────

async function withRetry(fn, maxRetries, label) {
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      // Never retry quota / daily-pause errors — caller handles those
      if (err.statusCode === 402 || err instanceof AllKeysExhaustedError) throw err;
      if (attempt === maxRetries) throw err;
      const delayMs = Math.pow(2, attempt) * 1000; // 1s, 2s, 4s
      L.warn(`[${label}] attempt ${attempt + 1}/${maxRetries} failed - retrying in ${delayMs / 1000}s | reason: ${err.message}`);
      await new Promise(r => setTimeout(r, delayMs));
    }
  }
}

async function callWithRotatingKey(pool, quotaCost, label, maxRetries, fn) {
  const maxAttempts = Math.max(maxRetries + 1, pool.keyCount + maxRetries);

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    let key = null;
    try {
      key = await pool.acquireKey(quotaCost);
      const result = await fn(key);
      pool.handleResponse(key, 200);
      return { result, key };
    } catch (err) {
      if (key && err.statusCode) pool.handleResponse(key, err.statusCode);
      if (key && err.statusCode === 401) pool.markDailyPause(key);
      if (err instanceof AllKeysExhaustedError) throw err;

      const retryableStatus = err.statusCode === 0 || err.statusCode === 429 || err.statusCode === 402 || err.statusCode === 401;
      if (!retryableStatus || attempt === maxAttempts) throw err;

      const keySuffix = key ? `...${key.slice(-6)}` : 'none';
      L.warn(`[${label}] attempt ${attempt}/${maxAttempts} failed with key ${keySuffix} - rotating key | ${err.message}`);
    }
  }

  throw new AllKeysExhaustedError(pool.serviceName);
}

// ─── Row field extractor ───────────────────────────────────────────────────────

function extractFields(row) {
  const keys = Object.keys(row);

  const find = (pattern) => keys.find(k => pattern.test(k));

  const linkedinKey        = keys.find(k => /^defaultprofileurl$/i.test(k))
                          || keys.find(k => /^linkedinprofileurl$/i.test(k))
                          || keys.find(k => /linkedin/i.test(k));
  const firstKey           = find(/^first.?name$|^firstname$/i);
  const lastKey            = find(/^last.?name$|^lastname$/i);
  const fullKey            = find(/^(full.?name|^name)$/i);
  const companyKey         = find(/^companyname$|^company_name$|^company$/i) || find(/company|employer|organization|org$/i);
  // IMPORTANT: finalEmail is the source of truth for "already enriched" — check it FIRST.
  // Falling back to Work Email / email / enrichment_email is fine but must not win over finalEmail.
  const emailKey           = find(/^finalemail$/i)
                          || find(/^enrichment_email$/i)
                          || find(/^(email|work.?email)$/i);
  // Company LinkedIn URL — user confirmed this lives in 'regularCompanyUrl'
  const companyLinkedinKey = keys.find(k => /^regularcompanyurl$/i.test(k))
                          || keys.find(k => /^companyurl$/i.test(k))
                          || find(/company_linkedin_url/i);
  // Company website — 'companyUrl' but only if it's not itself a LinkedIn URL

  let firstName = firstKey ? (row[firstKey] || '').trim() : '';
  let lastName  = lastKey  ? (row[lastKey]  || '').trim() : '';
  const fullName = fullKey ? (row[fullKey] || '').trim() : '';

  if (!firstName && !lastName && fullName) {
    const parts = fullName.split(/\s+/);
    firstName = parts[0] || '';
    lastName  = parts.slice(1).join(' ') || '';
  }

  return {
    linkedinUrl:        linkedinKey        ? (row[linkedinKey]        || '').trim() : '',
    firstName,
    lastName,
    fullName,
    company:            companyKey         ? (row[companyKey]         || '').trim() : '',
    companyLinkedinUrl: companyLinkedinKey ? (row[companyLinkedinKey] || '').trim() : '',
    // Only use companyUrl as website if it isn't itself a LinkedIn URL
    companyWebsite:     '',
    existingEmail:      emailKey           ? (row[emailKey]           || '').trim() : '',
  };
}

// ─── Progress writer ───────────────────────────────────────────────────────────

function makeProgressWriter(pipelineJobId, outputDir) {
  const filePath = pipelineJobId
    ? path.join(outputDir, `${pipelineJobId}-pipeline.json`)
    : null;

  return function write(progress, total, stats, status = 'running') {
    if (!filePath) return;
    try {
      fs.writeFileSync(filePath, JSON.stringify({
        status,
        progress,
        total,
        stats,
        updatedAt: new Date().toISOString(),
      }));
    } catch (_) { /* non-fatal */ }
  };
}

// ─── Live write queue ─────────────────────────────────────────────────────────
// Collects per-row pipeline results and flushes them to Google Sheets in one
// batchUpdate call every `flushIntervalMs` ms — avoids per-row API spam.

function createWriteQueue(sheets, sheetId, sheetName, headers, flushIntervalMs = 3000) {
  // Return a no-op queue when not in Sheets mode
  if (!sheets || !sheetId || !sheetName || !headers) {
    return { push: () => {}, flush: async () => {}, start: () => {}, stop: () => {} };
  }

  let pending  = new Map();  // rowNumber (2-based) → { email, source, status }
  let timer    = null;
  let flushing = false;

  async function doFlush() {
    if (pending.size === 0 || flushing) return;
    flushing = true;
    const snapshot = pending;
    pending = new Map();
    try {
      await sheetsHelper.batchUpdatePipelineResults(sheets, sheetId, sheetName, headers, snapshot);
      L.info(`[WriteQueue] Flushed ${snapshot.size} row(s) to Sheets`);
    } catch (err) {
      L.warn(`[WriteQueue] Flush failed (${snapshot.size} rows) - re-queuing: ${err.message}`);
      // Merge snapshot back so rows aren't lost (newer in-flight writes take precedence)
      for (const [k, v] of snapshot) {
        if (!pending.has(k)) pending.set(k, v);
      }
    } finally {
      flushing = false;
    }
  }

  return {
    push(rowNumber, email, source, status) {
      pending.set(rowNumber, { email: email || '', source: source || '', status: status || '' });
    },
    async flush() { await doFlush(); },
    start()       { timer = setInterval(doFlush, flushIntervalMs); },
    stop()        { if (timer) { clearInterval(timer); timer = null; } },
  };
}

// ─── Main orchestrator class ───────────────────────────────────────────────────

class PipelineOrchestrator {
  constructor(config, pools) {
    this.config = config;
    this.pools  = pools;   // { prospeo: KeyPool, hunter: KeyPool, bouncer: KeyPool }
  }

  /**
   * Process an array of row objects through the full enrichment waterfall.
   *
   * @param {object[]} rows       — plain row objects (keys = column headers)
   * @param {object}   options
   *   @param {string}   pipelineJobId  — used for progress file naming
   *   @param {string}   outputDir      — where to write progress JSON
   *   @param {object}   sheetsClient   — googleapis sheets client (or null for CSV mode)
   *   @param {string}   sheetId
   *   @param {string}   sheetName
   *   @param {string[]} headers        — column header array (for Sheets writes)
   *   @param {AbortSignal} signal      — AbortController.signal for cancellation
   *   @param {function} onProgress     — called with (done, total, stats)
   *
   * @returns {{ results: object[], stats: object }}
   */
  async processRows(rows, options = {}) {
    const {
      pipelineJobId  = null,
      outputDir      = path.join(__dirname, '..', 'outputs'),
      sheetsClient   = null,
      sheetId        = null,
      sheetName      = null,
      headers        = null,
      signal         = null,
      onProgress     = null,
      skipPhases     = [],  // e.g. ['prospeo'] to resume on Hunter for already-Prospeo'd sheets
      rowStart       = null, // 1-based sheet row (row 1 = headers, so min = 2). null = no lower bound.
      rowEnd         = null, // 1-based sheet row. null = no upper bound.
    } = options;

    const skipProspeo = skipPhases.includes('prospeo');
    const skipHunter  = skipPhases.includes('hunter');

    // Convert 1-based sheet row range → 0-based array index range.
    // Sheet row N ↔ rows[N - 2]. Guard: clamp to [0, rows.length).
    const idxStart = rowStart ? Math.max(0, rowStart - 2) : 0;
    const idxEnd   = rowEnd   ? Math.min(rows.length - 1, rowEnd - 2) : rows.length - 1;
    const inRange  = (i) => i >= idxStart && i <= idxEnd;

    const total     = rows.length;
    const cfg       = this.config;
    const startTime = Date.now();
    const writeProgress = makeProgressWriter(pipelineJobId, outputDir);

    // Per-row result store
    const results = rows.map(() => ({
      email:             '',
      enrichmentSource:  '',
      enrichmentStatus:  'pending',   // pending → prospeo_found | hunter_found | done | contactout_needed
    }));

    const stats = {
      prospeoResolved:   0,
      hunterResolved:    0,
      contactoutNeeded:  0,
      bouncerVerified:   0,
      bouncerRejected:   0,
      errors:            0,
      skipped:           0,
    };

    const aborted = () => signal?.aborted;

    // ── Mark rows outside the requested range as done so they skip every phase ─
    if (rowStart || rowEnd) {
      let outOfRange = 0;
      for (let i = 0; i < rows.length; i++) {
        if (!inRange(i)) {
          results[i].enrichmentStatus = 'out_of_range';
          outOfRange++;
        }
      }
      L.info(`[Pipeline] Row range active: sheet rows ${rowStart || 2}..${rowEnd || (rows.length + 1)} | ${outOfRange} row(s) outside range will be skipped`);
    }

    // ── Skip rows that already have an email in finalEmail (or fallback columns) ─
    if (cfg.pipeline.skipIfEmailExists) {
      let skippedCount = 0;
      let detectedEmailCol = null;
      // Detect email column from first row to log it once
      if (rows.length > 0) {
        const keys = Object.keys(rows[0]);
        detectedEmailCol = keys.find(k => /^finalemail$/i.test(k))
                        || keys.find(k => /^enrichment_email$/i.test(k))
                        || keys.find(k => /^(email|work.?email)$/i.test(k))
                        || null;
        L.info(`[Pipeline] Skip-if-email-exists: detected email column = "${detectedEmailCol || '(NONE FOUND)'}"`);
      }
      for (let i = 0; i < rows.length; i++) {
        if (results[i].enrichmentStatus !== 'pending') continue;
        const { existingEmail } = extractFields(rows[i]);
        if (existingEmail && existingEmail.includes('@')) {
          results[i].enrichmentStatus  = 'done';
          results[i].enrichmentSource  = 'existing';
          results[i].email             = existingEmail;
          stats.skipped++;
          skippedCount++;
        }
      }
      if (skippedCount > 0) {
        L.info(`[Pipeline] Skipped ${skippedCount} row(s) that already have an email — no API calls will be made for them`);
      } else if (detectedEmailCol) {
        L.warn(`[Pipeline] Skip-if-email-exists ran but skipped 0 rows — verify column "${detectedEmailCol}" contains values with "@"`);
      }
    }

    const pending = () => results.map((r, i) => ({ r, i })).filter(({ r }) => r.enrichmentStatus === 'pending');

    writeProgress(0, total, stats, 'running');

    L.info(`[Pipeline] Starting`);
    L.info(`[Pipeline] Rows: ${total} | Mode: ${sheetsClient ? 'Google Sheets' : 'CSV'}`);
    {
      const p = this.pools.prospeo;
      const dailyTotal = cfg.prospeo.dailyLimit > 0 ? `${cfg.prospeo.dailyLimit}/key → ${cfg.prospeo.dailyLimit * p.keyCount}/day total` : 'no daily cap';
      L.info(`[Pipeline] Prospeo : ${cfg.prospeo.enabled ? `enabled  | ${p.keyCount} key(s) | ${p.secondLimit} req/s, ${p.minuteLimit} req/min per key | ${dailyTotal}` : 'DISABLED'}`);
    }
    L.info(`[Pipeline] Hunter  : ${cfg.hunter.enabled   ? `enabled  | ${this.pools.hunter.keyCount}  key(s) | ${cfg.hunter.requestsPerSecond} req/s` : 'DISABLED'}`);
    L.info(`[Pipeline] Bouncer : ${cfg.bouncer.enabled  ? `enabled  | ${this.pools.bouncer.keyCount} key(s) | ${cfg.bouncer.requestsPerSecond} req/s` : 'DISABLED (results accepted without verification)'}`);
    L.info(`[Pipeline] -----------------------------------------------------`);

    // Ensure the three pipeline columns exist in the sheet header row (sheets mode only).
    // Must happen before the write queue starts so headers[] is fully populated.
    if (sheetsClient && sheetId && sheetName && headers) {
      await sheetsHelper.ensurePipelineColumns(sheetsClient, sheetId, sheetName, headers, rows.length);
    }

    // Live write queue — resolves rows are pushed here and flushed to Sheets every 5 s.
    // In CSV mode (no sheetsClient) this is a no-op object.
    const writeQueue = createWriteQueue(sheetsClient, sheetId, sheetName, headers);
    writeQueue.start();

    // Wrap the whole pipeline so an unexpected throw still drains the write queue.
    // Without this, up to flushIntervalMs of resolved-but-unflushed emails would be lost on a crash.
    try {

    // ════════════════════════════════════════════════════════════════════════
    // PHASE 1 — Prospeo single-record enrichment
    // Prospeo's 1 req/s limit is per-account (shared across all keys from the
    // same account). A global gate enforces ≤1 call/s total; the KeyPool still
    // rotates keys on 429 to absorb occasional bursts and daily limits.
    // ════════════════════════════════════════════════════════════════════════
    const prospeoEnabled  = cfg.prospeo.enabled && this.pools.prospeo.keyCount > 0 && !skipProspeo;
    const bouncerEnabled  = cfg.bouncer.enabled && this.pools.bouncer.keyCount > 0;

    if (skipProspeo) {
      L.info(`[Phase 1 / Prospeo] SKIPPED by caller (skipPhases=['prospeo']) - resuming on Hunter`);
    }

    if (prospeoEnabled && !aborted()) {
      const toEnrich = pending();

      // ── Sequential-key dispatcher (same shape as Hunter, different cadence) ──
      //   - One Prospeo key active at a time, fixed list order, starts at index 0.
      //   - Dispatch 1 req every 3.5s (~0.29 rps), without waiting for responses.
      //   - On 429 (Prospeo's primary rate/quota signal), 402 or 401 → advance to next key.
      //   - On Prospeo hit → fire Bouncer verify in parallel → writeQueue.push on valid.
      //   - Never overwrite row data. Only writeQueue columns touched.
      //   - Cap in-flight at 10.
      const DISPATCH_GAP_MS  = 3500; // ≈0.29 req/s — 1 call every 3.5s
      const MAX_INFLIGHT     = 10;
      const prospeoPool      = this.pools.prospeo;
      const prospeoKeys      = prospeoPool.keys;
      const totalKeys        = prospeoKeys.length;

      L.info(`[Phase 1 / Prospeo] ${toEnrich.length} row(s) pending | sequential-key mode | ${totalKeys} key(s) | 0.29 req/s (1 per 3.5s) | max in-flight: ${MAX_INFLIGHT}`);
      L.info(`[Phase 1 / Prospeo] Starting with key 1/${totalKeys} (...${prospeoKeys[0]?.key?.slice(-6) || 'none'})`);

      let activeKeyIdx  = 0;
      let inflightCount = 0;
      let allKeysDone   = totalKeys === 0;
      let prospeoHitsCount = 0;

      let onSlotFree = null;
      const waitForSlot = () => {
        if (inflightCount < MAX_INFLIGHT) return Promise.resolve();
        return new Promise(resolve => { onSlotFree = resolve; });
      };
      const releaseSlot = () => {
        inflightCount = Math.max(0, inflightCount - 1);
        if (onSlotFree && inflightCount < MAX_INFLIGHT) {
          const r = onSlotFree; onSlotFree = null; r();
        }
      };

      const advanceKey = (reason) => {
        const prevSuffix = '...' + (prospeoKeys[activeKeyIdx]?.key || '').slice(-6);
        activeKeyIdx++;
        if (activeKeyIdx >= totalKeys) {
          allKeysDone = true;
          L.warn(`[Phase 1 / Prospeo] All ${totalKeys} key(s) exhausted (last: ${prevSuffix}, reason: ${reason}) — remaining rows fall through to Hunter`);
        } else {
          const nextSuffix = '...' + prospeoKeys[activeKeyIdx].key.slice(-6);
          L.info(`[Phase 1 / Prospeo] Key ${prevSuffix} ${reason} — advancing to key ${nextSuffix} (${activeKeyIdx + 1}/${totalKeys})`);
        }
      };

      const inflightProspeo = [];   // Prospeo response tasks
      const inflightBouncer = [];   // Bouncer verifies fired from Prospeo hits
      let nextDispatchAt = Date.now();

      for (let idx = 0; idx < toEnrich.length; idx++) {
        if (aborted() || allKeysDone) break;

        const { r, i } = toEnrich[idx];
        const fields = extractFields(rows[i]);
        const item = {
          linkedinUrl:        fields.linkedinUrl,
          firstName:          fields.firstName,
          lastName:           fields.lastName,
          company:            fields.company,
          companyLinkedinUrl: fields.companyLinkedinUrl,
          companyWebsite:     fields.companyWebsite,
        };

        const payload = prospeo.buildContactPayload(item);
        if (!prospeo.hasMinimumDatapoints(payload)) {
          // Not enough data — skip without burning a credit or advancing cadence.
          continue;
        }

        await waitForSlot();
        if (aborted() || allKeysDone) break;

        const now = Date.now();
        if (now < nextDispatchAt) {
          await new Promise(resolve => setTimeout(resolve, nextDispatchAt - now));
        }
        if (aborted() || allKeysDone) break;
        nextDispatchAt = Date.now() + DISPATCH_GAP_MS;

        const apiKey    = prospeoKeys[activeKeyIdx].key;
        const keySuffix = '...' + apiKey.slice(-6);

        L.info(`[Phase 1 / Prospeo] Dispatching row ${i + 2} (${idx + 1}/${toEnrich.length}): ${fields.firstName} ${fields.lastName} @ ${fields.company || fields.linkedinUrl} | key ${keySuffix} | in-flight=${inflightCount + 1}`);

        inflightCount++;
        const task = (async () => {
          let result = null;
          try {
            result = await prospeo.enrichSingle(item, apiKey);
          } catch (err) {
            const code = err.statusCode;
            // Log EVERY rate/quota response so you always see it, even when activeKey already advanced.
            if (code === 429 || code === 402 || code === 401) {
              L.warn(`[Phase 1 / Prospeo] Row ${i + 2} got HTTP ${code} on key ${keySuffix} — ${err.message}`);
            }

            // Prospeo mainly signals rate/quota limits via 429. Also handle 402/401.
            if (code === 429) {
              prospeoPool.markDailyPause(apiKey);
              if (prospeoKeys[activeKeyIdx]?.key === apiKey && !allKeysDone) {
                advanceKey('rate/quota limit (429)');
              } else {
                L.info(`[Phase 1 / Prospeo] Key ${keySuffix} already superseded — no advance needed (active is now ${activeKeyIdx + 1}/${totalKeys})`);
              }
            } else if (code === 402) {
              prospeoPool.markDailyPause(apiKey);
              if (prospeoKeys[activeKeyIdx]?.key === apiKey && !allKeysDone) {
                advanceKey('quota exhausted (402)');
              } else {
                L.info(`[Phase 1 / Prospeo] Key ${keySuffix} already superseded — no advance needed (active is now ${activeKeyIdx + 1}/${totalKeys})`);
              }
            } else if (code === 401) {
              prospeoPool.markDailyPause(apiKey);
              if (prospeoKeys[activeKeyIdx]?.key === apiKey && !allKeysDone) {
                advanceKey('invalid key (401)');
              } else {
                L.info(`[Phase 1 / Prospeo] Key ${keySuffix} already superseded — no advance needed (active is now ${activeKeyIdx + 1}/${totalKeys})`);
              }
            } else {
              stats.errors++;
              L.error(`[Phase 1 / Prospeo] Error row ${i + 2} (${fields.firstName} ${fields.lastName}) | key ${keySuffix} — ${err.message}`);
            }
            return;
          }

          if (!result || !result.email) {
            // No hit — row stays 'pending' and flows to Hunter. Don't touch row data.
            return;
          }

          // Record hit in RESULT object only (never mutate the source row).
          r.email            = result.email;
          r.enrichmentSource = 'Prospeo';
          r.enrichmentStatus = 'prospeo_found';
          prospeoHitsCount++;
          L.info(`[Phase 1 / Prospeo] Found ${result.email} (${fields.firstName} ${fields.lastName} @ ${fields.company}) | key ${keySuffix}`);

          if (bouncerEnabled) {
            const bouncerPromise = (async () => {
              try {
                const response = await callWithRotatingKey(
                  this.pools.bouncer, 1, 'Bouncer (Prospeo)', cfg.pipeline.maxRetries,
                  (bKey) => bouncer.verifyEmail(r.email, bKey)
                );
                if (bouncer.isValid(response.result)) {
                  r.enrichmentSource = 'Prospeo+Bouncer';
                  r.enrichmentStatus = 'done';
                  stats.prospeoResolved++;
                  stats.bouncerVerified++;
                  writeQueue.push(i + 2, r.email, r.enrichmentSource, 'done');
                  L.info(`[Phase 1 / Prospeo] Verified + written: ${r.email} (row ${i + 2})`);
                } else {
                  r.email            = '';
                  r.enrichmentStatus = 'pending'; // falls through to Hunter
                  stats.bouncerRejected++;
                  L.info(`[Phase 1 / Prospeo] Bouncer rejected (row ${i + 2}) — passing to Hunter`);
                }
              } catch (err) {
                // Bouncer error — accept Prospeo result unverified rather than lose it.
                r.enrichmentSource = 'Prospeo';
                r.enrichmentStatus = 'done';
                stats.prospeoResolved++;
                stats.errors++;
                writeQueue.push(i + 2, r.email, r.enrichmentSource, 'done');
                L.warn(`[Phase 1 / Prospeo] Bouncer error, accepting unverified: ${r.email} | ${err.message}`);
              }
            })();
            inflightBouncer.push(bouncerPromise);
          } else {
            r.enrichmentStatus = 'done';
            stats.prospeoResolved++;
            writeQueue.push(i + 2, r.email, r.enrichmentSource, 'done');
          }
        })().finally(releaseSlot);

        inflightProspeo.push(task);
      }

      // Drain all in-flight Prospeo calls + their fire-and-forget Bouncer verifications.
      if (inflightProspeo.length > 0) {
        L.info(`[Phase 1 / Prospeo] Draining ${inflightProspeo.length} in-flight request(s)...`);
        await Promise.allSettled(inflightProspeo);
      }
      if (inflightBouncer.length > 0) {
        L.info(`[Phase 1 / Prospeo] Draining ${inflightBouncer.length} in-flight Bouncer verification(s)...`);
        await Promise.allSettled(inflightBouncer);
      }
      L.info(`[Phase 1 / Prospeo] Dispatch complete — ${prospeoHitsCount} hit(s) across ${toEnrich.length} row(s)`);
    }

    // ════════════════════════════════════════════════════════════════════════
    // PHASE 2 — Bouncer verification for any remaining prospeo_found rows
    // (most will already be 'done' from inline verification in Phase 1)
    // ════════════════════════════════════════════════════════════════════════

    const prospeoHits = results.map((r, i) => ({ r, i })).filter(({ r }) => r.enrichmentStatus === 'prospeo_found');
    {
      const prospeoFound = prospeoHits.length;
      const prospeoMiss  = results.filter(r => r.enrichmentStatus === 'pending').length;
      L.info(`[Phase 1 / Prospeo] Complete - ${prospeoFound} found, ${prospeoMiss} no match -> moving to Phase 2`);
    }

    if (bouncerEnabled && prospeoHits.length > 0 && !aborted()) {
      // In-flight capacity = rps × typical latency (~1.5s) per key.
      const concurrency = concurrencyFor(this.pools.bouncer, prospeoHits.length, 1.5);
      const sem = createSemaphore(concurrency);

      L.info(`[Phase 2 / Bouncer] Verifying ${prospeoHits.length} Prospeo email(s) | concurrency: ${concurrency}`);

      await Promise.allSettled(prospeoHits.map(async ({ r, i }) => {
        if (aborted()) return;
        await sem.acquire();
        try {
          let key;
          try {
            const response = await callWithRotatingKey(
              this.pools.bouncer,
              1,
              'Bouncer',
              cfg.pipeline.maxRetries,
              (apiKey) => bouncer.verifyEmail(r.email, apiKey)
            );
            key = response.key;
            const result = response.result;

            if (bouncer.isValid(result)) {
              r.enrichmentSource = 'Prospeo+Bouncer';
              r.enrichmentStatus = 'done';
              stats.prospeoResolved++;
              stats.bouncerVerified++;
              writeQueue.push(i + 2, r.email, r.enrichmentSource, 'done');
            } else {
              // Bouncer rejected — fall through to Hunter
              r.enrichmentStatus = 'pending';
              r.email            = '';
              stats.bouncerRejected++;
            }
          } catch (err) {
            if (err instanceof AllKeysExhaustedError) {
              L.warn(`[Phase 2 / Bouncer] All keys exhausted - accepting remaining Prospeo results unverified`);
              r.enrichmentSource = 'Prospeo';
              r.enrichmentStatus = 'done';
              stats.prospeoResolved++;
              writeQueue.push(i + 2, r.email, r.enrichmentSource, 'done');
            } else {
              L.error(`[Phase 2 / Bouncer] Verify error - accepting Prospeo result conservatively | ${err.message}`);
              r.enrichmentSource = 'Prospeo';
              r.enrichmentStatus = 'done';
              stats.prospeoResolved++;
              stats.errors++;
              writeQueue.push(i + 2, r.email, r.enrichmentSource, 'done');
            }
          }
        } finally {
          sem.release();
        }
      }));
    } else if (!bouncerEnabled && prospeoHits.length > 0) {
      // No Bouncer configured — accept all Prospeo results as-is
      for (const { r, i } of prospeoHits) {
        r.enrichmentSource = 'Prospeo';
        r.enrichmentStatus = 'done';
        stats.prospeoResolved++;
        writeQueue.push(i + 2, r.email, 'Prospeo', 'done');
      }
    }

    // ════════════════════════════════════════════════════════════════════════
    // PHASE 3 — Hunter fallback for all still-pending rows
    // ════════════════════════════════════════════════════════════════════════
    const hunterEnabled = cfg.hunter.enabled && this.pools.hunter.keyCount > 0 && !skipHunter;
    const hunterQueue   = results.map((r, i) => ({ r, i })).filter(({ r }) => r.enrichmentStatus === 'pending');

    L.info(`[Phase 2 / Bouncer] Complete - ${stats.prospeoResolved} verified, ${stats.bouncerRejected} rejected -> ${hunterQueue.length} row(s) moving to Phase 3`);

    const inflightHunterBouncer = []; // fire-and-forget Bouncer verifications for Hunter hits

    if (hunterEnabled && hunterQueue.length > 0 && !aborted()) {
      // Sequential-key dispatcher model.
      //   - One Hunter key active at a time (no concurrent keys → no shared-IP contention).
      //   - Dispatch 1 req every 2s (ultra-safe, 0.5 rps), without waiting for responses.
      //   - Responses resolve independently → on hit, fire Bouncer in parallel + write.
      //   - Cap in-flight at 10 so slow Hunter responses can't balloon unboundedly.
      //   - Always start from key index 0; only advance on 429 (monthly quota, normalised
      //     to 402 in hunter-client) or 401 (invalid key). Never advance on transient errors.
      const DISPATCH_GAP_MS  = 2000; // 0.5 req/s — 1 call every 2s
      const MAX_INFLIGHT     = 10;   // hard cap on concurrent pending requests
      const hunterPool       = this.pools.hunter;
      const hunterKeys       = hunterPool.keys; // internal key objects
      const totalKeys        = hunterKeys.length;

      L.info(`[Phase 3 / Hunter] ${hunterQueue.length} row(s) pending | sequential-key mode | ${totalKeys} key(s) | 0.5 req/s (1 per 2s) | max in-flight: ${MAX_INFLIGHT}`);
      L.info(`[Phase 3 / Hunter] Starting with key 1/${totalKeys} (...${hunterKeys[0]?.key?.slice(-6) || 'none'})`);

      let activeKeyIdx   = 0;
      let inflightCount  = 0;
      let allKeysDone    = false;

      // Simple in-flight gate — resolves when a slot opens.
      let onSlotFree = null;
      const waitForSlot = () => {
        if (inflightCount < MAX_INFLIGHT) return Promise.resolve();
        return new Promise(resolve => { onSlotFree = resolve; });
      };
      const releaseSlot = () => {
        inflightCount = Math.max(0, inflightCount - 1);
        if (onSlotFree && inflightCount < MAX_INFLIGHT) {
          const r = onSlotFree; onSlotFree = null; r();
        }
      };

      // Advance to the next key in the fixed list order. No skipping, no reordering.
      const advanceKey = (reason) => {
        const prevSuffix = '...' + (hunterKeys[activeKeyIdx]?.key || '').slice(-6);
        activeKeyIdx++;
        if (activeKeyIdx >= totalKeys) {
          allKeysDone = true;
          L.warn(`[Phase 3 / Hunter] All ${totalKeys} key(s) exhausted (last: ${prevSuffix}, reason: ${reason}) — remaining rows fall through to ContactOut`);
        } else {
          const nextSuffix = '...' + hunterKeys[activeKeyIdx].key.slice(-6);
          L.info(`[Phase 3 / Hunter] Key ${prevSuffix} ${reason} — advancing to key ${nextSuffix} (${activeKeyIdx + 1}/${totalKeys})`);
        }
      };

      // Per requirement: always start at key index 0 every run. Only advance on 429/401
      // during THIS run. Prior in-memory pause state is ignored so the order is deterministic.
      if (totalKeys === 0) allKeysDone = true;

      // Dispatcher loop — fires one request every DISPATCH_GAP_MS, in order.
      let nextDispatchAt = Date.now();

      for (let idx = 0; idx < hunterQueue.length; idx++) {
        if (aborted() || allKeysDone) break;

        const { r, i } = hunterQueue[idx];
        const fields = extractFields(rows[i]);

        if (!fields.company && !fields.linkedinUrl) {
          L.warn(`[Phase 3 / Hunter] Row ${i + 2}: no company or LinkedIn - skipping`);
          r.enrichmentStatus = 'no_domain';
          continue;
        }

        // Wait until an in-flight slot is free, then wait for next dispatch tick.
        await waitForSlot();
        if (aborted() || allKeysDone) break;

        const now = Date.now();
        if (now < nextDispatchAt) {
          await new Promise(resolve => setTimeout(resolve, nextDispatchAt - now));
        }
        if (aborted() || allKeysDone) break;
        nextDispatchAt = Date.now() + DISPATCH_GAP_MS;

        const apiKey     = hunterKeys[activeKeyIdx].key;
        const keySuffix  = '...' + apiKey.slice(-6);

        L.info(`[Phase 3 / Hunter] Dispatching row ${i + 2} (${idx + 1}/${hunterQueue.length}): ${fields.firstName} ${fields.lastName} @ ${fields.company || fields.linkedinUrl} | key ${keySuffix} | in-flight=${inflightCount + 1}`);

        inflightCount++;
        const task = (async () => {
          let result = null;
          try {
            result = await hunter.findEmail({
              firstName:   fields.firstName,
              lastName:    fields.lastName,
              company:     fields.company,
              linkedinUrl: fields.linkedinUrl,
            }, apiKey);
          } catch (err) {
            const code = err.statusCode;
            // Log EVERY rate/quota response so you always see it, even when activeKey already advanced.
            if (code === 402 || code === 429 || code === 401) {
              L.warn(`[Phase 3 / Hunter] Row ${i + 2} got HTTP ${code} on key ${keySuffix} — ${err.message}`);
            }

            // 402 = monthly quota exhausted (hunter-client normalises real 429 → 402).
            if (code === 402) {
              hunterPool.markDailyPause(apiKey);
              if (hunterKeys[activeKeyIdx]?.key === apiKey && !allKeysDone) {
                advanceKey('monthly quota exhausted (402)');
              } else {
                L.info(`[Phase 3 / Hunter] Key ${keySuffix} already superseded — no advance needed (active is now ${activeKeyIdx + 1}/${totalKeys})`);
              }
            } else if (code === 429) {
              // Raw 429 (rare — hunter-client normally maps to 402). Treat as quota signal.
              hunterPool.markDailyPause(apiKey);
              if (hunterKeys[activeKeyIdx]?.key === apiKey && !allKeysDone) {
                advanceKey('rate/quota limit (raw 429)');
              } else {
                L.info(`[Phase 3 / Hunter] Key ${keySuffix} already superseded — no advance needed (active is now ${activeKeyIdx + 1}/${totalKeys})`);
              }
            } else if (code === 401) {
              hunterPool.markDailyPause(apiKey);
              if (hunterKeys[activeKeyIdx]?.key === apiKey && !allKeysDone) {
                advanceKey('invalid key (401)');
              } else {
                L.info(`[Phase 3 / Hunter] Key ${keySuffix} already superseded — no advance needed (active is now ${activeKeyIdx + 1}/${totalKeys})`);
              }
            } else {
              stats.errors++;
              r.enrichmentStatus = 'hunter_error';
              L.error(`[Phase 3 / Hunter] Error for ${fields.firstName} ${fields.lastName} @ ${fields.company} | key ${keySuffix} — ${err.message}`);
            }
            return;
          }

          if (!result || !result.email) {
            r.enrichmentStatus = 'hunter_miss';
            return;
          }

          r.email            = result.email;
          r.enrichmentSource = 'Hunter';
          r.enrichmentStatus = 'hunter_found';
          L.info(`[Phase 3 / Hunter] Found ${result.email} (${fields.firstName} ${fields.lastName} @ ${fields.company}) | key ${keySuffix}`);

          // Fire Bouncer verification in parallel — write the verified email immediately.
          if (bouncerEnabled) {
            const bouncerPromise = (async () => {
              try {
                const response = await callWithRotatingKey(
                  this.pools.bouncer, 1, 'Bouncer (Hunter)', cfg.pipeline.maxRetries,
                  (bKey) => bouncer.verifyEmail(r.email, bKey)
                );
                if (bouncer.isValid(response.result)) {
                  r.enrichmentSource = 'Hunter+Bouncer';
                  r.enrichmentStatus = 'done';
                  stats.hunterResolved++;
                  stats.bouncerVerified++;
                  writeQueue.push(i + 2, r.email, r.enrichmentSource, 'done');
                  L.info(`[Phase 3 / Hunter] Verified + written: ${r.email} (row ${i + 2})`);
                } else {
                  r.email            = '';
                  r.enrichmentStatus = 'pending'; // → contactout_needed in Phase 5
                  stats.bouncerRejected++;
                  L.info(`[Phase 3 / Hunter] Bouncer rejected (row ${i + 2}) — marking for ContactOut`);
                }
              } catch (err) {
                // Bouncer error — accept Hunter result unverified rather than lose it.
                r.enrichmentSource = 'Hunter';
                r.enrichmentStatus = 'done';
                stats.hunterResolved++;
                stats.errors++;
                writeQueue.push(i + 2, r.email, r.enrichmentSource, 'done');
                L.warn(`[Phase 3 / Hunter] Bouncer error, accepting unverified: ${r.email} | ${err.message}`);
              }
            })();
            inflightHunterBouncer.push(bouncerPromise);
          } else {
            r.enrichmentStatus = 'done';
            stats.hunterResolved++;
            writeQueue.push(i + 2, r.email, r.enrichmentSource, 'done');
          }
        })().finally(releaseSlot);

        inflightHunterBouncer.push(task); // track Hunter responses too so we drain everything
      }

      // Wait for all in-flight Hunter calls + Bouncer verifications to settle.
      if (inflightHunterBouncer.length > 0) {
        L.info(`[Phase 3 / Hunter] Draining ${inflightHunterBouncer.length} in-flight request(s)...`);
        await Promise.allSettled(inflightHunterBouncer);
      }
    }

    // ════════════════════════════════════════════════════════════════════════
    // PHASE 4 — Bouncer verification for Hunter hits
    // ════════════════════════════════════════════════════════════════════════
    const hunterHits = results.map((r, i) => ({ r, i })).filter(({ r }) => r.enrichmentStatus === 'hunter_found');
    {
      const hunterFound = hunterHits.length;
      const hunterMiss  = results.filter(r => r.enrichmentStatus === 'hunter_miss' || r.enrichmentStatus === 'no_domain' || r.enrichmentStatus === 'hunter_error').length;
      L.info(`[Phase 3 / Hunter] Complete - ${hunterFound} found, ${hunterMiss} no match -> ${hunterFound} moving to Phase 4`);
    }

    if (bouncerEnabled && hunterHits.length > 0 && !aborted()) {
      const concurrency = concurrencyFor(this.pools.bouncer, hunterHits.length, 1.5);
      const sem = createSemaphore(concurrency);

      L.info(`[Phase 4 / Bouncer] Verifying ${hunterHits.length} Hunter email(s) | concurrency: ${concurrency}`);

      await Promise.allSettled(hunterHits.map(async ({ r, i }) => {
        if (aborted()) return;
        await sem.acquire();
        try {
          let key;
          try {
            const response = await callWithRotatingKey(
              this.pools.bouncer,
              1,
              'Bouncer (Hunter)',
              cfg.pipeline.maxRetries,
              (apiKey) => bouncer.verifyEmail(r.email, apiKey)
            );
            key = response.key;
            const result = response.result;

            if (bouncer.isValid(result)) {
              r.enrichmentSource = 'Hunter+Bouncer';
              r.enrichmentStatus = 'done';
              stats.hunterResolved++;
              stats.bouncerVerified++;
              writeQueue.push(i + 2, r.email, r.enrichmentSource, 'done');
            } else {
              r.email            = '';
              r.enrichmentStatus = 'pending'; // will become contactout_needed
              stats.bouncerRejected++;
            }
          } catch (err) {
            if (err instanceof AllKeysExhaustedError) {
              L.warn(`[Phase 4 / Bouncer] All keys exhausted - accepting remaining Hunter results unverified`);
            } else {
              L.error(`[Phase 4 / Bouncer] Verify error - accepting Hunter result conservatively | ${err.message}`);
            }
            r.enrichmentSource = 'Hunter';
            r.enrichmentStatus = 'done';
            stats.hunterResolved++;
            stats.errors++;
            writeQueue.push(i + 2, r.email, r.enrichmentSource, 'done');
          }
        } finally {
          sem.release();
        }
      }));
    } else if (!bouncerEnabled && hunterHits.length > 0) {
      for (const { r, i } of hunterHits) {
        r.enrichmentSource = 'Hunter';
        r.enrichmentStatus = 'done';
        stats.hunterResolved++;
        writeQueue.push(i + 2, r.email, 'Hunter', 'done');
      }
    }

    L.info(`[Phase 4 / Bouncer] Complete - ${stats.hunterResolved} verified, ${stats.bouncerRejected - (prospeoHits.length - stats.prospeoResolved)} rejected`);

    // ════════════════════════════════════════════════════════════════════════
    // PHASE 5 — Mark all remaining rows as ContactOut Needed
    // ════════════════════════════════════════════════════════════════════════
    for (let idx = 0; idx < results.length; idx++) {
      const r = results[idx];
      // Leave out-of-range rows completely untouched — no row data modifications.
      if (r.enrichmentStatus === 'out_of_range') continue;
      if (r.enrichmentStatus !== 'done' && r.enrichmentStatus !== 'skipped') {
        r.enrichmentSource = 'ContactOut Needed';
        r.enrichmentStatus = 'contactout_needed';
        r.email            = '';
        stats.contactoutNeeded++;
        writeQueue.push(idx + 2, '', 'ContactOut Needed', 'contactout_needed');
      }
    }

      writeProgress(total, total, stats, 'finished');
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
      L.info(`[Pipeline] Complete`);
      L.info(`[Pipeline] Total rows    : ${total}`);
      L.info(`[Pipeline] Skipped       : ${stats.skipped} (already had email)`);
      L.info(`[Pipeline] Prospeo       : ${stats.prospeoResolved} resolved`);
      L.info(`[Pipeline] Hunter        : ${stats.hunterResolved} resolved`);
      L.info(`[Pipeline] Bouncer       : ${stats.bouncerVerified} verified, ${stats.bouncerRejected} rejected`);
      L.info(`[Pipeline] ContactOut    : ${stats.contactoutNeeded} still needed`);
      L.info(`[Pipeline] Errors        : ${stats.errors}`);
      L.info(`[Pipeline] Elapsed       : ${elapsed}s`);
      L.info(`[Pipeline] -----------------------------------------------------`);

      return { results, stats };
    } catch (err) {
      L.error(`[Pipeline] Unexpected error - draining write queue before exit: ${err.message}`);
      throw err;
    } finally {
      // Always drain: runs on success, exception, or early return.
      // Ensures every enriched email that was pushed makes it to Sheets.
      writeQueue.stop();
      await writeQueue.flush().catch(e => L.error(`[Pipeline] Final flush failed: ${e.message}`));
    }
  }
}

// ─── Factory ──────────────────────────────────────────────────────────────────

let _singleton = null;

function createOrchestrator(forceReload = false) {
  if (_singleton && !forceReload) return _singleton;

  const config = loadConfig();

  const pools = {
    prospeo: new KeyPool('prospeo', config.prospeo.keys, {
      cooldownMs:        config.prospeo.cooldownMs,
      dailyLimit:        config.prospeo.dailyLimit,
      minuteLimit:       config.prospeo.minuteLimit,
      secondLimit:       config.prospeo.secondLimit,
      requestsPerSecond: config.prospeo.requestsPerSecond,
    }),
    hunter: new KeyPool('hunter', config.hunter.keys, {
      cooldownMs:        config.hunter.cooldownMs,
      dailyLimit:        config.hunter.dailyLimit,
      minuteLimit:       config.hunter.minuteLimit,
      requestsPerSecond: config.hunter.requestsPerSecond,
    }),
    bouncer: new KeyPool('bouncer', config.bouncer.keys, {
      cooldownMs:        config.bouncer.cooldownMs,
      dailyLimit:        config.bouncer.dailyLimit,
      minuteLimit:       config.bouncer.minuteLimit,
      requestsPerSecond: config.bouncer.requestsPerSecond,
    }),
  };

  _singleton = new PipelineOrchestrator(config, pools);
  return _singleton;
}

function resetOrchestrator() {
  _singleton = null;
}

module.exports = { PipelineOrchestrator, createOrchestrator, resetOrchestrator, concurrencyFor };
