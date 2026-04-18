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

function calcConcurrency(rowCount, keyCount) {
  let base;
  if (rowCount <= 200)      base = 80;
  else if (rowCount <= 5000) base = 120;
  else                       base = 150;

  // Each key can serve ~8 concurrent requests (token refill + latency overlap)
  const keyCap = Math.max(keyCount * 8, 20);
  return Math.min(base, keyCap);
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
      console.warn(`[Pipeline] ${label} retry ${attempt + 1}/${maxRetries} after ${delayMs}ms — ${err.message}`);
      await new Promise(r => setTimeout(r, delayMs));
    }
  }
}

// ─── Row field extractor ───────────────────────────────────────────────────────

function extractFields(row) {
  const keys = Object.keys(row);

  const find = (pattern) => keys.find(k => pattern.test(k));

  const linkedinKey  = find(/linkedin|profileurl|profile_url|defaultprofile/i);
  const firstKey     = find(/^first.?name$|^firstname$/i);
  const lastKey      = find(/^last.?name$|^lastname$/i);
  const fullKey      = find(/^(full.?name|^name)$/i);
  const companyKey   = find(/company|employer|organization|org$/i);
  const emailKey     = find(/^(email|work.?email|finalemail|enrichment_email)$/i);

  let firstName = firstKey ? (row[firstKey] || '').trim() : '';
  let lastName  = lastKey  ? (row[lastKey]  || '').trim() : '';

  if (!firstName && !lastName && fullKey) {
    const parts = (row[fullKey] || '').trim().split(/\s+/);
    firstName = parts[0] || '';
    lastName  = parts.slice(1).join(' ') || '';
  }

  return {
    linkedinUrl:   linkedinKey  ? (row[linkedinKey]  || '').trim() : '',
    firstName,
    lastName,
    company:       companyKey   ? (row[companyKey]   || '').trim() : '',
    existingEmail: emailKey     ? (row[emailKey]     || '').trim() : '',
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
    } = options;

    const total = rows.length;
    const cfg   = this.config;
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
    let done = 0;

    const tick = (n = 1) => {
      done = Math.min(done + n, total);
      writeProgress(done, total, stats);
      if (onProgress) onProgress(done, total, stats);
    };

    // ── Skip rows that already have an email ─────────────────────────────────
    if (cfg.pipeline.skipIfEmailExists) {
      for (let i = 0; i < rows.length; i++) {
        const { existingEmail } = extractFields(rows[i]);
        if (existingEmail && existingEmail.includes('@')) {
          results[i].enrichmentStatus  = 'done';
          results[i].enrichmentSource  = 'existing';
          results[i].email             = existingEmail;
          stats.skipped++;
        }
      }
    }

    const pending = () => results.map((r, i) => ({ r, i })).filter(({ r }) => r.enrichmentStatus === 'pending');

    writeProgress(0, total, stats, 'running');

    // ════════════════════════════════════════════════════════════════════════
    // PHASE 1 — Bulk Prospeo
    // ════════════════════════════════════════════════════════════════════════
    const prospeoEnabled = cfg.prospeo.enabled && this.pools.prospeo.keyCount > 0;

    if (prospeoEnabled && !aborted()) {
      const toEnrich = pending();
      const bulkSize = cfg.prospeo.bulkSize || 50;

      // Split into chunks of bulkSize
      const chunks = [];
      for (let i = 0; i < toEnrich.length; i += bulkSize) {
        chunks.push(toEnrich.slice(i, i + bulkSize));
      }

      // Concurrency at chunk level: each chunk processes bulkSize rows at once
      const chunkConcurrency = Math.max(1, Math.ceil(calcConcurrency(total, this.pools.prospeo.keyCount) / bulkSize));
      const sem = createSemaphore(chunkConcurrency);

      console.log(`[Pipeline] Phase 1: Prospeo bulk — ${toEnrich.length} rows in ${chunks.length} chunks (chunk concurrency: ${chunkConcurrency})`);

      await Promise.allSettled(chunks.map(async (chunk) => {
        if (aborted()) return;
        await sem.acquire();
        try {
          const contacts = chunk.map(({ r, i }) => extractFields(rows[i]));
          const items    = contacts.map(f => ({
            linkedinUrl: f.linkedinUrl,
            firstName:   f.firstName,
            lastName:    f.lastName,
            company:     f.company,
          }));

          let key;
          let chunkResults = new Array(chunk.length).fill(null);

          try {
            key = await this.pools.prospeo.acquireKey();
            chunkResults = await withRetry(
              () => prospeo.enrichBulk(items, key),
              cfg.pipeline.maxRetries,
              'Prospeo bulk'
            );
            this.pools.prospeo.handleResponse(key, 200);
          } catch (err) {
            if (key && err.statusCode) this.pools.prospeo.handleResponse(key, err.statusCode);
            if (err instanceof AllKeysExhaustedError) {
              console.warn('[Pipeline] Prospeo: all keys exhausted — skipping remaining Prospeo calls');
              return; // leave results as null, fall through to Hunter
            }
            stats.errors++;
            console.error('[Pipeline] Prospeo bulk error:', err.message);
          }

          for (let j = 0; j < chunk.length; j++) {
            const r = chunkResults[j];
            if (r && r.email) {
              chunk[j].r.email            = r.email;
              chunk[j].r.enrichmentSource = 'Prospeo';
              chunk[j].r.enrichmentStatus = 'prospeo_found';
            }
          }

          tick(chunk.length);
        } finally {
          sem.release();
        }
      }));
    }

    // ════════════════════════════════════════════════════════════════════════
    // PHASE 2 — Bouncer verification for Prospeo hits
    // ════════════════════════════════════════════════════════════════════════
    const bouncerEnabled = cfg.bouncer.enabled && this.pools.bouncer.keyCount > 0;

    const prospeoHits = results.map((r, i) => ({ r, i })).filter(({ r }) => r.enrichmentStatus === 'prospeo_found');

    if (bouncerEnabled && prospeoHits.length > 0 && !aborted()) {
      const concurrency = calcConcurrency(total, this.pools.bouncer.keyCount);
      const sem = createSemaphore(concurrency);

      console.log(`[Pipeline] Phase 2: Bouncer verify ${prospeoHits.length} Prospeo hits (concurrency: ${concurrency})`);

      await Promise.allSettled(prospeoHits.map(async ({ r }) => {
        if (aborted()) return;
        await sem.acquire();
        try {
          let key;
          try {
            key = await this.pools.bouncer.acquireKey();
            const result = await withRetry(
              () => bouncer.verifyEmail(r.email, key),
              cfg.pipeline.maxRetries,
              'Bouncer'
            );
            this.pools.bouncer.handleResponse(key, 200);

            if (bouncer.isValid(result)) {
              r.enrichmentSource = 'Prospeo+Bouncer';
              r.enrichmentStatus = 'done';
              stats.prospeoResolved++;
              stats.bouncerVerified++;
            } else {
              // Bouncer rejected — fall through to Hunter
              r.enrichmentStatus = 'pending';
              r.email            = '';
              stats.bouncerRejected++;
            }
          } catch (err) {
            if (key && err.statusCode) this.pools.bouncer.handleResponse(key, err.statusCode);
            if (err instanceof AllKeysExhaustedError) {
              // No bouncer keys — just accept Prospeo result
              r.enrichmentSource = 'Prospeo';
              r.enrichmentStatus = 'done';
              stats.prospeoResolved++;
            } else {
              // Error — accept Prospeo result (conservative)
              r.enrichmentSource = 'Prospeo';
              r.enrichmentStatus = 'done';
              stats.prospeoResolved++;
              stats.errors++;
            }
          }
        } finally {
          sem.release();
        }
      }));
    } else if (!bouncerEnabled && prospeoHits.length > 0) {
      // No Bouncer configured — accept all Prospeo results as-is
      for (const { r } of prospeoHits) {
        r.enrichmentSource = 'Prospeo';
        r.enrichmentStatus = 'done';
        stats.prospeoResolved++;
      }
    }

    // ════════════════════════════════════════════════════════════════════════
    // PHASE 3 — Hunter fallback for all still-pending rows
    // ════════════════════════════════════════════════════════════════════════
    const hunterEnabled = cfg.hunter.enabled && this.pools.hunter.keyCount > 0;
    const hunterQueue   = results.map((r, i) => ({ r, i })).filter(({ r }) => r.enrichmentStatus === 'pending');

    if (hunterEnabled && hunterQueue.length > 0 && !aborted()) {
      const concurrency = calcConcurrency(total, this.pools.hunter.keyCount);
      const sem = createSemaphore(concurrency);

      console.log(`[Pipeline] Phase 3: Hunter fallback — ${hunterQueue.length} rows (concurrency: ${concurrency})`);

      let hunterKeysGone = false;

      await Promise.allSettled(hunterQueue.map(async ({ r, i }) => {
        if (aborted() || hunterKeysGone) return;
        await sem.acquire();
        try {
          const fields = extractFields(rows[i]);
          const domain = hunter.companyToDomain(fields.company);

          if (!domain) {
            r.enrichmentStatus = 'no_domain';
            return;
          }

          let key;
          try {
            key = await this.pools.hunter.acquireKey();
            const result = await withRetry(
              () => hunter.findEmail(fields.firstName, fields.lastName, domain, key),
              cfg.pipeline.maxRetries,
              'Hunter'
            );
            this.pools.hunter.handleResponse(key, 200);

            if (result && result.email) {
              r.email            = result.email;
              r.enrichmentSource = 'Hunter';
              r.enrichmentStatus = 'hunter_found';
            } else {
              r.enrichmentStatus = 'hunter_miss';
            }
          } catch (err) {
            if (key && err.statusCode) this.pools.hunter.handleResponse(key, err.statusCode);
            if (err instanceof AllKeysExhaustedError) {
              hunterKeysGone = true;
              console.warn('[Pipeline] Hunter: all keys exhausted');
            } else {
              stats.errors++;
              r.enrichmentStatus = 'hunter_error';
              console.error('[Pipeline] Hunter error:', err.message);
            }
          }
        } finally {
          sem.release();
        }
      }));
    }

    // ════════════════════════════════════════════════════════════════════════
    // PHASE 4 — Bouncer verification for Hunter hits
    // ════════════════════════════════════════════════════════════════════════
    const hunterHits = results.map((r, i) => ({ r, i })).filter(({ r }) => r.enrichmentStatus === 'hunter_found');

    if (bouncerEnabled && hunterHits.length > 0 && !aborted()) {
      const concurrency = calcConcurrency(total, this.pools.bouncer.keyCount);
      const sem = createSemaphore(concurrency);

      console.log(`[Pipeline] Phase 4: Bouncer verify ${hunterHits.length} Hunter hits (concurrency: ${concurrency})`);

      await Promise.allSettled(hunterHits.map(async ({ r }) => {
        if (aborted()) return;
        await sem.acquire();
        try {
          let key;
          try {
            key = await this.pools.bouncer.acquireKey();
            const result = await withRetry(
              () => bouncer.verifyEmail(r.email, key),
              cfg.pipeline.maxRetries,
              'Bouncer (Hunter)'
            );
            this.pools.bouncer.handleResponse(key, 200);

            if (bouncer.isValid(result)) {
              r.enrichmentSource = 'Hunter+Bouncer';
              r.enrichmentStatus = 'done';
              stats.hunterResolved++;
              stats.bouncerVerified++;
            } else {
              r.email            = '';
              r.enrichmentStatus = 'pending'; // will become contactout_needed
              stats.bouncerRejected++;
            }
          } catch (err) {
            if (key && err.statusCode) this.pools.bouncer.handleResponse(key, err.statusCode);
            // On bouncer error, accept Hunter result (conservative)
            r.enrichmentSource = 'Hunter';
            r.enrichmentStatus = 'done';
            stats.hunterResolved++;
            stats.errors++;
          }
        } finally {
          sem.release();
        }
      }));
    } else if (!bouncerEnabled && hunterHits.length > 0) {
      for (const { r } of hunterHits) {
        r.enrichmentSource = 'Hunter';
        r.enrichmentStatus = 'done';
        stats.hunterResolved++;
      }
    }

    // ════════════════════════════════════════════════════════════════════════
    // PHASE 5 — Mark all remaining rows as ContactOut Needed
    // ════════════════════════════════════════════════════════════════════════
    for (const r of results) {
      if (r.enrichmentStatus !== 'done' && r.enrichmentStatus !== 'skipped') {
        r.enrichmentSource = 'ContactOut Needed';
        r.enrichmentStatus = 'contactout_needed';
        r.email            = '';
        stats.contactoutNeeded++;
      }
    }

    // ════════════════════════════════════════════════════════════════════════
    // Write results back to Google Sheets (if in Sheets mode)
    // ════════════════════════════════════════════════════════════════════════
    if (sheetsClient && sheetId && headers && !aborted()) {
      const sheetsHelper = require('../google-sheets-helper');
      await sheetsHelper.ensurePipelineColumns(sheetsClient, sheetId, sheetName, headers, rows.length);

      const SHEET_BATCH = 30;
      for (let i = 0; i < rows.length; i += SHEET_BATCH) {
        if (aborted()) break;
        const batch = rows.slice(i, i + SHEET_BATCH);
        await Promise.allSettled(batch.map((row, j) => {
          const idx = i + j;
          const res = results[idx];
          return sheetsHelper.updatePipelineResult(
            sheetsClient, sheetId, sheetName,
            idx + 2,   // row 1 = header, row 2 = first data row (1-indexed)
            headers,
            { ...row, enrichment_email: res.email, enrichment_source: res.enrichmentSource, enrichment_status: res.enrichmentStatus }
          );
        }));
        writeProgress(Math.min(i + SHEET_BATCH, rows.length), total, stats, 'writing_sheets');
      }
    }

    writeProgress(total, total, stats, 'finished');
    console.log(`[Pipeline] Complete — Prospeo: ${stats.prospeoResolved}, Hunter: ${stats.hunterResolved}, ContactOut needed: ${stats.contactoutNeeded}, Errors: ${stats.errors}`);

    return { results, stats };
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
      requestsPerSecond: config.prospeo.requestsPerSecond,
    }),
    hunter: new KeyPool('hunter', config.hunter.keys, {
      cooldownMs:        config.hunter.cooldownMs,
      dailyLimit:        config.hunter.dailyLimit,
      requestsPerSecond: config.hunter.requestsPerSecond,
    }),
    bouncer: new KeyPool('bouncer', config.bouncer.keys, {
      cooldownMs:        config.bouncer.cooldownMs,
      dailyLimit:        config.bouncer.dailyLimit,
      requestsPerSecond: config.bouncer.requestsPerSecond,
    }),
  };

  _singleton = new PipelineOrchestrator(config, pools);
  return _singleton;
}

function resetOrchestrator() {
  _singleton = null;
}

module.exports = { PipelineOrchestrator, createOrchestrator, resetOrchestrator, calcConcurrency };
