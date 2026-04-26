'use strict';

/**
 * ContactOut email verification queue.
 *
 * Strictly sequential per worker: one Bouncer call at a time, so we never waste
 * credits. Jobs are processed in FIFO order. For each row, emails are checked in
 * priority order — workEmail → otherWorkEmails → personalEmail → otherPersonalEmails —
 * and the first `deliverable` email is written to the `finalEmail` column.
 *
 * Risky / undeliverable / unknown results are all rejected. If Bouncer returns a
 * credits-exhausted status (402/401), we switch to fallback mode and pick the first
 * non-empty email by the same priority order without verification. If nothing
 * validates (and credits are available), finalEmail stays blank.
 *
 * Playwright keeps scraping even while Bouncer is behind; rows are written to the
 * correct sheet row by number. Call `drain()` before exiting the worker.
 */

const { verifyEmail, isValid } = require('./bouncer-client');

function splitList(str) {
  if (!str) return [];
  return String(str)
    .split(/[;,]/)
    .map(s => s.trim())
    .filter(Boolean);
}

/**
 * Build ordered, deduped email list following the priority rule.
 * @param {{workEmail:string, otherWorkEmails:string|string[], personalEmail:string, otherPersonalEmails:string|string[]}} emails
 */
function buildPriorityList(emails) {
  const list = [];
  const seen = new Set();
  const push = (e) => {
    if (!e) return;
    const v = String(e).trim().toLowerCase();
    if (!v || seen.has(v)) return;
    seen.add(v);
    list.push(String(e).trim());
  };

  push(emails.workEmail);
  const otherWork = Array.isArray(emails.otherWorkEmails)
    ? emails.otherWorkEmails
    : splitList(emails.otherWorkEmails);
  otherWork.forEach(push);

  push(emails.personalEmail);
  const otherPersonal = Array.isArray(emails.otherPersonalEmails)
    ? emails.otherPersonalEmails
    : splitList(emails.otherPersonalEmails);
  otherPersonal.forEach(push);

  return list;
}

/**
 * Factory for a single-consumer sequential verification queue.
 *
 * @param {object}   opts
 * @param {string}   opts.bouncerKey      Bouncer API key
 * @param {object}   opts.sheetsClient    googleapis sheets client
 * @param {string}   opts.sheetId
 * @param {string}   opts.sheetName
 * @param {string}   opts.finalEmailCol   A1 column letter for the finalEmail column (e.g. "AC")
 * @param {boolean} [opts.bouncerEnabled] If false, go straight to fallback mode
 * @returns {{ enqueue: function, drain: function, stats: function }}
 */
function createVerifier({ bouncerKey, sheetsClient, sheetId, sheetName, finalEmailCol, bouncerEnabled = true }) {
  const queue = [];
  let running = false;
  let creditsExhausted = !bouncerEnabled || !bouncerKey;
  const stats = { verified: 0, deliverable: 0, rejected: 0, fallback: 0, blank: 0, errors: 0 };

  let drainResolvers = [];

  function notifyDrained() {
    const rs = drainResolvers.splice(0);
    for (const r of rs) r();
  }

  async function writeFinalEmail(rowNumber, email) {
    if (!finalEmailCol) return;
    try {
      await sheetsClient.spreadsheets.values.update({
        spreadsheetId: sheetId,
        range: `${sheetName}!${finalEmailCol}${rowNumber}`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [[email || '']] },
      });
    } catch (err) {
      stats.errors++;
      console.error(`[Verifier] Failed to write finalEmail row ${rowNumber}: ${err.message}`);
    }
  }

  async function checkOne(email) {
    try {
      const result = await verifyEmail(email, bouncerKey);
      stats.verified++;
      return { ok: isValid(result), status: result.status, fatal: false };
    } catch (err) {
      const code = err.statusCode;
      // Credits / auth failures → flip to fallback mode permanently
      if (code === 402 || code === 401) {
        creditsExhausted = true;
        console.warn(`[Verifier] Bouncer credits exhausted (HTTP ${code}). Falling back to priority-order email picking without verification.`);
        return { ok: false, status: 'credits-exhausted', fatal: true };
      }
      stats.errors++;
      console.warn(`[Verifier] Bouncer error for ${email}: ${err.message} (status=${code})`);
      // Treat other errors as "not deliverable" and move on — don't block pipeline
      return { ok: false, status: `error-${code || 'net'}`, fatal: false };
    }
  }

  async function processJob(job) {
    const { rowNumber, emails } = job;
    const priority = buildPriorityList(emails);

    if (priority.length === 0) {
      stats.blank++;
      return; // nothing to write, leave finalEmail blank
    }

    if (creditsExhausted) {
      const pick = priority[0];
      stats.fallback++;
      await writeFinalEmail(rowNumber, pick);
      console.log(`[Verifier] Row ${rowNumber}: fallback → ${pick} (no verification)`);
      return;
    }

    for (const email of priority) {
      const { ok, status, fatal } = await checkOne(email);
      if (ok) {
        stats.deliverable++;
        await writeFinalEmail(rowNumber, email);
        console.log(`[Verifier] Row ${rowNumber}: ${email} deliverable → finalEmail`);
        return;
      }
      if (fatal) {
        // Bouncer just ran out of credits during this row — fallback for THIS row too
        const pick = priority[0];
        stats.fallback++;
        await writeFinalEmail(rowNumber, pick);
        console.log(`[Verifier] Row ${rowNumber}: fallback → ${pick} (credits exhausted mid-row)`);
        return;
      }
      stats.rejected++;
      console.log(`[Verifier] Row ${rowNumber}: ${email} rejected (${status})`);
    }

    // Nothing validated — leave finalEmail blank
    stats.blank++;
    console.log(`[Verifier] Row ${rowNumber}: no deliverable email, finalEmail left blank`);
  }

  async function runLoop() {
    if (running) return;
    running = true;
    try {
      while (queue.length > 0) {
        const job = queue.shift();
        try {
          await processJob(job);
        } catch (err) {
          stats.errors++;
          console.error(`[Verifier] Job failed for row ${job.rowNumber}: ${err.message}`);
        }
      }
    } finally {
      running = false;
      if (queue.length === 0) notifyDrained();
    }
  }

  function enqueue(rowNumber, emails) {
    if (!rowNumber || !emails) return;
    queue.push({ rowNumber, emails });
    // Fire-and-forget; caller does not await
    runLoop().catch(err => {
      console.error(`[Verifier] runLoop error: ${err.message}`);
    });
  }

  function drain() {
    if (!running && queue.length === 0) return Promise.resolve();
    return new Promise(resolve => { drainResolvers.push(resolve); });
  }

  return {
    enqueue,
    drain,
    stats: () => ({ ...stats, queued: queue.length, running }),
  };
}

module.exports = { createVerifier, buildPriorityList };
