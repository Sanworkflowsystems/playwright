// AnyMailFinder client — find email by LinkedIn URL.
// Uses the v5.1 /find-email/linkedin-url endpoint.
// Only accepts email_status === 'valid'; risky/not_found/blacklisted are treated as misses.
// No rate-limit header from AMF, so we self-throttle: 1 req/s with MAX_INFLIGHT=10.

const https = require('https');

const DISPATCH_GAP_MS = 1000;   // 1 request per second
const MAX_INFLIGHT    = 10;

/**
 * Run AnyMailFinder over a set of rows from a Google Sheet and write results back.
 *
 * @param {object} opts
 * @param {object}   opts.sheetsClient   - authenticated googleapis sheets client
 * @param {string}   opts.sheetId        - spreadsheet ID
 * @param {string}   opts.sheetName      - tab name
 * @param {object[]} opts.rows           - array of row objects (from readSheetData)
 * @param {string[]} opts.headers        - column headers array
 * @param {string}   opts.apiKey         - AnyMailFinder API key
 * @param {number}   opts.rowStart       - 1-based sheet row start (inclusive, ≥2), or null
 * @param {number}   opts.rowEnd         - 1-based sheet row end (inclusive), or null
 * @param {Function} opts.onProgress     - callback({ processed, total, row, email, status })
 * @param {object}   opts.signal         - AbortSignal
 * @returns {Promise<{ processed, found, notFound, credits, stoppedAtRow }>}
 */
async function runAnyMailFinder(opts) {
  const { sheetsClient, sheetId, sheetName, rows, headers, apiKey, rowStart, rowEnd, onProgress, signal } = opts;

  // Locate key columns (case-insensitive)
  const colIdx = (pattern) => headers.findIndex(h => pattern.test(h));
  const linkedInIdx   = colIdx(/^defaultProfileUrl$/i);
  const finalEmailIdx = colIdx(/^finalEmail$/i);

  if (linkedInIdx === -1) throw new Error('Column "defaultProfileUrl" not found in sheet headers.');
  if (finalEmailIdx === -1) throw new Error('Column "finalEmail" not found in sheet headers.');

  const finalEmailCol = columnLetter(finalEmailIdx);

  // Index bounds (0-based array, 1-based sheet row N → rows[N-2])
  const idxStart = rowStart ? Math.max(0, rowStart - 2) : 0;
  const idxEnd   = rowEnd   ? Math.min(rows.length - 1, rowEnd - 2) : rows.length - 1;

  const stats = { processed: 0, found: 0, notFound: 0, credits: 0, stoppedAtRow: null };
  let inflight = 0;
  let creditsExhausted = false;

  // Collect eligible rows (no finalEmail already, within range)
  const eligible = [];
  for (let i = idxStart; i <= idxEnd; i++) {
    const row = rows[i];
    const existingEmail = String(row['finalEmail'] || row['finalemail'] || '').trim();
    if (existingEmail.includes('@')) continue; // already done
    const linkedInUrl = String(row['defaultProfileUrl'] || '').trim();
    if (!linkedInUrl.startsWith('http')) continue; // no URL, skip
    eligible.push({ i, row, linkedInUrl, sheetRow: i + 2 });
  }

  const total = eligible.length;
  console.log(`[AnyMailFinder] ${total} eligible rows (range ${rowStart || 2}..${rowEnd || rows.length + 1})`);

  for (let ei = 0; ei < eligible.length; ei++) {
    if (signal?.aborted || creditsExhausted) break;

    // Rate-limit: wait for a slot
    while (inflight >= MAX_INFLIGHT) {
      await sleep(50);
    }

    const { i, row, linkedInUrl, sheetRow } = eligible[ei];
    inflight++;

    console.log(`[AnyMailFinder] [${ei + 1}/${total}] Sheet row ${sheetRow} | ${linkedInUrl}`);

    // Fire request (non-blocking within gap)
    const reqPromise = callAnyMailFinder(apiKey, linkedInUrl).then(async (result) => {
      inflight--;
      stats.processed++;

      if (result.creditsExhausted) {
        creditsExhausted = true;
        stats.stoppedAtRow = sheetRow;
        console.warn(`[AnyMailFinder] Credits exhausted at sheet row ${sheetRow}. Stopping.`);
        onProgress?.({ processed: stats.processed, total, row: sheetRow, email: null, status: 'credits_exhausted', stoppedAtRow: sheetRow });
        return;
      }

      if (result.email) {
        stats.found++;
        stats.credits += result.creditsCharged || 0;
        console.log(`[AnyMailFinder] Row ${sheetRow} → ${result.email} (${result.emailStatus})`);

        // Write finalEmail back to sheet
        await writeEmailToSheet(sheetsClient, sheetId, sheetName, sheetRow, finalEmailCol, result.email);
        onProgress?.({ processed: stats.processed, total, row: sheetRow, email: result.email, status: 'found' });
      } else {
        stats.notFound++;
        console.log(`[AnyMailFinder] Row ${sheetRow} → not found (${result.emailStatus})`);
        onProgress?.({ processed: stats.processed, total, row: sheetRow, email: null, status: result.emailStatus || 'not_found' });
      }
    }).catch(err => {
      inflight--;
      stats.processed++;
      console.error(`[AnyMailFinder] Row ${sheetRow} error: ${err.message}`);
      onProgress?.({ processed: stats.processed, total, row: sheetRow, email: null, status: 'error' });
    });

    // Don't await — fire and move on after gap
    reqPromise; // eslint-disable-line no-unused-expressions

    // Dispatch gap: 1 req/s
    if (ei < eligible.length - 1) await sleep(DISPATCH_GAP_MS);
  }

  // Wait for all in-flight to finish
  while (inflight > 0) {
    await sleep(100);
  }

  return stats;
}

async function callAnyMailFinder(apiKey, linkedInUrl) {
  return new Promise((resolve) => {
    const body = JSON.stringify({ linkedin_url: linkedInUrl });
    const options = {
      hostname: 'api.anymailfinder.com',
      path:     '/v5.1/find-email/linkedin-url',
      method:   'POST',
      headers: {
        'Authorization':  apiKey,
        'Content-Type':   'application/json',
        'Content-Length': Buffer.byteLength(body),
      },
      timeout: 180000,
    };

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          if (res.statusCode === 402 || json.error === 'upgrade_needed') {
            return resolve({ email: null, creditsExhausted: true });
          }
          if (res.statusCode === 401) {
            return resolve({ email: null, creditsExhausted: true, emailStatus: 'unauthorized' });
          }
          // Only accept valid_email (email_status === 'valid')
          const email = json.valid_email || null;
          resolve({
            email,
            emailStatus:    json.email_status || 'not_found',
            creditsCharged: json.credits_charged || 0,
            creditsExhausted: false,
          });
        } catch (e) {
          resolve({ email: null, emailStatus: 'parse_error' });
        }
      });
    });

    req.on('error', (e) => resolve({ email: null, emailStatus: 'network_error' }));
    req.on('timeout', ()  => { req.destroy(); resolve({ email: null, emailStatus: 'timeout' }); });
    req.write(body);
    req.end();
  });
}

async function writeEmailToSheet(sheetsClient, sheetId, sheetName, sheetRow, col, email) {
  const range = `${sheetName}!${col}${sheetRow}`;
  await sheetsClient.spreadsheets.values.update({
    spreadsheetId: sheetId,
    range,
    valueInputOption: 'RAW',
    requestBody: { values: [[email]] },
  });
}

function columnLetter(index) {
  let letter = '';
  let n = index + 1;
  while (n > 0) {
    const rem = (n - 1) % 26;
    letter = String.fromCharCode(65 + rem) + letter;
    n = Math.floor((n - 1) / 26);
  }
  return letter;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

module.exports = { runAnyMailFinder };
