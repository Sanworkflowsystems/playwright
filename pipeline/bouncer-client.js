'use strict';

/**
 * Bouncer email verification client.
 * POST https://api.usebouncer.com/v1/email/verify
 *
 * Returned status values:
 *   "deliverable"   → safe to send
 *   "risky"         → might bounce, but we accept it
 *   "undeliverable" → reject, try next service
 *   "unknown"       → inconclusive, we treat as undeliverable
 */

const BOUNCER_BASE = 'https://api.usebouncer.com/v1';
const TIMEOUT_MS = 15000;

/**
 * Verify a single email address.
 * @param {string} email
 * @param {string} apiKey
 * @returns {{ status: string, reason: string|null, raw: object }}
 * @throws {Error} with .statusCode set to HTTP status on API errors
 */
async function verifyEmail(email, apiKey) {
  let res;
  try {
    res = await fetch(`${BOUNCER_BASE}/email/verify`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': apiKey,
      },
      body: JSON.stringify({ email }),
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });
  } catch (err) {
    // Network error / timeout
    const e = new Error(`Bouncer network error: ${err.message}`);
    e.statusCode = 0;
    throw e;
  }

  if (res.status === 429) {
    const e = new Error('Bouncer: rate limited (429)');
    e.statusCode = 429;
    throw e;
  }
  if (res.status === 402) {
    const e = new Error('Bouncer: daily quota exhausted (402)');
    e.statusCode = 402;
    throw e;
  }
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    const e = new Error(`Bouncer: HTTP ${res.status} — ${body.slice(0, 200)}`);
    e.statusCode = res.status;
    throw e;
  }

  const data = await res.json();
  return {
    status: data.status || 'unknown',   // deliverable | undeliverable | risky | unknown
    reason: data.reason || null,
    score: data.score || null,
    raw: data,
  };
}

/**
 * Returns true if the verification result means we should use this email.
 */
function isValid(result) {
  if (!result) return false;
  return result.status === 'deliverable' || result.status === 'risky';
}

module.exports = { verifyEmail, isValid };
