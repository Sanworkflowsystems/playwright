'use strict';

/**
 * Prospeo enrichment client.
 *
 * Single:  POST https://api.prospeo.io/enrich-person
 * Bulk:    POST https://api.prospeo.io/bulk-enrich-person  (max 50 contacts per call)
 *
 * Auth: X-KEY header
 */

const PROSPEO_BASE = 'https://api.prospeo.io';
const SINGLE_TIMEOUT_MS = 20000;
const BULK_TIMEOUT_MS   = 90000;  // bulk can take longer

/**
 * Build a Prospeo contact object from options.
 * Prefers LinkedIn URL; falls back to name + company.
 */
function buildContactPayload(opts) {
  if (opts.linkedinUrl && opts.linkedinUrl.startsWith('http')) {
    return { url: opts.linkedinUrl };
  }
  return {
    first_name: opts.firstName || '',
    last_name:  opts.lastName  || '',
    company:    opts.company   || '',
  };
}

/**
 * Parse a raw Prospeo result object → { email, confidence } or null
 */
function parseResult(raw) {
  if (!raw) return null;
  const email = raw?.email?.value || (typeof raw.email === 'string' ? raw.email : null);
  if (!email || !email.includes('@')) return null;
  return {
    email: email.toLowerCase().trim(),
    confidence: raw?.email?.confidence ?? null,
  };
}

/**
 * Enrich a single person (LinkedIn URL or name+company).
 *
 * @param {{ linkedinUrl?, firstName?, lastName?, company? }} opts
 * @param {string} apiKey
 * @returns {{ email: string, confidence: number|null }} or null
 */
async function enrichSingle(opts, apiKey) {
  const body = buildContactPayload(opts);

  let res;
  try {
    res = await fetch(`${PROSPEO_BASE}/enrich-person`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-KEY': apiKey,
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(SINGLE_TIMEOUT_MS),
    });
  } catch (err) {
    const e = new Error(`Prospeo network error: ${err.message}`);
    e.statusCode = 0;
    throw e;
  }

  if (res.status === 429) { const e = new Error('Prospeo: rate limited (429)'); e.statusCode = 429; throw e; }
  if (res.status === 402) { const e = new Error('Prospeo: quota exhausted (402)'); e.statusCode = 402; throw e; }
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    const e = new Error(`Prospeo single: HTTP ${res.status} — ${body.slice(0, 200)}`);
    e.statusCode = res.status;
    throw e;
  }

  const data = await res.json();
  return parseResult(data);
}

/**
 * Bulk enrich up to 50 contacts in a single API call.
 *
 * @param {Array<{ linkedinUrl?, firstName?, lastName?, company? }>} contacts  — max 50
 * @param {string} apiKey
 * @returns {Array<{ email, confidence }|null>}  — same length & order as input
 */
async function enrichBulk(contacts, apiKey) {
  if (!contacts || contacts.length === 0) return [];

  const payload = contacts.map(buildContactPayload);

  let res;
  try {
    res = await fetch(`${PROSPEO_BASE}/bulk-enrich-person`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-KEY': apiKey,
      },
      body: JSON.stringify({ contacts: payload }),
      signal: AbortSignal.timeout(BULK_TIMEOUT_MS),
    });
  } catch (err) {
    const e = new Error(`Prospeo bulk network error: ${err.message}`);
    e.statusCode = 0;
    throw e;
  }

  if (res.status === 429) { const e = new Error('Prospeo bulk: rate limited (429)'); e.statusCode = 429; throw e; }
  if (res.status === 402) { const e = new Error('Prospeo bulk: quota exhausted (402)'); e.statusCode = 402; throw e; }
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    const e = new Error(`Prospeo bulk: HTTP ${res.status} — ${body.slice(0, 200)}`);
    e.statusCode = res.status;
    throw e;
  }

  const data = await res.json();

  // Prospeo bulk returns results array in same order as input
  const rawResults = Array.isArray(data) ? data : (data.results || data.data || []);

  // Pad to match input length (defensive)
  while (rawResults.length < contacts.length) rawResults.push(null);

  return rawResults.map(r => parseResult(r));
}

module.exports = { enrichSingle, enrichBulk, buildContactPayload };
