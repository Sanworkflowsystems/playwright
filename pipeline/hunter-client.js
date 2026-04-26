'use strict';

/**
 * Hunter.io email finder client.
 * GET https://api.hunter.io/v2/email-finder
 *
 * Rate limits: 15 req/s, 500 req/min per key.
 * HTTP 403 = per-second/per-minute rate limit hit.
 * HTTP 429 = monthly quota exhausted.
 */

const HUNTER_BASE = 'https://api.hunter.io/v2';
const TIMEOUT_MS  = 25000; // max_duration=20 + network headroom
const MAX_DURATION = '20'; // gives Hunter more time to refine results

/**
 * Extract the LinkedIn vanity handle from a profile URL.
 * https://linkedin.com/in/john-doe-123 → "john-doe-123"
 * Returns null for VMID-style URLs (all digits/alphanumeric without hyphens = not a vanity slug).
 */
function extractLinkedinHandle(url) {
  if (!url) return null;
  const m = url.match(/linkedin\.com\/in\/([^/?#]+)/i);
  if (!m) return null;
  const handle = m[1].replace(/\/$/, '');
  // VMID handles are pure alphanumeric with no hyphens — Hunter needs vanity slugs
  if (/^[A-Za-z0-9]+$/.test(handle) && handle.length > 20) return null;
  return handle || null;
}

/**
 * Find an email via Hunter.
 *
 * @param {object} opts
 *   firstName, lastName, company, linkedinUrl
 * @param {string} apiKey
 */
async function findEmail(opts, apiKey) {
  const firstName    = (opts.firstName    || '').trim();
  const lastName     = (opts.lastName     || '').trim();
  const company      = (opts.company      || '').trim();
  const linkedinUrl  = (opts.linkedinUrl  || '').trim();

  const linkedinHandle = extractLinkedinHandle(linkedinUrl);

  // Hunter requires: (domain OR company OR linkedin_handle) AND (name OR linkedin_handle)
  const hasIdentifier = company || linkedinHandle;
  const hasName       = (firstName && lastName) || linkedinHandle;
  if (!hasIdentifier || !hasName) return null;

  const params = new URLSearchParams({ api_key: apiKey });

  // Always pass company — Hunter's own resolution is more accurate than our domain guess.
  // Do not pass a guessed domain alongside company: Hunter docs say domain takes precedence
  // and a wrong guess would block Hunter's own lookup.
  if (company)        params.set('company', company);
  if (firstName)      params.set('first_name', firstName);
  if (lastName)       params.set('last_name', lastName);
  if (linkedinHandle) params.set('linkedin_handle', linkedinHandle);

  params.set('max_duration', MAX_DURATION);

  let res;
  try {
    res = await fetch(`${HUNTER_BASE}/email-finder?${params}`, {
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });
  } catch (err) {
    const e = new Error(`Hunter network error: ${err.message}`);
    e.statusCode = 0;
    throw e;
  }

  // 403 = per-second/per-minute rate limit (docs: "You have reached the rate limit")
  if (res.status === 403) {
    const e = new Error('Hunter: rate limited (403)');
    e.statusCode = 429; // normalise to 429 so KeyPool cooldown logic fires
    throw e;
  }
  // 429 = monthly usage quota exhausted
  if (res.status === 429) {
    const e = new Error('Hunter: monthly quota exhausted (429)');
    e.statusCode = 402; // normalise to 402 so KeyPool marks daily-pause
    throw e;
  }
  if (res.status === 404) return null;
  if (res.status === 451) return null; // legal opt-out — treat as no result

  if (!res.ok) {
    const body = await res.text().catch(() => '');
    const e = new Error(`Hunter: HTTP ${res.status} - ${body.slice(0, 200)}`);
    e.statusCode = res.status;
    throw e;
  }

  const data = await res.json();
  const email = data?.data?.email || null;
  if (!email || !email.includes('@')) return null;

  return {
    email:      email.toLowerCase().trim(),
    confidence: data?.data?.score ?? null,
    status:     data?.data?.verification?.status ?? null,
  };
}

module.exports = { findEmail, extractLinkedinHandle };
