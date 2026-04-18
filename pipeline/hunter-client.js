'use strict';

/**
 * Hunter.io email finder client.
 * GET https://api.hunter.io/v2/email-finder
 *
 * Auth: api_key query param
 */

const HUNTER_BASE = 'https://api.hunter.io/v2';
const TIMEOUT_MS = 20000;

// Company name suffixes to strip when deriving domain
const STRIP_SUFFIXES = new Set([
  'inc', 'incorporated', 'llc', 'ltd', 'limited', 'corp', 'corporation',
  'co', 'company', 'group', 'holdings', 'international', 'intl',
  'technologies', 'technology', 'tech', 'solutions', 'services', 'global',
  'consulting', 'partners', 'ventures', 'enterprises', 'systems', 'software',
  'digital', 'media', 'agency', 'studio', 'labs', 'lab', 'ai', 'io',
  'gmbh', 'ag', 'bv', 'srl', 'sarl', 'sa', 'pty', 'pvt',
]);

/**
 * Derive a best-guess domain from a company name.
 * e.g. "EMCD Tech Ltd." → "emcdtech.com"
 *      "Workflow Systems Inc" → "workflowsystems.com"
 * Returns null if no valid domain can be derived.
 */
function companyToDomain(company) {
  if (!company || typeof company !== 'string') return null;

  // Remove everything after common separators (parens, pipes, slashes)
  let cleaned = company.split(/[|()/\\–—]/)[0];

  // Lowercase, remove non-alphanumeric except spaces
  cleaned = cleaned.toLowerCase().replace(/[^a-z0-9\s]/g, '').trim();

  if (!cleaned) return null;

  // Split into words, remove trailing suffixes
  const words = cleaned.split(/\s+/).filter(Boolean);
  while (words.length > 1 && STRIP_SUFFIXES.has(words[words.length - 1])) {
    words.pop();
  }

  const domain = words.join('');
  if (!domain || domain.length < 2) return null;

  return `${domain}.com`;
}

/**
 * Find an email for a person at a given domain.
 *
 * @param {string} firstName
 * @param {string} lastName
 * @param {string} domain   — e.g. "acme.com"
 * @param {string} apiKey
 * @returns {{ email: string, confidence: number|null }} or null
 */
async function findEmail(firstName, lastName, domain, apiKey) {
  if (!domain) return null;

  const params = new URLSearchParams({
    first_name: firstName || '',
    last_name:  lastName  || '',
    domain,
    api_key: apiKey,
  });

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

  if (res.status === 429) { const e = new Error('Hunter: rate limited (429)'); e.statusCode = 429; throw e; }
  if (res.status === 402) { const e = new Error('Hunter: quota exhausted (402)'); e.statusCode = 402; throw e; }
  if (res.status === 404) return null;  // no result found — not an error

  if (!res.ok) {
    const body = await res.text().catch(() => '');
    const e = new Error(`Hunter: HTTP ${res.status} — ${body.slice(0, 200)}`);
    e.statusCode = res.status;
    throw e;
  }

  const data = await res.json();
  const email = data?.data?.email || null;
  if (!email || !email.includes('@')) return null;

  return {
    email: email.toLowerCase().trim(),
    confidence: data?.data?.score ?? null,
  };
}

module.exports = { findEmail, companyToDomain };
