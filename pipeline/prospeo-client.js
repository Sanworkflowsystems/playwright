'use strict';

/**
 * Prospeo enrichment client.
 *
 * Single: POST https://api.prospeo.io/enrich-person
 * Bulk:   POST https://api.prospeo.io/bulk-enrich-person
 *
 * API notes:
 * - Bulk accepts up to 50 people per request.
 * - Bulk requires an identifier on each input record so results can be mapped
 *   back to the original row.
 * - Send every reliable datapoint we have. Prospeo explicitly recommends
 *   linkedin_url plus company_website/company_linkedin_url for accuracy.
 * - only_verified_email=true prevents spending credits on unverified emails.
 */

const PROSPEO_BASE = 'https://api.prospeo.io';
const SINGLE_TIMEOUT_MS = 20000;
const BULK_TIMEOUT_MS = 90000;

function cleanString(value) {
  return value == null ? '' : String(value).trim();
}

function normalizeWebsite(value) {
  const raw = cleanString(value);
  if (!raw || raw.includes('linkedin.com')) return '';
  return raw.replace(/^https?:\/\//i, '').replace(/^www\./i, '').replace(/\/.*$/, '');
}

// Prospeo only accepts slug-based company URLs, not numeric company IDs.
// /company/68912117 → rejected (INVALID_DATAPOINTS); /company/snitch-com → accepted.
function isSlugCompanyUrl(url) {
  if (!url) return false;
  const match = url.match(/\/company\/([^/?#]+)/);
  if (!match) return false;
  return /[a-zA-Z]/.test(match[1]);
}

function hasMinimumDatapoints(payload) {
  if (payload.linkedin_url || payload.email || payload.person_id) return true;
  if (payload.first_name && payload.last_name && (payload.company_name || payload.company_website || payload.company_linkedin_url)) return true;
  if (payload.full_name && (payload.company_name || payload.company_website || payload.company_linkedin_url)) return true;
  return false;
}

function buildContactPayload(opts = {}, identifier) {
  const payload = {};

  if (identifier !== undefined) payload.identifier = String(identifier);

  const linkedinUrl = cleanString(opts.linkedinUrl);
  const companyLinkedinUrl = cleanString(opts.companyLinkedinUrl);
  const companyWebsite = normalizeWebsite(opts.companyWebsite);
  const firstName = cleanString(opts.firstName);
  const lastName = cleanString(opts.lastName);
  const fullName = cleanString(opts.fullName || [firstName, lastName].filter(Boolean).join(' '));
  const company = cleanString(opts.company);
  const email = cleanString(opts.email);

  // Only send proper public /in/ profile URLs — normalize to www. and reject Sales Navigator
  const normalizedLinkedin = linkedinUrl
    ? linkedinUrl.replace(/^https?:\/\/(www\.)?linkedin\.com/i, 'https://www.linkedin.com')
    : '';
  if (normalizedLinkedin && normalizedLinkedin.includes('/in/')) {
    payload.linkedin_url = normalizedLinkedin;
  }
  if (email && email.includes('@')) payload.email = email;
  if (firstName) payload.first_name = firstName;
  if (lastName) payload.last_name = lastName;
  if (fullName) payload.full_name = fullName;
  if (company) payload.company_name = company;
  if (companyWebsite) payload.company_website = companyWebsite;
  if (companyLinkedinUrl && companyLinkedinUrl.includes('linkedin.com') && isSlugCompanyUrl(companyLinkedinUrl)) {
    payload.company_linkedin_url = companyLinkedinUrl;
  }

  return payload;
}

function parseResult(raw) {
  if (!raw) return null;

  const emailObj = raw?.person?.email || raw?.email || null;
  const email = cleanString(emailObj?.email || emailObj?.value || raw?.email);

  if (!email || !email.includes('@') || email.includes('*')) return null;

  return {
    email: email.toLowerCase(),
    confidence: emailObj?.confidence ?? null,
    status: emailObj?.status ?? null,
  };
}

async function parseErrorResponse(res, label) {
  let body = '';
  try {
    body = await res.text();
  } catch (_) {
    body = '';
  }

  let errorCode = '';
  try {
    errorCode = JSON.parse(body).error_code || '';
  } catch (_) {
    errorCode = '';
  }

  const e = new Error(`${label}: HTTP ${res.status}${errorCode ? ` ${errorCode}` : ''}${body ? ` - ${body.slice(0, 200)}` : ''}`);
  e.statusCode = res.status;
  e.errorCode = errorCode;
  throw e;
}

async function enrichSingle(opts, apiKey) {
  const payload = buildContactPayload(opts);
  if (!hasMinimumDatapoints(payload)) return null;

  let res;
  try {
    res = await fetch(`${PROSPEO_BASE}/enrich-person`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-KEY': apiKey },
      body: JSON.stringify({
        only_verified_email: true,
        enrich_mobile: false,
        data: payload,
      }),
      signal: AbortSignal.timeout(SINGLE_TIMEOUT_MS),
    });
  } catch (err) {
    const e = new Error(`Prospeo network error: ${err.message}`);
    e.statusCode = 0;
    throw e;
  }

  if (res.status === 429) {
    const e = new Error('Prospeo: rate limited (429)');
    e.statusCode = 429;
    throw e;
  }
  if (res.status === 401) {
    const e = new Error('Prospeo: invalid API key (401)');
    e.statusCode = 401;
    throw e;
  }
  if (res.status === 402) {
    const e = new Error('Prospeo: insufficient credits/quota exhausted (402)');
    e.statusCode = 402;
    throw e;
  }
  if (!res.ok) {
    let body = '';
    try { body = await res.text(); } catch (_) {}
    let parsed = {};
    try { parsed = JSON.parse(body); } catch (_) {}
    const errorCode = parsed.error_code || '';
    // NO_MATCH / INVALID_DATAPOINTS = expected "no result" 400s — treat as no-match
    if (res.status === 400 && parsed.error === true) return null;
    const e = new Error(`Prospeo single: HTTP ${res.status}${errorCode ? ` ${errorCode}` : ''} - ${body.slice(0, 200)}`);
    e.statusCode = res.status;
    e.errorCode = errorCode;
    throw e;
  }

  const data = await res.json();
  if (data.error) return null;
  return parseResult(data);
}

async function enrichBulk(contacts, apiKey) {
  if (!Array.isArray(contacts) || contacts.length === 0) return [];
  if (contacts.length > 50) {
    throw new Error(`Prospeo bulk accepts max 50 contacts; got ${contacts.length}`);
  }

  const payload = contacts.map((opts, idx) => buildContactPayload(opts, idx));
  const invalidIndexes = new Set();
  const validPayload = payload.filter((item, idx) => {
    const valid = hasMinimumDatapoints(item);
    if (!valid) invalidIndexes.add(idx);
    return valid;
  });

  if (validPayload.length === 0) return contacts.map(() => null);

  let res;
  try {
    res = await fetch(`${PROSPEO_BASE}/bulk-enrich-person`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-KEY': apiKey },
      body: JSON.stringify({
        only_verified_email: true,
        enrich_mobile: false,
        data: validPayload,
      }),
      signal: AbortSignal.timeout(BULK_TIMEOUT_MS),
    });
  } catch (err) {
    const e = new Error(`Prospeo bulk network error: ${err.message}`);
    e.statusCode = 0;
    throw e;
  }

  if (res.status === 429) {
    const e = new Error('Prospeo bulk: rate limited (429)');
    e.statusCode = 429;
    throw e;
  }
  if (res.status === 401) {
    const e = new Error('Prospeo bulk: invalid API key (401)');
    e.statusCode = 401;
    throw e;
  }
  if (res.status === 402) {
    const e = new Error('Prospeo bulk: insufficient credits/quota exhausted (402)');
    e.statusCode = 402;
    throw e;
  }
  if (!res.ok) await parseErrorResponse(res, 'Prospeo bulk');

  const data = await res.json();
  if (data.error) {
    const e = new Error(`Prospeo bulk API error: ${data.error_code || 'unknown'}`);
    e.statusCode = 400;
    e.errorCode = data.error_code || '';
    throw e;
  }

  const byIdentifier = new Map();
  for (const match of data.matched || []) {
    const parsed = parseResult(match);
    if (parsed) byIdentifier.set(String(match.identifier), parsed);
  }

  return contacts.map((_, idx) => {
    if (invalidIndexes.has(idx)) return null;
    return byIdentifier.get(String(idx)) || null;
  });
}

module.exports = {
  enrichSingle,
  enrichBulk,
  buildContactPayload,
  hasMinimumDatapoints,
};
