// parallel-worker.js
// Invocation: node parallel-worker.js <batchId>
// Env vars: BATCH_JOBS (JSON array), JOB_SELECTORS, JOB_COOKIES, JOB_MANUAL
//           BATCH_SHEET_MODE, GOOGLE_TOKENS, GOOGLE_CREDENTIALS (sheet mode)
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const { chromium } = require('playwright');

const batchId = process.argv[2];
if (!batchId) {
  console.error('Usage: node parallel-worker.js <batchId>');
  process.exit(2);
}

const jobSpecsRaw = process.env.BATCH_JOBS;
if (!jobSpecsRaw) {
  console.error('BATCH_JOBS env var is required');
  process.exit(2);
}

const jobSpecs = JSON.parse(jobSpecsRaw);
const selectorsConfig = process.env.JOB_SELECTORS ? JSON.parse(process.env.JOB_SELECTORS) : {};
const COOKIES_STRING = process.env.JOB_COOKIES || '';
const MANUAL_LOGIN = process.env.JOB_MANUAL === '1';
const IS_HEADLESS = false;
const SHEET_MODE = process.env.BATCH_SHEET_MODE === '1';

const {
  FULL_NAME_COLUMN_INDEX,
  COMPANY_NAME_COLUMN_INDEX,
  SEARCH_PAGE_URL,
  NAME_INPUT_SELECTOR,
  COMPANY_INPUT_SELECTOR,
  SUBMIT_BUTTON_SELECTOR,
  RESULT_CONTAINER_SELECTOR,
  EMAIL_ITEM_SELECTOR,
  PHONE_REVEAL_BUTTON_SELECTOR,
  PHONE_ITEM_SELECTOR
} = selectorsConfig;

const userDataDir = path.join(__dirname, 'playwright_profile');
if (!fs.existsSync(userDataDir)) fs.mkdirSync(userDataDir);

const personalEmailDomains = [
  'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com', 'icloud.com', 'protonmail.com', 'zoho.com'
];

function randBetween(min, max) { return Math.floor(Math.random() * (max - min + 1)) + min; }

function getDynamicWaitTime() {
  return randBetween(3000, 5000);
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function writeBatchProgress(batchId, allJobStates) {
  const batchPath = path.join(__dirname, 'outputs', `${batchId}.json`);
  const jobs = Object.entries(allJobStates).map(([jobId, state]) => ({ jobId, ...state }));
  const allTerminal = jobs.every(j => ['finished', 'stopped', 'error'].includes(j.status));
  const overallStatus = allTerminal ? 'finished' : 'running';
  fs.writeFileSync(batchPath, JSON.stringify({ batchId, status: overallStatus, jobs }));
}

function writeJobProgress(spec, status, progress, total, error) {
  const payload = { status, progress, total };
  if (error) payload.error = error;
  fs.writeFileSync(spec.statusPath, JSON.stringify(payload));
}

async function waitForStartSignal(batchId) {
  const signalPath = path.join(__dirname, 'outputs', `${batchId}.start`);
  console.log(`[batch:${batchId}] Waiting for start signal from UI...`);
  while (true) {
    if (fs.existsSync(signalPath)) {
      try { fs.unlinkSync(signalPath); } catch (e) { /* ignore */ }
      console.log(`[batch:${batchId}] Start signal received.`);
      break;
    }
    await delay(1000);
  }
}

async function readCSV(file) {
  return new Promise((res, rej) => {
    const rows = [];
    const stream = fs.createReadStream(file).pipe(csv());
    stream.on('data', data => rows.push(data));
    stream.on('end', () => res({ rows, headers: stream.headers }));
    stream.on('error', err => rej(err));
  });
}

// ---- Google Sheets client (sheet mode only) ----
let sheetsClient = null;
const sheetsHelper = SHEET_MODE ? require('./google-sheets-helper') : null;

if (SHEET_MODE) {
  const { google } = require('googleapis');
  const { OAuth2Client } = require('google-auth-library');
  const rawCreds = JSON.parse(process.env.GOOGLE_CREDENTIALS);
  const { client_id, client_secret } = rawCreds.installed || rawCreds.web;
  const oauth2Client = new OAuth2Client(client_id, client_secret, 'http://localhost:3000/auth/google/callback');
  oauth2Client.setCredentials(JSON.parse(process.env.GOOGLE_TOKENS));
  oauth2Client.on('tokens', (tokens) => {
    const existing = process.env.GOOGLE_TOKENS ? JSON.parse(process.env.GOOGLE_TOKENS) : {};
    fs.writeFileSync(path.join(__dirname, 'tokens.json'), JSON.stringify({ ...existing, ...tokens }));
  });
  sheetsClient = google.sheets({ version: 'v4', auth: oauth2Client });
}

// Process a single job on a given page
async function processJob(page, spec, batchId, allJobStates) {
  const { jobId, originalFilename } = spec;
  const displayName = SHEET_MODE ? (spec.sheetName || spec.sheetId) : originalFilename;
  console.log(`[job:${jobId}] Starting processing of ${displayName}`);

  let rows, headers;
  try {
    if (SHEET_MODE) {
      ({ rows, headers } = await sheetsHelper.readSheetData(sheetsClient, spec.sheetId, spec.sheetName));
      await sheetsHelper.ensureProcessedColumn(sheetsClient, spec.sheetId, spec.sheetName, headers, rows.length);
      rows.forEach(row => { if (!row.hasOwnProperty('contactout_processed')) row['contactout_processed'] = '0'; });
    } else {
      ({ rows, headers } = await readCSV(spec.inputPath));
    }
  } catch (e) {
    console.error(`[job:${jobId}] Failed to read data: ${e.message}`);
    allJobStates[jobId].status = 'error';
    allJobStates[jobId].error = `Failed to read data: ${e.message}`;
    writeJobProgress(spec, 'error', 0, 0, allJobStates[jobId].error);
    writeBatchProgress(batchId, allJobStates);
    return;
  }

  const processedColIndex = headers.indexOf('contactout_processed');
  const processedColLetter = processedColIndex >= 0 ? sheetsHelper?.columnLetter(processedColIndex) : null;

  allJobStates[jobId].total = rows.length;
  writeJobProgress(spec, 'running', 0, rows.length);
  writeBatchProgress(batchId, allJobStates);

  let csvWriter = null;
  if (!SHEET_MODE) {
    const outputHeaders = headers.map(h => ({ id: h, title: h }));
    csvWriter = createCsvWriter({ path: spec.outputPath, header: outputHeaders });
  }

  for (let i = 0; i < rows.length; i++) {
    if (allJobStates[jobId].status === 'stopped') {
      console.log(`[job:${jobId}] Job was stopped at row ${i}.`);
      break;
    }

    const row = rows[i];

    // Skip already-processed rows instantly (sheet mode)
    if (SHEET_MODE && row['contactout_processed'] === '1') {
      console.log(`[job:${jobId}] [${i + 1}/${rows.length}] Skipping already-processed row (${row[headers[FULL_NAME_COLUMN_INDEX]] || 'unknown'})`);
      allJobStates[jobId].progress = i + 1;
      writeJobProgress(spec, 'running', i + 1, rows.length);
      writeBatchProgress(batchId, allJobStates);
      continue;
    }

    const fullName = row[headers[FULL_NAME_COLUMN_INDEX]] || '';
    const companyName = row[headers[COMPANY_NAME_COLUMN_INDEX]] || '';

    console.log(`[job:${jobId}] [${i + 1}/${rows.length}] Name: ${fullName}, Company: ${companyName}`);
    allJobStates[jobId].progress = i + 1;
    writeJobProgress(spec, 'running', i + 1, rows.length);
    writeBatchProgress(batchId, allJobStates);

    try {
      if (i > 0) {
        const clearAllButton = page.locator('div.contactout-select__clear-indicator');
        if (await clearAllButton.isVisible({ timeout: 1000 }).catch(() => false)) {
          await clearAllButton.click();
          await page.waitForTimeout(randBetween(150, 300));
        } else {
          let removeBtn = page.locator('div.contactout-select__multi-value__remove').first();
          while (await removeBtn.isVisible({ timeout: 500 }).catch(() => false)) {
            await removeBtn.click();
            await page.waitForTimeout(150);
          }
        }
        const nameInput = page.locator(NAME_INPUT_SELECTOR);
        const nameValue = await nameInput.inputValue();
        if (nameValue) {
          await nameInput.click();
          for (let k = 0; k < nameValue.length; k++) {
            await page.keyboard.press('Backspace');
            await page.waitForTimeout(randBetween(20, 50));
          }
        }
        await page.waitForTimeout(randBetween(300, 600));
      }

      await page.fill(NAME_INPUT_SELECTOR, fullName);
      try {
        await page.fill(COMPANY_INPUT_SELECTOR, companyName, { timeout: 30000 });
      } catch (e) {
        if (e.name === 'TimeoutError') {
          console.log(`[job:${jobId}] Timeout filling company name. Attempting to clear and retry.`);
          const clearButton = page.locator('div.contactout-select__clear-indicator');
          if (await clearButton.isVisible()) {
            await clearButton.click();
            await page.waitForTimeout(randBetween(300, 600));
            await page.fill(COMPANY_INPUT_SELECTOR, companyName);
          } else {
            throw e;
          }
        } else {
          throw e;
        }
      }

      await Promise.all([
        page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {}),
        page.click(SUBMIT_BUTTON_SELECTOR)
      ]);

      await page.waitForTimeout(randBetween(2000, 4000));

      console.log(`[job:${jobId}] Searching for "View email" buttons...`);
      const viewEmailButtons = await page.locator('button:has-text("View email")').all();
      if (viewEmailButtons.length > 0) {
        try {
          if (await viewEmailButtons[0].isVisible()) {
            await viewEmailButtons[0].click({ timeout: 5000 });
            await page.waitForTimeout(randBetween(300, 600));
          }
        } catch (clickErr) {
          console.log(`[job:${jobId}] Could not click "View email" button.`);
        }
      }

      console.log(`[job:${jobId}] Searching for "Find phone" buttons...`);
      const findPhoneButtons = await page.locator('button.w-\\[79px\\].h-5.rounded-md.text-\\[12px\\].leading-\\[18px\\].font-semibold.bg-\\[\\#F0EEFF\\].ml-3.reveal-btn.css-1oga2ar:has-text("Find phone")').all();
      if (findPhoneButtons.length > 0) {
        try {
          if (await findPhoneButtons[0].isVisible()) {
            await findPhoneButtons[0].click({ timeout: 5000 });
            await page.waitForTimeout(randBetween(300, 600));
          }
        } catch (clickErr) {
          console.log(`[job:${jobId}] Could not click "Find phone" button.`);
        }
      }

      await page.waitForTimeout(3000);

      let extractedEmails = [];
      let extractedPhones = [];

      const firstCard = page.locator('div[data-testid="contact-infotext-wrapper"]').first();
      const infoDivs = await firstCard.locator('div.css-bsfhvb').all();
      for (const div of infoDivs) {
        const text = await div.innerText();
        if (text.includes('@')) {
          if (!text.includes('*')) extractedEmails.push(text);
        } else {
          const phoneRe = /^\+?[0-9\s\-()]+$/;
          if (phoneRe.test(text) && !text.includes('*') && text.length > 5) {
            extractedPhones.push(text);
          }
        }
      }

      if (extractedEmails.length === 0 && extractedPhones.length === 0) {
        if (RESULT_CONTAINER_SELECTOR && await page.locator(RESULT_CONTAINER_SELECTOR).first().isVisible({ timeout: 2000 }).catch(() => false)) {
          const raw = await page.locator(RESULT_CONTAINER_SELECTOR).first().innerText();
          const emailRe = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-z]{2,}/g;
          extractedEmails = (raw.match(emailRe) || []).filter(e => !e.includes('*'));
          const phoneRe = /(?:\+?\d{1,3})?[-.\s]?\(?\d{1,4}\)?[-.\s]?\d{1,4}[-.\s]?\d{1,9}/g;
          extractedPhones = (raw.match(phoneRe) || []).filter(p => !p.includes('*'));
        }
      }

      const personalEmails = [];
      const workEmails = [];
      for (const email of extractedEmails) {
        const domain = email.split('@')[1];
        if (personalEmailDomains.includes(domain)) personalEmails.push(email);
        else workEmails.push(email);
      }

      if (row.hasOwnProperty('Personal Email')) row['Personal Email'] = personalEmails.shift() || row['Personal Email'];
      if (row.hasOwnProperty('Other Personal Emails')) row['Other Personal Emails'] = personalEmails.join('; ') || row['Other Personal Emails'];
      if (row.hasOwnProperty('Work Email')) row['Work Email'] = workEmails.shift() || row['Work Email'];
      if (row.hasOwnProperty('Other Work Emails')) row['Other Work Emails'] = workEmails.join('; ') || row['Other Work Emails'];
      if (row.hasOwnProperty('Work Email Status')) row['Work Email Status'] = (workEmails.length > 0 || (row['Work Email'] && row['Work Email'].length > 0)) ? 'Found' : '';
      if (row.hasOwnProperty('Phone Number')) row['Phone Number'] = extractedPhones.shift() || row['Phone Number'];
      if (row.hasOwnProperty('Other Phone Numbers')) row['Other Phone Numbers'] = extractedPhones.join('; ') || row['Other Phone Numbers'];

    } catch (err) {
      console.error(`[job:${jobId}] Error processing row ${i}:`, err.message);
      if (row.hasOwnProperty('Notes')) row['Notes'] = err.message.substring(0, 500);
    }

    // Write results
    if (SHEET_MODE) {
      const sheetRowNumber = i + 2;
      await sheetsHelper.updateSheetRow(sheetsClient, spec.sheetId, spec.sheetName, sheetRowNumber, headers, row);
      await sheetsHelper.markRowProcessed(sheetsClient, spec.sheetId, spec.sheetName, sheetRowNumber, processedColLetter);
      console.log(`[job:${jobId}] Row ${i + 1} written to Google Sheet.`);
    } else {
      await csvWriter.writeRecords([row]);
    }

    const wait = getDynamicWaitTime();
    console.log(`[job:${jobId}] Waiting ${Math.round(wait / 1000)}s before next row`);
    await page.waitForTimeout(wait);
  }

  if (allJobStates[jobId].status !== 'stopped') {
    allJobStates[jobId].status = 'finished';
    writeJobProgress(spec, 'finished', rows.length, rows.length);
    writeBatchProgress(batchId, allJobStates);
    console.log(`[job:${jobId}] Finished.`);
  }
}

// ---- Main ----
let context = null;

process.on('SIGTERM', async () => {
  console.log(`[batch:${batchId}] SIGTERM received. Stopping all jobs...`);
  const allJobStates = buildInitialStates();
  for (const spec of jobSpecs) {
    if (['running', 'pending', 'queued'].includes(allJobStates[spec.jobId]?.status)) {
      allJobStates[spec.jobId].status = 'stopped';
      try {
        const existing = fs.existsSync(spec.statusPath)
          ? JSON.parse(fs.readFileSync(spec.statusPath, 'utf8'))
          : {};
        fs.writeFileSync(spec.statusPath, JSON.stringify({ ...existing, status: 'stopped' }));
      } catch (e) {
        fs.writeFileSync(spec.statusPath, JSON.stringify({ status: 'stopped' }));
      }
    }
  }
  writeBatchProgress(batchId, allJobStates);
  if (context) {
    try { await context.close(); } catch (e) { /* ignore */ }
  }
  process.exit(0);
});

function buildInitialStates() {
  const states = {};
  for (const spec of jobSpecs) {
    states[spec.jobId] = {
      status: 'pending',
      progress: 0,
      total: 0,
      originalFilename: spec.originalFilename || spec.sheetName,
      slotIndex: spec.slotIndex
    };
  }
  return states;
}

(async () => {
  const allJobStates = buildInitialStates();
  writeBatchProgress(batchId, allJobStates);

  const browserContextOptions = {
    viewport: { width: 1280, height: 800 },
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36'
  };

  context = await chromium.launchPersistentContext(userDataDir, {
    headless: IS_HEADLESS,
    args: ['--disable-blink-features=AutomationControlled', '--no-sandbox'],
    ...browserContextOptions
  });

  if (COOKIES_STRING) {
    const pairs = COOKIES_STRING.split(';').map(s => s.trim()).filter(Boolean);
    const cookies = pairs.map(p => {
      const [name, ...rest] = p.split('=');
      return { name: name.trim(), value: rest.join('=').trim(), domain: '.contactout.com', path: '/' };
    });
    await context.addCookies(cookies);
  }

  const pages = [];
  const initialUrl = SEARCH_PAGE_URL || 'https://contactout.com/dashboard/search';
  for (let i = 0; i < jobSpecs.length; i++) {
    if (i > 0) await delay(1000);
    const p = await context.newPage();
    p.setDefaultTimeout(60000);
    await p.goto(initialUrl, { waitUntil: 'domcontentloaded' });
    pages.push(p);
  }

  if (MANUAL_LOGIN) {
    await waitForStartSignal(batchId);
  }

  if (pages.length > 1) {
    console.log(`[batch:${batchId}] Re-navigating ${pages.length - 1} additional tab(s) to search page after login...`);
    for (let i = 1; i < pages.length; i++) {
      await pages[i].goto(initialUrl, { waitUntil: 'domcontentloaded' });
      await delay(500);
    }
  }

  try {
    console.log(`[batch:${batchId}] Verifying login status...`);
    await pages[0].waitForSelector(NAME_INPUT_SELECTOR, { state: 'visible', timeout: 15000 });
    console.log(`[batch:${batchId}] Login verified.`);
  } catch (e) {
    console.error(`[batch:${batchId}] Pre-flight check failed. Search form not visible.`);
    const errorHtmlPath = path.join(__dirname, 'outputs', `${batchId}-pre-flight-error.html`);
    fs.writeFileSync(errorHtmlPath, await pages[0].content());
    const errorMsg = 'Login failed or search form not found. Please check your cookies or manual login.';
    for (const spec of jobSpecs) {
      allJobStates[spec.jobId].status = 'error';
      allJobStates[spec.jobId].error = errorMsg;
      writeJobProgress(spec, 'error', 0, 0, errorMsg);
    }
    writeBatchProgress(batchId, allJobStates);
    await context.close();
    process.exit(1);
  }

  for (const spec of jobSpecs) {
    allJobStates[spec.jobId].status = 'running';
  }

  const jobPromises = jobSpecs.map((spec, i) =>
    delay(i * 5000).then(() => processJob(pages[i], spec, batchId, allJobStates))
  );

  await Promise.allSettled(jobPromises);

  writeBatchProgress(batchId, allJobStates);
  await context.close();
  console.log(`[batch:${batchId}] All jobs complete.`);
  process.exit(0);
})();
