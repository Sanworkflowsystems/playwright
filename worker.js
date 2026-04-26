// worker.js
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const { chromium } = require('playwright');

const jobId = process.argv[2];
const SHEET_MODE = process.argv[3] === '--sheet-mode';
const inputPath  = SHEET_MODE ? null : process.argv[3];
const outputPath = SHEET_MODE ? null : process.argv[4];

if (!jobId) {
  console.error('Usage: node worker.js <jobId> [--sheet-mode | <inputCsv> <outputCsv>]');
  process.exit(2);
}
if (!SHEET_MODE && (!inputPath || !outputPath)) {
  console.error('Usage: node worker.js <jobId> <inputCsv> <outputCsv>');
  process.exit(2);
}

const SHEET_ID   = process.env.SHEET_ID || '';
const SHEET_NAME = process.env.SHEET_NAME || '';

const userDataDir = path.join(__dirname, 'playwright_profile'); // persistent profile
if (!fs.existsSync(userDataDir)) fs.mkdirSync(userDataDir);

const selectorsConfig = process.env.JOB_SELECTORS ? JSON.parse(process.env.JOB_SELECTORS) : {};
const COOKIES_STRING = process.env.JOB_COOKIES || '';
const MANUAL_LOGIN = process.env.JOB_MANUAL === '1';
const JOB_ACCOUNT = process.env.JOB_ACCOUNT || 'none';
const IS_HEADLESS = false; // FOR LOCAL DEBUGGING

// Resolve credentials from env based on selected account
const ACCOUNT_EMAIL = JOB_ACCOUNT === '1' ? process.env.ACCOUNT1_EMAIL
  : JOB_ACCOUNT === '2' ? process.env.ACCOUNT2_EMAIL
  : null;
const ACCOUNT_PASSWORD = JOB_ACCOUNT === '1' ? process.env.ACCOUNT1_PASSWORD
  : JOB_ACCOUNT === '2' ? process.env.ACCOUNT2_PASSWORD
  : null;

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

const personalEmailDomains = [
  'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com', 'icloud.com', 'protonmail.com', 'zoho.com'
];

function randBetween(min, max) { return Math.floor(Math.random() * (max - min + 1)) + min; }

function getDynamicWaitTime() {
  return randBetween(5000, 8000); // 5-8 seconds
}

// Move mouse in a loose arc toward a target element before clicking — feels human
async function humanMouseMove(page, locator) {
  const box = await locator.boundingBox();
  if (!box) return;
  // Target a random spot inside the element (not always dead centre)
  const targetX = box.x + box.width  * (0.3 + Math.random() * 0.4);
  const targetY = box.y + box.height * (0.3 + Math.random() * 0.4);
  // Move in 3-6 small steps with slight random drift
  const steps = randBetween(3, 6);
  const startX = randBetween(200, 800);
  const startY = randBetween(200, 500);
  await page.mouse.move(startX, startY);
  for (let i = 1; i <= steps; i++) {
    const t = i / steps;
    const jitterX = randBetween(-8, 8);
    const jitterY = randBetween(-5, 5);
    await page.mouse.move(
      startX + (targetX - startX) * t + jitterX,
      startY + (targetY - startY) * t + jitterY
    );
    await page.waitForTimeout(randBetween(20, 60));
  }
  await page.mouse.move(targetX, targetY);
}

// Click an element with human-like mouse approach + small pre-click pause
async function humanClick(page, locator) {
  await humanMouseMove(page, locator);
  await page.waitForTimeout(randBetween(80, 200));
  await locator.click();
}

// Type into a field one character at a time with realistic rhythm
async function humanType(page, selector, text) {
  const el = page.locator(selector).first();
  await humanMouseMove(page, el);
  await page.waitForTimeout(randBetween(120, 300));
  // Click to focus, then clear any existing value
  await el.click({ clickCount: 3 }); // triple-click selects all existing text
  await page.waitForTimeout(randBetween(100, 200));
  await page.keyboard.press('Backspace'); // clear selection
  await page.waitForTimeout(randBetween(150, 300));

  for (let i = 0; i < text.length; i++) {
    const char = text[i];

    // ~3% chance of a typo — type a wrong adjacent key then backspace
    if (Math.random() < 0.03 && char.match(/[a-zA-Z]/)) {
      const wrongKey = String.fromCharCode(char.charCodeAt(0) + (Math.random() < 0.5 ? 1 : -1));
      await el.pressSequentially(wrongKey, { delay: randBetween(55, 130) });
      await page.waitForTimeout(randBetween(80, 180));
      await page.keyboard.press('Backspace');
      await page.waitForTimeout(randBetween(100, 250));
    }

    // Slower at word boundaries (@, .), faster in the middle
    const isWordBoundary = char === ' ' || char === '@' || char === '.' || i === 0 || i === text.length - 1;
    const delay = isWordBoundary ? randBetween(120, 220) : randBetween(55, 145);
    await el.pressSequentially(char, { delay });

    // ~7% chance of a brief mid-typing pause
    if (Math.random() < 0.07) {
      await page.waitForTimeout(randBetween(300, 700));
    }
  }
}

// Detect if we're on a login page
function isLoginPage(url) {
  return url.includes('/login') || url.includes('/signin');
}

// Check current page and login if needed — safe to call at any point
async function ensureLoggedIn(page, email, password, jobId) {
  if (!email || !password) return;
  if (isLoginPage(page.url())) {
    console.log('[Auto-login] Login page detected — logging in now.');
    await loginWithGoogle(page, email, password, jobId);
  }
}

// Human-like login using ContactOut's own email/password form
async function loginWithGoogle(page, email, password, jobId) {
  console.log(`[Auto-login] Starting ContactOut form login for: ${email}`);

  // Wait for the email field to be visible and ready
  await page.waitForSelector('input[type="email"]', { state: 'visible', timeout: 15000 });
  console.log('[Auto-login] Login form is visible.');

  // Pause like a person reading the page
  await page.waitForTimeout(randBetween(1200, 2200));

  // Click email field, pause, then type character by character
  await page.click('input[type="email"]');
  await page.waitForTimeout(randBetween(300, 600));
  for (const char of email) {
    await page.type('input[type="email"]', char, { delay: randBetween(60, 150) });
    if (Math.random() < 0.06) await page.waitForTimeout(randBetween(200, 500));
  }
  console.log('[Auto-login] Email typed.');

  // Pause before moving to password
  await page.waitForTimeout(randBetween(700, 1300));

  // Click password field, pause, then type
  await page.click('input[type="password"]');
  await page.waitForTimeout(randBetween(300, 600));
  for (const char of password) {
    await page.type('input[type="password"]', char, { delay: randBetween(60, 150) });
    if (Math.random() < 0.06) await page.waitForTimeout(randBetween(200, 500));
  }
  console.log('[Auto-login] Password typed.');

  // Pause before clicking Login — like double-checking
  await page.waitForTimeout(randBetween(800, 1500));

  // Click the Login button
  await page.click('button[type="submit"]');
  console.log('[Auto-login] Clicked Login. Waiting for redirect...');

  // Wait for redirect — could go to /login/verify/... or straight to dashboard
  await page.waitForTimeout(3000);
  const urlAfterLogin = page.url();
  console.log(`[Auto-login] URL after submit: ${urlAfterLogin}`);

  if (urlAfterLogin.includes('/login/verify')) {
    console.log('[Auto-login] Verification code required. Waiting for code from UI...');
    await handleVerifyCode(page, jobId);
  } else if (urlAfterLogin.includes('/login')) {
    console.log('[Auto-login] Still on login page — credentials may be wrong.');
  } else {
    console.log('[Auto-login] Login successful — redirected to dashboard.');
  }

  await page.waitForTimeout(randBetween(1500, 2500));
  console.log('[Auto-login] Login flow complete.');
}

// Wait for the user to submit a verify code via the UI, then fill it into the 6 boxes
async function handleVerifyCode(page, jobId) {
  const verifySignalPath = path.join(__dirname, 'outputs', `${jobId}.verify`);
  const verifyCodePath   = path.join(__dirname, 'outputs', `${jobId}.verifycode`);

  // Write the signal file so the UI knows to show the code input box
  fs.writeFileSync(verifySignalPath, '');
  console.log('[Verify] Waiting for user to enter code in UI...');

  // Poll for the code file (written by the server when user submits)
  let code = null;
  for (let i = 0; i < 120; i++) { // wait up to 2 minutes
    if (fs.existsSync(verifyCodePath)) {
      code = fs.readFileSync(verifyCodePath, 'utf8').trim();
      fs.unlinkSync(verifyCodePath);
      break;
    }
    await new Promise(r => setTimeout(r, 1000));
  }

  // Remove the signal file regardless
  try { fs.unlinkSync(verifySignalPath); } catch (e) {}

  if (!code || code.length !== 6) {
    console.log('[Verify] No valid code received. Skipping verify step.');
    return;
  }

  console.log(`[Verify] Got code: ${code}. Filling into verify boxes...`);

  // Fill each digit into its own input box
  const boxes = page.locator('input.email-code');
  const count = await boxes.count();
  for (let i = 0; i < Math.min(code.length, count); i++) {
    await boxes.nth(i).click();
    await page.waitForTimeout(randBetween(80, 180));
    await boxes.nth(i).type(code[i], { delay: randBetween(60, 130) });
    await page.waitForTimeout(randBetween(60, 150));
  }

  await page.waitForTimeout(randBetween(500, 900));
  await page.click('#verify');
  console.log('[Verify] Clicked Verify button. Waiting for redirect...');

  try {
    await page.waitForURL(url => !url.includes('/login'), { timeout: 15000 });
    console.log('[Verify] Verification successful.');
  } catch (e) {
    console.log('[Verify] Still on verify page — code may have been wrong.');
  }
}

async function waitForStartSignal(jobId) {
  const signalPath = path.join(__dirname, 'outputs', `${jobId}.start`);
  console.log('Waiting for start signal from UI...');
  while (true) {
    if (fs.existsSync(signalPath)) {
      try { fs.unlinkSync(signalPath); } catch (e) { console.error("Could not remove signal file, continuing...", e); }
      console.log('Start signal received. Starting processing...');
      break;
    }
    await new Promise(resolve => setTimeout(resolve, 1000));
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

function writeProgress(jobId, progress, total) {
  const statusPayload = { progress, total, status: 'running' };
  const statusPath = path.join(__dirname, 'outputs', `${jobId}.json`);
  fs.writeFileSync(statusPath, JSON.stringify(statusPayload));
}

// ---- Google Sheets client (sheet mode only) ----
let sheetsClient = null;
if (SHEET_MODE) {
  const { google } = require('googleapis');
  const { OAuth2Client } = require('google-auth-library');
  const rawCreds = JSON.parse(process.env.GOOGLE_CREDENTIALS);
  const { client_id, client_secret } = rawCreds.installed || rawCreds.web;
  const oauth2Client = new OAuth2Client(client_id, client_secret, 'urn:ietf:wg:oauth:2.0:oob');
  oauth2Client.setCredentials(JSON.parse(process.env.GOOGLE_TOKENS));
  oauth2Client.on('tokens', (tokens) => {
    const existing = process.env.GOOGLE_TOKENS ? JSON.parse(process.env.GOOGLE_TOKENS) : {};
    fs.writeFileSync(path.join(__dirname, 'tokens.json'), JSON.stringify({ ...existing, ...tokens }));
  });
  sheetsClient = google.sheets({ version: 'v4', auth: oauth2Client });
}

const sheetsHelper = SHEET_MODE ? require('./google-sheets-helper') : null;

// Bouncer verifier (sheet mode only) — sequential priority-based finalEmail selection
let verifier = null;
function initVerifier(headers, activeSheetName) {
  if (!SHEET_MODE) return;
  try {
    const { loadConfig } = require('./pipeline/pipeline-config');
    const { createVerifier } = require('./pipeline/contactout-verifier');
    const cfg = loadConfig();
    const bouncerKey = (cfg.bouncer && cfg.bouncer.enabled && (cfg.bouncer.keys || [])[0]) || null;

    const finalEmailIdx = headers.indexOf('finalEmail');
    const finalEmailCol = finalEmailIdx >= 0 ? sheetsHelper.columnLetter(finalEmailIdx) : null;
    if (!finalEmailCol) {
      console.log('[Verifier] finalEmail column not found in sheet — skipping verification.');
      return;
    }

    verifier = createVerifier({
      bouncerKey,
      sheetsClient,
      sheetId: SHEET_ID,
      sheetName: activeSheetName,
      finalEmailCol,
      bouncerEnabled: !!bouncerKey,
    });
    console.log(`[Verifier] Ready. Bouncer ${bouncerKey ? 'enabled' : 'disabled (fallback mode)'} | finalEmail col=${finalEmailCol}`);
  } catch (err) {
    console.error(`[Verifier] Init failed: ${err.message} — continuing without verification.`);
    verifier = null;
  }
}

(async () => {
  let rows, headers;
  let activeSheetName = SHEET_NAME;

  if (SHEET_MODE) {
    if (!activeSheetName) {
      activeSheetName = await sheetsHelper.getFirstSheetName(sheetsClient, SHEET_ID);
    }
    console.log(`Reading data from Google Sheet: ${SHEET_ID} / ${activeSheetName}`);
    ({ rows, headers } = await sheetsHelper.readSheetData(sheetsClient, SHEET_ID, activeSheetName));
    await sheetsHelper.ensureProcessedColumn(sheetsClient, SHEET_ID, activeSheetName, headers, rows.length);
    rows.forEach(row => { if (!row.hasOwnProperty('contactout_processed')) row['contactout_processed'] = '0'; });
  } else {
    ({ rows, headers } = await readCSV(inputPath));
  }

  const processedColIndex = headers.indexOf('contactout_processed');
  const processedColLetter = processedColIndex >= 0 ? sheetsHelper.columnLetter(processedColIndex) : null;

  initVerifier(headers, activeSheetName);

  const browserContextOptions = {
    viewport: { width: 1280, height: 800 },
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36'
  };
  const context = await chromium.launchPersistentContext(userDataDir, {
    headless: IS_HEADLESS,
    args: [
      '--disable-blink-features=AutomationControlled',
      '--no-sandbox',
      '--restore-last-session=false',   // don't restore previous session
      '--no-session-restore',
      '--hide-crash-restore-bubble',    // suppress "restore pages?" bubble
    ],
    ...browserContextOptions
  });

  // Close any extra pages that Chrome opens on restore (about:blank tabs etc.)
  const existingPages = context.pages();
  const page = existingPages.length > 0 ? existingPages[existingPages.length - 1] : await context.newPage();

  // Dismiss any native "restore session" dialog that may appear
  page.on('dialog', async dialog => {
    console.log(`[Browser] Dismissing dialog: ${dialog.message()}`);
    await dialog.dismiss();
  });

  page.setDefaultTimeout(60000);

  if (COOKIES_STRING) {
    const pairs = COOKIES_STRING.split(';').map(s => s.trim()).filter(Boolean);
    const cookies = pairs.map(p => {
      const [name, ...rest] = p.split('=');
      return { name: name.trim(), value: rest.join('=').trim(), domain: '.contactout.com', path: '/' };
    });
    await context.addCookies(cookies);
  }

  const initialUrl = SEARCH_PAGE_URL || 'https://contactout.com/dashboard/search';

  if (ACCOUNT_EMAIL && ACCOUNT_PASSWORD) {
    console.log(`[Auto-login] Navigating to login page...`);
    await page.goto('https://contactout.com/login', { waitUntil: 'domcontentloaded' });
    // Wait for the page to fully settle (dismiss any restore bubble first)
    await page.waitForTimeout(3000);
    const currentUrl = page.url();
    console.log(`[Auto-login] Current URL: ${currentUrl}`);
    if (isLoginPage(currentUrl)) {
      try {
        await loginWithGoogle(page, ACCOUNT_EMAIL, ACCOUNT_PASSWORD, jobId);
      } catch (loginErr) {
        console.error(`[Auto-login] Login threw an error: ${loginErr.message}`);
        console.error(loginErr.stack);
      }
    } else {
      console.log('[Auto-login] Already logged in, navigating to search page.');
    }
    await page.goto(initialUrl, { waitUntil: 'domcontentloaded' });
  } else if (MANUAL_LOGIN) {
    await page.goto(initialUrl, { waitUntil: 'domcontentloaded' });
    await waitForStartSignal(jobId);
  } else {
    await page.goto(initialUrl, { waitUntil: 'domcontentloaded' });
  }

  try {
    console.log('Verifying login status and page readiness...');
    await page.waitForSelector(NAME_INPUT_SELECTOR, { state: 'visible', timeout: 15000 });
    console.log('Login verified. Starting job...');
  } catch (e) {
    // Search form not visible — might have been redirected to login mid-session
    console.log('Pre-flight check failed — checking if login page appeared...');
    await ensureLoggedIn(page, ACCOUNT_EMAIL, ACCOUNT_PASSWORD, jobId);
    await page.goto(initialUrl, { waitUntil: 'domcontentloaded' });
    // One more attempt
    try {
      await page.waitForSelector(NAME_INPUT_SELECTOR, { state: 'visible', timeout: 15000 });
      console.log('Login recovered. Starting job...');
    } catch (e2) {
      const errorHtmlPath = path.join(__dirname, 'outputs', `${jobId}-pre-flight-error.html`);
      fs.writeFileSync(errorHtmlPath, await page.content());
      throw new Error('Login failed or search form not found. Snapshot saved to outputs folder.');
    }
  }

  // CSV writer setup (CSV mode only)
  let csvWriter = null;
  if (!SHEET_MODE) {
    const outputHeaders = headers.map(h => ({ id: h, title: h }));
    csvWriter = createCsvWriter({ path: outputPath, header: outputHeaders });
  }

  const ROW_LIMIT = 400;
  let processedCount = 0;

  for (let i = 0; i < rows.length; i++) {
    if (processedCount >= ROW_LIMIT) {
      console.log(`Row limit of ${ROW_LIMIT} reached. Stopping.`);
      break;
    }

    const row = rows[i];

    // Skip already-processed rows instantly (sheet mode)
    if (SHEET_MODE && row['contactout_processed'] === '1') {
      console.log(`[${i + 1}/${rows.length}] Skipping already-processed row (${row[headers[FULL_NAME_COLUMN_INDEX]] || 'unknown'})`);
      writeProgress(jobId, i + 1, rows.length);
      continue;
    }

    const fullName = row[headers[FULL_NAME_COLUMN_INDEX]] || '';
    const companyName = row[headers[COMPANY_NAME_COLUMN_INDEX]] || '';

    console.log(`[${i + 1}/${rows.length}] Processing Name: ${fullName}, Company: ${companyName}`);
    writeProgress(jobId, i + 1, rows.length);

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
        await page.waitForTimeout(randBetween(500, 1000));
      }

      await page.fill(NAME_INPUT_SELECTOR, fullName);
      try {
        await page.fill(COMPANY_INPUT_SELECTOR, companyName, { timeout: 30000 });
      } catch (e) {
        if (e.name === 'TimeoutError') {
          console.log('Timeout filling company name. Attempting to clear the field and retry.');
          const clearButton = page.locator('div.contactout-select__clear-indicator');
          if (await clearButton.isVisible()) {
            console.log('Found clear button. Clicking to clear company field.');
            await clearButton.click();
            await page.waitForTimeout(randBetween(500, 1000));
            await page.fill(COMPANY_INPUT_SELECTOR, companyName);
          } else {
            console.error('Company field fill timed out, but clear button was not found. The row will likely fail.');
            throw e;
          }
        } else {
          throw e;
        }
      }

      await Promise.all([
        page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 30000 }).catch(e => console.log("No navigation after click, continuing...")),
        page.click(SUBMIT_BUTTON_SELECTOR)
      ]);

      await page.waitForTimeout(randBetween(4000, 7000)); // Wait for results to load

      console.log('Searching for "View email" buttons...');
      const viewEmailButtons = await page.locator('button:has-text("View email")').all();
      console.log(`Found ${viewEmailButtons.length} "View email" buttons.`);

      if (viewEmailButtons.length > 0) {
        try {
          if (await viewEmailButtons[0].isVisible()) {
            await viewEmailButtons[0].click({ timeout: 5000 });
            console.log('Clicked the first "View email" button.');
            await page.waitForTimeout(randBetween(2300, 2600));
          }
        } catch (clickErr) {
          console.log("Could not click the 'View email' button, it might have disappeared or was not clickable.");
        }
      }

      console.log('Searching for "Find phone" buttons...');
      const findPhoneButtons = await page.locator('button.w-\\[79px\\].h-5.rounded-md.text-\\[12px\\].leading-\\[18px\\].font-semibold.bg-\\[\\#F0EEFF\\].ml-3.reveal-btn.css-1oga2ar:has-text("Find phone")').all();
      console.log(`Found ${findPhoneButtons.length} "Find phone" buttons.`);

      if (findPhoneButtons.length > 0) {
        try {
          if (await findPhoneButtons[0].isVisible()) {
            await findPhoneButtons[0].click({ timeout: 5000 });
            console.log('Clicked the first "Find phone" button.');
            await page.waitForTimeout(randBetween(2300, 2600));
          }
        } catch (clickErr) {
          console.log("Could not click the 'Find phone' button, it might have disappeared or was not clickable.");
        }
      }

      await page.waitForTimeout(5000); // Final wait for all content to be revealed

      let extractedEmails = [];
      let extractedPhones = [];

      const infoDivs = await page.locator('div[data-testid="contact-infotext-wrapper"] div.css-bsfhvb').all();
      console.log(`Found ${infoDivs.length} potential contact info divs.`);
      for (const div of infoDivs) {
        const text = await div.innerText();
        if (text.includes('@')) {
          if (!text.includes('*')) {
            console.log(`Found email: ${text}`);
            extractedEmails.push(text);
          }
        } else {
          const phoneRe = /^\+?[0-9\s-()]+$/;
          if (phoneRe.test(text) && !text.includes('*') && text.length > 5) {
            console.log(`Found phone: ${text}`);
            extractedPhones.push(text);
          }
        }
      }

      // Fallback if nothing was found
      if (extractedEmails.length === 0 && extractedPhones.length === 0) {
        if (RESULT_CONTAINER_SELECTOR && await page.locator(RESULT_CONTAINER_SELECTOR).first().isVisible({ timeout: 2000 }).catch(() => false)) {
          console.log('Using fallback regex search...');
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
        if (personalEmailDomains.includes(domain)) {
          personalEmails.push(email);
        } else {
          workEmails.push(email);
        }
      }

      if (row.hasOwnProperty('Personal Email')) row['Personal Email'] = personalEmails.shift() || row['Personal Email'];
      if (row.hasOwnProperty('Other Personal Emails')) row['Other Personal Emails'] = personalEmails.join('; ') || row['Other Personal Emails'];
      if (row.hasOwnProperty('Work Email')) row['Work Email'] = workEmails.shift() || row['Work Email'];
      if (row.hasOwnProperty('Other Work Emails')) row['Other Work Emails'] = workEmails.join('; ') || row['Other Work Emails'];
      if (row.hasOwnProperty('Work Email Status')) row['Work Email Status'] = (workEmails.length > 0 || (row['Work Email'] && row['Work Email'].length > 0)) ? 'Found' : '';
      if (row.hasOwnProperty('Phone Number')) row['Phone Number'] = extractedPhones.shift() || row['Phone Number'];
      if (row.hasOwnProperty('Other Phone Numbers')) row['Other Phone Numbers'] = extractedPhones.join('; ') || row['Other Phone Numbers'];

    } catch (err) {
      console.error('Error processing row', i, err);
      if (row.hasOwnProperty('Notes')) row['Notes'] = err.message.substring(0, 500);
    }

    // Write results
    if (SHEET_MODE) {
      const sheetRowNumber = i + 2; // header = row 1, first data = row 2
      await sheetsHelper.updateSheetRow(sheetsClient, SHEET_ID, activeSheetName, sheetRowNumber, headers, row);
      await sheetsHelper.markRowProcessed(sheetsClient, SHEET_ID, activeSheetName, sheetRowNumber, processedColLetter);
      console.log(`Row ${i + 1} written to Google Sheet.`);

      // Fire-and-forget Bouncer verification → finalEmail (non-blocking, sequential per worker)
      if (verifier) {
        verifier.enqueue(sheetRowNumber, {
          workEmail:            row['Work Email'] || '',
          otherWorkEmails:      row['Other Work Emails'] || '',
          personalEmail:        row['Personal Email'] || '',
          otherPersonalEmails:  row['Other Personal Emails'] || '',
        });
      }
    } else {
      await csvWriter.writeRecords([row]);
    }

    processedCount++;
    const wait = getDynamicWaitTime();
    console.log(`Waiting ${Math.round(wait / 1000)}s before next`);
    await page.waitForTimeout(wait);
  }

  await context.close();

  if (SHEET_MODE) {
    if (verifier) {
      const pending = verifier.stats();
      if (pending.queued > 0 || pending.running) {
        console.log(`[Verifier] Waiting for ${pending.queued} pending verifications to finish...`);
      }
      await verifier.drain();
      const final = verifier.stats();
      console.log(`[Verifier] Done. verified=${final.verified} deliverable=${final.deliverable} rejected=${final.rejected} fallback=${final.fallback} blank=${final.blank} errors=${final.errors}`);
    }
    console.log('Worker finished. Results written to Google Sheet.');
  } else {
    console.log('Worker finished, output saved to', outputPath);
  }

  process.exit(0);
})();
