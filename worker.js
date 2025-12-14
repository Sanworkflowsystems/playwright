// worker.js
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const { chromium } = require('playwright');

const jobId = process.argv[2];
const inputPath = process.argv[3];
const outputPath = process.argv[4];

if(!jobId || !inputPath || !outputPath) {
  console.error('Usage: node worker.js <jobId> <inputCsv> <outputCsv>');
  process.exit(2);
}

const userDataDir = path.join(__dirname, 'playwright_profile'); // persistent profile
if(!fs.existsSync(userDataDir)) fs.mkdirSync(userDataDir);

const selectorsConfig = process.env.JOB_SELECTORS ? JSON.parse(process.env.JOB_SELECTORS) : {};
const COOKIES_STRING = process.env.JOB_COOKIES || ''; // optional cookie string
const MANUAL_LOGIN = process.env.JOB_MANUAL === '1';
const IS_HEADLESS = process.env.HEADLESS_MODE === 'true';

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
  // Wait for 5-10 seconds
  return randBetween(5000, 10000);
}

// New function to wait for a file signal from the UI
async function waitForStartSignal(jobId) {
    const signalPath = path.join(__dirname, 'outputs', `${jobId}.start`);
    console.log('Waiting for start signal from UI...');
    while (true) {
        if (fs.existsSync(signalPath)) {
            try {
                fs.unlinkSync(signalPath); // Clean up the signal file
            } catch (e) {
                console.error("Could not remove signal file, continuing...", e);
            }
            console.log('Start signal received. Starting processing...');
            break;
        }
        await new Promise(resolve => setTimeout(resolve, 1000)); // Check every second
    }
}

// Updated to return headers
async function readCSV(file) {
  return new Promise((res, rej) => {
    const rows = [];
    const stream = fs.createReadStream(file).pipe(csv());
    stream.on('data', data => rows.push(data));
    stream.on('end', () => res({rows: rows, headers: stream.headers}));
    stream.on('error', err => rej(err));
  });
}

function writeProgress(jobId, progress, total) {
    const statusPayload = { progress, total, status: 'running' };
    const statusPath = path.join(__dirname, 'outputs', `${jobId}.json`);
    fs.writeFileSync(statusPath, JSON.stringify(statusPayload));
}

(async () => {
  const { rows, headers } = await readCSV(inputPath);

  // 1. Initialize the csvWriter before the loop
  const outputHeaders = headers.map(h => ({id: h, title: h}));
  const csvWriter = createCsvWriter({
    path: outputPath,
    header: outputHeaders
  });

  const browserContextOptions = {
    viewport: { width:1280, height:800 },
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36'
  };
  const context = await chromium.launchPersistentContext(userDataDir, {
    headless: IS_HEADLESS,
    args: ['--disable-blink-features=AutomationControlled', '--no-sandbox'],
    ...browserContextOptions
  });
  const page = await context.newPage();
  page.setDefaultTimeout(60000);

  if (COOKIES_STRING) {
    const pairs = COOKIES_STRING.split(';').map(s => s.trim()).filter(Boolean);
    const cookies = pairs.map(p => {
      const [name, ...rest] = p.split('=');
      return { name: name.trim(), value: rest.join('=').trim(), domain: '.contactout.com', path: '/' };
    }
    );
    await context.addCookies(cookies);
  }
  const initialUrl = SEARCH_PAGE_URL || 'https://contactout.com/dashboard/search';
  await page.goto(initialUrl, { waitUntil: 'domcontentloaded' });
  if (MANUAL_LOGIN) {
    await waitForStartSignal(jobId);
  }
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    const fullName = row[headers[FULL_NAME_COLUMN_INDEX]] || '';
    const companyName = row[headers[COMPANY_NAME_COLUMN_INDEX]] || '';

    console.log(`[${i + 1}/${rows.length}] processing Name: ${fullName}, Company: ${companyName}`);
    writeProgress(jobId, i + 1, rows.length);

    try {
      if (i > 0) {
        const removeTagSelector = "div.contactout-select__multi-value__remove";
        const removeTagButton = page.locator(removeTagSelector);
        if (await removeTagButton.isVisible({ timeout: 1000 }).catch(() => false)) {
            await removeTagButton.click();
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

      // Fill in search inputs
      await page.fill(NAME_INPUT_SELECTOR, fullName);
      await page.fill(COMPANY_INPUT_SELECTOR, companyName);

      await Promise.all([
        page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 30000 }).catch(e => console.log("No navigation after click, continuing...")),
        page.click(SUBMIT_BUTTON_SELECTOR)
      ]);

      await page.waitForTimeout(randBetween(2500, 4000));

      let extractedEmails = [];
      let extractedPhones = [];

      if (EMAIL_ITEM_SELECTOR && await page.locator(EMAIL_ITEM_SELECTOR).first().isVisible({ timeout: 5000 }).catch(() => false)) {
        extractedEmails = await page.locator(EMAIL_ITEM_SELECTOR).allInnerTexts();
      }
      const revealButton = page.locator(PHONE_REVEAL_BUTTON_SELECTOR);
      if (PHONE_REVEAL_BUTTON_SELECTOR && await revealButton.isVisible().catch(() => false)) {
        await revealButton.click();
        await page.waitForTimeout(randBetween(1500, 2500));
      }
      if (PHONE_ITEM_SELECTOR && await page.locator(PHONE_ITEM_SELECTOR).first().isVisible().catch(() => false)) {
        const phoneText = await page.locator(PHONE_ITEM_SELECTOR).first().innerText();
        if (!phoneText.includes('*')) {
            extractedPhones.push(phoneText);
        }
      }
      if (extractedEmails.length === 0 && extractedPhones.length === 0) {
          if (RESULT_CONTAINER_SELECTOR && await page.locator(RESULT_CONTAINER_SELECTOR).first().isVisible({ timeout: 2000 }).catch(()=>false)) {
              const raw = await page.locator(RESULT_CONTAINER_SELECTOR).first().innerText();
              const emailRe = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-z]{2,}/g;
              extractedEmails = raw.match(emailRe) || [];
              const phoneRe = /(\+\d{1,3}[-.\s]?)?\d{6,15}/g;
              const foundPhones = (raw.match(phoneRe) || []);
              extractedPhones = foundPhones.filter(p => !p.includes('*'));
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
      
      // Directly modify the row object
      if (row.hasOwnProperty('Personal Email')) row['Personal Email'] = personalEmails.shift() || row['Personal Email'];
      if (row.hasOwnProperty('Other Personal Emails')) row['Other Personal Emails'] = personalEmails.join('; ') || row['Other Personal Emails'];
      if (row.hasOwnProperty('Work Email')) row['Work Email'] = workEmails.shift() || row['Work Email'];
      if (row.hasOwnProperty('Other Work Emails')) row['Other Work Emails'] = workEmails.join('; ') || row['Other Work Emails'];
      if (row.hasOwnProperty('Work Email Status')) row['Work Email Status'] = (workEmails.length > 0 || (row['Work Email'] && row['Work Email'].length > 0)) ? 'Found' : '';
      if (row.hasOwnProperty('Phone Number')) row['Phone Number'] = extractedPhones.shift() || row['Phone Number'];
      if (row.hasOwnProperty('Other Phone Numbers')) row['Other Phone Numbers'] = extractedPhones.join('; ') || row['Other Phone Numbers'];
      
    } catch (err) {
      console.error('Error processing row', i, err);
      if(row.hasOwnProperty('Notes')) row['Notes'] = err.message.substring(0, 500);
    }
    
    // 2. Write incrementally after every row
    await csvWriter.writeRecords(rows); 

    const wait = getDynamicWaitTime();
    console.log(`Waiting ${Math.round(wait/1000)}s before next`);
    await page.waitForTimeout(wait);
  }

  await context.close();
  console.log('Worker finished, output saved to', outputPath);
  process.exit(0);
})();
