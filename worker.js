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

const {
  PROFILE_URL_COLUMN_INDEX = 0, // Kept for backwards compatibility
  FULL_NAME_COLUMN_INDEX = 0,
  COMPANY_NAME_COLUMN_INDEX = 1,
  SEARCH_PAGE_URL = selectorsConfig.SEARCH_PAGE_URL || '',
  SINGLE_INPUT_SELECTOR = selectorsConfig.SINGLE_INPUT_SELECTOR || '',
  NAME_INPUT_SELECTOR = selectorsConfig.NAME_INPUT_SELECTOR || '',
  COMPANY_INPUT_SELECTOR = selectorsConfig.COMPANY_INPUT_SELECTOR || '',
  SUBMIT_BUTTON_SELECTOR = selectorsConfig.SUBMIT_BUTTON_SELECTOR || '',
  RESULT_CONTAINER_SELECTOR = selectorsConfig.RESULT_CONTAINER_SELECTOR || '',
  EMAIL_ITEM_SELECTOR = selectorsConfig.EMAIL_ITEM_SELECTOR || '',
  PHONE_REVEAL_BUTTON_SELECTOR = selectorsConfig.PHONE_REVEAL_BUTTON_SELECTOR || '',
  PHONE_ITEM_SELECTOR = selectorsConfig.PHONE_ITEM_SELECTOR || ''
} = selectorsConfig;

const personalEmailDomains = [
  'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com', 'icloud.com', 'protonmail.com', 'zoho.com'
];

function randBetween(min, max) { return Math.floor(Math.random() * (max - min + 1)) + min; }

// New function to get the dynamic wait time
function getDynamicWaitTime() {
  if (Math.random() < 0.8) { // 80% chance
    return randBetween(20000, 25000); // 20-25 seconds
  } else { // 20% chance
    return randBetween(25000, 30000); // 25-30 seconds
  }
}

async function readCSV(file) {
  return new Promise((res, rej) => {
    const rows = [];
    fs.createReadStream(file)
      .pipe(csv())
      .on('data', data => rows.push(data))
      .on('end', () => res(rows))
      .on('error', err => rej(err));
  });
}

(async () => {
  const rows = await readCSV(inputPath);
  const csvWriter = createCsvWriter({
    path: outputPath,
    header: [
      {id:'full_name', title:'Full Name'},
      {id:'company_name', title:'Company Name'},
      {id:'personal_email', title:'Personal Email'},
      {id:'other_personal_emails', title:'Other Personal Emails'},
      {id:'work_email', title:'Work Email'},
      {id:'work_email_status', title:'Work Email Status'},
      {id:'other_work_emails', title:'Other Work Emails'},
      {id:'phone_number', title:'Phone Number'},
      {id:'other_phone_numbers', title:'Other Phone Numbers'},
      {id:'notes', title:'Notes'}
    ]
  });
  await csvWriter.writeRecords([]); // create file

  // launch persistent browser
  const browserContextOptions = {
    viewport: { width:1280, height:800 },
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36'
  };
  const context = await chromium.launchPersistentContext(userDataDir, {
    headless: false,
    args: ['--disable-blink-features=AutomationControlled'],
    ...browserContextOptions
  });
  const page = await context.newPage();
  page.setDefaultTimeout(60000);

  // If cookies provided, parse and set
  if (COOKIES_STRING) {
    // COOKIES_STRING example: "key1=val1; key2=val2"
    const pairs = COOKIES_STRING.split(';').map(s => s.trim()).filter(Boolean);
    const cookies = pairs.map(p => {
      const [name, ...rest] = p.split('=');
      return { name: name.trim(), value: rest.join('=').trim(), domain: '.contactout.com', path: '/' };
    });
    await context.addCookies(cookies);
  }

  // Go to the search page and wait for login
  const initialUrl = SEARCH_PAGE_URL || 'https://contactout.com/dashboard/search';
  await page.goto(initialUrl, { waitUntil: 'domcontentloaded' });


  if (MANUAL_LOGIN) {
    console.log('Manual login requested. Please login in the opened browser. Press Enter here once logged in.');
    await new Promise(resolve => process.stdin.once('data', resolve));
  }

  const processedRows = []; // To store rows for incremental saving

  for (let i=0;i<rows.length;i++) {
    const row = rows[i];
    const rowValues = Object.values(row);
    const fullName = rowValues[FULL_NAME_COLUMN_INDEX] || '';
    const companyName = rowValues[COMPANY_NAME_COLUMN_INDEX] || '';

    console.log(`[${i+1}/${rows.length}] processing Name: ${fullName}, Company: ${companyName}`);

    let record = {
      full_name: fullName,
      company_name: companyName,
      personal_email: '',
      other_personal_emails: '',
      work_email: '',
      work_email_status: 'Not Found',
      other_work_emails: '',
      phone_number: '',
      other_phone_numbers: '',
      notes: ''
    };

    try {
      if (!SEARCH_PAGE_URL || !NAME_INPUT_SELECTOR || !COMPANY_INPUT_SELECTOR || !SUBMIT_BUTTON_SELECTOR) {
        throw new Error('Search page selectors (URL, Name, Company, Submit) must be configured.');
      }

      // On subsequent runs, clear inputs before the next search
      if (i > 0) {
        // Clear the company tag input by clicking the 'x'
        const removeTagSelector = "div.contactout-select__multi-value__remove";
        const removeTagButton = page.locator(removeTagSelector);
        if (await removeTagButton.isVisible({ timeout: 1000 }).catch(() => false)) {
            await removeTagButton.click();
        }

        // Clear the simple name input with backspaces
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

      // Fill in search inputs with human-like delays
      await page.type(NAME_INPUT_SELECTOR, fullName, { delay: randBetween(50, 150) });
      await page.type(COMPANY_INPUT_SELECTOR, companyName, { delay: randBetween(50, 150) });

      // Click submit and wait for navigation/results
      await Promise.all([
        page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 30000 }).catch(e => console.log("No navigation after click, continuing...")),
        page.click(SUBMIT_BUTTON_SELECTOR)
      ]);

      await page.waitForTimeout(randBetween(2500, 4000)); // give site time to load dynamic content

      let extractedEmails = [];
      let extractedPhones = [];

      // 1. Scrape Emails specifically
      if (EMAIL_ITEM_SELECTOR && await page.locator(EMAIL_ITEM_SELECTOR).first().isVisible({ timeout: 5000 }).catch(() => false)) {
        extractedEmails = await page.locator(EMAIL_ITEM_SELECTOR).allInnerTexts();
      }

      // 2. Click to reveal phone number
      const revealButton = page.locator(PHONE_REVEAL_BUTTON_SELECTOR);
      if (PHONE_REVEAL_BUTTON_SELECTOR && await revealButton.isVisible().catch(() => false)) {
        await revealButton.click();
        await page.waitForTimeout(randBetween(1500, 2500)); // Wait for phone to appear
      }

      // 3. Scrape Phone number specifically
      if (PHONE_ITEM_SELECTOR && await page.locator(PHONE_ITEM_SELECTOR).first().isVisible().catch(() => false)) {
        const phoneText = await page.locator(PHONE_ITEM_SELECTOR).first().innerText();
        if (!phoneText.includes('*')) {
            extractedPhones.push(phoneText);
        }
      }

      // 4. Fallback to regex on the main container if specific selectors fail
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

      // New Categorization Logic
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

      if (personalEmails.length > 0) {
        record.personal_email = personalEmails.shift();
        record.other_personal_emails = personalEmails.join('; ');
      }

      if (workEmails.length > 0) {
        record.work_email = workEmails.shift();
        record.other_work_emails = workEmails.join('; ');
        record.work_email_status = 'Found';
      }

      if (extractedPhones.length > 0) {
        record.phone_number = extractedPhones.shift();
        record.other_phone_numbers = extractedPhones.join('; ');
      }
      
    } catch (err) {
      console.error('Error processing row', i, err);
      record.notes = err.message.substring(0, 500); // Truncate long errors
    }

    processedRows.push(record);
    await csvWriter.writeRecords(processedRows); // Write incrementally

    // Rate limiting: use new dynamic wait time
    const wait = getDynamicWaitTime();
    console.log(`Waiting ${Math.round(wait/1000)}s before next`);
    await page.waitForTimeout(wait);
  }

  await context.close();
  console.log('Worker finished, output saved to', outputPath);
  process.exit(0);
})();

