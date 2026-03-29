// server.js
require('dotenv').config();
const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const { spawn } = require('child_process');
const { v4: uuidv4 } = require('uuid');
const { google } = require('googleapis');
const { OAuth2Client } = require('google-auth-library');

const UPLOAD_DIR = path.join(__dirname, 'uploads');
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR);
const OUTPUT_DIR = path.join(__dirname, 'outputs');
if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR);

// Hardcode the selectors as requested
const HARDCODED_SELECTORS = {
    "SEARCH_PAGE_URL": "https://contactout.com/dashboard/search",
    "FULL_NAME_COLUMN_INDEX": 1,
    "COMPANY_NAME_COLUMN_INDEX": 4,
    "NAME_INPUT_SELECTOR": "input[name='nm']",
    "COMPANY_INPUT_SELECTOR": "div.contactout-select__placeholder:has-text('e.g. Contactout') >> xpath=ancestor::div[contains(@class, 'contactout-select__control')] >> input.contactout-select__input",
    "SUBMIT_BUTTON_SELECTOR": "button[type='submit']",
    "RESULT_CONTAINER_SELECTOR": "div.min-xl\\:pl-\\[1\\.4375rem\\]",
    "EMAIL_ITEM_SELECTOR": "div[data-for*='@']",
    "PHONE_REVEAL_BUTTON_SELECTOR": "button.reveal-btn",
    "PHONE_ITEM_SELECTOR": "div.css-2c062s div.css-bsfhvb"
};

// ---- Google OAuth2 Setup ----
const SCOPES = ['https://www.googleapis.com/auth/spreadsheets'];
const TOKENS_PATH = path.join(__dirname, 'tokens.json');
const CREDENTIALS_PATH = path.join(__dirname, 'credentials.json');

let oauth2Client = null;
let sheetsClient = null;

function loadGoogleCredentials() {
  if (!fs.existsSync(CREDENTIALS_PATH)) {
    console.warn('credentials.json not found — Google Sheets mode will be unavailable.');
    return;
  }
  try {
    const creds = JSON.parse(fs.readFileSync(CREDENTIALS_PATH));
    const { client_id, client_secret } = creds.installed || creds.web;
    oauth2Client = new OAuth2Client(client_id, client_secret, 'urn:ietf:wg:oauth:2.0:oob');

    if (fs.existsSync(TOKENS_PATH)) {
      oauth2Client.setCredentials(JSON.parse(fs.readFileSync(TOKENS_PATH)));
      sheetsClient = google.sheets({ version: 'v4', auth: oauth2Client });
    }

    // Auto-save refreshed tokens
    oauth2Client.on('tokens', (tokens) => {
      const current = fs.existsSync(TOKENS_PATH) ? JSON.parse(fs.readFileSync(TOKENS_PATH)) : {};
      fs.writeFileSync(TOKENS_PATH, JSON.stringify({ ...current, ...tokens }));
      sheetsClient = google.sheets({ version: 'v4', auth: oauth2Client });
    });
  } catch (e) {
    console.error('Failed to load Google credentials:', e.message);
  }
}
loadGoogleCredentials();

function extractSheetId(url) {
  const match = url.match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/);
  if (!match) throw new Error('Invalid Google Sheet URL — cannot extract sheet ID');
  return match[1];
}

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const upload = multer({ dest: UPLOAD_DIR });

// Build output filename: "originalname_enriched.csv"
function enrichedFilename(originalname, jobId) {
  const ext = path.extname(originalname);
  const base = path.basename(originalname, ext);
  return `${base}_enriched${ext}`;
}

// In-memory stores
const jobs = {};
const queue = [];
const batches = {};
const batchQueue = [];

// Unified mutex: only one worker process at a time
let anyWorkerRunning = false;

// ---- Google Auth endpoints ----

app.get('/auth/google', (req, res) => {
  if (!oauth2Client) return res.status(500).send('credentials.json not found on server.');
  const url = oauth2Client.generateAuthUrl({ access_type: 'offline', scope: SCOPES, prompt: 'consent' });
  // Show a page with the link + a code input box (Desktop app OOB flow)
  res.send(`
    <!doctype html><html><head><meta charset="utf-8"><title>Google Auth</title>
    <style>body{font-family:sans-serif;padding:2em;max-width:600px;margin:0 auto;}
    input{width:100%;padding:0.4em;font-size:1em;margin:0.5em 0;}
    button{padding:0.5em 1.5em;font-size:1em;cursor:pointer;background:#4caf50;color:white;border:none;border-radius:4px;}
    </style></head><body>
    <h2>Authenticate with Google</h2>
    <p><strong>Step 1:</strong> <a href="${url}" target="_blank">Click here to open Google sign-in</a></p>
    <p><strong>Step 2:</strong> Sign in and approve access. Google will show you a code — copy it.</p>
    <p><strong>Step 3:</strong> Paste the code below and click Submit.</p>
    <input type="text" id="code" placeholder="Paste the code here..." />
    <br/>
    <button onclick="submitCode()">Submit Code</button>
    <p id="msg"></p>
    <script>
      async function submitCode() {
        const code = document.getElementById('code').value.trim();
        if (!code) { document.getElementById('msg').textContent = 'Please paste the code first.'; return; }
        const res = await fetch('/auth/google/submit', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({ code }) });
        const data = await res.json();
        if (data.success) {
          document.getElementById('msg').textContent = 'Authentication successful! You can close this tab and return to the app.';
          document.getElementById('msg').style.color = 'green';
        } else {
          document.getElementById('msg').textContent = 'Error: ' + data.error;
          document.getElementById('msg').style.color = 'red';
        }
      }
    </script>
    </body></html>
  `);
});

app.post('/auth/google/submit', async (req, res) => {
  if (!oauth2Client) return res.status(500).json({ error: 'OAuth client not initialised.' });
  const { code } = req.body;
  try {
    const { tokens } = await oauth2Client.getToken(code);
    oauth2Client.setCredentials(tokens);
    fs.writeFileSync(TOKENS_PATH, JSON.stringify(tokens));
    sheetsClient = google.sheets({ version: 'v4', auth: oauth2Client });
    res.json({ success: true });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

app.get('/auth/status', (req, res) => {
  res.json({ authenticated: sheetsClient !== null });
});

// ---- Google Sheets batch endpoint ----

app.post('/start-sheet-batch', async (req, res) => {
  if (!sheetsClient) return res.status(401).json({ error: 'Not authenticated with Google. Visit /auth/google first.' });

  const { sheetJobs, cookies, manual_login } = req.body;
  if (!sheetJobs || sheetJobs.length === 0) return res.status(400).json({ error: 'No sheet jobs provided.' });

  const batchId = uuidv4();
  const jobSpecsList = [];

  for (let i = 0; i < sheetJobs.length; i++) {
    const { sheetUrl, sheetName = 'Sheet1' } = sheetJobs[i];
    let sheetId;
    try { sheetId = extractSheetId(sheetUrl); }
    catch (e) { return res.status(400).json({ error: `Slot ${i + 1}: ${e.message}` }); }

    const jobId = uuidv4();
    const statusPath = path.join(__dirname, 'outputs', `${jobId}.json`);

    const spec = {
      jobId,
      slotIndex: i,
      sheetId,
      sheetName,
      sheetUrl,
      statusPath,
      originalFilename: sheetName
    };
    jobSpecsList.push(spec);

    jobs[jobId] = {
      id: jobId,
      status: 'queued',
      sheetId,
      sheetName,
      sheetUrl,
      statusPath,
      createdAt: new Date().toISOString(),
      progress: 0,
      total: 0,
      details: { cookies, manual_login: manual_login === 'true', selectors: HARDCODED_SELECTORS }
    };

    fs.writeFileSync(statusPath, JSON.stringify({ status: 'queued', progress: 0, total: 0 }));
  }

  const batch = {
    batchId,
    status: 'queued',
    jobSpecs: jobSpecsList,
    details: { cookies, manual_login: manual_login === 'true' },
    createdAt: new Date().toISOString(),
    process: null,
    isSheetMode: true
  };
  batches[batchId] = batch;
  batchQueue.push(batchId);

  const batchStatusPath = path.join(__dirname, 'outputs', `${batchId}.json`);
  const initialJobs = jobSpecsList.map(s => ({
    jobId: s.jobId,
    slotIndex: s.slotIndex,
    originalFilename: s.sheetName,
    sheetUrl: s.sheetUrl,
    status: 'queued',
    progress: 0,
    total: 0
  }));
  fs.writeFileSync(batchStatusPath, JSON.stringify({ batchId, status: 'queued', jobs: initialJobs }));

  res.json({ batchId, jobSpecs: jobSpecsList });
  runBatchQueue();
});

// ---- Single-file endpoints (backward compat) ----

app.post('/upload', upload.single('file'), (req, res) => {
  const file = req.file;
  if (!file) return res.status(400).send('No file uploaded.');

  const { cookies, manual_login } = req.body;
  const jobId = uuidv4();
  const outputPath = path.join(__dirname, 'outputs', enrichedFilename(file.originalname, jobId));
  const statusPath = path.join(__dirname, 'outputs', `${jobId}.json`);

  jobs[jobId] = {
    id: jobId,
    status: 'queued',
    inputPath: file.path,
    outputPath,
    statusPath,
    createdAt: new Date().toISOString(),
    progress: 0,
    total: 0,
    details: { cookies, manual_login: manual_login === 'true', selectors: HARDCODED_SELECTORS, originalFilename: file.originalname }
  };
  queue.push(jobId);
  res.json({ jobId });
  runQueue();
});

app.get('/status/:jobId', (req, res) => {
  const j = jobs[req.params.jobId];
  if (!j) return res.status(404).send('Not found');

  let progressData = {};
  if (fs.existsSync(j.statusPath)) {
    try {
      progressData = JSON.parse(fs.readFileSync(j.statusPath, 'utf8'));
    } catch (e) {
      console.error('Error reading status file:', e);
    }
  }
  res.json({ ...j, ...progressData });
});

app.get('/download/:jobId', (req, res) => {
  const j = jobs[req.params.jobId];
  if (!j) return res.status(404).send('Not found');

  if (!fs.existsSync(j.outputPath)) {
    return res.status(404).send('Output file not found.');
  }

  const originalFilename = (j.details && j.details.originalFilename) ? j.details.originalFilename : `${j.id}.csv`;
  res.download(j.outputPath, enrichedFilename(originalFilename, j.id));
});

app.post('/signal-start/:jobId', (req, res) => {
  const job = jobs[req.params.jobId];
  if (!job) return res.status(404).send('Job not found.');
  const signalPath = path.join(__dirname, 'outputs', `${req.params.jobId}.start`);
  fs.writeFileSync(signalPath, '');
  res.status(200).send('Start signal sent.');
});

app.post('/stop/:jobId', (req, res) => {
  const job = jobs[req.params.jobId];
  if (!job) return res.status(404).send('Job not found.');

  if (job.status !== 'running' || !job.process) {
    return res.status(400).send('Job is not currently running or has no process handle.');
  }

  console.log(`Sending stop signal to job ${job.id}...`);
  job.process.kill('SIGTERM');
  res.status(200).send('Job stop signal sent.');
});

// ---- Batch endpoints ----

app.post('/upload-batch', upload.array('files', 5), (req, res) => {
  const files = req.files;
  if (!files || files.length === 0) return res.status(400).send('No files uploaded.');

  const { cookies, manual_login } = req.body;
  const batchId = uuidv4();
  const jobSpecsList = [];

  for (let i = 0; i < files.length; i++) {
    const file = files[i];
    const jobId = uuidv4();
    const outputPath = path.join(__dirname, 'outputs', enrichedFilename(file.originalname, jobId));
    const statusPath = path.join(__dirname, 'outputs', `${jobId}.json`);

    const spec = {
      jobId,
      slotIndex: i,
      inputPath: file.path,
      outputPath,
      statusPath,
      originalFilename: file.originalname
    };
    jobSpecsList.push(spec);

    jobs[jobId] = {
      id: jobId,
      status: 'queued',
      inputPath: file.path,
      outputPath,
      statusPath,
      createdAt: new Date().toISOString(),
      progress: 0,
      total: 0,
      details: { cookies, manual_login: manual_login === 'true', selectors: HARDCODED_SELECTORS, originalFilename: file.originalname }
    };

    fs.writeFileSync(statusPath, JSON.stringify({ status: 'queued', progress: 0, total: 0 }));
  }

  const batch = {
    batchId,
    status: 'queued',
    jobSpecs: jobSpecsList,
    details: { cookies, manual_login: manual_login === 'true' },
    createdAt: new Date().toISOString(),
    process: null
  };
  batches[batchId] = batch;
  batchQueue.push(batchId);

  const batchStatusPath = path.join(__dirname, 'outputs', `${batchId}.json`);
  const initialJobs = jobSpecsList.map(s => ({
    jobId: s.jobId,
    slotIndex: s.slotIndex,
    originalFilename: s.originalFilename,
    status: 'queued',
    progress: 0,
    total: 0
  }));
  fs.writeFileSync(batchStatusPath, JSON.stringify({ batchId, status: 'queued', jobs: initialJobs }));

  res.json({ batchId, jobSpecs: jobSpecsList });
  runBatchQueue();
});

app.get('/batch-status/:batchId', (req, res) => {
  const batch = batches[req.params.batchId];
  if (!batch) return res.status(404).send('Batch not found');

  const batchStatusPath = path.join(__dirname, 'outputs', `${req.params.batchId}.json`);
  let batchData = { batchId: req.params.batchId, status: batch.status, jobs: [] };

  if (fs.existsSync(batchStatusPath)) {
    try {
      batchData = JSON.parse(fs.readFileSync(batchStatusPath, 'utf8'));
    } catch (e) {
      console.error('Error reading batch status file:', e);
    }
  }

  const mergedJobs = batch.jobSpecs.map(spec => {
    let slotData = {
      jobId: spec.jobId,
      slotIndex: spec.slotIndex,
      originalFilename: spec.originalFilename || spec.sheetName,
      sheetUrl: spec.sheetUrl || null,
      status: 'queued',
      progress: 0,
      total: 0
    };
    if (fs.existsSync(spec.statusPath)) {
      try {
        const slotFile = JSON.parse(fs.readFileSync(spec.statusPath, 'utf8'));
        slotData = { ...slotData, ...slotFile };
      } catch (e) { /* use defaults */ }
    }
    return slotData;
  });

  res.json({ ...batchData, jobs: mergedJobs });
});

app.post('/signal-start-batch/:batchId', (req, res) => {
  const batch = batches[req.params.batchId];
  if (!batch) return res.status(404).send('Batch not found.');
  const signalPath = path.join(__dirname, 'outputs', `${req.params.batchId}.start`);
  fs.writeFileSync(signalPath, '');
  res.status(200).send('Batch start signal sent.');
});

app.post('/stop-batch/:batchId', (req, res) => {
  const batch = batches[req.params.batchId];
  if (!batch) return res.status(404).send('Batch not found.');

  if (!batch.process) {
    return res.status(400).send('Batch is not currently running or has no process handle.');
  }

  console.log(`Sending stop signal to batch ${batch.batchId}...`);
  batch.process.kill('SIGTERM');
  res.status(200).send('Batch stop signal sent.');
});

// ---- Single-job queue runner ----

async function runQueue() {
  if (anyWorkerRunning) return;
  anyWorkerRunning = true;
  while (queue.length > 0) {
    const jobId = queue.shift();
    const job = jobs[jobId];
    if (!job) continue;
    job.status = 'running';
    try {
      await runWorker(job);
      job.status = 'finished';
      const statusPayload = { progress: job.total, total: job.total, status: 'finished' };
      fs.writeFileSync(job.statusPath, JSON.stringify(statusPayload));
    } catch (e) {
      console.error('Worker failed', e);
      job.status = 'error';
      job.error = e.message;
      const statusPayload = { status: 'error', error: e.message };
      fs.writeFileSync(job.statusPath, JSON.stringify(statusPayload));
    }
  }
  anyWorkerRunning = false;
  runBatchQueue();
}

function runWorker(job) {
  return new Promise((resolve, reject) => {
    const args = [job.id, job.inputPath, job.outputPath];
    const env = Object.assign({}, process.env, {
      JOB_SELECTORS: JSON.stringify(job.details.selectors),
      JOB_COOKIES: job.details.cookies || '',
      JOB_MANUAL: job.details.manual_login ? '1' : '0'
    });
    const child = spawn('node', ['worker.js', ...args], { stdio: 'inherit', env });
    job.process = child;

    child.on('exit', (code, signal) => {
      job.process = null;
      if (signal === 'SIGTERM') {
        console.log(`Job ${job.id} was stopped intentionally.`);
        job.status = 'stopped';
        try {
          const statusPayload = { ...JSON.parse(fs.readFileSync(job.statusPath, 'utf8')), status: 'stopped' };
          fs.writeFileSync(job.statusPath, JSON.stringify(statusPayload));
        } catch (e) {
          fs.writeFileSync(job.statusPath, JSON.stringify({ status: 'stopped' }));
        }
        resolve();
      } else if (code === 0) {
        resolve();
      } else {
        reject(new Error('Worker exited with code ' + code));
      }
    });
  });
}

// ---- Batch queue runner ----

async function runBatchQueue() {
  if (anyWorkerRunning) return;
  anyWorkerRunning = true;
  while (batchQueue.length > 0) {
    const batchId = batchQueue.shift();
    const batch = batches[batchId];
    if (!batch) continue;
    batch.status = 'running';
    for (const spec of batch.jobSpecs) {
      if (jobs[spec.jobId]) jobs[spec.jobId].status = 'running';
    }
    try {
      await runBatchWorker(batch);
      batch.status = 'finished';
    } catch (e) {
      console.error('Batch worker failed', e);
      batch.status = 'error';
    }
  }
  anyWorkerRunning = false;
  runQueue();
}

function runBatchWorker(batch) {
  return new Promise((resolve, reject) => {
    const isSheetMode = !!batch.isSheetMode;
    const env = Object.assign({}, process.env, {
      BATCH_JOBS: JSON.stringify(batch.jobSpecs),
      JOB_SELECTORS: JSON.stringify(HARDCODED_SELECTORS),
      JOB_COOKIES: batch.details.cookies || '',
      JOB_MANUAL: batch.details.manual_login ? '1' : '0',
      BATCH_SHEET_MODE: isSheetMode ? '1' : '',
      GOOGLE_TOKENS: isSheetMode && fs.existsSync(TOKENS_PATH) ? fs.readFileSync(TOKENS_PATH, 'utf8') : '',
      GOOGLE_CREDENTIALS: isSheetMode && fs.existsSync(CREDENTIALS_PATH) ? fs.readFileSync(CREDENTIALS_PATH, 'utf8') : '',
    });
    const child = spawn('node', ['parallel-worker.js', batch.batchId], { stdio: 'inherit', env });
    batch.process = child;

    child.on('exit', (code, signal) => {
      batch.process = null;
      if (signal === 'SIGTERM') {
        console.log(`Batch ${batch.batchId} was stopped intentionally.`);
        batch.status = 'stopped';
        resolve();
      } else if (code === 0) {
        resolve();
      } else {
        reject(new Error('Batch worker exited with code ' + code));
      }
    });
  });
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server listening ${PORT}`));
