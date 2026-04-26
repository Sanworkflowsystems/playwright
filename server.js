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

function ts() { return new Date().toTimeString().slice(0, 8); }
const LOG = {
  info:  (msg, extra) => console.log(`[${ts()}] INFO  ${msg}`, extra || ''),
  warn:  (msg, extra) => console.warn(`[${ts()}] WARN  ${msg}`, extra || ''),
  error: (msg, extra) => console.error(`[${ts()}] ERROR ${msg}`, extra || ''),
};

// ---- Pipeline modules ----
const { createOrchestrator, resetOrchestrator } = require('./pipeline/orchestrator');
const { loadConfig, saveConfig }               = require('./pipeline/pipeline-config');
const csvParser = require('csv-parser');

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
    LOG.warn('credentials.json not found - Google Sheets mode will be unavailable.');
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
    LOG.error(`Failed to load Google credentials: ${e.message}`);
  }
}
loadGoogleCredentials();

function extractSheetId(url) {
  const match = url.match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/);
  if (!match) throw new Error('Invalid Google Sheet URL - cannot extract sheet ID');
  return match[1];
}

// Pulls the numeric gid from a Sheets URL (either ?gid= or #gid=). Returns null if absent.
function extractGid(url) {
  const m = String(url || '').match(/[?#&]gid=(\d+)/);
  return m ? m[1] : null;
}

// Resolve a tab name for a sheet URL: use providedName if set, else try gid, else first sheet.
// Always safe: errors fall through so the caller's downstream logic can run.
async function resolveSheetName(sheets, sheetId, sheetUrl, providedName) {
  if (providedName && providedName.trim()) return providedName.trim();
  const gid = extractGid(sheetUrl);
  if (gid) {
    try {
      const { getSheetNameByGid } = require('./google-sheets-helper');
      const name = await getSheetNameByGid(sheets, sheetId, gid);
      if (name) return name;
    } catch (e) {
      LOG.warn(`[resolveSheetName] gid lookup failed for ${sheetId} gid=${gid}: ${e.message}`);
    }
  }
  return null;
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

  const { sheetJobs, cookies, manual_login, account, rowStart: rawRowStart, rowEnd: rawRowEnd } = req.body;
  if (!sheetJobs || sheetJobs.length === 0) return res.status(400).json({ error: 'No sheet jobs provided.' });

  const parseRow = (v) => {
    const n = parseInt(v, 10);
    return Number.isFinite(n) && n >= 2 ? n : null;
  };
  const rowStart = parseRow(rawRowStart);
  const rowEnd   = parseRow(rawRowEnd);

  const batchId = uuidv4();
  const jobSpecsList = [];

  for (let i = 0; i < sheetJobs.length; i++) {
    const { sheetUrl, sheetName: providedName = '' } = sheetJobs[i];
    let sheetId;
    try { sheetId = extractSheetId(sheetUrl); }
    catch (e) { return res.status(400).json({ error: `Slot ${i + 1}: ${e.message}` }); }

    // Resolve tab name: user-provided → gid lookup → null (worker falls back to first sheet).
    const sheetName = (await resolveSheetName(sheetsClient, sheetId, sheetUrl, providedName)) || '';

    const jobId = uuidv4();
    const statusPath = path.join(__dirname, 'outputs', `${jobId}.json`);

    const spec = {
      jobId,
      slotIndex: i,
      sheetId,
      sheetName,
      sheetUrl,
      statusPath,
      originalFilename: sheetName,
      rowStart: rowStart || null,
      rowEnd:   rowEnd   || null,
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
      details: { cookies, manual_login: manual_login === 'true', selectors: HARDCODED_SELECTORS, account: account || 'none' }
    };

    fs.writeFileSync(statusPath, JSON.stringify({ status: 'queued', progress: 0, total: 0 }));
  }

  const batch = {
    batchId,
    status: 'queued',
    jobSpecs: jobSpecsList,
    details: { cookies, manual_login: manual_login === 'true', account: account || 'none' },
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

  const { cookies, manual_login, account } = req.body;
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
    details: { cookies, manual_login: manual_login === 'true', selectors: HARDCODED_SELECTORS, originalFilename: file.originalname, account: account || 'none' }
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

  const { cookies, manual_login, account } = req.body;
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
      details: { cookies, manual_login: manual_login === 'true', selectors: HARDCODED_SELECTORS, originalFilename: file.originalname, account: account || 'none' }
    };

    fs.writeFileSync(statusPath, JSON.stringify({ status: 'queued', progress: 0, total: 0 }));
  }

  const batch = {
    batchId,
    status: 'queued',
    jobSpecs: jobSpecsList,
    details: { cookies, manual_login: manual_login === 'true', account: account || 'none' },
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

// ---- Verify code endpoints ----
// Worker writes a .verify file when it needs a code; UI polls this.
// User submits code via POST; worker reads it and continues.

app.get('/verify-status/:jobId', (req, res) => {
  const verifyPath = path.join(__dirname, 'outputs', `${req.params.jobId}.verify`);
  res.json({ waiting: fs.existsSync(verifyPath) });
});

app.post('/submit-verify/:jobId', (req, res) => {
  const { code } = req.body;
  if (!code) return res.status(400).json({ error: 'No code provided.' });
  const codePath = path.join(__dirname, 'outputs', `${req.params.jobId}.verifycode`);
  fs.writeFileSync(codePath, code.trim());
  res.json({ ok: true });
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

// ════════════════════════════════════════════════════════════════════════════
// PIPELINE ENDPOINTS
// ════════════════════════════════════════════════════════════════════════════

// In-memory store for running pipeline jobs
const pipelineJobs = {};   // pipelineJobId → { abortController, status }

function getOrInitOrchestrator() {
  return createOrchestrator(false);
}

// Helper: parse a CSV file into headers + rows
function parseCsvFile(filePath) {
  return new Promise((resolve, reject) => {
    const rows = [];
    let headers = null;
    fs.createReadStream(filePath)
      .pipe(csvParser())
      .on('headers', (h) => { headers = h; })
      .on('data',    (row) => rows.push(row))
      .on('error',   reject)
      .on('end',     () => resolve({ headers: headers || [], rows }));
  });
}

/**
 * POST /start-pipeline
 * Body (JSON, sheets mode):   { sheetJobs: [{ sheetUrl, sheetName }] }
 * Body (multipart, CSV mode): files[] field with one CSV
 *
 * Returns: { pipelineJobId, totalRows }
 * Pipeline runs in background — poll /pipeline-status/:id
 */
app.post('/start-pipeline', upload.array('files', 1), async (req, res) => {
  const pipelineJobId = uuidv4();
  const progressPath  = path.join(OUTPUT_DIR, `${pipelineJobId}-pipeline.json`);

  fs.writeFileSync(progressPath, JSON.stringify({
    status: 'starting', progress: 0, total: 0, stats: {}, updatedAt: new Date().toISOString()
  }));

  let rows    = [];
  let headers = [];
  let sheetId   = null;
  let sheetName = null;

  try {
    // ── Sheets mode ──
    if (req.body && req.body.sheetJobs) {
      if (!sheetsClient) {
        return res.status(401).json({ error: 'Not authenticated with Google. Visit /auth/google first.' });
      }
      const sheetJobs = typeof req.body.sheetJobs === 'string'
        ? JSON.parse(req.body.sheetJobs)
        : req.body.sheetJobs;

      if (!sheetJobs || sheetJobs.length === 0) {
        return res.status(400).json({ error: 'No sheet jobs provided.' });
      }

      const { sheetUrl, sheetName: sn } = sheetJobs[0];
      sheetId   = extractSheetId(sheetUrl);

      const { readSheetData, getFirstSheetName } = require('./google-sheets-helper');
      sheetName = await resolveSheetName(sheetsClient, sheetId, sheetUrl, sn);
      if (!sheetName) sheetName = await getFirstSheetName(sheetsClient, sheetId);
      const data = await readSheetData(sheetsClient, sheetId, sheetName);
      rows    = data.rows;
      headers = data.headers;

    // ── CSV mode ──
    } else if (req.files && req.files.length > 0) {
      const file = req.files[0];
      const data = await parseCsvFile(file.path);
      rows    = data.rows;
      headers = data.headers;

    } else {
      return res.status(400).json({ error: 'Provide sheetJobs (JSON) or a CSV file.' });
    }

    if (rows.length === 0) {
      return res.status(400).json({ error: 'No rows found to process.' });
    }

    LOG.info(`[Pipeline ${pipelineJobId}] Starting pre-enrichment | rows=${rows.length} | mode=${sheetId ? 'Google Sheets' : 'CSV'}${sheetName ? ` | sheet=${sheetName}` : ''}`);
    res.json({ pipelineJobId, totalRows: rows.length });

    // ── Run pipeline in background (no await) ──
    const abortController = new AbortController();
    pipelineJobs[pipelineJobId] = { abortController, status: 'running' };

    const orchestrator = getOrInitOrchestrator();

    // Optional: skip individual phases (e.g. resume Hunter+Bouncer after a completed Prospeo run).
    // Accepts "prospeo" or "hunter" either as an array or a comma-separated string.
    let skipPhases = [];
    const rawSkip = req.body?.skipPhases;
    if (Array.isArray(rawSkip)) skipPhases = rawSkip;
    else if (typeof rawSkip === 'string' && rawSkip.trim()) skipPhases = rawSkip.split(',').map(s => s.trim()).filter(Boolean);
    skipPhases = skipPhases.filter(p => p === 'prospeo' || p === 'hunter');

    // Optional row range (1-based sheet row numbers; row 1 = headers, so min = 2).
    const parseRow = (v) => {
      const n = parseInt(v, 10);
      return Number.isFinite(n) && n >= 2 ? n : null;
    };
    const rowStart = parseRow(req.body?.rowStart);
    const rowEnd   = parseRow(req.body?.rowEnd);
    if (rowStart || rowEnd) {
      LOG.info(`[Pipeline ${pipelineJobId}] Row range: ${rowStart || 2}..${rowEnd || 'end'}`);
    }

    orchestrator.processRows(rows, {
      pipelineJobId,
      outputDir:   OUTPUT_DIR,
      sheetsClient: sheetId ? sheetsClient : null,
      sheetId,
      sheetName,
      headers,
      signal: abortController.signal,
      skipPhases,
      rowStart,
      rowEnd,
    }).then(({ stats }) => {
      pipelineJobs[pipelineJobId].status = 'finished';
      pipelineJobs[pipelineJobId].stats  = stats;
      LOG.info(`[Pipeline ${pipelineJobId}] Finished`, stats);
    }).catch(err => {
      LOG.error(`[Pipeline ${pipelineJobId}] Fatal error: ${err.message}`);
      pipelineJobs[pipelineJobId].status = 'error';
      fs.writeFileSync(progressPath, JSON.stringify({
        status: 'error', error: err.message, updatedAt: new Date().toISOString()
      }));
    });

  } catch (err) {
    LOG.error(`[/start-pipeline] ${err.message}`);
    res.status(500).json({ error: err.message });
  }
});

/**
 * GET /pipeline-status/:id
 * Returns the progress JSON written by the orchestrator.
 */
app.get('/pipeline-status/:id', (req, res) => {
  const progressPath = path.join(OUTPUT_DIR, `${req.params.id}-pipeline.json`);
  if (!fs.existsSync(progressPath)) return res.status(404).json({ error: 'Pipeline job not found.' });
  try {
    res.json(JSON.parse(fs.readFileSync(progressPath, 'utf8')));
  } catch (e) {
    res.status(500).json({ error: 'Failed to read pipeline status.' });
  }
});

/**
 * POST /stop-pipeline/:id
 * Aborts the running pipeline via AbortController.
 */
app.post('/stop-pipeline/:id', (req, res) => {
  const job = pipelineJobs[req.params.id];
  if (!job) return res.status(404).json({ error: 'Pipeline job not found.' });
  job.abortController.abort();
  job.status = 'stopped';
  res.json({ ok: true });
});

/**
 * GET /pipeline-keys
 * Returns live key health stats for all three services.
 */
app.get('/pipeline-keys', (req, res) => {
  try {
    const orch = createOrchestrator(false);
    res.json({
      prospeo: orch.pools.prospeo.getStats(),
      hunter:  orch.pools.hunter.getStats(),
      bouncer: orch.pools.bouncer.getStats(),
    });
  } catch (e) {
    res.json({ prospeo: null, hunter: null, bouncer: null });
  }
});

/**
 * POST /pipeline-keys
 * Body: { prospeo: { keys: [] }, hunter: { keys: [] }, bouncer: { keys: [] }, pipeline: {} }
 * Saves to pipeline-config.json and re-initialises key pools.
 */
app.post('/pipeline-keys', (req, res) => {
  try {
    const current = loadConfig();
    const { prospeo, hunter, bouncer, pipeline } = req.body || {};

    if (prospeo?.keys !== undefined) current.prospeo.keys = prospeo.keys;
    if (hunter?.keys  !== undefined) current.hunter.keys  = hunter.keys;
    if (bouncer?.keys !== undefined) current.bouncer.keys = bouncer.keys;
    if (pipeline)                    current.pipeline      = { ...current.pipeline, ...pipeline };

    saveConfig(current);
    resetOrchestrator();   // force re-init on next request
    res.json({ ok: true, message: 'Keys saved. Orchestrator will reload on next pipeline run.' });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/**
 * GET /pipeline-config
 * Returns current pipeline configuration (keys redacted for safety).
 */
app.get('/pipeline-config', (req, res) => {
  try {
    const cfg = loadConfig();
    // Redact actual key values
    const safe = JSON.parse(JSON.stringify(cfg));
    ['prospeo', 'hunter', 'bouncer'].forEach(svc => {
      if (safe[svc]?.keys) {
        safe[svc].keyCount = safe[svc].keys.length;
        safe[svc].keys = safe[svc].keys.map(k => '…' + k.slice(-6));
      }
    });
    res.json(safe);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ════════════════════════════════════════════════════════════════════════════
// ANYMAILFINDER ENDPOINTS
// ════════════════════════════════════════════════════════════════════════════

const amfJobs = {};  // amfJobId → status/progress file path + abort flag

/**
 * POST /start-anymailfinder
 * Body: { sheetUrl, sheetName, apiKey, rowStart, rowEnd }
 */
app.post('/start-anymailfinder', async (req, res) => {
  if (!sheetsClient) return res.status(401).json({ error: 'Not authenticated with Google. Visit /auth/google first.' });

  const { sheetUrl, sheetName: providedName, apiKey, rowStart: rawRowStart, rowEnd: rawRowEnd } = req.body;
  if (!sheetUrl)  return res.status(400).json({ error: 'sheetUrl is required.' });
  if (!apiKey)    return res.status(400).json({ error: 'apiKey is required.' });

  let sheetId;
  try { sheetId = extractSheetId(sheetUrl); }
  catch (e) { return res.status(400).json({ error: e.message }); }

  const parseRow = (v) => { const n = parseInt(v, 10); return Number.isFinite(n) && n >= 2 ? n : null; };
  const rowStart = parseRow(rawRowStart);
  const rowEnd   = parseRow(rawRowEnd);

  const amfJobId   = uuidv4();
  const statusPath = path.join(OUTPUT_DIR, `${amfJobId}-amf.json`);

  const initialStatus = { status: 'starting', processed: 0, total: 0, found: 0, notFound: 0, credits: 0, stoppedAtRow: null, updatedAt: new Date().toISOString() };
  fs.writeFileSync(statusPath, JSON.stringify(initialStatus));
  amfJobs[amfJobId] = { statusPath, aborted: false };

  res.json({ amfJobId });

  // Run in background
  (async () => {
    try {
      const { readSheetData, getFirstSheetName } = require('./google-sheets-helper');
      const sheetName = (await resolveSheetName(sheetsClient, sheetId, sheetUrl, providedName)) || await getFirstSheetName(sheetsClient, sheetId);
      const { rows, headers } = await readSheetData(sheetsClient, sheetId, sheetName);

      LOG.info(`[AnyMailFinder ${amfJobId}] Starting | rows=${rows.length} | sheet=${sheetName}${rowStart ? ` | range=${rowStart}..${rowEnd || 'end'}` : ''}`);

      const writeStatus = (patch) => {
        try {
          const current = JSON.parse(fs.readFileSync(statusPath, 'utf8'));
          fs.writeFileSync(statusPath, JSON.stringify({ ...current, ...patch, updatedAt: new Date().toISOString() }));
        } catch (_) {}
      };

      writeStatus({ status: 'running' });

      const { runAnyMailFinder } = require('./pipeline/anymailfinder-client');
      const abortController = new AbortController();
      amfJobs[amfJobId].abort = () => abortController.abort();

      const stats = await runAnyMailFinder({
        sheetsClient,
        sheetId,
        sheetName,
        rows,
        headers,
        apiKey,
        rowStart,
        rowEnd,
        signal: abortController.signal,
        onProgress: ({ processed, total, row, email, status, stoppedAtRow }) => {
          if (amfJobs[amfJobId].aborted) abortController.abort();
          writeStatus({ processed, total, stoppedAtRow: stoppedAtRow || null });
        },
      });

      const finalStatus = stats.stoppedAtRow ? 'credits_exhausted' : 'finished';
      writeStatus({ status: finalStatus, ...stats });
      LOG.info(`[AnyMailFinder ${amfJobId}] ${finalStatus} | found=${stats.found} credits=${stats.credits}${stats.stoppedAtRow ? ` stoppedAtRow=${stats.stoppedAtRow}` : ''}`);
    } catch (err) {
      LOG.error(`[AnyMailFinder ${amfJobId}] Fatal: ${err.message}`);
      try {
        const current = JSON.parse(fs.readFileSync(statusPath, 'utf8'));
        fs.writeFileSync(statusPath, JSON.stringify({ ...current, status: 'error', error: err.message, updatedAt: new Date().toISOString() }));
      } catch (_) {}
    }
  })();
});

app.get('/anymailfinder-status/:id', (req, res) => {
  const job = amfJobs[req.params.id];
  if (!job) return res.status(404).json({ error: 'AnyMailFinder job not found.' });
  try {
    res.json(JSON.parse(fs.readFileSync(job.statusPath, 'utf8')));
  } catch (e) {
    res.status(500).json({ error: 'Failed to read status.' });
  }
});

app.post('/stop-anymailfinder/:id', (req, res) => {
  const job = amfJobs[req.params.id];
  if (!job) return res.status(404).json({ error: 'Not found.' });
  job.aborted = true;
  if (job.abort) job.abort();
  res.json({ ok: true });
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
  if (batchQueue.length > 0) setTimeout(runBatchQueue, 0);
}

function runWorker(job) {
  return new Promise((resolve, reject) => {
    const args = [job.id, job.inputPath, job.outputPath];
    const env = Object.assign({}, process.env, {
      JOB_SELECTORS: JSON.stringify(job.details.selectors),
      JOB_COOKIES: job.details.cookies || '',
      JOB_MANUAL: job.details.manual_login ? '1' : '0',
      JOB_ACCOUNT: job.details.account || 'none'
    });
    LOG.info(`[ContactOut ${job.id}] Launching single worker`);
    const child = spawn('node', ['worker.js', ...args], { stdio: 'inherit', env });
    job.process = child;

    child.on('exit', (code, signal) => {
      job.process = null;
      if (signal === 'SIGTERM') {
        LOG.warn(`[ContactOut ${job.id}] Worker stopped intentionally`);
        job.status = 'stopped';
        try {
          const statusPayload = { ...JSON.parse(fs.readFileSync(job.statusPath, 'utf8')), status: 'stopped' };
          fs.writeFileSync(job.statusPath, JSON.stringify(statusPayload));
        } catch (e) {
          fs.writeFileSync(job.statusPath, JSON.stringify({ status: 'stopped' }));
        }
        resolve();
      } else if (code === 0) {
        LOG.info(`[ContactOut ${job.id}] Worker exited successfully`);
        resolve();
      } else {
        LOG.error(`[ContactOut ${job.id}] Worker exited with code ${code}`);
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
  if (queue.length > 0) setTimeout(runQueue, 0);
}

function runBatchWorker(batch) {
  return new Promise((resolve, reject) => {
    const isSheetMode = !!batch.isSheetMode;
    const env = Object.assign({}, process.env, {
      BATCH_JOBS: JSON.stringify(batch.jobSpecs),
      JOB_SELECTORS: JSON.stringify(HARDCODED_SELECTORS),
      JOB_COOKIES: batch.details.cookies || '',
      JOB_MANUAL: batch.details.manual_login ? '1' : '0',
      JOB_ACCOUNT: batch.details.account || 'none',
      BATCH_SHEET_MODE: isSheetMode ? '1' : '',
      GOOGLE_TOKENS: isSheetMode && fs.existsSync(TOKENS_PATH) ? fs.readFileSync(TOKENS_PATH, 'utf8') : '',
      GOOGLE_CREDENTIALS: isSheetMode && fs.existsSync(CREDENTIALS_PATH) ? fs.readFileSync(CREDENTIALS_PATH, 'utf8') : '',
    });
    LOG.info(`[ContactOut batch ${batch.batchId}] Launching batch worker | jobs=${batch.jobSpecs.length} | mode=${isSheetMode ? 'Google Sheets' : 'CSV'}`);
    const child = spawn('node', ['parallel-worker.js', batch.batchId], { stdio: 'inherit', env });
    batch.process = child;

    child.on('exit', (code, signal) => {
      batch.process = null;
      if (signal === 'SIGTERM') {
        LOG.warn(`[ContactOut batch ${batch.batchId}] Batch worker stopped intentionally`);
        batch.status = 'stopped';
        resolve();
      } else if (code === 0) {
        LOG.info(`[ContactOut batch ${batch.batchId}] Batch worker exited successfully`);
        resolve();
      } else {
        LOG.error(`[ContactOut batch ${batch.batchId}] Batch worker exited with code ${code}`);
        reject(new Error('Batch worker exited with code ' + code));
      }
    });
  });
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => LOG.info(`Server listening ${PORT}`));
