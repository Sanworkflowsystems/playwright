// server.js
require('dotenv').config();
const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const { spawn } = require('child_process');
const { v4: uuidv4 } = require('uuid');

const UPLOAD_DIR = path.join(__dirname, 'uploads');
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR);
const OUTPUT_DIR = path.join(__dirname, 'outputs');
if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR);

// Hardcode the selectors as requested
const HARDCODED_SELECTORS = {
    "SEARCH_PAGE_URL": "https://contactout.com/dashboard/search",
    "FULL_NAME_COLUMN_INDEX": 1,
    "COMPANY_NAME_COLUMN_INDEX": 14,
    "NAME_INPUT_SELECTOR": "input[name='nm']",
    "COMPANY_INPUT_SELECTOR": "div.contactout-select__placeholder:has-text('e.g. Contactout') >> xpath=ancestor::div[contains(@class, 'contactout-select__control')] >> input.contactout-select__input",
    "SUBMIT_BUTTON_SELECTOR": "button[type='submit']",
    "RESULT_CONTAINER_SELECTOR": "div.min-xl\\:pl-\\[1\\.4375rem\\]",
    "EMAIL_ITEM_SELECTOR": "div[data-for*='@']",
    "PHONE_REVEAL_BUTTON_SELECTOR": "button.reveal-btn",
    "PHONE_ITEM_SELECTOR": "div.css-2c062s div.css-bsfhvb"
};

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const upload = multer({ dest: UPLOAD_DIR });

// Simple in-memory job store
const jobs = {};
const queue = [];

// POST /upload -> accepts a CSV, returns job id
app.post('/upload', upload.single('file'), (req, res) => {
  const file = req.file;
  if (!file) return res.status(400).send('No file uploaded.');

  // Accept session options from user, selectors are now hardcoded
  const { cookies, manual_login } = req.body;
  const jobId = uuidv4();
  const outputPath = path.join(__dirname, 'outputs', `${jobId}.csv`);
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
    details: { cookies, manual_login: manual_login === 'true', selectors: HARDCODED_SELECTORS }
  };
  queue.push(jobId);
  res.json({ jobId });
  // kick worker if not running
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
        console.error("Error reading status file: ", e);
      }
  }
  
  res.json({ ...j, ...progressData });
});

app.get('/download/:jobId', (req, res) => {
  const j = jobs[req.params.jobId];
  if (!j) return res.status(404).send('Not found');

  // Allow download if job is finished or has errored out, as partial data may exist.
  if (j.status === 'queued' || j.status === 'running') {
    return res.status(400).send('Job is still in progress.');
  }
  
  if (!fs.existsSync(j.outputPath)) {
      return res.status(404).send('Output file not found. It may have been cleaned up or the job may have failed before any data was written.');
  }
  
  // Provide the original uploaded filename for the download
  const originalFilename = (j.details && j.details.originalFilename) ? j.details.originalFilename : `${j.id}.csv`;
  res.download(j.outputPath, `enriched_${originalFilename}`);
});

// Endpoint to create the start signal for the worker
app.post('/signal-start/:jobId', (req, res) => {
    const job = jobs[req.params.jobId];
    if (!job) {
        return res.status(404).send('Job not found.');
    }
    const signalPath = path.join(__dirname, 'outputs', `${req.params.jobId}.start`);
    fs.writeFileSync(signalPath, ''); // Create the empty signal file
    res.status(200).send('Start signal sent.');
});

// Simple singleton worker spawn
let workerRunning = false;
async function runQueue() {
  if (workerRunning) return;
  workerRunning = true;
  while (queue.length > 0) {
    const jobId = queue.shift();
    const job = jobs[jobId];
    if (!job) continue;
    job.status = 'running';
    try {
      // call worker.js with job info
      await runWorker(job);
      job.status = 'finished';
      // Write final status
      const statusPayload = { progress: job.total, total: job.total, status: 'finished' };
      fs.writeFileSync(job.statusPath, JSON.stringify(statusPayload));
    } catch (e) {
      console.error('Worker failed', e);
      job.status = 'error';
      job.error = e.message;
       // Write error status
      const statusPayload = { status: 'error', error: e.message };
      fs.writeFileSync(job.statusPath, JSON.stringify(statusPayload));
    }
  }
  workerRunning = false;
}

// wrapper to spawn worker.js
function runWorker(job) {
  return new Promise((resolve, reject) => {
    const args = [job.id, job.inputPath, job.outputPath];
    // pass options via env or args
    const env = Object.assign({}, process.env, { JOB_SELECTORS: JSON.stringify(job.details.selectors), JOB_COOKIES: job.details.cookies || '', JOB_MANUAL: job.details.manual_login ? '1' : '0' });
    const child = spawn('node', ['worker.js', ...args], { stdio: 'inherit', env });
    child.on('exit', (code) => {
      if (code === 0) resolve();
      else reject(new Error('Worker exited with ' + code));
    });
  });
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server listening ${PORT}`));

