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

  // Accept session options from user: either cookies string or "manual_login": true
  const { cookies, manual_login, selectors } = req.body;
  const jobId = uuidv4();
  const outputPath = path.join(__dirname, 'outputs', `${jobId}.csv`);
  jobs[jobId] = {
    id: jobId,
    status: 'queued',
    inputPath: file.path,
    outputPath,
    createdAt: new Date().toISOString(),
    progress: 0,
    details: { cookies, manual_login: manual_login === 'true', selectors: selectors ? JSON.parse(selectors) : {} }
  };
  queue.push(jobId);
  res.json({ jobId });
  // kick worker if not running
  runQueue();
});

app.get('/status/:jobId', (req, res) => {
  const j = jobs[req.params.jobId];
  if (!j) return res.status(404).send('Not found');
  res.json(j);
});

app.get('/download/:jobId', (req, res) => {
  const j = jobs[req.params.jobId];
  if (!j) return res.status(404).send('Not found');
  if (!fs.existsSync(j.outputPath)) return res.status(404).send('Output not ready');
  res.download(j.outputPath);
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
    } catch (e) {
      console.error('Worker failed', e);
      job.status = 'error';
      job.error = e.message;
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
