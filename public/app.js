// app.js — Batch enrichment UI logic

// Show filename next to each slot input when a file is selected
document.querySelectorAll('.slot-file-input').forEach(input => {
  input.addEventListener('change', () => {
    const row = input.closest('.slot-row');
    const label = row.querySelector('.slot-filename');
    label.textContent = input.files[0] ? input.files[0].name : '';
  });
});

// Batch state
let batchState = {
  batchId: null,
  jobSpecs: [],
  pollingInterval: null,
  isManual: false,
  isSheetMode: false
};

// ---- Mode switching ----

function getMode() {
  return document.querySelector('input[name="mode"]:checked').value;
}

document.querySelectorAll('input[name="mode"]').forEach(radio => {
  radio.addEventListener('change', () => {
    const mode = getMode();
    document.getElementById('csvMode').style.display = mode === 'csv' ? 'block' : 'none';
    document.getElementById('sheetsMode').style.display = mode === 'sheets' ? 'block' : 'none';
    document.getElementById('uploadBtn').textContent = mode === 'csv'
      ? 'Upload Files & Open Browser'
      : 'Start Sheet Job & Open Browser';
    if (mode === 'sheets') checkAuthStatus();
  });
});

// ---- Google Auth ----

async function checkAuthStatus() {
  const banner = document.getElementById('authBanner');
  const linkSection = document.getElementById('authLinkSection');
  banner.className = 'checking';
  banner.textContent = 'Checking Google authentication status...';
  try {
    const res = await fetch('/auth/status');
    const data = await res.json();
    if (data.authenticated) {
      banner.className = 'authed';
      banner.textContent = 'Authenticated with Google.';
      linkSection.style.display = 'none';
    } else {
      banner.className = 'unauthed';
      banner.textContent = 'Not authenticated with Google. ';
      linkSection.style.display = 'block';
    }
  } catch (e) {
    banner.className = 'unauthed';
    banner.textContent = 'Could not reach server to check auth status.';
  }
}

document.getElementById('checkAuthBtn').addEventListener('click', checkAuthStatus);

// ---- Upload / Start button ----

document.getElementById('uploadBtn').addEventListener('click', async () => {
  const mode = getMode();
  const manual = document.getElementById('manualLogin').checked;
  const cookies = document.getElementById('cookies').value;
  const btn = document.getElementById('uploadBtn');

  if (mode === 'csv') {
    // --- CSV mode (existing logic) ---
    const slotInputs = document.querySelectorAll('.slot-file-input');
    const files = [];
    slotInputs.forEach(input => { if (input.files[0]) files.push(input.files[0]); });

    if (files.length === 0) { alert('Please select at least one CSV file.'); return; }

    const fd = new FormData();
    files.forEach(f => fd.append('files', f));
    fd.append('manual_login', manual ? 'true' : 'false');
    fd.append('cookies', cookies);

    btn.disabled = true;
    btn.textContent = 'Uploading...';

    try {
      const res = await fetch('/upload-batch', { method: 'POST', body: fd });
      if (!res.ok) throw new Error(`Upload failed: ${await res.text()}`);
      const data = await res.json();
      batchState.batchId = data.batchId;
      batchState.jobSpecs = data.jobSpecs;
      batchState.isManual = manual;
      batchState.isSheetMode = false;
      switchToDashboard(data.jobSpecs, manual, false);
    } catch (err) {
      alert(`Error: ${err.message}`);
      btn.disabled = false;
      btn.textContent = 'Upload Files & Open Browser';
    }

  } else {
    // --- Google Sheets mode ---
    const sheetJobs = [];
    document.querySelectorAll('#sheetSlots .sheet-slot-row').forEach(row => {
      const url = row.querySelector('.sheet-url-input').value.trim();
      const tabName = row.querySelector('.sheet-tab-input').value.trim() || 'Sheet1';
      if (url) sheetJobs.push({ sheetUrl: url, sheetName: tabName });
    });

    if (sheetJobs.length === 0) { alert('Please enter at least one Google Sheet URL.'); return; }

    btn.disabled = true;
    btn.textContent = 'Starting...';

    try {
      const res = await fetch('/start-sheet-batch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sheetJobs, cookies, manual_login: manual ? 'true' : 'false' })
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: res.statusText }));
        throw new Error(err.error || 'Failed to start sheet job.');
      }
      const data = await res.json();
      batchState.batchId = data.batchId;
      batchState.jobSpecs = data.jobSpecs;
      batchState.isManual = manual;
      batchState.isSheetMode = true;
      switchToDashboard(data.jobSpecs, manual, true);
    } catch (err) {
      alert(`Error: ${err.message}`);
      btn.disabled = false;
      btn.textContent = 'Start Sheet Job & Open Browser';
    }
  }
});

function switchToDashboard(jobSpecs, isManual, isSheetMode) {
  document.getElementById('uploadSection').style.display = 'none';
  document.getElementById('dashboardSection').style.display = 'block';

  for (let i = 0; i < 5; i++) {
    const panel = document.getElementById(`panel-${i}`);
    const spec = jobSpecs[i];
    if (spec) {
      panel.classList.add('visible');
      const displayName = isSheetMode ? (spec.sheetName || `Sheet ${i + 1}`) : spec.originalFilename;
      document.getElementById(`filename-${i}`).textContent = `Slot ${i + 1}: ${displayName}`;
      document.getElementById(`status-${i}`).textContent = 'Queued — waiting for browser';

      const dl = document.getElementById(`download-${i}`);
      if (isSheetMode) {
        dl.href = spec.sheetUrl || '#';
        dl.textContent = 'Open Google Sheet';
        dl.target = '_blank';
        dl.style.display = 'block';
      } else {
        dl.href = `/download/${spec.jobId}`;
        dl.textContent = 'Download Enriched CSV';
        dl.style.display = 'block';
      }
    } else {
      panel.classList.remove('visible');
    }
  }

  if (isManual) {
    const startBtn = document.getElementById('startBatchBtn');
    startBtn.style.display = 'inline-block';
    startBtn.onclick = sendStartSignal;
    document.getElementById('batchStatusText').textContent = 'Browser is opening. Please log in, then click Start.';
  } else {
    document.getElementById('batchStatusText').textContent = 'Processing started automatically.';
    showStopButton();
    startPolling();
  }
}

async function sendStartSignal() {
  const startBtn = document.getElementById('startBatchBtn');
  startBtn.disabled = true;
  startBtn.textContent = 'Sending start signal...';
  try {
    const res = await fetch(`/signal-start-batch/${batchState.batchId}`, { method: 'POST' });
    if (!res.ok) throw new Error('Failed to send start signal.');
    startBtn.style.display = 'none';
    document.getElementById('batchStatusText').textContent = 'Start signal sent. Processing beginning...';
    showStopButton();
    startPolling();
  } catch (err) {
    document.getElementById('batchStatusText').textContent = `Error: ${err.message}`;
    startBtn.disabled = false;
    startBtn.textContent = "I've logged in — Start Processing All Tabs";
  }
}

function showStopButton() {
  const stopBtn = document.getElementById('stopBatchBtn');
  stopBtn.style.display = 'inline-block';
  stopBtn.disabled = false;
  stopBtn.textContent = 'Stop All Jobs';
  stopBtn.onclick = stopBatch;
}

async function stopBatch() {
  const stopBtn = document.getElementById('stopBatchBtn');
  stopBtn.disabled = true;
  stopBtn.textContent = 'Stopping...';
  try {
    const res = await fetch(`/stop-batch/${batchState.batchId}`, { method: 'POST' });
    if (!res.ok) throw new Error(await res.text());
  } catch (err) {
    document.getElementById('batchStatusText').textContent = `Stop error: ${err.message}`;
    stopBtn.disabled = false;
    stopBtn.textContent = 'Stop All Jobs';
  }
}

function startPolling() {
  if (batchState.pollingInterval) clearInterval(batchState.pollingInterval);
  batchState.pollingInterval = setInterval(pollBatchStatus, 2500);
  pollBatchStatus();
}

async function pollBatchStatus() {
  try {
    const res = await fetch(`/batch-status/${batchState.batchId}`);
    if (!res.ok) throw new Error(`Server returned ${res.status}`);
    const data = await res.json();

    let allTerminal = true;

    for (const job of data.jobs) {
      const i = job.slotIndex;
      const bar = document.getElementById(`bar-${i}`);
      const statusEl = document.getElementById(`status-${i}`);
      if (!bar || !statusEl) continue;

      const status = job.status || 'queued';
      const progress = job.progress || 0;
      const total = job.total || 0;
      const pct = total > 0 ? Math.round((progress / total) * 100) : 0;

      bar.style.width = `${pct}%`;
      bar.textContent = `${pct}%`;

      if (status === 'running') {
        bar.style.backgroundColor = '#4caf50';
        allTerminal = false;
      } else if (status === 'finished') {
        bar.style.width = '100%';
        bar.textContent = '100%';
        bar.style.backgroundColor = '#1976d2';
      } else if (status === 'stopped') {
        bar.style.backgroundColor = '#ff9800';
      } else if (status === 'error') {
        bar.style.backgroundColor = '#f44336';
      } else {
        allTerminal = false;
      }

      if (status === 'running') {
        statusEl.textContent = total > 0
          ? `Running — ${progress} / ${total} rows (${pct}%)`
          : 'Running — starting...';
      } else if (status === 'finished') {
        statusEl.textContent = 'Complete!';
      } else if (status === 'stopped') {
        statusEl.textContent = `Stopped — ${progress} / ${total} rows saved`;
      } else if (status === 'error') {
        statusEl.textContent = `Error: ${job.error || 'unknown error'}`;
      } else {
        statusEl.textContent = 'Queued — waiting for browser';
        allTerminal = false;
      }
    }

    if (allTerminal) {
      clearInterval(batchState.pollingInterval);
      batchState.pollingInterval = null;
      document.getElementById('stopBatchBtn').style.display = 'none';
      document.getElementById('batchStatusText').textContent = 'All jobs finished.';
    }
  } catch (err) {
    console.error('Poll error:', err.message);
  }
}
