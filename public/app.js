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
  isManual: false
};

document.getElementById('uploadBtn').addEventListener('click', async () => {
  const slotInputs = document.querySelectorAll('.slot-file-input');
  const files = [];
  slotInputs.forEach(input => {
    if (input.files[0]) files.push(input.files[0]);
  });

  if (files.length === 0) {
    alert('Please select at least one CSV file.');
    return;
  }

  const manual = document.getElementById('manualLogin').checked;
  const cookies = document.getElementById('cookies').value;

  const fd = new FormData();
  files.forEach(f => fd.append('files', f));
  fd.append('manual_login', manual ? 'true' : 'false');
  fd.append('cookies', cookies);

  document.getElementById('uploadBtn').disabled = true;
  document.getElementById('uploadBtn').textContent = 'Uploading...';

  try {
    const res = await fetch('/upload-batch', { method: 'POST', body: fd });
    if (!res.ok) {
      const errText = await res.text();
      throw new Error(`Upload failed: ${errText}`);
    }
    const data = await res.json();

    batchState.batchId = data.batchId;
    batchState.jobSpecs = data.jobSpecs;
    batchState.isManual = manual;

    switchToDashboard(data.jobSpecs, manual);
  } catch (err) {
    alert(`Error: ${err.message}`);
    document.getElementById('uploadBtn').disabled = false;
    document.getElementById('uploadBtn').textContent = 'Upload Files & Open Browser';
  }
});

function switchToDashboard(jobSpecs, isManual) {
  document.getElementById('uploadSection').style.display = 'none';
  document.getElementById('dashboardSection').style.display = 'block';

  // Show only the panels for uploaded slots; hide the rest
  for (let i = 0; i < 5; i++) {
    const panel = document.getElementById(`panel-${i}`);
    const spec = jobSpecs[i];
    if (spec) {
      panel.classList.add('visible');
      document.getElementById(`filename-${i}`).textContent = `Slot ${i + 1}: ${spec.originalFilename}`;
      document.getElementById(`status-${i}`).textContent = 'Queued — waiting for browser';
      // Make download link available immediately (partial CSV)
      const dl = document.getElementById(`download-${i}`);
      dl.href = `/download/${spec.jobId}`;
      dl.style.display = 'block';
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
    // Auto-start: just begin polling
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
    // Polling will detect the stopped state
  } catch (err) {
    document.getElementById('batchStatusText').textContent = `Stop error: ${err.message}`;
    stopBtn.disabled = false;
    stopBtn.textContent = 'Stop All Jobs';
  }
}

function startPolling() {
  if (batchState.pollingInterval) clearInterval(batchState.pollingInterval);
  batchState.pollingInterval = setInterval(pollBatchStatus, 2500);
  // Poll immediately too
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

      // Update progress bar
      bar.style.width = `${pct}%`;
      bar.textContent = `${pct}%`;

      // Color coding
      if (status === 'running') {
        bar.style.backgroundColor = '#4caf50'; // green
        allTerminal = false;
      } else if (status === 'finished') {
        bar.style.width = '100%';
        bar.textContent = '100%';
        bar.style.backgroundColor = '#1976d2'; // blue
      } else if (status === 'stopped') {
        bar.style.backgroundColor = '#ff9800'; // orange
      } else if (status === 'error') {
        bar.style.backgroundColor = '#f44336'; // red
      } else {
        // queued / pending
        allTerminal = false;
      }

      // Status text
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
