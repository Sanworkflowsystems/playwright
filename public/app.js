// app.js — Batch enrichment UI logic (ContactOut + Pre-Enrichment Pipeline)

// ---- Account selector ----
let selectedAccount = 'none';

function selectAccount(account) {
  selectedAccount = account;
  document.querySelectorAll('.account-btn').forEach(btn => {
    btn.classList.toggle('selected', btn.dataset.account === account);
  });
  const note = document.getElementById('accountNote');
  if (account === 'none') {
    note.textContent = 'Select an account to have the script log in automatically via Google. Or choose "No auto-login" to use manual login or cookies.';
    document.getElementById('manualLogin').disabled = false;
  } else {
    note.textContent = `Account ${account} selected — the script will handle Google login automatically. Manual login checkbox is ignored.`;
    document.getElementById('manualLogin').disabled = true;
  }
}

// Default selection
selectAccount('none');

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
    fd.append('account', selectedAccount);

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
      const tabName = row.querySelector('.sheet-tab-input').value.trim() || '';
      if (url) sheetJobs.push({ sheetUrl: url, sheetName: tabName });
    });

    if (sheetJobs.length === 0) { alert('Please enter at least one Google Sheet URL.'); return; }

    // Read row range (shared with pipeline mode). Applies to ContactOut-only runs too.
    const rowStartRaw = (document.getElementById('rowStart')?.value || '').trim();
    const rowEndRaw   = (document.getElementById('rowEnd')?.value || '').trim();
    const rowStart = rowStartRaw ? Math.max(2, parseInt(rowStartRaw, 10)) : null;
    const rowEnd   = rowEndRaw   ? parseInt(rowEndRaw, 10) : null;
    if (rowStart && rowEnd && rowEnd < rowStart) {
      alert('Row range end must be ≥ start.');
      return;
    }

    btn.disabled = true;
    btn.textContent = 'Starting...';

    try {
      const res = await fetch('/start-sheet-batch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sheetJobs, cookies, manual_login: manual ? 'true' : 'false', account: selectedAccount, rowStart, rowEnd })
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

  // Always start verify polling so we catch the email code prompt
  const jobIds = jobSpecs.map(s => s.jobId);
  startVerifyPolling(jobIds);

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

// ---- Verify code handling ----
let verifyPollInterval = null;
let currentVerifyJobId = null;

function startVerifyPolling(jobIds) {
  if (verifyPollInterval) clearInterval(verifyPollInterval);
  verifyPollInterval = setInterval(() => pollVerifyStatus(jobIds), 2000);
}

async function pollVerifyStatus(jobIds) {
  for (const jobId of jobIds) {
    try {
      const res = await fetch(`/verify-status/${jobId}`);
      const data = await res.json();
      if (data.waiting) {
        currentVerifyJobId = jobId;
        document.getElementById('verifyBanner').style.display = 'block';
        document.getElementById('verifyBannerMsg').textContent =
          `ContactOut sent a 6-digit code to the account email. Enter it below:`;
        document.getElementById('verifyMsg').textContent = '';
        return;
      }
    } catch (e) { /* ignore */ }
  }
  // No job waiting — hide banner
  document.getElementById('verifyBanner').style.display = 'none';
  currentVerifyJobId = null;
}

async function submitVerifyCode() {
  const code = document.getElementById('verifyCodeInput').value.trim();
  if (!code || code.length !== 6) {
    document.getElementById('verifyMsg').textContent = 'Please enter the full 6-digit code.';
    return;
  }
  if (!currentVerifyJobId) {
    document.getElementById('verifyMsg').textContent = 'No job is waiting for a code right now.';
    return;
  }
  try {
    const res = await fetch(`/submit-verify/${currentVerifyJobId}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ code })
    });
    if (res.ok) {
      document.getElementById('verifyMsg').textContent = 'Code submitted! The script will continue shortly...';
      document.getElementById('verifyCodeInput').value = '';
      document.getElementById('verifyBanner').style.display = 'none';
      currentVerifyJobId = null;
    } else {
      document.getElementById('verifyMsg').textContent = 'Failed to submit code. Try again.';
    }
  } catch (e) {
    document.getElementById('verifyMsg').textContent = 'Error: ' + e.message;
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

// ════════════════════════════════════════════════════════════════════════════
// PIPELINE — state, controls, polling
// ════════════════════════════════════════════════════════════════════════════

let pipelineState = {
  pipelineJobId:   null,
  pollingInterval: null,
  sheetJobs:       null,   // remembered so we can hand off to ContactOut
  isSheetMode:     false,
  lastStats:       null,
};

// ── Toggle panel visibility ──────────────────────────────────────────────────

document.getElementById('pipelineEnabled').addEventListener('change', function () {
  document.getElementById('pipelinePanel').style.display = this.checked ? 'block' : 'none';
  const coOnly = document.getElementById('contactoutOnlyPanel');
  if (coOnly) coOnly.style.display = this.checked ? 'none' : 'block';
  if (this.checked) loadKeyHealth();
});

document.getElementById('amfEnabled').addEventListener('change', function () {
  document.getElementById('amfPanel').style.display = this.checked ? 'block' : 'none';
});

// ── Save keys to server ──────────────────────────────────────────────────────

async function savePipelineKeys() {
  const prospeoRaw = (document.getElementById('prospeoKeys').value || '').trim();
  const hunterRaw  = (document.getElementById('hunterKeys').value  || '').trim();
  const bouncerRaw = (document.getElementById('bouncerKey').value  || '').trim();

  const parseLines = (s) => s.split('\n').map(l => l.trim()).filter(Boolean);

  const body = {
    prospeo: { keys: parseLines(prospeoRaw) },
    hunter:  { keys: parseLines(hunterRaw)  },
    bouncer: { keys: bouncerRaw ? [bouncerRaw] : [] },
  };

  const msg = document.getElementById('saveKeysMsg');
  msg.textContent = 'Saving...';
  msg.style.color = '#555';

  try {
    const res = await fetch('/pipeline-keys', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(await res.text());
    msg.textContent = 'Saved!';
    msg.style.color = 'green';
    await loadKeyHealth();
  } catch (err) {
    msg.textContent = 'Error: ' + err.message;
    msg.style.color = 'red';
  }
  setTimeout(() => { msg.textContent = ''; }, 4000);
}

// ── Key health display ───────────────────────────────────────────────────────

async function loadKeyHealth() {
  try {
    const res  = await fetch('/pipeline-keys');
    const data = await res.json();

    for (const svc of ['prospeo', 'hunter', 'bouncer']) {
      const stats = data[svc];
      const dot   = document.getElementById(`dot-${svc}`);
      const label = document.getElementById(`health-${svc}`);
      if (!stats || !dot || !label) continue;

      const { total, available, onCooldown, dailyPaused } = stats;
      label.textContent = `${available}/${total} available`;
      dot.className = 'key-dot ' + (
        available === total          ? 'green'  :
        available > 0                ? 'yellow' : 'red'
      );
    }
  } catch (_) { /* ignore — server may not have keys yet */ }
}

// ── Intercept the Upload button when pipeline is enabled ─────────────────────

const _origUploadClick = document.getElementById('uploadBtn').onclick;

document.getElementById('uploadBtn').addEventListener('click', async function (e) {
  if (!document.getElementById('pipelineEnabled').checked) return; // normal flow
  e.stopImmediatePropagation();  // prevent the existing listener from firing

  const mode    = getMode();
  const btn     = document.getElementById('uploadBtn');
  btn.disabled  = true;
  btn.textContent = 'Starting pipeline...';

  try {
    let body, fetchOpts;

    const skipProspeo = document.getElementById('skipProspeoPhase')?.checked;
    const skipPhases  = skipProspeo ? ['prospeo'] : [];

    const rowStartRaw = document.getElementById('rowStart')?.value?.trim();
    const rowEndRaw   = document.getElementById('rowEnd')?.value?.trim();
    const rowStart = rowStartRaw ? Math.max(2, parseInt(rowStartRaw, 10)) : null;
    const rowEnd   = rowEndRaw   ? parseInt(rowEndRaw,   10)              : null;
    if (rowStart && rowEnd && rowEnd < rowStart) {
      alert('Row End must be >= Row Start.');
      btn.disabled = false;
      btn.textContent = mode === 'sheets' ? 'Start Sheet Job & Open Browser' : 'Upload Files & Open Browser';
      return;
    }
    pipelineState.rowStart = rowStart;
    pipelineState.rowEnd   = rowEnd;

    if (mode === 'sheets') {
      const sheetJobs = [];
      document.querySelectorAll('#sheetSlots .sheet-slot-row').forEach(row => {
        const url     = row.querySelector('.sheet-url-input').value.trim();
        const tabName = row.querySelector('.sheet-tab-input').value.trim() || '';
        if (url) sheetJobs.push({ sheetUrl: url, sheetName: tabName });
      });
      if (sheetJobs.length === 0) { alert('Enter at least one Google Sheet URL.'); btn.disabled = false; btn.textContent = 'Start Sheet Job & Open Browser'; return; }

      pipelineState.sheetJobs  = sheetJobs;
      pipelineState.isSheetMode = true;
      body = JSON.stringify({ sheetJobs, skipPhases, rowStart, rowEnd });
      fetchOpts = { method: 'POST', headers: { 'Content-Type': 'application/json' }, body };

    } else {
      const slotInputs = document.querySelectorAll('.slot-file-input');
      const files = [];
      slotInputs.forEach(inp => { if (inp.files[0]) files.push(inp.files[0]); });
      if (files.length === 0) { alert('Select at least one CSV file.'); btn.disabled = false; btn.textContent = 'Upload Files & Open Browser'; return; }

      pipelineState.isSheetMode = false;
      const fd = new FormData();
      files.forEach(f => fd.append('files', f));
      if (skipPhases.length) fd.append('skipPhases', skipPhases.join(','));
      if (rowStart) fd.append('rowStart', String(rowStart));
      if (rowEnd)   fd.append('rowEnd',   String(rowEnd));
      fetchOpts = { method: 'POST', body: fd };
    }

    const res = await fetch('/start-pipeline', fetchOpts);
    if (!res.ok) {
      const err = await res.json().catch(() => ({ error: res.statusText }));
      throw new Error(err.error || 'Failed to start pipeline.');
    }
    const data = await res.json();
    pipelineState.pipelineJobId = data.pipelineJobId;

    // Switch to pipeline dashboard
    document.getElementById('uploadSection').style.display = 'none';
    document.getElementById('pipelineDashboard').style.display = 'block';
    document.getElementById('pipelinePhaseLabel').textContent = `Processing ${data.totalRows} rows...`;
    document.getElementById('stopPipelineBtn').style.display = 'inline-block';
    document.getElementById('continueToCOBtn').style.display = 'none';
    document.getElementById('pipelineStatusText').textContent = '';

    startPipelinePolling();

  } catch (err) {
    alert('Pipeline error: ' + err.message);
    btn.disabled = false;
    btn.textContent = mode === 'sheets' ? 'Start Sheet Job & Open Browser' : 'Upload Files & Open Browser';
  }
}, true);   // capture phase so we run before the existing bubble-phase listener

// ── Pipeline polling ─────────────────────────────────────────────────────────

function startPipelinePolling() {
  if (pipelineState.pollingInterval) clearInterval(pipelineState.pollingInterval);
  pipelineState.pollingInterval = setInterval(pollPipelineStatus, 2000);
  pollPipelineStatus();
}

async function pollPipelineStatus() {
  if (!pipelineState.pipelineJobId) return;
  try {
    const res  = await fetch(`/pipeline-status/${pipelineState.pipelineJobId}`);
    if (!res.ok) return;
    const data = await res.json();

    const progress = data.progress || 0;
    const total    = data.total    || 0;
    const pct      = total > 0 ? Math.round((progress / total) * 100) : 0;
    const stats    = data.stats   || {};
    const status   = data.status  || 'running';

    // Progress bar
    const bar = document.getElementById('pipelineProgressBar');
    bar.style.width   = `${pct}%`;
    bar.textContent   = total > 0 ? `${progress} / ${total} (${pct}%)` : 'Starting...';
    bar.style.background = status === 'finished' ? '#1565c0' : status === 'error' ? '#e53935' : '#43a047';

    // Stats
    document.getElementById('stat-prospeo').textContent = stats.prospeoResolved  || 0;
    document.getElementById('stat-hunter').textContent  = stats.hunterResolved   || 0;
    document.getElementById('stat-bouncer').textContent = stats.bouncerVerified  || 0;
    document.getElementById('stat-co').textContent      = stats.contactoutNeeded || 0;
    document.getElementById('stat-errors').textContent  = stats.errors           || 0;

    pipelineState.lastStats = stats;

    // Phase label
    const phaseLabel = document.getElementById('pipelinePhaseLabel');
    if (status === 'writing_sheets') {
      phaseLabel.textContent = 'Writing results to Google Sheets...';
    } else if (status === 'finished') {
      phaseLabel.textContent = `Done! ${stats.prospeoResolved + stats.hunterResolved} emails found via API, ${stats.contactoutNeeded || 0} need ContactOut.`;
    } else if (status === 'error') {
      phaseLabel.textContent = `Error: ${data.error || 'unknown error'}`;
    } else {
      phaseLabel.textContent = `Processing ${total} rows... (${progress} done)`;
    }

    document.getElementById('pipelineStatusText').textContent = '';

    if (status === 'finished' || status === 'error' || status === 'stopped') {
      clearInterval(pipelineState.pollingInterval);
      pipelineState.pollingInterval = null;
      document.getElementById('stopPipelineBtn').style.display = 'none';
      if (status === 'finished' && pipelineState.isSheetMode) {
        document.getElementById('continueToCOBtn').style.display = 'inline-block';
      }
    }
  } catch (err) {
    console.error('Pipeline poll error:', err.message);
  }
}

// ── Stop pipeline ────────────────────────────────────────────────────────────

async function stopPipeline() {
  if (!pipelineState.pipelineJobId) return;
  try {
    await fetch(`/stop-pipeline/${pipelineState.pipelineJobId}`, { method: 'POST' });
    document.getElementById('pipelineStatusText').textContent = 'Stop signal sent.';
  } catch (err) {
    document.getElementById('pipelineStatusText').textContent = 'Stop error: ' + err.message;
  }
}

// ── Continue to ContactOut for unresolved rows ────────────────────────────────

async function continueToContactOut() {
  if (!pipelineState.isSheetMode || !pipelineState.sheetJobs) {
    alert('ContactOut handoff only available in Google Sheets mode.');
    return;
  }

  const btn = document.getElementById('continueToCOBtn');
  btn.disabled    = true;
  btn.textContent = 'Launching ContactOut...';

  const manual  = document.getElementById('manualLogin').checked;
  const cookies = document.getElementById('cookies').value;

  try {
    const res = await fetch('/start-sheet-batch', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sheetJobs:    pipelineState.sheetJobs,
        cookies,
        manual_login: manual ? 'true' : 'false',
        account:      selectedAccount,
        rowStart:     pipelineState.rowStart || null,
        rowEnd:       pipelineState.rowEnd   || null,
      }),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({ error: res.statusText }));
      throw new Error(err.error || 'Failed to start ContactOut batch.');
    }
    const data = await res.json();

    batchState.batchId    = data.batchId;
    batchState.jobSpecs   = data.jobSpecs;
    batchState.isManual   = manual;
    batchState.isSheetMode = true;

    document.getElementById('pipelineDashboard').style.display = 'none';
    switchToDashboard(data.jobSpecs, manual, true);
  } catch (err) {
    alert('ContactOut launch error: ' + err.message);
    btn.disabled    = false;
    btn.textContent = 'Continue to ContactOut →';
  }
}

// ── Load saved keys into the textareas on page load ──────────────────────────

(async function loadSavedKeys() {
  try {
    const res  = await fetch('/pipeline-config');
    if (!res.ok) return;
    const cfg  = await res.json();

    // Keys are redacted (…suffix) so just show count hints, not values
    if (cfg.prospeo?.keyCount > 0) {
      document.getElementById('prospeoKeys').placeholder =
        `${cfg.prospeo.keyCount} key(s) saved — paste to replace`;
    }
    if (cfg.hunter?.keyCount > 0) {
      document.getElementById('hunterKeys').placeholder =
        `${cfg.hunter.keyCount} key(s) saved — paste to replace`;
    }
    if (cfg.bouncer?.keyCount > 0) {
      document.getElementById('bouncerKey').placeholder =
        `1 key saved — paste to replace`;
    }
  } catch (_) { /* ignore */ }
})();

// ════════════════════════════════════════════════════════════════════════════
// AnyMailFinder mode
// ════════════════════════════════════════════════════════════════════════════

let amfState = { jobId: null, interval: null };

async function startAnyMailFinder() {
  const apiKey = document.getElementById('amfApiKey').value.trim();
  if (!apiKey) { alert('Please enter your AnyMailFinder API key.'); return; }

  // Get sheet URL from the first filled slot
  const sheetRows = document.querySelectorAll('#sheetSlots .sheet-slot-row');
  let sheetUrl = '', sheetName = '';
  for (const row of sheetRows) {
    const u = row.querySelector('.sheet-url-input').value.trim();
    if (u) { sheetUrl = u; sheetName = row.querySelector('.sheet-tab-input').value.trim(); break; }
  }
  if (!sheetUrl) { alert('Please enter a Google Sheet URL in Slot 1.'); return; }

  // Row range (shared with all other modes)
  const rowStartRaw = (document.getElementById('rowStart')?.value || '').trim();
  const rowEndRaw   = (document.getElementById('rowEnd')?.value || '').trim();
  const rowStart = rowStartRaw ? Math.max(2, parseInt(rowStartRaw, 10)) : null;
  const rowEnd   = rowEndRaw   ? parseInt(rowEndRaw, 10) : null;
  if (rowStart && rowEnd && rowEnd < rowStart) { alert('Row range end must be ≥ start.'); return; }

  const btn = document.getElementById('startAmfBtn');
  btn.disabled = true;
  btn.textContent = 'Starting...';

  try {
    const res = await fetch('/start-anymailfinder', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sheetUrl, sheetName, apiKey, rowStart, rowEnd }),
    });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || res.statusText); }
    const data = await res.json();
    amfState.jobId = data.amfJobId;

    document.getElementById('amfProgress').style.display = 'block';
    document.getElementById('amfStopRow').style.display = 'none';
    document.getElementById('amfStatusLine').textContent = 'Running...';
    document.getElementById('amfProgressBar').style.width = '0%';
    document.getElementById('amfProgressBar').textContent = '0%';
    document.getElementById('amfFound').textContent = '0';
    document.getElementById('amfNotFound').textContent = '0';
    document.getElementById('amfCredits').textContent = '0';

    btn.textContent = 'Running...';
    amfPoll();
    amfState.interval = setInterval(amfPoll, 2000);
  } catch (err) {
    alert('AnyMailFinder error: ' + err.message);
    btn.disabled = false;
    btn.textContent = 'Start AnyMailFinder';
  }
}

async function amfPoll() {
  if (!amfState.jobId) return;
  try {
    const res = await fetch(`/anymailfinder-status/${amfState.jobId}`);
    if (!res.ok) return;
    const data = await res.json();

    const processed = data.processed || 0;
    const total     = data.total     || 0;
    const pct       = total > 0 ? Math.round((processed / total) * 100) : 0;

    const bar = document.getElementById('amfProgressBar');
    bar.style.width   = `${pct}%`;
    bar.textContent   = total > 0 ? `${processed} / ${total} (${pct}%)` : 'Working...';

    document.getElementById('amfFound').textContent    = data.found    || 0;
    document.getElementById('amfNotFound').textContent = data.notFound || 0;
    document.getElementById('amfCredits').textContent  = data.credits  || 0;

    const statusLine = document.getElementById('amfStatusLine');
    const stopRowDiv = document.getElementById('amfStopRow');
    const btn        = document.getElementById('startAmfBtn');

    if (data.status === 'finished') {
      clearInterval(amfState.interval);
      statusLine.textContent = `Done — found ${data.found || 0} emails, used ${data.credits || 0} credits.`;
      bar.style.background = '#43a047';
      btn.disabled = false;
      btn.textContent = 'Start AnyMailFinder';
    } else if (data.status === 'credits_exhausted') {
      clearInterval(amfState.interval);
      statusLine.textContent = 'Credits exhausted — stopped.';
      bar.style.background = '#f59e0b';
      stopRowDiv.style.display = 'block';
      stopRowDiv.textContent = `Credits ran out at sheet row ${data.stoppedAtRow}. Update your API key and re-run from row ${data.stoppedAtRow}.`;
      btn.disabled = false;
      btn.textContent = 'Start AnyMailFinder';
    } else if (data.status === 'error') {
      clearInterval(amfState.interval);
      statusLine.textContent = `Error: ${data.error || 'unknown'}`;
      bar.style.background = '#e53935';
      btn.disabled = false;
      btn.textContent = 'Start AnyMailFinder';
    } else if (data.status === 'stopped') {
      clearInterval(amfState.interval);
      statusLine.textContent = 'Stopped.';
      btn.disabled = false;
      btn.textContent = 'Start AnyMailFinder';
    } else {
      statusLine.textContent = `Running — processed ${processed} / ${total || '?'}...`;
    }
  } catch (_) { /* ignore transient errors */ }
}

async function stopAnyMailFinder() {
  if (!amfState.jobId) return;
  clearInterval(amfState.interval);
  try { await fetch(`/stop-anymailfinder/${amfState.jobId}`, { method: 'POST' }); } catch (_) {}
  document.getElementById('amfStatusLine').textContent = 'Stopping...';
}
