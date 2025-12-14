document.getElementById('uploadForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const file = document.getElementById('file').files[0];
  if(!file) return alert('Please upload a CSV');
  const manual = document.getElementById('manualLogin').checked;
  const cookies = document.getElementById('cookies').value;
  const selectors = document.getElementById('selectors').value;

  const fd = new FormData();
  fd.append('file', file);
  fd.append('manual_login', manual ? 'true' : 'false');
  fd.append('cookies', cookies);
  fd.append('selectors', selectors);

  const res = await fetch('/upload', { method:'POST', body: fd });
  const data = await res.json();
  document.getElementById('result').innerHTML = `Job started: ${data.jobId} <br/>Check /status/${data.jobId}`;
});
