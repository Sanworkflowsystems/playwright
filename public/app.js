document.getElementById('uploadForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    const fileInput = document.getElementById('file');
    const file = fileInput.files[0];
    if (!file) return alert('Please upload a CSV');

    const manual = document.getElementById('manualLogin').checked;
    const cookies = document.getElementById('cookies').value;

    const fd = new FormData();
    fd.append('file', file);
    fd.append('manual_login', manual ? 'true' : 'false');
    fd.append('cookies', cookies);
    
    // Hide form, show status area
    document.getElementById('uploadForm').style.display = 'none';
    document.getElementById('statusArea').style.display = 'block';
    document.getElementById('statusText').textContent = 'Uploading file and starting job...';

    try {
        const res = await fetch('/upload', { method: 'POST', body: fd });
        if (!res.ok) {
            const errorText = await res.text();
            throw new Error(`Upload failed: ${errorText}`);
        }
        const data = await res.json();
        pollStatus(data.jobId);
    } catch (err) {
        document.getElementById('statusText').textContent = `Error: ${err.message}`;
    }
});

function pollStatus(jobId) {
    const statusText = document.getElementById('statusText');
    const progressBar = document.getElementById('progressBar');
    const downloadLink = document.getElementById('downloadLink');

    const intervalId = setInterval(async () => {
        try {
            const res = await fetch(`/status/${jobId}`);
            if (!res.ok) {
                throw new Error(`Server returned status ${res.status}`);
            }
            const data = await res.json();

            statusText.textContent = `Status: ${data.status}`;

            if (data.status === 'running' && data.total > 0) {
                const percentage = Math.round((data.progress / data.total) * 100);
                progressBar.style.width = `${percentage}%`;
                progressBar.textContent = `${percentage}%`;
                statusText.textContent = `Status: ${data.status} (Processed ${data.progress} of ${data.total} rows)`;
            }

            if (data.status === 'finished') {
                clearInterval(intervalId);
                statusText.textContent = 'Job complete!';
                progressBar.style.width = `100%`;
                progressBar.textContent = `100%`;
                downloadLink.setAttribute('href', `/download/${jobId}`);
                downloadLink.style.display = 'block';
            }

            if (data.status === 'error') {
                clearInterval(intervalId);
                statusText.textContent = `Error: ${data.error || 'An unknown error occurred.'}`;
                progressBar.style.backgroundColor = '#f44336'; // Red for error
            }

        } catch (err) {
            clearInterval(intervalId);
            statusText.textContent = `Error polling status: ${err.message}`;
        }
    }, 2000); // Poll every 2 seconds
}
