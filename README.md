# Hosted Enrichment Scraper

This project provides a minimal web interface and a Playwright-powered backend to automate data enrichment tasks from web pages. You can upload a CSV with profile URLs or search terms, and the worker will visit each page, extract information like emails and phone numbers, and provide an enriched CSV for download.

## Project Structure

```
enrich-hosted/
  server.js            # Express app + upload endpoints + job queue
  worker.js            # Playwright worker that performs UI actions
  public/
    index.html         # Simple frontend to upload CSV and show job status
    app.js             # Frontend JS (fetch endpoints)
  input-samples/
    input.csv          # Sample input file
  playwright_profile/  # Persistent browser profile (created by Playwright)
  outputs/             # Enriched CSV files are saved here (created by script)
  uploads/             # Temporary storage for uploaded CSVs (created by script)
  .env                 # Secrets and configuration
  package.json         # Project dependencies
  README.md            # This file
```

## Quick Start (Local Development)

1.  **Install Dependencies:**
    ```bash
    # Navigate into the project directory
    cd enrich-hosted

    # Install backend, playwright, and CSV libraries
    npm install

    # Install Playwright browsers
    npx playwright install
    ```

2.  **Run the Server:**
    ```bash
    node server.js
    ```
    This will start the Express server on `http://localhost:3000`.

3.  **Use the Application:**
    *   Open your web browser and navigate to `http://localhost:3000`.
    *   Select a CSV file to upload. The default configuration assumes the first column (`PROFILE_URL_COLUMN_INDEX: 0`) contains the URLs to visit.
    *   Choose your login method:
        *   **Manual Login (Recommended):** Leave the "Manual login?" box checked. A browser window will open. Log into the target site manually. Once logged in, press `Enter` in the console window that's running `server.js` to begin processing. The session will be saved in the `playwright_profile` directory for future runs.
        *   **Cookies:** Uncheck "Manual login" and paste your session cookies into the "Cookies" text area.
    *   (Optional) Provide custom selectors in the "Selectors JSON" field if the target site's structure differs from the defaults.
    *   Click "Upload & Start".
    *   The server will log progress to the console, and the final enriched CSV will be available in the `outputs/` directory.

## How to Get Selectors, Cookies, and XHR Details

For the scraper to work, you must provide it with CSS selectors to find elements on the target website.

1.  **Open Developer Tools:** Open the target site in your browser (e.g., Chrome, Firefox) and open the Developer Tools (usually by pressing F12 or right-clicking and selecting "Inspect").

2.  **Find CSS Selectors:**
    *   Use the "Elements" panel to inspect the page's HTML.
    *   Right-click the element you need (e.g., the search input field, the results container).
    *   Go to **Copy > Copy selector**.
    *   Paste this value into the "Selectors JSON" text area on the web UI, using the corresponding key (e.g., `NAME_INPUT_SELECTOR`).
    *   **Tip:** If selectors look brittle (e.g., `div#aB-c123_d4`), try to create a more robust one based on stable attributes like `input[name="search_query"]`.

3.  **Find Cookies:**
    *   In Developer Tools, go to the "Application" tab.
    *   Under "Storage" on the left, find "Cookies" and select the domain for the site.
    *   You can copy the values from here. A simple way is to find the main session cookie and copy its entire `name=value` string. Combine multiple cookies with a semicolon: `cookie1=value1; cookie2=value2`.

4.  **(Advanced) Find XHR API Calls:**
    *   Go to the "Network" tab in Developer Tools and filter by "XHR" or "Fetch".
    *   Perform a search on the website.
    *   Look for a request that returns the data you need (often in JSON format).
    *   Click on the request and view the "Headers" tab to get the **Request URL** and any required **Request Headers** (like `Authorization`, `X-CSRF-Token`, etc.). Using an XHR endpoint is generally faster and more reliable than scraping the DOM.

### Checklist of Placeholders to Fill

You will need to provide these values, either in the frontend's "Selectors JSON" text area or by modifying the `worker.js` script.

*   `SEARCH_PAGE_URL`: The URL of the page where you perform searches.
*   `SINGLE_INPUT_SELECTOR`: If there's one input field for name, company, etc.
*   `NAME_INPUT_SELECTOR`: Selector for the "name" input field.
*   `COMPANY_INPUT_SELECTOR`: Selector for the "company" input field.
*   `SUBMIT_BUTTON_SELECTOR`: Selector for the form submission button.
*   `RESULT_CONTAINER_SELECTOR`: A selector for the element that contains the results after a search.
*   `EMAIL_ITEM_SELECTOR`: A specific selector for elements containing email addresses within the results.
*   `PROFILE_URL_COLUMN_INDEX`: The 0-based index of the column in your CSV that contains the profile URL.

## Safety and Best Practices

*   **Rate Limiting:** The script defaults to a **45-90 second** randomized wait between requests. Be respectful of the target website's terms of service. Do not decrease this delay unless you are certain it is permitted.
*   **Concurrency:** The server runs only one worker at a time to avoid overwhelming the target service.
*   **CAPTCHA:** If a CAPTCHA appears, the worker will pause. You must solve it manually in the visible browser window for the script to continue.
*   **Secrets:** Never commit secrets like API keys or login credentials to your repository. Use the `.env` file for local development and a secrets manager (like AWS Secrets Manager or HashiCorp Vault) for production.
*   **Legal:** Ensure you have the right to access and process the data from the target website. Always include a TOS/legal consent checkbox in any user-facing application.

## Testing Tips

*   Start with a small `input.csv` (2-3 rows) to test your selectors and logic.
*   Add `console.log` statements inside `worker.js` to debug.
*   To diagnose errors, you can instruct Playwright to take a screenshot and save the page's HTML:
    ```javascript
    // Inside the catch block in worker.js
    await page.screenshot({ path: `debug-error-${i}.png`, fullPage: true });
    const html = await page.content();
    fs.writeFileSync(`debug-error-${i}.html`, html);
    ```
