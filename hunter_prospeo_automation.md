# Enrich Hosted Context Reference

This document summarizes the current implementation of the `enrich-hosted` app so a future coding session can quickly understand the system without rediscovering the architecture.

## Purpose

`enrich-hosted` is a local lead-enrichment app that runs on `localhost:3000` with `node server.js`.

It has two enrichment layers:

- API pre-enrichment using Prospeo, Hunter.io, and Bouncer.
- ContactOut browser automation using Playwright for the remaining hard-to-find rows.

The intended workflow is:

- Load leads from a Google Sheet or CSV.
- Run the API pre-enrichment pipeline first.
- Write any verified final emails into the existing `finalEmail` column.
- Mark rows that still need browser scraping as `contactout_needed`.
- Continue to ContactOut automation for those unresolved rows.

## Main Files

- `server.js` is the Express server. It serves the UI, handles uploads, manages Google Sheets OAuth, starts ContactOut jobs, starts API pipeline jobs, saves API keys, exposes progress endpoints, and coordinates worker processes.
- `worker.js` is the single-job ContactOut worker. It opens one Playwright browser context and processes one CSV or Google Sheet job sequentially.
- `parallel-worker.js` is the batch ContactOut worker. It opens one browser and multiple tabs, one tab per file or sheet, and processes tabs concurrently.
- `google-sheets-helper.js` contains shared Google Sheets read/write helpers, including pipeline metadata column creation and batched pipeline writes.
- `pipeline/orchestrator.js` runs the Prospeo, Bouncer, Hunter, Bouncer waterfall and writes live progress/results.
- `pipeline/prospeo-client.js` wraps Prospeo single and bulk enrichment endpoints.
- `pipeline/hunter-client.js` wraps Hunter email finder.
- `pipeline/bouncer-client.js` wraps Bouncer email verification.
- `pipeline/key-pool.js` handles API key rotation, rate limits, cooldowns, daily limits, and key health.
- `pipeline/pipeline-config.js` loads `pipeline-config.json`.
- `pipeline-config.json` contains local API keys and current rate-limit configuration.
- `public/index.html` and `public/app.js` are the local browser UI.

## Google Sheets OAuth

Google Sheets access is already implemented through OAuth in `server.js`.

The app expects `credentials.json` and stores the OAuth token in `tokens.json`. The UI can authenticate through `/auth/google`, then `/auth/status` reports whether the server has a usable Sheets client.

This OAuth is only for Google Sheets read/write access. ContactOut login is separate and is handled through Playwright browser automation, cookies, manual login, or configured ContactOut account credentials.

## Current Input Sheet Format

The known Google Sheet columns are:

- `profileUrl`
- `fullName`
- `firstName`
- `lastName`
- `companyName`
- `title`
- `companyId`
- `companyUrl`
- `regularCompanyUrl`
- `summary`
- `titleDescription`
- `industry`
- `companyLocation`
- `location`
- `durationInRole`
- `durationInCompany`
- `pastExperienceCompanyName`
- `pastExperienceCompanyUrl`
- `pastExperienceCompanyTitle`
- `pastExperienceDate`
- `pastExperienceDuration`
- `connectionDegree`
- `profileImageUrl`
- `sharedConnectionsCount`
- `name`
- `vmid`
- `linkedInProfileUrl`
- `isPremium`
- `isOpenLink`
- `query`
- `timestamp`
- `defaultProfileUrl`
- `icebreaker_processed`
- `contactout_processed`
- `Processed`
- `Personal Email`
- `Other Personal Emails`
- `Work Email`
- `Work Email Status`
- `Other Work Emails`
- `Phone Number`
- `Other Phone Numbers`
- `finalEmail`
- `icebreaker`
- `shortCompanyName`
- `research`

## API Pipeline Field Mapping

The API pipeline extracts these fields from the sheet:

- Person LinkedIn URL comes from `defaultProfileUrl` first. This is the strongest Prospeo signal.
- If `defaultProfileUrl` is unavailable, `linkedInProfileUrl` can be used as a fallback.
- First name comes from `firstName`.
- Last name comes from `lastName`.
- Full name comes from `fullName`.
- Company name comes from `companyName`.
- Company LinkedIn URL comes from `regularCompanyUrl` first.
- `companyUrl` is treated only as a backup company LinkedIn URL, not as a company website.
- Company website is currently left blank because this sheet does not contain a reliable company website/domain column.
- Existing email detection includes `finalEmail`, `Work Email`, generic `email`, and old `enrichment_email`.

Prospeo documentation says match accuracy is best when company website is available, but this sheet currently does not include a real company website. The best available Prospeo inputs are therefore person LinkedIn URL, company LinkedIn URL, company name, first name, last name, and full name.

## Prospeo Implementation

Prospeo uses the bulk enrichment endpoint.

The current implementation is intended to be compliant with the provided Prospeo docs:

- Uses `/bulk-enrich-person`.
- Sends a `data` array.
- Sends a required `identifier` per row.
- Sends `only_verified_email` as true.
- Sends `enrich_mobile` as false to avoid 10-credit mobile enrichment.
- Sends person LinkedIn as `linkedin_url`.
- Sends company name as `company_name`.
- Sends company LinkedIn as `company_linkedin_url`.
- Sends first name, last name, and full name when available.
- Does not submit `company_website` unless a real non-LinkedIn website is available.
- Parses results from the `matched` array and maps them back by `identifier`.
- Treats missing, masked, or malformed emails as no result.

## Prospeo Rate Limits

Current Prospeo plan limits per key:

- 1 request per second.
- 20 submitted enrich records per minute.
- 50 submitted enrich records per day.
- Bulk endpoint accepts up to 50 records, but the quota counts one request per submitted record.

Current local config:

- 8 Prospeo keys.
- `bulkSize` is 10.
- `minuteLimit` is 20 per key.
- `dailyLimit` is 50 per key.

Why `bulkSize` is 10:

- Each 10-record bulk call costs 10 submitted-record units.
- Each key can run two 10-record bulk calls per minute, reaching 20 submitted records per minute.
- Each key can run five 10-record bulk calls per day, reaching the full 50 submitted records per day.
- Across 8 keys, Prospeo can submit up to 160 records per minute and 400 records per day.

## Hunter Implementation

Hunter is used as fallback after Prospeo and Bouncer.

Current behavior:

- Runs on rows still unresolved after Prospeo.
- Derives a guessed domain from `companyName`.
- Calls Hunter email finder with first name, last name, guessed domain, and a rotating Hunter key.
- Uses 9 Hunter keys from local config.
- Default config uses 10 requests per second per key.
- Hunter does not currently use a real company website/domain from the sheet because the sheet does not contain one.

Important limitation:

Hunter accuracy depends heavily on the domain. Since the current sheet has company LinkedIn URLs but not company websites/domains, Hunter currently guesses domains from company names. This is fast, but not as accurate as using real domains.

## Bouncer Implementation

Bouncer verifies emails returned by Prospeo and Hunter.

Current behavior:

- Prospeo hit goes to Bouncer before being accepted.
- If Bouncer accepts the email, it is written to `finalEmail`.
- If Bouncer rejects the email, the row falls through to Hunter.
- Hunter hit goes to Bouncer before being accepted.
- If Bouncer accepts the Hunter email, it is written to `finalEmail`.
- If Bouncer rejects the Hunter email, the row becomes `ContactOut Needed`.
- If Bouncer is unavailable or exhausted, the current fallback is conservative acceptance of the found email.

Current local config has 1 Bouncer key.

## Google Sheet Writes During API Pipeline

The API pipeline now uses a live write queue instead of waiting until the very end.

Current behavior:

- As rows resolve, writes are queued in memory.
- The queue flushes to Google Sheets every few seconds using batched Sheets updates.
- Found emails are written to the existing `finalEmail` column.
- `enrichment_source` and `enrichment_status` are appended if missing.
- Rows that need ContactOut get `enrichment_source` as `ContactOut Needed` and `enrichment_status` as `contactout_needed`.
- Rows marked `contactout_needed` do not blank out `finalEmail`.

This reduces Google Sheets API throttling risk while still saving partial progress during the run.

## ContactOut Automation

ContactOut automation is implemented in `worker.js` and `parallel-worker.js`.

Current behavior:

- Opens ContactOut in Playwright.
- Supports persistent browser profile through `playwright_profile`.
- Supports manual login, cookies, or configured account credentials.
- Supports verification-code prompts through status files and UI input.
- Searches each lead by full name and company name.
- Clicks View email and Find phone where available.
- Scrapes emails and phone numbers from the result card.
- Splits personal vs work emails.
- Writes results back to Google Sheets row by row.
- Marks `contactout_processed` as 1 after writing a row.
- Skips rows where `contactout_processed` is already 1.

Current ContactOut output columns:

- `Personal Email`
- `Other Personal Emails`
- `Work Email`
- `Work Email Status`
- `Other Work Emails`
- `Phone Number`
- `Other Phone Numbers`

Planned improvement:

- ContactOut should also write the best found work email to `finalEmail` when `finalEmail` is blank. This has not yet been completed.

## Server and UI Flow

Normal ContactOut-only flow:

- User chooses CSV or Google Sheets.
- User uploads files or enters sheet URLs.
- Server creates jobs.
- ContactOut worker starts.
- UI polls job or batch status.
- Google Sheet rows are updated immediately as ContactOut finishes each row.

API pipeline flow:

- User enables Pre-Enrichment Pipeline in the UI.
- User starts the job.
- UI calls `/start-pipeline`.
- Server reads rows from the first selected sheet or uploaded CSV.
- Orchestrator runs Prospeo, Bouncer, Hunter, Bouncer.
- Progress is written to `outputs/<jobId>-pipeline.json`.
- UI polls `/pipeline-status/<jobId>`.
- Google Sheet rows update in batches as the API pipeline resolves rows.
- When finished, the UI can continue to ContactOut for unresolved rows.

## Concurrency and Rate-Limit Model

The key pool rotates API keys and tracks:

- Per-key minute usage.
- Per-key daily usage.
- Temporary cooldowns after 429 responses.
- Daily pause after quota exhaustion.
- Invalid key handling through daily pause behavior.

Prospeo uses quota cost equal to the number of submitted records in the bulk call. Hunter and Bouncer use quota cost of 1 per API request.

The pipeline tries to maximize speed while respecting service limits:

- Prospeo runs chunks concurrently, but the key pool only allows work when a key has enough minute and daily quota for that chunk.
- Hunter runs many rows concurrently because the configured key pool has more throughput.
- Bouncer runs verification concurrently but is capped by its key pool settings.

## Expected Timing for 500 Leads

With the current keys and limits:

- Prospeo has 8 keys.
- Each Prospeo key can submit 20 records per minute and 50 records per day.
- Total Prospeo capacity is 160 records per minute and 400 records per day.
- A 500-lead list will submit at most 400 leads to Prospeo before daily Prospeo quota is exhausted.
- The remaining 100 leads, plus Prospeo misses or Bouncer rejects, fall through to Hunter.

Estimated pre-ContactOut timing for 500 leads:

- Prospeo: about 2 to 3 minutes for 400 submitted records because it can process around 160 submitted records per minute across all 8 keys.
- Bouncer verification for Prospeo hits: usually about 10 to 30 seconds, depending on hit count and API latency.
- Hunter fallback: usually about 10 to 30 seconds for the unresolved set, assuming Hunter keys are healthy.
- Bouncer verification for Hunter hits: usually about 5 to 20 seconds.
- Google Sheets writes: usually a few seconds, because writes are batched.

Practical estimate:

- Around 3 to 5 minutes before ContactOut starts, assuming APIs respond normally and no unexpected throttling occurs.
- If Prospeo responds slowly or keys are already partially used for the day, it can be slower and will fall through to Hunter sooner.

Expected output:

- Prospeo can only cover up to 400 of the 500 leads per day with the current 8-key setup.
- Hunter will attempt the unresolved portion.
- Any rows still unresolved after Hunter and Bouncer become `contactout_needed`.

## Current API Key Counts

The local config currently contains:

- 8 Prospeo keys.
- 9 Hunter keys.
- 1 Bouncer key.

The exact key values are intentionally not repeated in this document. They live in `pipeline-config.json`.

## Important Caveats

- There is no reliable company website/domain column in the current sheet. This affects both Prospeo match quality and Hunter domain accuracy.
- Prospeo daily quota is currently the main API bottleneck.
- Hunter currently guesses domains from company names, which can produce misses or inaccurate searches.
- ContactOut currently writes work email fields, but still needs to be updated to populate `finalEmail`.
- Some existing files include non-ASCII separator/comment characters from earlier edits. This does not block execution, but future cleanup would improve readability.
- The repo is inside the `enrich-hosted` folder. Use that as the Git working directory.

## Suggested Next Improvements

- Update ContactOut workers so a found work email also fills `finalEmail` when blank.
- Add a real company domain or website column upstream if possible.
- If adding a company website column, map it to Prospeo `company_website` and use it for Hunter domain lookups.
- Add a filter so ContactOut only runs rows with `enrichment_status` equal to `contactout_needed` or blank.
- Add a small dry-run/test mode for the API pipeline that prints payloads without calling external APIs.
- Clean terminal log formatting so all INFO/WARN/ERROR lines are ASCII and easy to scan.
