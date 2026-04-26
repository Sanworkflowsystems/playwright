// google-sheets-helper.js
// Shared utility for reading from and writing to Google Sheets

function columnLetter(zeroBasedIndex) {
  let letter = '';
  let n = zeroBasedIndex + 1;
  while (n > 0) {
    const rem = (n - 1) % 26;
    letter = String.fromCharCode(65 + rem) + letter;
    n = Math.floor((n - 1) / 26);
  }
  return letter;
}

async function getFirstSheetName(sheets, sheetId) {
  // Try with fields filter first; fall back to unfiltered if the API rejects it.
  let response;
  try {
    response = await sheets.spreadsheets.get({ spreadsheetId: sheetId, fields: 'sheets(properties(title))' });
  } catch (_) {
    response = await sheets.spreadsheets.get({ spreadsheetId: sheetId });
  }
  return response.data.sheets[0].properties.title;
}

// Look up the tab title for a given numeric gid (from the ?gid= / #gid= part of a Sheets URL).
// Returns the tab name or null if no sheet in the spreadsheet has that gid.
async function getSheetNameByGid(sheets, sheetId, gid) {
  if (gid == null || gid === '') return null;
  let sheetsList;
  try {
    const response = await sheets.spreadsheets.get({ spreadsheetId: sheetId, fields: 'sheets(properties(sheetId,title))' });
    sheetsList = response.data.sheets || [];
  } catch (_) {
    try {
      const response = await sheets.spreadsheets.get({ spreadsheetId: sheetId });
      sheetsList = response.data.sheets || [];
    } catch (e2) {
      return null;
    }
  }
  const target = sheetsList.find(s => String(s.properties.sheetId) === String(gid));
  return target ? target.properties.title : null;
}

async function getSheetProperties(sheets, spreadsheetId, sheetName) {
  // Try with fields filter; fall back to unfiltered response if the API rejects it.
  let response;
  try {
    response = await sheets.spreadsheets.get({
      spreadsheetId,
      fields: 'sheets(properties(sheetId,title,gridProperties(columnCount,rowCount)))',
    });
  } catch (_) {
    response = await sheets.spreadsheets.get({ spreadsheetId });
  }
  const sheet = (response.data.sheets || []).find(s => s.properties.title === sheetName);
  if (!sheet) throw new Error(`Sheet not found: ${sheetName}`);
  return sheet.properties;
}

async function ensureColumnCapacity(sheets, spreadsheetId, sheetName, requiredColumnCount) {
  const props = await getSheetProperties(sheets, spreadsheetId, sheetName);
  const currentColumnCount = props.gridProperties?.columnCount || 0;
  if (currentColumnCount >= requiredColumnCount) return;

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId,
    requestBody: {
      requests: [{
        appendDimension: {
          sheetId: props.sheetId,
          dimension: 'COLUMNS',
          length: requiredColumnCount - currentColumnCount,
        },
      }],
    },
  });
}

async function readSheetData(sheets, sheetId, sheetName) {
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: sheetId,
    range: sheetName,
  });
  const values = response.data.values || [];
  if (values.length === 0) return { headers: [], rows: [] };

  const headers = values[0];
  const rows = values.slice(1).map(rowArr => {
    const obj = {};
    headers.forEach((h, i) => { obj[h] = rowArr[i] !== undefined ? rowArr[i] : ''; });
    return obj;
  });
  return { headers, rows };
}

async function ensureProcessedColumn(sheets, sheetId, sheetName, headers, totalDataRows) {
  if (headers.includes('contactout_processed')) return;

  // Append the header in the next available column
  const newColIndex = headers.length;
  await ensureColumnCapacity(sheets, sheetId, sheetName, newColIndex + 1);
  const newColLetter = columnLetter(newColIndex);

  // Write the header cell
  await sheets.spreadsheets.values.update({
    spreadsheetId: sheetId,
    range: `${sheetName}!${newColLetter}1`,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: [['contactout_processed']] },
  });

  // Fill all existing data rows with '0'
  if (totalDataRows > 0) {
    const zeros = Array.from({ length: totalDataRows }, () => ['0']);
    await sheets.spreadsheets.values.update({
      spreadsheetId: sheetId,
      range: `${sheetName}!${newColLetter}2:${newColLetter}${totalDataRows + 1}`,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: zeros },
    });
  }

  headers.push('contactout_processed');
}

async function updateSheetRow(sheets, sheetId, sheetName, rowNumber, headers, rowData) {
  const values = [headers.map(h => (rowData[h] !== undefined ? rowData[h] : ''))];
  const lastCol = columnLetter(headers.length - 1);
  await sheets.spreadsheets.values.update({
    spreadsheetId: sheetId,
    range: `${sheetName}!A${rowNumber}:${lastCol}${rowNumber}`,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values },
  });
}

async function markRowProcessed(sheets, sheetId, sheetName, rowNumber, processedColLetter) {
  await sheets.spreadsheets.values.update({
    spreadsheetId: sheetId,
    range: `${sheetName}!${processedColLetter}${rowNumber}`,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: [['1']] },
  });
}

// ── Pipeline column helpers ───────────────────────────────────────────────────

const PIPELINE_COLUMNS = ['enrichment_source', 'enrichment_status'];

/**
 * Ensure the three pipeline output columns exist in the sheet header row.
 * Adds any missing ones to the right of existing columns, fills data rows with ''.
 */
async function ensurePipelineColumns(sheets, sheetId, sheetName, headers, totalDataRows) {
  const missing = PIPELINE_COLUMNS.filter(col => !headers.includes(col));
  if (missing.length === 0) return;

  await ensureColumnCapacity(sheets, sheetId, sheetName, headers.length + missing.length);

  for (const col of missing) {
    const colIndex  = headers.length;
    const colLetter = columnLetter(colIndex);

    // Write header
    await sheets.spreadsheets.values.update({
      spreadsheetId: sheetId,
      range: `${sheetName}!${colLetter}1`,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: [[col]] },
    });

    // Fill data rows with empty string so the column exists
    if (totalDataRows > 0) {
      const empty = Array.from({ length: totalDataRows }, () => ['']);
      await sheets.spreadsheets.values.update({
        spreadsheetId: sheetId,
        range: `${sheetName}!${colLetter}2:${colLetter}${totalDataRows + 1}`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: empty },
      });
    }

    headers.push(col);
  }
}

/**
 * Write pipeline result columns for a single row.
 * Only updates the three pipeline columns (enrichment_email, enrichment_source, enrichment_status).
 * rowNumber is 1-indexed (2 = first data row).
 */
async function updatePipelineResult(sheets, sheetId, sheetName, rowNumber, headers, rowData) {
  const writes = [];

  // Write email to finalEmail only when a value is present
  const finalEmailIdx = headers.indexOf('finalEmail');
  if (finalEmailIdx !== -1 && rowData['enrichment_email']) {
    writes.push(sheets.spreadsheets.values.update({
      spreadsheetId: sheetId,
      range: `${sheetName}!${columnLetter(finalEmailIdx)}${rowNumber}`,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: [[rowData['enrichment_email']]] },
    }));
  }

  for (const col of PIPELINE_COLUMNS) {
    const colIndex = headers.indexOf(col);
    if (colIndex === -1) continue;
    writes.push(sheets.spreadsheets.values.update({
      spreadsheetId: sheetId,
      range: `${sheetName}!${columnLetter(colIndex)}${rowNumber}`,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: [[rowData[col] !== undefined ? rowData[col] : '']] },
    }));
  }

  await Promise.allSettled(writes);
}

/**
 * Write pipeline result columns for multiple rows in a single API request.
 * Chunks into batches of 100 rows (300 ranges) to stay within Sheets API limits.
 *
 * @param {object}   sheets    - googleapis sheets client
 * @param {string}   sheetId
 * @param {string}   sheetName
 * @param {string[]} headers   - must already contain the three pipeline column names
 * @param {Map<number, {email:string, source:string, status:string}>} updates
 *                             - keyed by 1-based sheet row number (2 = first data row)
 */
async function batchUpdatePipelineResults(sheets, sheetId, sheetName, headers, updates) {
  if (!updates || updates.size === 0) return;

  const finalEmailIdx = headers.indexOf('finalEmail');
  const sourceIdx     = headers.indexOf('enrichment_source');
  const statusIdx     = headers.indexOf('enrichment_status');
  if (sourceIdx === -1 || statusIdx === -1) return;

  const finalEmailCol = finalEmailIdx !== -1 ? columnLetter(finalEmailIdx) : null;
  const sourceCol     = columnLetter(sourceIdx);
  const statusCol     = columnLetter(statusIdx);

  const allData = [];
  for (const [rowNumber, { email, source, status }] of updates) {
    // Only write to finalEmail when an email was actually found — never blank it out,
    // because ContactOut automation will fill it later for contactout_needed rows.
    if (finalEmailCol && email) {
      allData.push({ range: `${sheetName}!${finalEmailCol}${rowNumber}`, values: [[email]] });
    }
    allData.push(
      { range: `${sheetName}!${sourceCol}${rowNumber}`, values: [[source || '']] },
      { range: `${sheetName}!${statusCol}${rowNumber}`, values: [[status || '']] },
    );
  }

  if (allData.length === 0) return;

  // Chunk at 300 ranges — one API call per chunk
  const CHUNK = 300;
  for (let i = 0; i < allData.length; i += CHUNK) {
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: sheetId,
      requestBody: {
        valueInputOption: 'USER_ENTERED',
        data: allData.slice(i, i + CHUNK),
      },
    });
  }
}

module.exports = {
  columnLetter,
  getFirstSheetName,
  getSheetNameByGid,
  readSheetData,
  ensureProcessedColumn,
  updateSheetRow,
  markRowProcessed,
  ensurePipelineColumns,
  updatePipelineResult,
  batchUpdatePipelineResults,
};
