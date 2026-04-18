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
  const response = await sheets.spreadsheets.get({ spreadsheetId: sheetId });
  return response.data.sheets[0].properties.title;
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

const PIPELINE_COLUMNS = ['enrichment_email', 'enrichment_source', 'enrichment_status'];

/**
 * Ensure the three pipeline output columns exist in the sheet header row.
 * Adds any missing ones to the right of existing columns, fills data rows with ''.
 */
async function ensurePipelineColumns(sheets, sheetId, sheetName, headers, totalDataRows) {
  const missing = PIPELINE_COLUMNS.filter(col => !headers.includes(col));
  if (missing.length === 0) return;

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
  const updates = PIPELINE_COLUMNS.map(col => {
    const colIndex  = headers.indexOf(col);
    if (colIndex === -1) return null;
    const colLetter = columnLetter(colIndex);
    return sheets.spreadsheets.values.update({
      spreadsheetId: sheetId,
      range: `${sheetName}!${colLetter}${rowNumber}`,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: [[rowData[col] !== undefined ? rowData[col] : '']] },
    });
  }).filter(Boolean);

  await Promise.allSettled(updates);
}

module.exports = {
  columnLetter,
  getFirstSheetName,
  readSheetData,
  ensureProcessedColumn,
  updateSheetRow,
  markRowProcessed,
  ensurePipelineColumns,
  updatePipelineResult,
};
