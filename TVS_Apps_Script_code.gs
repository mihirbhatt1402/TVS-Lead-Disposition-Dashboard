/*=================================================================
  TVS Lead Disposition — Google Apps Script
  ─────────────────────────────────────────────────────────────────
  SETUP STEPS:
  1. Go to https://script.google.com → New Project
  2. Paste this code, save as "TVS LDR Script"
  3. Fill in CONFIG below (get file IDs from Google Drive URLs)
  4. Create a new blank Google Sheet → copy its ID into CACHE_SHEET_ID
  5. Deploy → New Deployment → Web App → "Anyone" → copy URL to Dashboard HTML
  6. Run push_tvs_data.py to populate data

  ACCESS CONTROL:
  - ADMIN_EMAILS: always have admin role (hardcoded)
  - ALLOWED_DOMAINS: only these email domains can request access
  - Roles (full/viewer) are stored in Script Properties as 'tvs_roles' JSON
  - Pending requests stored as 'tvs_pending' JSON
=================================================================*/

const CONFIG = {
  // Drive folder containing monthly TVS CPS lead files (all files with "TVS" in name are used)
  LEAD_FOLDER_ID:       '1lZ4l1LemSolnGwAiqPWwQS8CUdF0LfZf',

  // Current month live Google Sheets
  CURR_LEADS_SHEET_ID:   '1iSw5zXF67q5Wkoz2mSPFqql9OPAcqmd0um5BEHUGf4o',
  CURR_LEADS_TAB:        'TVS',
  CURR_RETAILS_SHEET_ID: '1ZWBlzxX-g2R5iCcrsGUWrqSvxIHcchFHtajDDPcFJgE',
  CURR_RETAILS_TAB:      'Raw',

  // Cache (output)
  CACHE_SHEET_ID:  '1leebtjg8P7bKRrwfAolCNcDHrmM18GQVclD9xzhayIk',
  CACHE_TAB:       'Data',
  CACHE_TTL_MS:    25 * 60 * 60 * 1000,  // 25 h — survives until next daily pipeline run
};

const PUSH_SECRET    = 'tvs2026push';
const CHUNK_SIZE     = 40000;

const ADMIN_EMAILS   = ['mihir.bhatt@girnarsoft.com', 'aditya.kumar@girnarsoft.com'];
const ALLOWED_DOMAINS = ['girnarsoft.com', 'girnarcare.com'];

/* ─── Auth helpers ─── */

function jsonOut(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}

function isAdmin(email) {
  return ADMIN_EMAILS.indexOf(email) >= 0;
}

function getRoles() {
  try {
    return JSON.parse(PropertiesService.getScriptProperties().getProperty('tvs_roles') || '{}');
  } catch(e) { return {}; }
}

function saveRoles(roles) {
  // Never persist admin emails — they are always admin by code
  const toSave = Object.assign({}, roles);
  ADMIN_EMAILS.forEach(function(e) { delete toSave[e]; });
  PropertiesService.getScriptProperties().setProperty('tvs_roles', JSON.stringify(toSave));
}

function getPendingMap() {
  try {
    return JSON.parse(PropertiesService.getScriptProperties().getProperty('tvs_pending') || '{}');
  } catch(e) { return {}; }
}

function savePendingMap(pending) {
  PropertiesService.getScriptProperties().setProperty('tvs_pending', JSON.stringify(pending));
}

function checkUserRole(email) {
  email = (email || '').toLowerCase().trim();
  if (!email) return { role: 'none' };
  if (isAdmin(email)) return { role: 'admin' };
  const roles = getRoles();
  if (roles[email]) return { role: roles[email] };
  const pending = getPendingMap();
  if (pending[email]) return { role: 'pending', requestedAt: pending[email].requestedAt };
  const domain = email.split('@')[1] || '';
  if (ALLOWED_DOMAINS.indexOf(domain) < 0) return { role: 'restricted' };
  return { role: 'none' };
}

/* ─── doGet: serve JSON to dashboard, or handle auth actions ─── */
function doGet(e) {
  try {
    const action = e.parameter && e.parameter.action;

    if (action === 'checkRole') {
      const email = ((e.parameter.email) || '').toLowerCase().trim();
      return jsonOut(checkUserRole(email));
    }

    if (action === 'getPending') {
      const email = ((e.parameter.email) || '').toLowerCase().trim();
      if (!isAdmin(email)) return jsonOut({ error: 'Unauthorized' });
      return jsonOut({ pending: getPendingMap() });
    }

    // Data proxy endpoints (protected by PUSH_SECRET)
    const secret = e.parameter && e.parameter.secret;

    if (action === 'getConfig') {
      if (secret !== PUSH_SECRET) return jsonOut({ error: 'Unauthorized' });
      return jsonOut({
        leadFolderId: CONFIG.LEAD_FOLDER_ID,
      });
    }

    if (action === 'getLeadFileList') {
      if (secret !== PUSH_SECRET) return jsonOut({ error: 'Unauthorized' });
      return handleGetLeadFileList();
    }

    if (action === 'getSheetData') {
      if (secret !== PUSH_SECRET) return jsonOut({ error: 'Unauthorized' });
      var fileId   = e.parameter.fileId;
      var page     = parseInt(e.parameter.page     || '0');
      var pageSize = parseInt(e.parameter.pageSize || '50000');
      var tabName  = e.parameter.tabName || '';
      var cols     = e.parameter.cols    || '';
      return handleGetSheetData(fileId, page, pageSize, tabName, cols);
    }

    if (action === 'getCurrentLeads') {
      if (secret !== PUSH_SECRET) return jsonOut({ error: 'Unauthorized' });
      var page     = parseInt(e.parameter.page     || '0');
      var pageSize = parseInt(e.parameter.pageSize || '25000');
      return handleGetCurrentLeads(page, pageSize);
    }

    if (action === 'getCurrentRetails') {
      if (secret !== PUSH_SECRET) return jsonOut({ error: 'Unauthorized' });
      var page     = parseInt(e.parameter.page     || '0');
      var pageSize = parseInt(e.parameter.pageSize || '25000');
      return handleGetCurrentRetails(page, pageSize);
    }

    // Default: serve dashboard data
    const json = getOrBuildJson();
    return ContentService.createTextOutput(json).setMimeType(ContentService.MimeType.JSON);

  } catch (err) {
    Logger.log('doGet error: ' + err.stack);
    return jsonOut({ error: err.message });
  }
}

/* ─── doPost: push data or handle access requests ─── */
function doPost(e) {
  try {
    const secret = e.parameter && e.parameter.secret;

    // Data push from Python script
    if (secret === PUSH_SECRET) {
      const json = e.postData.contents;
      writeChunked(json);
      Logger.log('Push stored: ' + json.length + ' chars');
      return jsonOut({ ok: true, bytes: json.length });
    }

    const action = e.parameter && e.parameter.action;
    const body   = JSON.parse(e.postData.contents || '{}');

    if (action === 'requestAccess') {
      const email  = (body.email  || '').toLowerCase().trim();
      const name   = (body.name   || '').trim();
      const domain = email.split('@')[1] || '';
      if (ALLOWED_DOMAINS.indexOf(domain) < 0) return jsonOut({ error: 'Domain not allowed' });
      if (isAdmin(email))                      return jsonOut({ role: 'admin' });
      const roles = getRoles();
      if (roles[email])                        return jsonOut({ role: roles[email] });
      const pending = getPendingMap();
      pending[email] = { name: name, requestedAt: new Date().toISOString() };
      savePendingMap(pending);
      return jsonOut({ ok: true });
    }

    if (action === 'reviewRequest') {
      const adminEmail  = (body.adminEmail  || '').toLowerCase().trim();
      const targetEmail = (body.targetEmail || '').toLowerCase().trim();
      const decision    = body.decision;  // 'approve' | 'reject'
      const role        = body.role || 'viewer';
      if (!isAdmin(adminEmail)) return jsonOut({ error: 'Unauthorized' });
      const pending = getPendingMap();
      delete pending[targetEmail];
      savePendingMap(pending);
      if (decision === 'approve') {
        const roles = getRoles();
        roles[targetEmail] = role;
        saveRoles(roles);
      }
      return jsonOut({ ok: true });
    }

    return jsonOut({ error: 'Unknown action' });

  } catch (err) {
    Logger.log('doPost error: ' + err.stack);
    return jsonOut({ error: err.message });
  }
}

/* ─── Write JSON in CHUNK_SIZE pieces to Sheet column A ─── */
function writeChunked(json) {
  const ss = SpreadsheetApp.openById(CONFIG.CACHE_SHEET_ID);
  let sh = ss.getSheetByName(CONFIG.CACHE_TAB);
  if (!sh) sh = ss.insertSheet(CONFIG.CACHE_TAB);
  sh.clearContents();
  const chunks = Math.ceil(json.length / CHUNK_SIZE);
  const rows = [];
  for (var i = 0; i < chunks; i++) {
    rows.push([json.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE)]);
  }
  sh.getRange(1, 1, rows.length, 1).setValues(rows);
  PropertiesService.getScriptProperties().setProperties({
    'tvs_cache_ts': String(Date.now()),
    'tvs_chunks':   String(chunks),
  });
}

/* ─── Read from cache sheet (always — pipeline is the sole writer) ─── */
function getOrBuildJson() {
  const props = PropertiesService.getScriptProperties();
  try {
    const ss = SpreadsheetApp.openById(CONFIG.CACHE_SHEET_ID);
    const sh = ss.getSheetByName(CONFIG.CACHE_TAB);
    if (sh) {
      const chunks = parseInt(props.getProperty('tvs_chunks') || '1');
      var val;
      if (chunks <= 1) {
        val = String(sh.getRange('A1').getValue());
      } else {
        val = sh.getRange(1, 1, chunks, 1).getValues().map(function(r) { return String(r[0]); }).join('');
      }
      if (val && val.length > 10) {
        const cacheAge = props.getProperty('tvs_cache_ts');
        const stale    = cacheAge && (Date.now() - parseInt(cacheAge)) > CONFIG.CACHE_TTL_MS;
        Logger.log('Serving from sheet (' + chunks + ' chunks, ' + val.length + ' chars' + (stale ? ', stale' : '') + ')');
        return val;
      }
    }
  } catch (e) { Logger.log('Cache read failed: ' + e); }
  return JSON.stringify({ error: 'No data — run push_tvs_data.py to populate' });
}

/* ─── Force cache clear ─── */
function clearCache() {
  PropertiesService.getScriptProperties().deleteProperty('tvs_cache_ts');
  Logger.log('Cache cleared.');
}

/* ─── Current-month leads proxy (paginated) ─── */
function handleGetCurrentLeads(page, pageSize) {
  var ss        = SpreadsheetApp.openById(CONFIG.CURR_LEADS_SHEET_ID);
  var sh        = ss.getSheetByName(CONFIG.CURR_LEADS_TAB);
  var lastRow   = sh.getLastRow();
  var totalData = lastRow - 1;

  var startRow = 2 + page * pageSize;
  if (startRow > lastRow) {
    return jsonOut({ headers: [], rows: [], done: true, total: totalData });
  }

  var count   = Math.min(pageSize, lastRow - startRow + 1);
  var numCols = sh.getLastColumn();
  var headers = sh.getRange(1, 1, 1, numCols).getValues()[0].map(String);
  var data    = sh.getRange(startRow, 1, count, numCols).getValues();

  var needed = [
    'opty_id', 'Lead_Month', 'model', 'City', 'State',
    'Dealer_Name', 'lead_type', 'Medium',
    'DMS_Retail_Month', 'Retail Date', 'Retail By'
  ];
  var colIdx = needed.map(function(n) { return headers.indexOf(n); });

  var rows = data.map(function(row) {
    return colIdx.map(function(i) {
      if (i < 0) return '';
      var v = row[i];
      if (v instanceof Date) return Utilities.formatDate(v, 'Asia/Kolkata', 'yyyy-MM-dd');
      return String(v == null ? '' : v);
    });
  });

  return jsonOut({
    headers:      needed,
    rows:         rows,
    done:         (startRow + count - 1) >= lastRow,
    total:        totalData,
  });
}

/* ─── Current-month retails proxy (paginated) ─── */
function handleGetCurrentRetails(page, pageSize) {
  page     = page     || 0;
  pageSize = pageSize || 25000;

  var ss      = SpreadsheetApp.openById(CONFIG.CURR_RETAILS_SHEET_ID);
  var sh      = ss.getSheetByName(CONFIG.CURR_RETAILS_TAB);
  var lastRow = sh.getLastRow();
  var numCols = sh.getLastColumn();

  var hdr = sh.getRange(1, 1, 1, numCols).getValues()[0].map(String);

  var processIdx   = hdr.findIndex(function(h) { return h.toLowerCase() === 'process'; });
  var leadIdIdx    = hdr.findIndex(function(h) { return h.toLowerCase() === 'sourceleadid'; });
  var retailDtIdx  = hdr.findIndex(function(h) {
    return h.toLowerCase().replace(/[_ ]/g, '') === 'retailattributiondate';
  });
  var modelIdx     = hdr.findIndex(function(h) { return h.toLowerCase() === 'purchasedmodel'; });

  var needed  = ['sourceLeadId', 'Retail_Attribution_Date', 'purchasedModel'];
  var indices = [leadIdIdx, retailDtIdx, modelIdx];

  var startRow = 2 + page * pageSize;
  if (startRow > lastRow) {
    return jsonOut({ headers: needed, rows: [], done: true, total: lastRow - 1 });
  }

  var count = Math.min(pageSize, lastRow - startRow + 1);
  var data  = sh.getRange(startRow, 1, count, numCols).getValues();

  var rows = [];
  for (var i = 0; i < data.length; i++) {
    var row = data[i];
    if (processIdx >= 0 && String(row[processIdx] || '').trim().toUpperCase() !== 'TVS') continue;
    var out = indices.map(function(idx) {
      if (idx < 0) return '';
      var v = row[idx];
      if (v instanceof Date) return Utilities.formatDate(v, 'Asia/Kolkata', 'yyyy-MM-dd');
      return String(v == null ? '' : v);
    });
    rows.push(out);
  }

  return jsonOut({
    headers: needed,
    rows:    rows,
    done:    (startRow + count - 1) >= lastRow,
    total:   lastRow - 1,
  });
}

/* ─── List TVS lead files in the configured Drive folder ─── */
function handleGetLeadFileList() {
  var folderId = CONFIG.LEAD_FOLDER_ID;
  if (!folderId) return jsonOut({ error: 'No LEAD_FOLDER_ID configured' });
  try {
    var folder = DriveApp.getFolderById(folderId);
    var iter   = folder.getFiles();
    var files  = [];
    while (iter.hasNext()) {
      var f    = iter.next();
      var name = f.getName();
      if (name.toUpperCase().indexOf('TVS') >= 0) {
        files.push({ id: f.getId(), name: name });
      }
    }
    // Sort by name so months are processed in order
    files.sort(function(a, b) { return a.name.localeCompare(b.name); });
    return jsonOut({ files: files });
  } catch (e) {
    return jsonOut({ error: 'getLeadFileList failed: ' + e.message });
  }
}

/* ─── Generic sheet reader via proxy (paginated) — used for all monthly lead sheets ─── */
function handleGetSheetData(fileId, page, pageSize, tabName, cols) {
  if (!fileId) return jsonOut({ error: 'fileId required' });
  try {
    var ss      = SpreadsheetApp.openById(fileId);
    var sh      = (tabName && ss.getSheetByName(tabName)) || ss.getSheets()[0];
    var lastRow = sh.getLastRow();
    var numCols = sh.getLastColumn();

    if (lastRow < 2) {
      return jsonOut({ headers: [], rows: [], done: true, total: 0 });
    }

    var allHeaders = sh.getRange(1, 1, 1, numCols).getValues()[0].map(String);
    var startRow   = 2 + page * pageSize;

    if (startRow > lastRow) {
      return jsonOut({ headers: allHeaders, rows: [], done: true, total: lastRow - 1 });
    }

    // Only return requested columns (cols = comma-separated names); empty = all columns
    var wantedNames = cols ? cols.split(',').map(function(c) { return c.trim(); }) : null;
    var colIndices  = wantedNames
      ? wantedNames.map(function(n) { return allHeaders.indexOf(n); })
      : allHeaders.map(function(_, i) { return i; });
    var outHeaders  = wantedNames || allHeaders;

    var count = Math.min(pageSize, lastRow - startRow + 1);
    var data  = sh.getRange(startRow, 1, count, numCols).getValues();

    var rows = data.map(function(row) {
      return colIndices.map(function(i) {
        if (i < 0) return '';
        var v = row[i];
        if (v instanceof Date) return Utilities.formatDate(v, 'Asia/Kolkata', 'yyyy-MM-dd');
        return String(v == null ? '' : v);
      });
    });

    return jsonOut({
      headers: outHeaders,
      rows:    rows,
      done:    (startRow + count - 1) >= lastRow,
      total:   lastRow - 1,
    });
  } catch (e) {
    return jsonOut({ error: 'getSheetData failed: ' + e.message });
  }
}

/* ─── Debug: view current roles and pending ─── */
function debugRoles() {
  Logger.log('Roles: ' + PropertiesService.getScriptProperties().getProperty('tvs_roles'));
  Logger.log('Pending: ' + PropertiesService.getScriptProperties().getProperty('tvs_pending'));
}
