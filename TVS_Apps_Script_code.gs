/*=================================================================
  TVS Lead Disposition — Google Apps Script
  ─────────────────────────────────────────────────────────────────
  SETUP STEPS:
  1. Go to https://script.google.com → New Project
  2. Paste this code, save as "TVS LDR Script"
  3. Enable Drive API: Extensions → Apps Script Services → Drive API v2
  4. Fill in CONFIG below (get file IDs from Google Drive URLs)
  5. Create a new blank Google Sheet → copy its ID into CACHE_SHEET_ID
  6. Run testRun() once to verify (check Execution Log)
  7. Run setupDailyTrigger() once to automate daily refresh
  8. Deploy → New Deployment → Web App → "Anyone" (for doGet access)
  9. Copy the deployment URL into the Dashboard HTML
=================================================================*/

const CONFIG = {
  // Google Drive file IDs — found in the file URL after /d/
  LEADS_FILE_ID:   'PASTE_LEADS_XLSX_FILE_ID_HERE',
  RETAILS_FILE_ID: 'PASTE_RETAILS_XLSX_FILE_ID_HERE',

  // A blank Google Sheet you create to hold the processed cache
  CACHE_SHEET_ID:  'PASTE_CACHE_GOOGLE_SHEET_ID_HERE',
  CACHE_TAB:       'Data',

  // Cache TTL in milliseconds (4 hours)
  CACHE_TTL_MS: 4 * 60 * 60 * 1000,
};

const PUSH_SECRET = 'tvs2026push';
const CHUNK_SIZE  = 40000;  // chars per Sheet cell (safe under 50K cell limit)

/* ─── doGet: serve JSON to dashboard ─── */
function doGet(e) {
  try {
    const json = getOrBuildJson();
    return ContentService
      .createTextOutput(json)
      .setMimeType(ContentService.MimeType.JSON);
  } catch (err) {
    Logger.log('doGet error: ' + err.stack);
    return ContentService
      .createTextOutput(JSON.stringify({ error: err.message }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

/* ─── doPost: receive pushed JSON from Python script ─── */
function doPost(e) {
  try {
    const secret = e.parameter && e.parameter.secret;
    if (secret !== PUSH_SECRET) {
      return ContentService.createTextOutput(JSON.stringify({ error: 'Unauthorized' }))
        .setMimeType(ContentService.MimeType.JSON);
    }
    const json = e.postData.contents;
    writeChunked(json);
    Logger.log('Push stored: ' + json.length + ' chars');
    return ContentService.createTextOutput(JSON.stringify({ ok: true, bytes: json.length }))
      .setMimeType(ContentService.MimeType.JSON);
  } catch (err) {
    Logger.log('doPost error: ' + err.stack);
    return ContentService.createTextOutput(JSON.stringify({ error: err.message }))
      .setMimeType(ContentService.MimeType.JSON);
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
  for (let i = 0; i < chunks; i++) {
    rows.push([json.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE)]);
  }
  sh.getRange(1, 1, rows.length, 1).setValues(rows);
  PropertiesService.getScriptProperties().setProperties({
    'tvs_cache_ts': String(Date.now()),
    'tvs_chunks':   String(chunks),
  });
}

/* ─── Read from cache if fresh, otherwise recompute ─── */
function getOrBuildJson() {
  const props = PropertiesService.getScriptProperties();
  const cacheAge = props.getProperty('tvs_cache_ts');
  if (cacheAge && (Date.now() - parseInt(cacheAge)) < CONFIG.CACHE_TTL_MS) {
    try {
      const ss = SpreadsheetApp.openById(CONFIG.CACHE_SHEET_ID);
      const sh = ss.getSheetByName(CONFIG.CACHE_TAB);
      if (sh) {
        const chunks = parseInt(props.getProperty('tvs_chunks') || '1');
        let val;
        if (chunks <= 1) {
          val = String(sh.getRange('A1').getValue());
        } else {
          val = sh.getRange(1, 1, chunks, 1).getValues().map(r => String(r[0])).join('');
        }
        if (val && val.length > 10) {
          Logger.log('Serving from cache (' + chunks + ' chunks, ' + val.length + ' chars)');
          return val;
        }
      }
    } catch (e) { Logger.log('Cache read failed: ' + e); }
  }
  return computeAndCache();
}

/* ─── Main aggregation pipeline ─── */
function computeAndCache() {
  Logger.log('=== computeAndCache START ===');
  const json = processData();
  Logger.log('Output JSON length: ' + json.length + ' chars');

  // Write to cache sheet (chunked to handle large payloads)
  try {
    writeChunked(json);
    Logger.log('Cache written to sheet (' + json.length + ' chars)');
  } catch (e) {
    Logger.log('Cache write failed (data still returned): ' + e);
  }

  return json;
}

/* ─── Core data processing ─── */
function processData() {
  // 1. Read Retails XLSX → build lookup map
  Logger.log('Reading Retails XLSX...');
  const retRows = readXlsxFromDrive(CONFIG.RETAILS_FILE_ID);
  Logger.log('Retails rows: ' + (retRows.length - 1));

  const rh = retRows[0].map(h => String(h).toLowerCase().trim());
  const rIdIdx  = colIdx(rh, ['sorceleadid', 'sourceleadid']);
  const rMthIdx = colIdx(rh, ['retail month']);
  const rZoneIdx = colIdx(rh, ['zone']);
  const rStateIdx = colIdx(rh, ['dealership-state(area)', 'dealershipstate']);

  const retailMap = new Map(); // SorceLeadId → { rm }
  for (let i = 1; i < retRows.length; i++) {
    const r = retRows[i];
    const id = toIdStr(r[rIdIdx]);
    if (!id) continue;
    retailMap.set(id, {
      rm: str(r[rMthIdx]),
    });
  }
  Logger.log('Retail map entries: ' + retailMap.size);

  // 2. Read Leads XLSX
  Logger.log('Reading Leads XLSX...');
  const leadRows = readXlsxFromDrive(CONFIG.LEADS_FILE_ID);
  Logger.log('Lead rows: ' + (leadRows.length - 1));

  const lh = leadRows[0].map(h => String(h).toLowerCase().trim());
  const lIdIdx   = colIdx(lh, ['sorceleadid', 'sourceleadid']);
  const lMthIdx  = colIdx(lh, ['lead month']);
  const lSrcIdx  = colIdx(lh, ['source']);
  const lLtIdx   = colIdx(lh, ['lead type']);
  const lMdlIdx  = colIdx(lh, ['modelname', 'model name']);
  const lStIdx   = colIdx(lh, ['state']);
  const lZoneIdx = colIdx(lh, ['zone']);
  const lBdIdx   = colIdx(lh, ['buying days']);

  // Index maps — each unique string value gets a compact integer index
  const lmArr=[], srcArr=[], ltArr=[], mdlArr=[], stArr=[], zoneArr=[];
  const lmMap=new Map(), srcMap=new Map(), ltMap=new Map(),
        mdlMap=new Map(), stMap=new Map(), zoneMap=new Map();

  function idx(map, arr, val) {
    if (!map.has(val)) { map.set(val, arr.length); arr.push(val); }
    return map.get(val);
  }

  // Aggregation maps — key = pipe-separated indexes, value = [leads, retails]
  const aMonthly = new Map();   // [lm_idx]
  const aSrcMon  = new Map();   // [src_idx|lm_idx]
  const aLtMon   = new Map();   // [lt_idx|src_idx|lm_idx]
  const aMdlMon  = new Map();   // [mdl_idx|src_idx|lm_idx]
  const aStMon   = new Map();   // [st_idx|src_idx|lm_idx]
  const aZoneMon = new Map();   // [zone_idx|lm_idx]
  const aBdMon   = new Map();   // [bd|src_idx|lm_idx]

  function bump(map, key, isRet) {
    let a = map.get(key);
    if (!a) { a = [0, 0]; map.set(key, a); }
    a[0]++;
    if (isRet) a[1]++;
  }

  // 3. Process leads
  Logger.log('Joining and aggregating...');
  for (let i = 1; i < leadRows.length; i++) {
    const r = leadRows[i];
    const id  = toIdStr(r[lIdIdx]);
    const lm  = str(r[lMthIdx]);
    const src = str(r[lSrcIdx])  || 'Unknown';
    const lt  = str(r[lLtIdx])   || 'Unknown';
    const mdl = str(r[lMdlIdx])  || 'Unknown';
    const st  = str(r[lStIdx])   || 'Unknown';
    const zon = str(r[lZoneIdx]) || 'Unknown';
    const bd  = str(r[lBdIdx])   || '0';

    if (!lm || !id) continue;

    const isRet = retailMap.has(id);
    const li  = idx(lmMap,   lmArr,   lm);
    const si  = idx(srcMap,  srcArr,  src);
    const tti = idx(ltMap,   ltArr,   lt);
    const mi  = idx(mdlMap,  mdlArr,  mdl);
    const sti = idx(stMap,   stArr,   st);
    const zi  = idx(zoneMap, zoneArr, zon);

    bump(aMonthly, li,                   isRet);
    bump(aSrcMon,  si+'|'+li,            isRet);
    bump(aLtMon,   tti+'|'+si+'|'+li,   isRet);
    bump(aMdlMon,  mi+'|'+si+'|'+li,    isRet);
    bump(aStMon,   sti+'|'+si+'|'+li,   isRet);
    bump(aZoneMon, zi+'|'+li,           isRet);
    bump(aBdMon,   bd+'|'+si+'|'+li,    isRet);
  }

  // 4. Convert maps to compact arrays
  function toRows(map, splitter) {
    const out = [];
    for (const [k, v] of map) {
      const parts = splitter(k);
      out.push([...parts, v[0], v[1]]);
    }
    return out;
  }

  const output = {
    t:    new Date().toISOString(),
    maps: {
      lm:   lmArr,
      src:  srcArr,
      lt:   ltArr,
      mdl:  mdlArr,
      st:   stArr,
      zone: zoneArr,
    },
    // Each row: [dim_idxs..., leads, retails]
    monthly: toRows(aMonthly, k => [+k]),
    sm:      toRows(aSrcMon,  k => k.split('|').map(Number)),
    ltm:     toRows(aLtMon,   k => k.split('|').map(Number)),
    mm:      toRows(aMdlMon,  k => k.split('|').map(Number)),
    stm:     toRows(aStMon,   k => k.split('|').map(Number)),
    zm:      toRows(aZoneMon, k => k.split('|').map(Number)),
    bdm:     toRows(aBdMon,   k => {
      const [bd, ...rest] = k.split('|');
      return [+bd, ...rest.map(Number)];
    }),
  };

  return JSON.stringify(output);
}

/* ─── Read XLSX from Google Drive (converts to Sheets format) ─── */
function readXlsxFromDrive(fileId) {
  // Drive API v2 must be enabled: Extensions → Services → Drive API
  const resource = {
    title: '_tvs_tmp_' + fileId,
    mimeType: MimeType.GOOGLE_SHEETS,
  };
  const copy = Drive.Files.copy(resource, fileId);
  try {
    const ss = SpreadsheetApp.openById(copy.id);
    return ss.getSheets()[0].getDataRange().getValues();
  } finally {
    try { Drive.Files.remove(copy.id); } catch (e) {
      Logger.log('Warning: could not delete temp file ' + copy.id + ': ' + e);
    }
  }
}

/* ─── Helpers ─── */
function str(v) {
  return String(v == null ? '' : v).trim();
}

function toIdStr(v) {
  // Large int64 IDs lose precision in float64; both files undergo the same
  // conversion so the join key is consistently rounded on both sides.
  if (v == null || v === '') return '';
  const n = Number(v);
  if (isNaN(n)) return str(v);
  return String(Math.round(n));
}

function colIdx(headers, candidates) {
  for (const c of candidates) {
    const i = headers.findIndex(h => h === c || h.replace(/[\s_\-]/g,'') === c.replace(/[\s_\-]/g,''));
    if (i >= 0) return i;
  }
  Logger.log('WARNING: could not find column: ' + candidates.join(' / '));
  return -1;
}

/* ─── Setup daily trigger (run once manually) ─── */
function setupDailyTrigger() {
  ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === 'computeAndCache')
    .forEach(t => ScriptApp.deleteTrigger(t));

  ScriptApp.newTrigger('computeAndCache')
    .timeBased()
    .atHour(1)       // 01:00 UTC = ~06:30 AM IST
    .nearMinute(0)
    .everyDays(1)
    .create();

  Logger.log('Daily trigger created: runs at ~01:00 UTC daily (06:30 AM IST)');
}

/* ─── Manual test — run this first after setup ─── */
function testRun() {
  Logger.log('=== TEST RUN ===');
  const json = processData();
  Logger.log('JSON length: ' + json.length + ' chars');
  const parsed = JSON.parse(json);
  Logger.log('Months: ' + parsed.maps.lm.join(', '));
  Logger.log('Sources: ' + parsed.maps.src.join(', '));
  Logger.log('Models count: ' + parsed.maps.mdl.length);
  Logger.log('States count: ' + parsed.maps.st.length);
  Logger.log('Monthly rows: ' + parsed.monthly.length);
  Logger.log('OK — run computeAndCache() to write to sheet, then setupDailyTrigger() to automate.');
}

/* ─── Force cache clear (run if you want to force a refresh) ─── */
function clearCache() {
  PropertiesService.getScriptProperties().deleteProperty('tvs_cache_ts');
  Logger.log('Cache cleared. Next doGet() will recompute.');
}
