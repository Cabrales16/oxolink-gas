/**
 * Oxo Partners - Panel de Administración  (v5 - escritura directa BD+Sheets+claspv1)
 *
 * Script Properties requeridas:
 *   SPREADSHEET_ID   - ID del Google Sheets con los datos
 *   SHEET_NAME       - Nombre de la hoja
 *   ADMIN_PASSWORD   - Contraseña para acceder al panel
 *   DB_HOST          - Host del servidor MySQL
 *   DB_NAME          - Nombre de la base de datos
 *   DB_USER          - Usuario MySQL
 *   DB_PASS          - Contraseña MySQL
 *   DB_TABLE         - Nombre de la tabla (ej: usuariosInvers)
 *
 * Al editar un registro o cambiar estado:
 *   1. Si sync_status = PENDIENTE | ERROR → solo Sheets (aún no existe en MySQL).
 *   2. Si ya sincronizado → MySQL primero. Falla → error, Sheets intacto. MySQL OK → actualiza Sheets.
 */

// ─── Config ───────────────────────────────────────────────────────────────────

function getConfig_() {
  const sp = PropertiesService.getScriptProperties();
  const cfg = {
    SPREADSHEET_ID: (sp.getProperty("SPREADSHEET_ID") || "").trim(),
    SHEET_NAME: (sp.getProperty("SHEET_NAME") || "Users1").trim(),
    ADMIN_PASSWORD: (sp.getProperty("ADMIN_PASSWORD") || "").trim(),
    DB_HOST: (sp.getProperty("DB_HOST") || "").trim(),
    DB_NAME: (sp.getProperty("DB_NAME") || "").trim(),
    DB_USER: (sp.getProperty("DB_USER") || "").trim(),
    DB_PASS: sp.getProperty("DB_PASS") || "",
    DB_TABLE: (sp.getProperty("DB_TABLE") || "").trim(),
  };
  const missing = ["SPREADSHEET_ID", "SHEET_NAME", "ADMIN_PASSWORD",
    "DB_HOST", "DB_NAME", "DB_USER", "DB_PASS", "DB_TABLE"].filter(k => !cfg[k]);
  if (missing.length) throw new Error("Faltan propiedades: " + missing.join(", "));
  cfg.URLBD = `jdbc:mysql://${cfg.DB_HOST}:3306/${cfg.DB_NAME}?useUnicode=true&characterEncoding=UTF-8&useSSL=false&requireSSL=false`;
  return cfg;
}

// ─── Helper de conexión BD ────────────────────────────────────────────────────

function _getDbConn_(cfg) {
  const maxAttempts = 3;
  let lastErr = null;
  for (let i = 1; i <= maxAttempts; i++) {
    try { return Jdbc.getConnection(cfg.URLBD, cfg.DB_USER, cfg.DB_PASS); }
    catch (e) {
      lastErr = e;
      const m = String(e && e.message ? e.message : e).toLowerCase();
      const transient = m.includes("failed to establish") || m.includes("communications link") ||
        m.includes("timed out") || m.includes("timeout");
      if (!transient) break;
      if (i < maxAttempts) Utilities.sleep(1000 * i);
    }
  }
  throw lastErr;
}

// ─── doGet / include ──────────────────────────────────────────────────────────

function doGet(request) {
  const tmpl = HtmlService.createTemplateFromFile("Index");
  return tmpl.evaluate()
    .setTitle("Oxo Partners · Admin")
    .addMetaTag("viewport", "width=device-width, initial-scale=1, user-scalable=no")
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function include_(filename) {
  return HtmlService.createHtmlOutputFromFile(filename).getContent();
}

// ─── Auth ─────────────────────────────────────────────────────────────────────

function verifyPassword(pwd) {
  try {
    const cfg = getConfig_();
    return String(pwd || "") === cfg.ADMIN_PASSWORD;
  } catch (e) { return false; }
}

// ─── Sheets helpers ───────────────────────────────────────────────────────────

function openSheet_(cfg) {
  const c = cfg || getConfig_();
  const ss = SpreadsheetApp.openById(c.SPREADSHEET_ID);
  const ws = ss.getSheetByName(c.SHEET_NAME);
  if (!ws) throw new Error(`Hoja "${c.SHEET_NAME}" no encontrada.`);
  return ws;
}

function getAllRows_() {
  const cfg = getConfig_();
  const ws = openSheet_(cfg); // ← pasa cfg
  const lastRow = ws.getLastRow();
  if (lastRow < 2) return { headers: [], rows: [], ws, lastRow };
  const lastCol = ws.getLastColumn();
  const raw = ws.getRange(1, 1, lastRow, lastCol).getValues();
  const headers = raw[0].map(h => String(h || "").trim().toLowerCase().replace(/\s+/g, "_"));
  const rows = raw.slice(1);
  return { headers, rows, ws, lastRow, lastCol };
}

function rowToObj_(headers, row) {
  const obj = {};
  headers.forEach((h, i) => { obj[h] = row[i] !== undefined ? row[i] : ""; });
  return obj;
}

function splitHoteles_(raw) {
  return String(raw || "").split(";").map(h => h.trim()).filter(Boolean);
}

function parseDate_(v) {
  if (!v) return null;
  if (v instanceof Date) return v;
  const d = new Date(v);
  return isNaN(d.getTime()) ? null : d;
}

function formatDatetimeAmPm_(v) {
  const d = parseDate_(v);
  if (!d) return "";
  const pad = n => String(n).padStart(2, "0");
  let h = d.getHours();
  const ampm = h >= 12 ? "PM" : "AM";
  h = h % 12 || 12;
  return `${d.getFullYear()}/${pad(d.getMonth() + 1)}/${pad(d.getDate())}, ${pad(h)}:${pad(d.getMinutes())}:${pad(d.getSeconds())} ${ampm}`;
}

function _parseFmtDate_(s) {
  if (!s) return null;
  const m = String(s).match(/(\d{4})\/(\d{2})\/(\d{2}),\s*(\d+):(\d+):(\d+)\s*(AM|PM)/i);
  if (!m) return null;
  let [, y, mo, d, h, mi, se, ap] = m;
  h = parseInt(h, 10);
  if (ap.toUpperCase() === "PM" && h < 12) h += 12;
  if (ap.toUpperCase() === "AM" && h === 12) h = 0;
  return new Date(+y, +mo - 1, +d, h, +mi, +se);
}

// ─── getAllData ────────────────────────────────────────────────────────────────

function getAllData() {
  const { headers, rows } = getAllRows_();
  if (!headers.length) return {
    registros: [], resumen: _emptyResumen(),
    hoteles: [], actividad: _emptyActividad(),
    sync_stats: _emptySyncStats()
  };

  const validas = rows.map(r => rowToObj_(headers, r))
    .filter(o => (String(o.nombre || "") + String(o.correo || "")).trim() !== "");

  const registros = validas.map((o, idx) => {
    const hoteles = splitHoteles_(o.hotel);
    const estado = Number(o.estado) === 1 ? "activo" : "baja";
    const sync = String(o.sync_status || "").trim().toUpperCase();
    const syncedAt = formatDatetimeAmPm_(o.synced_at || "");
    const creadoRaw = o.sheet_created_at || o.created_at || o.autorizado_en || "";
    const creado = formatDatetimeAmPm_(creadoRaw);

    return {
      id: o.id || (idx + 1),
      nombre: String(o.nombre || "").trim(),
      apellido: String(o.apellido || "").trim(),
      correo: String(o.correo || "").trim(),
      telefono: String(o.telefono || "").trim(),
      cedula: String(o.cedula || "").trim(),
      tipo_doc: String(o.tipo_documento || "").trim(),
      hoteles,
      hotel_raw: String(o.hotel || "").trim(),
      estado,
      sync,
      synced_at: syncedAt,
      creado,
      fecha_nac: String(o.fecha_nacimiento || "").replace(/^'/, "").trim(),
      ip: String(o.ip_origen || "").trim(),
    };
  });

  const total = registros.length;
  const activos = registros.filter(r => r.estado === "activo").length;
  const bajas = registros.filter(r => r.estado === "baja").length;

  const now = new Date();
  const day7 = new Date(now); day7.setDate(now.getDate() - 6); day7.setHours(0, 0, 0, 0);
  const countByDay = {};
  registros.forEach(r => {
    const d = _parseFmtDate_(r.synced_at || r.creado);
    if (!d || d < day7) return;
    const key = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
    countByDay[key] = (countByDay[key] || 0) + 1;
  });
  const dias7 = [];
  for (let i = 6; i >= 0; i--) {
    const dd = new Date(now); dd.setDate(now.getDate() - i); dd.setHours(0, 0, 0, 0);
    const key = `${dd.getFullYear()}-${String(dd.getMonth() + 1).padStart(2, "0")}-${String(dd.getDate()).padStart(2, "0")}`;
    dias7.push({ fecha: key.slice(5), count: countByDay[key] || 0 });
  }

  const hotelMap = {};
  registros.forEach(r => {
    r.hoteles.forEach(h => { if (!h) return; hotelMap[h] = (hotelMap[h] || 0) + 1; });
  });
  const hoteles = Object.entries(hotelMap).sort((a, b) => b[1] - a[1]).map(([n, c]) => ({ nombre: n, count: c }));
  const propUniq = hoteles.length;
  const totalVinculos = hoteles.reduce((s, h) => s + h.count, 0);

  const hoy = new Date(); hoy.setHours(0, 0, 0, 0);
  const hace7 = new Date(hoy); hace7.setDate(hoy.getDate() - 7);
  const hace30 = new Date(hoy); hace30.setDate(hoy.getDate() - 30);
  let nuevosHoy = 0, nuevos7 = 0, nuevos30 = 0;
  registros.forEach(r => {
    const d = _parseFmtDate_(r.synced_at || r.creado);
    if (!d) return;
    if (d >= hoy) nuevosHoy++;
    if (d >= hace7) nuevos7++;
    if (d >= hace30) nuevos30++;
  });
  const hace14 = new Date(hace7); hace14.setDate(hace7.getDate() - 7);
  let nuevos7prev = 0;
  registros.forEach(r => {
    const d = _parseFmtDate_(r.synced_at || r.creado);
    if (!d) return;
    if (d >= hace14 && d < hace7) nuevos7prev++;
  });
  let tendencia7 = "+0%";
  if (nuevos7prev > 0) {
    const pct = Math.round(((nuevos7 - nuevos7prev) / nuevos7prev) * 100);
    tendencia7 = (pct >= 0 ? "+" : "") + pct + "%";
  } else if (nuevos7 > 0) { tendencia7 = "+100%"; }

  return {
    registros,
    resumen: {
      total, activos, bajas, dias7, propUniq, totalVinculos,
      ultima_actualizacion: formatDatetimeAmPm_(new Date())
    },
    hoteles,
    actividad: { nuevosHoy, nuevos7, nuevos30, tendencia7, dias7 },
    sync_stats: _syncStats(registros),
  };
}

function _emptyResumen() { return { total: 0, activos: 0, bajas: 0, dias7: [], propUniq: 0, totalVinculos: 0, ultima_actualizacion: "" }; }
function _emptyActividad() { return { nuevosHoy: 0, nuevos7: 0, nuevos30: 0, tendencia7: "+0%", dias7: [] }; }
function _emptySyncStats() { return { pendiente: 0, sincronizado: 0, error: 0, otro: 0 }; }
function _syncStats(registros) {
  let pendiente = 0, sincronizado = 0, error = 0, otro = 0;
  registros.forEach(r => {
    const s = r.sync;
    if (s === "PENDIENTE") pendiente++;
    else if (s === "SINCRONIZADO") sincronizado++;
    else if (s === "ERROR") error++;
    else otro++;
  });
  return { pendiente, sincronizado, error, otro };
}

// ─── bulkUpdateEstado ─────────────────────────────────────────────────────────
// Por cada correo, verifica sync_status en Sheets:
//   - PENDIENTE | ERROR → solo Sheets (aún no existe en MySQL).
//   - Sincronizado      → MySQL primero, luego Sheets.

function bulkUpdateEstado(correos, nuevoEstado) {
  if (!correos || !correos.length) return { updated: 0 };

  const cfg = getConfig_();
  const estadoNum = Number(nuevoEstado) === 1 ? 1 : 0;
  const correoSet = new Set(correos.map(c => String(c).trim().toLowerCase()));
  const now = new Date();

  // Leer Sheets una sola vez
  const { headers, rows, ws } = getAllRows_();
  if (!headers.length) return { updated: 0 };

  const hm = {};
  headers.forEach((h, i) => { hm[h] = i; });

  if (hm["correo"] === undefined || hm["estado"] === undefined)
    throw new Error("Columnas 'correo' o 'estado' no encontradas en la hoja.");

  const hasSyncCol = hm["sync_status"] !== undefined;

  // Clasificar: cuáles van a MySQL y cuáles solo Sheets
  const paraMySQL = [];
  const soloSheets = [];

  for (let i = 0; i < rows.length; i++) {
    const rowCorreo = String(rows[i][hm["correo"]] || "").trim().toLowerCase();
    if (!correoSet.has(rowCorreo)) continue;
    const sync = hasSyncCol ? String(rows[i][hm["sync_status"]] || "").trim().toUpperCase() : "SINCRONIZADO";
    const item = { correo: rowCorreo, sheetRow: i + 2 };
    if (sync === "PENDIENTE" || sync === "ERROR") soloSheets.push(item);
    else paraMySQL.push(item);
  }

  Logger.log(`bulkUpdateEstado: MySQL=${paraMySQL.length}, SoloSheets=${soloSheets.length}`);

  // ── MySQL para los ya sincronizados ───────────────────────
  if (paraMySQL.length > 0) {
    let conn = null;
    try {
      conn = _getDbConn_(cfg);
      conn.setAutoCommit(false);

      const setUnsub = estadoNum === 0 ? ", unsubscribed_at=NOW()" : "";
      const sql = `UPDATE ${cfg.DB_TABLE} SET estado=?${setUnsub}, updated_at=NOW() WHERE LOWER(correo) = ?`;
      const stmt = conn.prepareStatement(sql);
      stmt.setQueryTimeout(25);

      paraMySQL.forEach(item => {
        stmt.clearParameters();
        stmt.setInt(1, estadoNum);
        stmt.setString(2, item.correo);
        stmt.executeUpdate();
      });
      stmt.close();
      conn.commit();
      Logger.log(`bulkUpdateEstado MySQL OK: ${paraMySQL.length} correo(s)`);

    } catch (e) {
      try { conn && conn.rollback(); } catch (_) { }
      throw new Error("Error en base de datos al cambiar estado: " + (e && e.message ? e.message : String(e)));
    } finally {
      try { conn && conn.close(); } catch (_) { }
    }
  }

  // ── Actualizar Sheets (todos: sincronizados + solo-sheets) ─
  const setCell = (sheetRow, key, val) => {
    if (hm[key] !== undefined) ws.getRange(sheetRow, hm[key] + 1).setValue(val);
  };

  let updated = 0;
  [...paraMySQL, ...soloSheets].forEach(item => {
    setCell(item.sheetRow, "estado", estadoNum);
    if (estadoNum === 0) setCell(item.sheetRow, "unsubscribed_at", now);
    setCell(item.sheetRow, "sheet_updated_at", now);
    updated++;
  });

  return { updated };
}

// ─── updateRegistro ───────────────────────────────────────────────────────────
// Si el registro NO está sincronizado (sync_status = PENDIENTE | ERROR):
//   → solo actualiza Sheets (aún no existe en MySQL).
// Si ya está sincronizado (cualquier otro estado):
//   → MySQL primero. Falla → error, Sheets intacto. MySQL OK → actualiza Sheets.

const NO_SYNC_STATUSES_ = new Set(["PENDIENTE", "ERROR"]);

function updateRegistro(payload) {
  if (!payload || !payload.correo_original) throw new Error("correo_original requerido.");

  const cfg = getConfig_();
  const correoOriginal = String(payload.correo_original).trim().toLowerCase();
  const correoNuevo = String(payload.correo || payload.correo_original).trim().toLowerCase();
  const nombreNuevo = String(payload.nombre || "").trim();
  const apellidoNuevo = String(payload.apellido || "").trim();
  const telefonoNuevo = String(payload.telefono || "").replace(/\D/g, "");
  const estadoNuevo = Number(payload.estado) === 1 ? 1 : 0;
  const now = new Date();

  // Leer fila actual de Sheets para conocer el sync_status
  const { headers, rows, ws } = getAllRows_();
  if (!headers.length) throw new Error("La hoja está vacía.");

  const hm = {};
  headers.forEach((h, i) => { hm[h] = i; });

  let targetIndex = -1;
  for (let i = 0; i < rows.length; i++) {
    if (String(rows[i][hm["correo"]] || "").trim().toLowerCase() === correoOriginal) {
      targetIndex = i;
      break;
    }
  }
  if (targetIndex === -1) throw new Error(`Registro "${correoOriginal}" no encontrado en la hoja.`);

  // Validar que el correo nuevo no exista en otra fila distinta
  if (correoNuevo !== correoOriginal) {
    for (let i = 0; i < rows.length; i++) {
      if (i === targetIndex) continue;
      const otroCorreo = String(rows[i][hm["correo"]] || "").trim().toLowerCase();
      if (otroCorreo === correoNuevo)
        throw new Error("DUPLICADO|El correo " + correoNuevo + " ya está registrado en otro usuario.");
    }
  }

  const syncStatus = String(rows[targetIndex][hm["sync_status"]] || "").trim().toUpperCase();
  const noSincronizado = NO_SYNC_STATUSES_.has(syncStatus);

  const setCol = (sheetRow, key, val) => {
    if (hm[key] !== undefined) ws.getRange(sheetRow, hm[key] + 1).setValue(val);
  };
  const sheetRow = targetIndex + 2;

  if (noSincronizado) {
    // ── Solo Sheets (aún no existe en MySQL) ──────────────────
    Logger.log(`updateRegistro Sheets-only (${syncStatus}): ${correoOriginal}`);
    setCol(sheetRow, "nombre", nombreNuevo);
    setCol(sheetRow, "apellido", apellidoNuevo);
    setCol(sheetRow, "correo", correoNuevo);
    setCol(sheetRow, "telefono", telefonoNuevo);
    setCol(sheetRow, "usuario", telefonoNuevo || correoNuevo);
    setCol(sheetRow, "estado", estadoNuevo);
    setCol(sheetRow, "sheet_updated_at", now);
    if (estadoNuevo === 0) setCol(sheetRow, "unsubscribed_at", now);
    return { updated: 1, via: "sheets_only" };
  }

  // ── Paso 1: MySQL ─────────────────────────────────────────
  let conn = null;
  try {
    conn = _getDbConn_(cfg);
    conn.setAutoCommit(false);

    const setUnsub = estadoNuevo === 0 ? ", unsubscribed_at=NOW()" : "";
    const sql = `
      UPDATE ${cfg.DB_TABLE}
      SET nombre=?, apellido=?, correo=?, telefono=?, usuario=?, estado=?${setUnsub}, updated_at=NOW()
      WHERE LOWER(correo) = ?
    `;
    const stmt = conn.prepareStatement(sql);
    stmt.setQueryTimeout(25);
    stmt.setString(1, nombreNuevo);
    stmt.setString(2, apellidoNuevo);
    stmt.setString(3, correoNuevo);
    stmt.setString(4, telefonoNuevo);
    stmt.setString(5, telefonoNuevo || correoNuevo);
    stmt.setInt(6, estadoNuevo);
    stmt.setString(7, correoOriginal);

    const rowsAffected = stmt.executeUpdate();
    stmt.close();

    if (rowsAffected === 0)
      throw new Error(`No se encontró "${correoOriginal}" en la base de datos.`);

    conn.commit();
    Logger.log(`updateRegistro MySQL OK: ${rowsAffected} fila(s) → ${correoOriginal}`);

  } catch (e) {
    try { conn && conn.rollback(); } catch (_) { }
    throw new Error("Error en base de datos: " + (e && e.message ? e.message : String(e)));
  } finally {
    try { conn && conn.close(); } catch (_) { }
  }

  // ── Paso 2: Sheets ────────────────────────────────────────
  setCol(sheetRow, "nombre", nombreNuevo);
  setCol(sheetRow, "apellido", apellidoNuevo);
  setCol(sheetRow, "correo", correoNuevo);
  setCol(sheetRow, "telefono", telefonoNuevo);
  setCol(sheetRow, "usuario", telefonoNuevo || correoNuevo);
  setCol(sheetRow, "estado", estadoNuevo);
  setCol(sheetRow, "sheet_updated_at", now);
  if (estadoNuevo === 0) setCol(sheetRow, "unsubscribed_at", now);

  return { updated: 1, via: "mysql_and_sheets" };
}

// ─── Performance helpers ──────────────────────────────────────────────────────

function _getPerfConfig_() {
  const sp = PropertiesService.getScriptProperties();
  const sourceId = (sp.getProperty("PERF_SOURCE_ID") || "").trim();
  const histId   = (sp.getProperty("PERF_HIST_ID")   || "").trim();
  if (!sourceId) throw new Error("Falta la propiedad PERF_SOURCE_ID.");
  if (!histId)   throw new Error("Falta la propiedad PERF_HIST_ID.");
  return { sourceId, histId };
}

// col_base del mes en la hoja P&L (0-indexed en Apps Script = +1 en getRange)
// mes: 1=Ene … 12=Dic
function _plColBase_(mes) {
  return 3 + (mes - 1) * 9; // índice 0-based
}

function _plVal_(data, rowIdx, colBase, offset) {
  const row = data[rowIdx];
  if (!row) return null;
  const v = row[colBase + offset];
  return (v !== "" && v !== null && v !== undefined && !isNaN(Number(v)))
    ? Number(v) : null;
}

function _extractHoteles_(paramData) {
  const hoteles = [];
  for (let i = 0; i < paramData.length; i++) {
    const row = paramData[i];
    const orden  = row[1];
    const codigo = String(row[2] || "").trim();
    const link   = String(row[3] || "").trim();
    const nombre = String(row[5] || "").trim();
    if (!link.startsWith("https://docs.google.com/spreadsheets")) continue;
    if (!codigo || !nombre || nombre === "nan") continue;
    let ssId = "";
    try { ssId = link.split("/d/")[1].split("/")[0]; } catch (_) { continue; }
    if (!ssId) continue;
    hoteles.push({ orden: Number(orden) || 0, codigo, nombre, ssId });
  }
  return hoteles.sort((a, b) => a.orden - b.orden);
}

// ─── getHotelesList ───────────────────────────────────────────────────────────
// Devuelve la lista de hoteles activos (con Spreadsheet propio) desde Parametros.

function getHotelesList() {
  const { sourceId } = _getPerfConfig_();
  const ss = SpreadsheetApp.openById(sourceId);
  const ws = ss.getSheetByName("Parametros");
  if (!ws) throw new Error('Hoja "Parametros" no encontrada en PERF_SOURCE_ID.');
  const data = ws.getRange(1, 1, ws.getLastRow(), ws.getLastColumn()).getValues();
  return _extractHoteles_(data).map(h => ({
    codigo: h.codigo,
    nombre: h.nombre,
    ssId:   h.ssId,
  }));
}

// ─── getPerformanceData ───────────────────────────────────────────────────────
// Lee la hoja P&L del hotel (ssId) para el mes indicado (1–12).
// Devuelve métricas del mes actual: REAL, BUDGET, REAL año anterior.

function getPerformanceData(ssId, mes) {
  if (!ssId) throw new Error("ssId requerido.");
  if (!mes || mes < 1 || mes > 12) throw new Error("mes debe ser 1–12.");

  const ss = SpreadsheetApp.openById(ssId);
  const ws = ss.getSheetByName("P&L");
  if (!ws) throw new Error('Hoja "P&L" no encontrada para este hotel.');

  const data = ws.getRange(1, 1, Math.min(ws.getLastRow(), 510), ws.getLastColumn()).getValues();
  const cb   = _plColBase_(mes); // col_base 0-indexed

  const g = (rowIdx, offset) => _plVal_(data, rowIdx, cb, offset);

  // offset: 0=REAL, 2=BUDGET, 4=REAL año anterior
  return {
    mes,
    occ:    { real: g(3,0),   ppto: g(3,2),   ant: g(3,4)   },
    tarifa: { real: g(4,0),   ppto: g(4,2),   ant: g(4,4)   },
    revpar: { real: g(5,0),   ppto: g(5,2),   ant: g(5,4)   },
    ing: {
      aloj:  { real: g(129,0), ppto: g(129,2), ant: g(129,4) },
      ayb:   { real: g(506,0), ppto: g(506,2), ant: g(506,4) },
      otros: { real: g(497,0), ppto: g(497,2), ant: g(497,4) },
      total: { real: g(21,0),  ppto: g(21,2),  ant: g(21,4)  },
    },
  };
}

// ─── getHistoricoData ─────────────────────────────────────────────────────────
// Lee el histórico y devuelve filas filtradas por hotel(es).
// hotelesCodigos: array de strings, ej. ["HIEX BOGOTÁ", "HEX 94"]
// Si está vacío, devuelve todo.

function getHistoricoData(hotelesCodigos) {
  const { histId } = _getPerfConfig_();
  const ss = SpreadsheetApp.openById(histId);
  const ws = ss.getSheetByName("historico");
  if (!ws) return { headers: _histHeaders_(), rows: [] };

  const lastRow = ws.getLastRow();
  if (lastRow < 2) return { headers: _histHeaders_(), rows: [] };

  const raw = ws.getRange(1, 1, lastRow, _histHeaders_().length).getValues();
  const headers = raw[0].map(h => String(h || "").trim().toLowerCase().replace(/\s+/g, "_"));
  const idxCodigo = headers.indexOf("hotel_codigo");
  const rows = raw.slice(1).filter(r => {
    const codigo = String(r[idxCodigo] !== undefined ? r[idxCodigo] : "") .trim();
    if (!codigo) return false;
    if (!hotelesCodigos || !hotelesCodigos.length) return true;
    return hotelesCodigos.includes(codigo);
  });

  return {
    headers: _histHeaders_(),
    rows: rows.map(r => {
      const obj = {};
      headers.forEach((h, i) => {
        const v = r[i];
        obj[h] = v instanceof Date ? v.toISOString() : v;
      });
      return obj;
    }),
  };
}

function _histHeaders_() {
  return [
    "id",
    "hotel_codigo","hotel_nombre","anio","mes","mes_num",
    "occ_real","occ_ppto","occ_ant",
    "tarifa_real","tarifa_ppto","tarifa_ant",
    "revpar_real","revpar_ppto","revpar_ant",
    "ing_aloj","ing_ayb","ing_otros","ing_total",
    "cerrado_en"
  ];
}

// ─── cerrarMes ────────────────────────────────────────────────────────────────
// Cierra un mes para un hotel: lee P&L y guarda fila en el histórico.
// Si ya existe esa combinación hotel+año+mes, la sobreescribe.

const MESES_LABEL_ = ["ENE","FEB","MAR","ABR","MAY","JUN","JUL","AGO","SEPT","OCT","NOV","DIC"];

function cerrarMes(hotelCodigo, hotelNombre, ssId, mes, anio) {
  if (!hotelCodigo || !ssId || !mes || !anio)
    throw new Error("hotelCodigo, ssId, mes y anio son requeridos.");

  const perf = getPerformanceData(ssId, mes);
  const { histId } = _getPerfConfig_();
  const ss = SpreadsheetApp.openById(histId);

  let ws = ss.getSheetByName("historico");
  if (!ws) {
    ws = ss.insertSheet("historico");
    ws.getRange(1, 1, 1, _histHeaders_().length).setValues([_histHeaders_()]);
  }

  const mesLabel = MESES_LABEL_[mes - 1];
  const now = new Date();

  const rowId = `${hotelCodigo}_${anio}_${mes}`;
  const newRow = [
    rowId,
    hotelCodigo, hotelNombre, anio, mesLabel, mes,
    perf.occ.real,    perf.occ.ppto,    perf.occ.ant,
    perf.tarifa.real, perf.tarifa.ppto, perf.tarifa.ant,
    perf.revpar.real, perf.revpar.ppto, perf.revpar.ant,
    perf.ing.aloj.real, perf.ing.ayb.real, perf.ing.otros.real, perf.ing.total.real,
    now,
  ];

  // Buscar si ya existe la fila para sobreescribir (por id compuesto)
  const headers_ = _histHeaders_();
  const idCol = headers_.indexOf("id") + 1; // col 1-based; 0 si no existe aún

  const lastRow = ws.getLastRow();
  if (lastRow >= 2) {
    const checkCols = idCol > 0 ? idCol : 5;
    const existing = ws.getRange(2, 1, lastRow - 1, Math.max(checkCols, 5)).getValues();
    for (let i = 0; i < existing.length; i++) {
      const match = idCol > 0
        ? String(existing[i][idCol - 1]) === rowId
        : (String(existing[i][0]) === hotelCodigo &&
           Number(existing[i][2]) === Number(anio) &&
           Number(existing[i][4]) === Number(mes));
      if (match) {
        ws.getRange(i + 2, 1, 1, newRow.length).setValues([newRow]);
        Logger.log(`cerrarMes: sobreescrito ${hotelCodigo} ${mesLabel}/${anio}`);
        return { ok: true, action: "updated", hotel: hotelCodigo, mes: mesLabel, anio };
      }
    }
  }

  ws.appendRow(newRow);
  Logger.log(`cerrarMes: insertado ${hotelCodigo} ${mesLabel}/${anio}`);
  return { ok: true, action: "inserted", hotel: hotelCodigo, mes: mesLabel, anio };
}

// ─── cerrarMesTodos ───────────────────────────────────────────────────────────
// Trigger automático: cierra el mes anterior para todos los hoteles de Parametros.

function cerrarMesTodos() {
  const { sourceId } = _getPerfConfig_();
  const ss = SpreadsheetApp.openById(sourceId);
  const ws = ss.getSheetByName("Parametros");
  if (!ws) throw new Error('Hoja "Parametros" no encontrada.');

  const data = ws.getRange(1, 1, ws.getLastRow(), ws.getLastColumn()).getValues();
  const hoteles = _extractHoteles_(data);

  const now = new Date();
  let mes  = now.getMonth();
  let anio = now.getFullYear();
  if (mes === 0) { mes = 12; anio--; }

  const resultados = [];
  hoteles.forEach(h => {
    try {
      const res = cerrarMes(h.codigo, h.nombre, h.ssId, mes, anio);
      resultados.push(res);
    } catch (e) {
      Logger.log(`cerrarMesTodos ERROR ${h.codigo}: ${e.message}`);
      resultados.push({ ok: false, hotel: h.codigo, error: e.message });
    }
  });

  Logger.log(`cerrarMesTodos: ${resultados.filter(r => r.ok).length}/${hoteles.length} OK`);
  return resultados;
}

function testHistorico() {
  const result = getHistoricoData([]);
  Logger.log("Headers: " + JSON.stringify(result.headers));
  Logger.log("Rows count: " + result.rows.length);
  if (result.rows.length > 0) {
    Logger.log("Primera fila: " + JSON.stringify(result.rows[0]));
  }
}

function testIncludes() {
  try { HtmlService.createHtmlOutputFromFile("JS_Registros");  Logger.log("JS_Registros: OK"); } catch(e) { Logger.log("JS_Registros: ERROR - " + e.message); }
  try { HtmlService.createHtmlOutputFromFile("JS_Vistas");     Logger.log("JS_Vistas: OK"); }    catch(e) { Logger.log("JS_Vistas: ERROR - " + e.message); }
  try { HtmlService.createHtmlOutputFromFile("JS_Indicadores");Logger.log("JS_Indicadores: OK"); } catch(e) { Logger.log("JS_Indicadores: ERROR - " + e.message); }
  try { HtmlService.createHtmlOutputFromFile("JS_Charts");     Logger.log("JS_Charts: OK"); }    catch(e) { Logger.log("JS_Charts: ERROR - " + e.message); }
  try { HtmlService.createHtmlOutputFromFile("JavaScript");    Logger.log("JavaScript: OK"); }   catch(e) { Logger.log("JavaScript: ERROR - " + e.message); }
}

function testCerrarMesTodos() {
  Logger.log(JSON.stringify(cerrarMesTodos(3, 2026)));
}

// ─── EMAIL CAMPAIGN ───────────────────────────────────────────────

function testSimple() {
  try {
    Logger.log("=== TEST SIMPLE ===");
    
    const data = getAllRows_();
    Logger.log("✓ getAllRows_ ok");
    Logger.log("  Headers count: " + data.headers.length);
    Logger.log("  Rows count: " + data.rows.length);
    
    if (data.rows.length === 0) {
      Logger.log("✗ No hay rows");
      return;
    }
    
    Logger.log("\n=== Test primera fila ===");
    const firstRow = data.rows[0];
    Logger.log("✓ Primera fila obtenida: " + JSON.stringify(firstRow).substring(0, 100));
    
    Logger.log("\n=== Llamando rowToObj_ ===");
    const firstObj = rowToObj_(data.headers, firstRow);
    Logger.log("✓ rowToObj_ ok");
    Logger.log("  Resultado: " + JSON.stringify(firstObj).substring(0, 150));
    
    Logger.log("\n=== Iterando todas las filas ===");
    let count = 0;
    for (let i = 0; i < data.rows.length; i++) {
      try {
        const row = data.rows[i];
        const obj = rowToObj_(data.headers, row);
        const correo = String(obj.correo || "").trim().toLowerCase();
        const estado = Number(obj.estado);
        
        if (correo && correo.includes("@") && estado === 1) {
          count++;
        }
      } catch(e) {
        Logger.log("✗ Error en fila " + i + ": " + e.message);
        break;
      }
    }
    
    Logger.log("\n=== RESULTADO ===");
    Logger.log("Total válidos: " + count);
    
  } catch(e) {
    Logger.log("✗ ERROR GENERAL: " + e.message);
    Logger.log(e.stack);
  }
}

function getCampaignRecipients() {
  try {
    const data = getAllRows_();
    
    if (!data.headers || !data.headers.length) {
      Logger.log("No hay headers");
      return [];
    }
    
    const rows = data.rows;
    if (!rows.length) {
      Logger.log("No hay rows");
      return [];
    }
    
    Logger.log("Procesando " + rows.length + " filas...");
    
    const resultado = [];
    
    rows.forEach((row, idx) => {
      try {
        const obj = rowToObj_(data.headers, row);
        
        // Validar que tenga correo válido
        const correo = String(obj.correo || "").trim().toLowerCase();
        if (!correo || !correo.includes("@")) {
          return; // Skip
        }
        
        // Validar que esté activo
        const estado = Number(obj.estado);
        if (estado !== 1) {
          return; // Skip
        }
        
        // Si llegó aquí, incluir
        resultado.push({
          nombre: String(obj.nombre || "").trim(),
          correo: correo,
          cedula: String(obj.cedula || "").trim(),
          hotel: String(obj.hotel || "").trim()
        });
        
      } catch(e) {
        Logger.log("Error procesando fila " + idx + ": " + e.message);
      }
    });
    
    Logger.log("Resultado: " + resultado.length + " destinatarios válidos");
    return resultado;
    
  } catch(e) {
    Logger.log("getCampaignRecipients error: " + e.message);
    throw new Error("TRANSIENTE|" + _errMsg_(e));
  }
}

function sendCampaignEmails(config) {
  try {
    if (!config.recipients || !config.recipients.length) {
      throw new Error("No hay destinatarios seleccionados");
    }
    
    Logger.log("Iniciando envío a " + config.recipients.length + " destinatarios");
    Logger.log("Asunto: " + config.asunto);
    
    let sentCount = 0;
    const errors = [];
    
    config.recipients.forEach((r, idx) => {
      try {
        if (!r.correo || !String(r.correo).includes("@")) {
          throw new Error("Correo inválido: " + r.correo);
        }
        
        let htmlBody = config.htmlContent || "";
        
        // Reemplazar variables
        htmlBody = htmlBody
          .replace(/\{nombre\}/g, _htmlEsc(r.nombre || "Huésped"))
          .replace(/\{cedula\}/g, _htmlEsc(r.cedula || ""))
          .replace(/\{hotel\}/g, _htmlEsc(r.hotel || ""));
        
        Logger.log("[" + (idx + 1) + "/" + config.recipients.length + "] Enviando a: " + r.correo);
        
        // Usar MailApp en lugar de GmailApp
        MailApp.sendEmail(
          r.correo,
          config.asunto,
          "",
          {
            htmlBody: htmlBody,
            name: "OxoHotel",
            from: Session.getActiveUser().getEmail()
          }
        );
        
        sentCount++;
        Logger.log("  ✓ Enviado exitosamente");
        
      } catch(e) {
        const errMsg = e.message || String(e);
        Logger.log("  ✗ Error: " + errMsg);
        errors.push({
          email: r.correo || "sin-email",
          error: errMsg
        });
      }
      
      // Rate limit: espacio entre envíos (50 correos cada 500ms)
      if (idx % 50 === 49) {
        Logger.log("Pausa de 500ms por rate limit...");
        Utilities.sleep(500);
      }
    });
    
    const result = {
      success: true,
      sentCount: sentCount,
      totalRequested: config.recipients.length,
      errors: errors
    };
    
    Logger.log("Envío completado: " + sentCount + "/" + config.recipients.length);
    return result;
    
  } catch(e) {
    Logger.log("sendCampaignEmails error: " + e.message);
    throw new Error("TRANSIENTE|" + _errMsg_(e));
  }
}

function _htmlEsc(s) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function _errMsg_(e) {
  let m = (e && e.message) ? e.message : String(e || "Error desconocido");
  m = m.replace(/^(Error:\s*)+/i, "");
  const pipe = m.indexOf("|");
  if (pipe !== -1) m = m.slice(pipe + 1).trim();
  return m.trim() || "Error desconocido";
}