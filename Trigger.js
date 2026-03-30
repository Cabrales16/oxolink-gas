/**
 * Oxo Partners - Trigger de sincronización Sheets → MySQL  (v6 - sin UPD_PENDIENTE)
 *
 * INSTRUCCIONES:
 *   1. En el panel de "Activadores" (Triggers) de Apps Script, crea un
 *      activador de tiempo apuntando a la función: syncPendingChanges
 *      (cada 5 min, o el intervalo que prefieras).
 *   2. Los valores de BD se leen de Script Properties:
 *      DB_HOST, DB_NAME, DB_USER, DB_PASS, DB_TABLE
 *      SPREADSHEET_ID, SHEET_NAME
 *      SYNC_BATCH_SIZE (opcional, default 50)
 *
 * IMPORTANTE:
 *   - Las ediciones de registros ya NO pasan por aquí.
 *     El panel Admin escribe directo a MySQL + Sheets en tiempo real.
 *   - Este trigger solo procesa:
 *       PENDIENTE        → INSERT (nuevos registros del formulario)
 *       DELETE_PENDIENTE → DELETE
 *   - UPD_PENDIENTE ya no se genera ni se procesa.
 *
 * Funciones disponibles para uso manual desde el editor:
 *   syncPendingChanges()  → sincroniza PENDIENTE y DELETE_PENDIENTE
 *   forceSyncNow()        → alias para pruebas
 *   forzarSync()          → alias con log extendido
 *   diagnosticarHoja()    → verifica columnas y estados en el Sheets
 *   debugUpdate()         → prueba un UPDATE directo a la BD
 *   consultarBD()         → imprime en Logger todas las filas actuales de la tabla
 */

// ─── Config de BD ─────────────────────────────────────────────────────────────

function getTriggerConfig_() {
  const sp = PropertiesService.getScriptProperties();
  const cfg = {
    DB_HOST: (sp.getProperty("DB_HOST") || "").trim(),
    DB_NAME: (sp.getProperty("DB_NAME") || "").trim(),
    DB_USER: (sp.getProperty("DB_USER") || "").trim(),
    DB_PASS: sp.getProperty("DB_PASS") || "",
    DB_TABLE: (sp.getProperty("DB_TABLE") || "").trim(),

    SPREADSHEET_ID: (sp.getProperty("SPREADSHEET_ID") || "").trim(),
    SHEET_NAME: (sp.getProperty("SHEET_NAME") || "Users_OxoLink").trim(),

    SYNC_BATCH_SIZE: Math.max(1, Math.min(500, Number(sp.getProperty("SYNC_BATCH_SIZE") || 50)))
  };

  const required = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASS", "DB_TABLE", "SPREADSHEET_ID", "SHEET_NAME"];
  const missing = required.filter(k => !cfg[k]);
  if (missing.length) throw new Error("Faltan propiedades de BD: " + missing.join(", "));

  cfg.URLBD = `jdbc:mysql://${cfg.DB_HOST}:3306/${cfg.DB_NAME}?useUnicode=true&characterEncoding=UTF-8&useSSL=false&requireSSL=false`;
  return cfg;
}

// ─── Función principal ────────────────────────────────────────────────────────

function syncPendingChanges() {
  const cfg = getTriggerConfig_();

  const lock = LockService.getScriptLock();
  if (!lock.tryLock(15000)) {
    Logger.log("syncPendingChanges: no se obtuvo lock, otra ejecución en curso.");
    return;
  }

  try {
    const ws = _triggerOpenSheet_(cfg);
    const headerMap = _triggerGetHeaderMap_(ws);

    _triggerMustHaveCols_(headerMap, [
      "nombre", "apellido", "tipo_documento", "cedula", "correo", "telefono",
      "fecha_nacimiento", "hotel", "acepta_politica",
      "sync_status", "sync_attempts", "sync_last_error", "synced_at"
    ]);

    const lastRow = ws.getLastRow();
    if (lastRow < 2) return;

    const lastCol = Object.keys(headerMap).reduce((mx, k) => Math.max(mx, headerMap[k]), 0) + 1;
    const data = ws.getRange(2, 1, lastRow - 1, lastCol).getValues();

    const statusIdx = headerMap["sync_status"];
    const pending = [], deletes = [];

    for (let i = 0; i < data.length; i++) {
      const status = String(data[i][statusIdx] || "").trim().toUpperCase();
      if (status === "PENDIENTE" && pending.length < cfg.SYNC_BATCH_SIZE) pending.push({ sheetRow: i + 2, values: data[i] });
      else if (status === "DELETE_PENDIENTE" && deletes.length < cfg.SYNC_BATCH_SIZE) deletes.push({ sheetRow: i + 2, values: data[i] });
    }

    Logger.log(`Pendientes: ${pending.length} | Deletes: ${deletes.length}`);

    if (!pending.length && !deletes.length) {
      Logger.log("Nada que sincronizar.");
      return;
    }

    let conn = null;
    try {
      conn = _getConnWithRetry_(cfg);
      Logger.log("Conexión a BD establecida.");
    } catch (e) {
      const now = new Date();
      const msg = "Conexión fallida con la BD. Se reintentará.";
      [...pending, ...deletes].forEach(item => {
        const row = item.sheetRow;
        const attempts = Number(item.values[headerMap["sync_attempts"]] || 0) || 0;
        _setCell_(ws, row, headerMap, "sync_attempts", attempts + 1);
        _setCell_(ws, row, headerMap, "sync_last_error", msg);
        _setCell_(ws, row, headerMap, "sheet_updated_at", now);
      });
      Logger.log("ERROR conexión: " + (e && e.stack ? e.stack : e));
      return;
    }

    try {
      if (pending.length) processInserts_(cfg, conn, ws, headerMap, pending);
      if (deletes.length) processDeletes_(cfg, conn, ws, headerMap, deletes);
    } finally {
      try { conn.close(); } catch (_) { }
    }

  } finally {
    try { lock.releaseLock(); } catch (_) { }
  }
}

function forceSyncNow() { syncPendingChanges(); }

// ─── INSERT ───────────────────────────────────────────────────────────────────

function processInserts_(cfg, conn, ws, headerMap, pending) {
  conn.setAutoCommit(true);

  const sql = `
    INSERT INTO ${cfg.DB_TABLE}
    (nombre, apellido, tipo_documento, cedula, correo, telefono,
     fecha_nacimiento, hotel, acepta_politica, estado, usuario, ip_origen, sincronizacion)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
  `;
  const stmt = conn.prepareStatement(sql);
  stmt.setQueryTimeout(25);

  try {
    const now = new Date();

    pending.forEach(item => {
      const row = item.sheetRow;
      const rowVals = item.values;
      const attempts = Number(rowVals[headerMap["sync_attempts"]] || 0) || 0;

      const parsed = _sheetRowToPayload_(rowVals, headerMap);
      if (!parsed.ok) {
        _setCell_(ws, row, headerMap, "sync_status", "ERROR");
        _setCell_(ws, row, headerMap, "sync_attempts", attempts + 1);
        _setCell_(ws, row, headerMap, "sync_last_error", parsed.err);
        _setCell_(ws, row, headerMap, "sheet_updated_at", now);
        return;
      }

      try {
        _insertDbSingle_(stmt, parsed.payload);
        Logger.log(`INSERT OK: ${parsed.payload.correo}`);
        _setCell_(ws, row, headerMap, "sync_status", "SINCRONIZADO");
        _setCell_(ws, row, headerMap, "sync_attempts", attempts);
        _setCell_(ws, row, headerMap, "sync_last_error", "");
        _setCell_(ws, row, headerMap, "synced_at", now);
        _setCell_(ws, row, headerMap, "sheet_updated_at", now);
      } catch (e) {
        Logger.log(`ERROR INSERT ${parsed.payload.correo}: ${e.message}`);
        if (_isDuplicate_(e)) {
          _setCell_(ws, row, headerMap, "sync_status", "ERROR");
          _setCell_(ws, row, headerMap, "sync_attempts", attempts + 1);
          _setCell_(ws, row, headerMap, "sync_last_error", "Duplicado: ya existe correo o documento.");
          _setCell_(ws, row, headerMap, "synced_at", "");
          _setCell_(ws, row, headerMap, "sheet_updated_at", now);
        } else if (_isTransient_(e)) {
          _setCell_(ws, row, headerMap, "sync_status", "PENDIENTE");
          _setCell_(ws, row, headerMap, "sync_attempts", attempts + 1);
          _setCell_(ws, row, headerMap, "sync_last_error", "Conexión inestable. Se reintentará.");
          _setCell_(ws, row, headerMap, "sheet_updated_at", now);
        } else {
          _setCell_(ws, row, headerMap, "sync_status", "ERROR");
          _setCell_(ws, row, headerMap, "sync_attempts", attempts + 1);
          _setCell_(ws, row, headerMap, "sync_last_error", String(e && e.message ? e.message : e).slice(0, 240));
          _setCell_(ws, row, headerMap, "synced_at", "");
          _setCell_(ws, row, headerMap, "sheet_updated_at", now);
        }
      }
    });
  } finally {
    stmt.close();
  }
}

// ─── DELETE ───────────────────────────────────────────────────────────────────

function processDeletes_(cfg, conn, ws, headerMap, deletes) {
  conn.setAutoCommit(true);

  const sqlDel = `DELETE FROM ${cfg.DB_TABLE} WHERE LOWER(correo) = ?`;
  const stmt = conn.prepareStatement(sqlDel);
  stmt.setQueryTimeout(25);

  // Procesamos de mayor a menor sheetRow para no alterar índices al borrar filas
  deletes.sort((a, b) => b.sheetRow - a.sheetRow);

  try {
    const now = new Date();

    deletes.forEach(item => {
      const row = item.sheetRow;
      const rowVals = item.values;
      const attempts = Number(rowVals[headerMap["sync_attempts"]] || 0) || 0;
      const correo = String(rowVals[headerMap["correo"]] || "").trim().toLowerCase();

      if (!correo) {
        ws.deleteRow(row);
        return;
      }

      try {
        stmt.clearParameters();
        stmt.setString(1, correo);
        const rowsAffected = stmt.executeUpdate();
        Logger.log(`DELETE ${correo} → filas afectadas: ${rowsAffected}`);
        ws.deleteRow(row);
      } catch (e) {
        Logger.log(`ERROR DELETE ${correo}: ${e.message}`);
        const errMsg = _isTransient_(e)
          ? "Conexión inestable. Se reintentará."
          : String(e && e.message ? e.message : e).slice(0, 240);
        _setCell_(ws, row, headerMap, "sync_status", "DELETE_PENDIENTE");
        _setCell_(ws, row, headerMap, "sync_attempts", attempts + 1);
        _setCell_(ws, row, headerMap, "sync_last_error", errMsg);
        _setCell_(ws, row, headerMap, "sheet_updated_at", now);
        Logger.log(`DELETE falló para ${correo}: ${errMsg}`);
      }
    });
  } finally {
    stmt.close();
  }
}

// ─── Diagnóstico ──────────────────────────────────────────────────────────────

function diagnosticarHoja() {
  const cfg = getTriggerConfig_();
  const ws = _triggerOpenSheet_(cfg);
  const headerMap = _triggerGetHeaderMap_(ws);
  Logger.log("Columnas encontradas: " + JSON.stringify(Object.keys(headerMap)));

  const required = [
    "nombre", "apellido", "tipo_documento", "cedula", "correo", "telefono",
    "fecha_nacimiento", "hotel", "acepta_politica",
    "sync_status", "sync_attempts", "sync_last_error", "synced_at"
  ];

  const faltantes = required.filter(k => headerMap[k] === undefined);
  Logger.log("Columnas FALTANTES: " + (faltantes.length ? faltantes.join(", ") : "ninguna"));

  const lastRow = ws.getLastRow();
  if (lastRow < 2) { Logger.log("La hoja está vacía"); return; }
  const lastCol = ws.getLastColumn();
  const data = ws.getRange(2, 1, lastRow - 1, lastCol).getValues();
  const statusIdx = headerMap["sync_status"];
  const conteo = {};
  data.forEach(row => {
    const s = String(row[statusIdx] || "").trim().toUpperCase() || "(vacío)";
    conteo[s] = (conteo[s] || 0) + 1;
  });
  Logger.log("Estados sync_status: " + JSON.stringify(conteo));
}

function debugUpdate() {
  const cfg = getTriggerConfig_();
  Logger.log("Config BD: " + cfg.DB_HOST + " / " + cfg.DB_NAME + " / tabla: " + cfg.DB_TABLE);

  let conn = null;
  try {
    conn = _getConnWithRetry_(cfg);
    Logger.log("Conexión establecida OK");
    conn.setAutoCommit(true);

    const correoTest = "correo_de_prueba@ejemplo.com"; // cambia por uno real
    const stmt = conn.prepareStatement(
      `UPDATE ${cfg.DB_TABLE} SET updated_at = NOW() WHERE LOWER(correo) = ?`
    );
    stmt.setString(1, correoTest.toLowerCase());
    const rowsAffected = stmt.executeUpdate();
    Logger.log(`executeUpdate completado → filas afectadas: ${rowsAffected}`);
    stmt.close();
  } catch (e) {
    Logger.log("ERROR: " + e.message);
    Logger.log("Stack: " + e.stack);
  } finally {
    if (conn) { try { conn.close(); } catch (_) { } }
  }
}

function forzarSync() {
  Logger.log("Iniciando sync forzado: " + new Date());
  try {
    syncPendingChanges();
    Logger.log("Sync completado sin errores");
  } catch (e) {
    Logger.log("ERROR: " + e.message + "\n" + e.stack);
  }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function _triggerOpenSheet_(cfg) {
  const ss = SpreadsheetApp.openById(cfg.SPREADSHEET_ID);
  const ws = ss.getSheetByName(cfg.SHEET_NAME);
  if (!ws) throw new Error(`Hoja "${cfg.SHEET_NAME}" no encontrada.`);
  return ws;
}

function _triggerGetHeaderMap_(ws) {
  const lastCol = ws.getLastColumn();
  if (!lastCol) throw new Error("La hoja no tiene encabezados.");
  const headers = ws.getRange(1, 1, 1, lastCol).getValues()[0];
  const map = {};
  headers.forEach((h, i) => {
    const k = String(h || "").trim().toLowerCase().replace(/\s+/g, "_");
    if (k) map[k] = i;
  });
  return map;
}

function _triggerMustHaveCols_(headerMap, requiredKeys) {
  const missing = requiredKeys.filter(k => headerMap[k] === undefined);
  if (missing.length) throw new Error("Faltan columnas en la hoja: " + missing.join(", "));
}

function _setCell_(ws, row, headerMap, key, value) {
  const col = headerMap[key];
  if (col === undefined) return;
  ws.getRange(row, col + 1).setValue(value);
}

function _getConnWithRetry_(cfg) {
  const maxAttempts = 5;
  let lastErr = null;
  for (let i = 1; i <= maxAttempts; i++) {
    try { return Jdbc.getConnection(cfg.URLBD, cfg.DB_USER, cfg.DB_PASS); }
    catch (e) {
      lastErr = e;
      if (!_isTransient_(e)) break;
      Utilities.sleep(Math.min(5000, 800 + (i - 1) * 900));
    }
  }
  throw lastErr;
}

function _isDuplicate_(e) {
  const m = String(e && e.message ? e.message : e).toLowerCase();
  return m.includes("duplicate entry") || (m.includes("duplicate") && m.includes("key")) || m.includes("sqlintegrityconstraint");
}

function _isTransient_(e) {
  const m = String(e && e.message ? e.message : e).toLowerCase();
  return m.includes("failed to establish") || m.includes("communications link") ||
    m.includes("timed out") || m.includes("timeout") ||
    (m.includes("exception") && m.includes("jdbc"));
}

function _sheetRowToPayload_(values, headerMap) {
  const get = key => { const c = headerMap[key]; return c !== undefined ? values[c] : ""; };

  const nombre = String(get("nombre") || "").trim();
  const apellido = String(get("apellido") || "").trim();
  const tipo_documento = String(get("tipo_documento") || "").trim();
  const cedula = String(get("cedula") || "").replace(/\D/g, "").trim();
  const correo = String(get("correo") || "").trim().toLowerCase();
  const telefono = String(get("telefono") || "").replace(/\D/g, "").trim();
  const fecha_nacimiento = String(get("fecha_nacimiento") || "").trim().replace(/^'/, "");
  const hotel = String(get("hotel") || "").trim();
  const acepta_politica = Number(get("acepta_politica") || 0) ? 1 : 0;
  const estado = Number(get("estado") || 0) || 0;
  const usuario = String(get("usuario") || "").trim() || cedula;
  const ip_origen = String(get("ip_origen") || "").trim() || null;
  const sincronizacion = "PENDIENTE";

  if (!nombre || !apellido || !tipo_documento || !cedula || !correo || !telefono || !fecha_nacimiento || !hotel) {
    return { ok: false, err: "Faltan datos obligatorios para sincronizar." };
  }
  return {
    ok: true, payload: {
      nombre, apellido, tipo_documento, cedula, correo, telefono,
      fecha_nacimiento, hotel, acepta_politica, estado,
      usuario, ip_origen, sincronizacion
    }
  };
}

function _insertDbSingle_(stmt, p) {
  stmt.clearParameters();
  stmt.setString(1, p.nombre);
  stmt.setString(2, p.apellido);
  stmt.setString(3, p.tipo_documento);
  stmt.setString(4, p.cedula);
  stmt.setString(5, p.correo);
  stmt.setString(6, p.telefono);
  stmt.setString(7, p.fecha_nacimiento);
  stmt.setString(8, p.hotel);
  stmt.setInt(9, Number(p.acepta_politica) || 0);
  stmt.setInt(10, Number(p.estado) || 0);
  stmt.setString(11, p.usuario || p.cedula);
  if (p.ip_origen) stmt.setString(12, p.ip_origen);
  else stmt.setNull(12, Jdbc.Type.VARCHAR);
  stmt.setString(13, p.sincronizacion);
  stmt.execute();
}

// ─── Consulta BD → Logger ─────────────────────────────────────────────────────

function consultarBD() {
  const cfg = getTriggerConfig_();

  let conn = null;
  try {
    conn = _getConnWithRetry_(cfg);
    Logger.log("=== consultarBD: conexión establecida ===");
    Logger.log("Tabla: " + cfg.DB_TABLE);
    Logger.log("Fecha consulta: " + new Date());
    Logger.log("─────────────────────────────────────────────────");

    const stmt = conn.prepareStatement(`
      SELECT id, nombre, apellido, tipo_documento, cedula, correo,
             telefono, fecha_nacimiento, hotel, acepta_politica,
             estado, usuario, autorizado_en, created_at, updated_at,
             unsubscribed_at, ip_origen
      FROM   ${cfg.DB_TABLE}
      ORDER  BY id DESC
    `);
    stmt.setQueryTimeout(30);

    const rs = stmt.executeQuery();
    const meta = rs.getMetaData();
    const colCount = meta.getColumnCount();

    const headers = [];
    for (let i = 1; i <= colCount; i++) headers.push(meta.getColumnName(i));
    Logger.log("COLUMNAS: " + headers.join(" | "));
    Logger.log("─────────────────────────────────────────────────");

    let rowCount = 0;
    while (rs.next()) {
      const row = [];
      for (let i = 1; i <= colCount; i++) {
        row.push(rs.getString(i) !== null ? rs.getString(i) : "NULL");
      }
      Logger.log(row.join(" | "));
      rowCount++;
    }

    Logger.log("─────────────────────────────────────────────────");
    Logger.log(`=== Total filas: ${rowCount} ===`);

    rs.close();
    stmt.close();
  } catch (e) {
    Logger.log("ERROR consultarBD: " + e.message);
    Logger.log("Stack: " + e.stack);
  } finally {
    if (conn) { try { conn.close(); } catch (_) { } }
  }
}