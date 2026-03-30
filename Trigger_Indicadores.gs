/**
 * Oxo Partners - Trigger cierre mensual automático
 *
 * INSTRUCCIONES:
 *   1. Ejecutar setupTriggerMensual() UNA sola vez desde el editor.
 *   2. El trigger correrá automáticamente el día 1 de cada mes a las 2–3 AM.
 *   3. Para eliminar el trigger: ejecutar teardownTriggerMensual()
 */

function cerrarMesAutomatico() {
  Logger.log("cerrarMesAutomatico: iniciando " + new Date());
  try {
    const resultados = cerrarMesTodos();
    const ok    = resultados.filter(r => r.ok).length;
    const total = resultados.length;
    Logger.log("cerrarMesAutomatico: " + ok + "/" + total + " hoteles cerrados OK");
    resultados.filter(r => !r.ok).forEach(r => {
      Logger.log("ERROR " + r.hotel + ": " + r.error);
    });
  } catch(e) {
    Logger.log("cerrarMesAutomatico ERROR: " + e.message);
  }
}

function setupTriggerMensual() {
  // Eliminar triggers previos del mismo nombre para evitar duplicados
  ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === "cerrarMesAutomatico")
    .forEach(t => ScriptApp.deleteTrigger(t));

  // Crear trigger mensual: día 1 de cada mes, entre 2 AM y 3 AM
  ScriptApp.newTrigger("cerrarMesAutomatico")
    .timeBased()
    .onMonthDay(1)
    .atHour(2)
    .create();

  Logger.log("setupTriggerMensual: trigger instalado — corre el día 1 de cada mes a las 2 AM");
}

function teardownTriggerMensual() {
  const deleted = ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === "cerrarMesAutomatico");
  deleted.forEach(t => ScriptApp.deleteTrigger(t));
  Logger.log("teardownTriggerMensual: " + deleted.length + " trigger(s) eliminado(s)");
}