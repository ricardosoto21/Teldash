/**
 * Creates a new tariff database, installs the schema and stores its ID.
 * Run this once from the Apps Script editor for a new installation.
 */
function createTariffDatabase() {
  const spreadsheet = SpreadsheetApp.create('Telvoice - Base de tarifas');
  PropertiesService.getScriptProperties().setProperty(
    TARIFF_CONFIG.SPREADSHEET_PROPERTY,
    spreadsheet.getId()
  );
  ensureSchema_(spreadsheet, true);
  installFiveMinuteTrigger();
  console.log('Tariff database created: ' + spreadsheet.getUrl());
  return spreadsheet.getUrl();
}

/**
 * Connects the automation to an existing Google Spreadsheet.
 */
function initializeTariffAutomation(spreadsheetId) {
  if (!spreadsheetId) {
    throw new Error('A spreadsheetId is required.');
  }

  const spreadsheet = SpreadsheetApp.openById(String(spreadsheetId).trim());
  PropertiesService.getScriptProperties().setProperty(
    TARIFF_CONFIG.SPREADSHEET_PROPERTY,
    spreadsheet.getId()
  );
  ensureSchema_(spreadsheet, true);
  installFiveMinuteTrigger();
  return spreadsheet.getUrl();
}

function installFiveMinuteTrigger() {
  removeTariffTriggers();
  ScriptApp.newTrigger(TARIFF_CONFIG.TRIGGER_HANDLER)
    .timeBased()
    .everyMinutes(5)
    .create();
}

function removeTariffTriggers() {
  ScriptApp.getProjectTriggers().forEach(function(trigger) {
    if (trigger.getHandlerFunction() === TARIFF_CONFIG.TRIGGER_HANDLER) {
      ScriptApp.deleteTrigger(trigger);
    }
  });
}

function setTariffRuntimeOptions(scanDays, maxThreads, maxAttachments) {
  const properties = PropertiesService.getScriptProperties();
  properties.setProperties({
    TARIFF_SCAN_DAYS: String(positiveInteger_(scanDays, TARIFF_CONFIG.DEFAULT_SCAN_DAYS)),
    TARIFF_MAX_THREADS: String(positiveInteger_(maxThreads, TARIFF_CONFIG.DEFAULT_MAX_THREADS)),
    TARIFF_MAX_ATTACHMENTS: String(
      positiveInteger_(maxAttachments, TARIFF_CONFIG.DEFAULT_MAX_ATTACHMENTS)
    )
  });
}

function getTariffSpreadsheetUrl() {
  return getTariffSpreadsheet_().getUrl();
}

function ensureSchema_(spreadsheet, includeExamples) {
  const sheets = TARIFF_CONFIG.SHEETS;
  ensureSheet_(spreadsheet, sheets.ACTORS, TARIFF_CONFIG.HEADERS.actores);
  ensureSheet_(spreadsheet, sheets.TEMPLATES, TARIFF_CONFIG.HEADERS.plantillas);
  ensureSheet_(spreadsheet, sheets.ROUTES, TARIFF_CONFIG.HEADERS.catalogo_rutas);
  ensureSheet_(spreadsheet, sheets.COSTS, TARIFF_CONFIG.HEADERS.currentRates);
  ensureSheet_(spreadsheet, sheets.SALES, TARIFF_CONFIG.HEADERS.currentRates);
  ensureSheet_(spreadsheet, sheets.HISTORY, TARIFF_CONFIG.HEADERS.historial_importaciones);
  ensureSheet_(spreadsheet, sheets.ERRORS, TARIFF_CONFIG.HEADERS.errores);

  const defaultSheet = spreadsheet.getSheetByName('Sheet1') ||
    spreadsheet.getSheetByName('Hoja 1');
  if (defaultSheet && spreadsheet.getSheets().length > 1 && defaultSheet.getLastRow() === 0) {
    spreadsheet.deleteSheet(defaultSheet);
  }

  if (includeExamples) {
    seedExamples_(spreadsheet);
  }
}

function ensureSheet_(spreadsheet, name, headers) {
  let sheet = spreadsheet.getSheetByName(name);
  if (!sheet) {
    sheet = spreadsheet.insertSheet(name);
  }

  const currentHeaders = sheet.getRange(1, 1, 1, headers.length).getDisplayValues()[0];
  const needsHeaders = currentHeaders.join('|') !== headers.join('|');
  if (needsHeaders) {
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  }

  sheet.setFrozenRows(1);
  sheet.getRange(1, 1, 1, headers.length)
    .setBackground('#1e3c72')
    .setFontColor('#ffffff')
    .setFontWeight('bold');
}

function seedExamples_(spreadsheet) {
  const actorsSheet = spreadsheet.getSheetByName(TARIFF_CONFIG.SHEETS.ACTORS);
  if (actorsSheet.getLastRow() === 1) {
    actorsSheet.appendRow([
      'proveedor_demo',
      'Proveedor Demo',
      'costos',
      'received',
      'tarifas@proveedor.example',
      'tabular_demo',
      'USD',
      false
    ]);
    actorsSheet.appendRow([
      'cliente_demo',
      'Cliente Demo',
      'ventas',
      'sent',
      'compras@cliente.example',
      'tabular_demo',
      'USD',
      false
    ]);
  }

  const templatesSheet = spreadsheet.getSheetByName(TARIFF_CONFIG.SHEETS.TEMPLATES);
  if (templatesSheet.getLastRow() === 1) {
    templatesSheet.appendRow([
      'tabular_demo',
      'csv,xlsx,xls',
      '.*',
      '',
      1,
      JSON.stringify({
        country: ['Country', 'Pais', 'Pa\u00eds'],
        route: ['Route', 'Ruta', 'Destination'],
        mcc: ['MCC'],
        mnc: ['MNC'],
        operator: ['Operator', 'Operador', 'Network'],
        rate: ['Rate', 'Tarifa', 'Price'],
        currency: ['Currency', 'Moneda'],
        effective_from: ['Effective Date', 'Vigente desde', 'Fecha']
      }),
      JSON.stringify({
        defaults: {},
        rate_multiplier: 1
      }),
      true
    ]);
  }

  const routesSheet = spreadsheet.getSheetByName(TARIFF_CONFIG.SHEETS.ROUTES);
  if (routesSheet.getLastRow() === 1) {
    routesSheet.appendRow([
      'proveedor_demo',
      'Chile Mobile Entel',
      'Chile',
      'Chile Mobile',
      '730',
      '01',
      'Entel',
      false
    ]);
  }
}

function positiveInteger_(value, fallback) {
  const parsed = parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}
