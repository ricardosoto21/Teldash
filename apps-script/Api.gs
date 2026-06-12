function doGet() {
  try {
    const spreadsheet = getTariffSpreadsheet_();
    ensureSchema_(spreadsheet, false);
    const costs = getCurrentRatesForApi_(
      spreadsheet,
      TARIFF_CONFIG.SHEETS.COSTS,
      'Proveedor'
    );
    const sales = getCurrentRatesForApi_(
      spreadsheet,
      TARIFF_CONFIG.SHEETS.SALES,
      'Cliente'
    );
    const historySheet = spreadsheet.getSheetByName(TARIFF_CONFIG.SHEETS.HISTORY);
    const errorSheet = spreadsheet.getSheetByName(TARIFF_CONFIG.SHEETS.ERRORS);

    return jsonResponse_({
      costos: costs,
      ventas: sales,
      meta: {
        generatedAt: new Date().toISOString(),
        lastProcessedMessageAt: getLastValue_(
          historySheet,
          TARIFF_CONFIG.HEADERS.historial_importaciones.indexOf('message_at') + 1
        ),
        errorCount: Math.max(0, errorSheet.getLastRow() - 1)
      }
    });
  } catch (error) {
    console.error(error.stack || error.message);
    return jsonResponse_({
      costos: [],
      ventas: [],
      meta: {
        generatedAt: new Date().toISOString(),
        error: 'Tariff service is not configured or temporarily unavailable.'
      }
    });
  }
}

function jsonResponse_(payload) {
  return ContentService.createTextOutput(JSON.stringify(payload))
    .setMimeType(ContentService.MimeType.JSON);
}

function getLastValue_(sheet, column) {
  if (!sheet || sheet.getLastRow() < 2 || column < 1) {
    return null;
  }
  const value = sheet.getRange(sheet.getLastRow(), column).getValue();
  if (value instanceof Date) {
    return value.toISOString();
  }
  return value || null;
}
