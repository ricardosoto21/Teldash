function getTariffSpreadsheet_() {
  const spreadsheetId = PropertiesService.getScriptProperties().getProperty(
    TARIFF_CONFIG.SPREADSHEET_PROPERTY
  );
  if (!spreadsheetId) {
    throw new Error(
      'Tariff database is not configured. Run createTariffDatabase() or ' +
      'initializeTariffAutomation(spreadsheetId).'
    );
  }
  return SpreadsheetApp.openById(spreadsheetId);
}

function readConfiguration_(spreadsheet) {
  return {
    actors: readEnabledRows_(
      spreadsheet.getSheetByName(TARIFF_CONFIG.SHEETS.ACTORS),
      TARIFF_CONFIG.HEADERS.actores
    ).map(buildActor_),
    templates: readEnabledRows_(
      spreadsheet.getSheetByName(TARIFF_CONFIG.SHEETS.TEMPLATES),
      TARIFF_CONFIG.HEADERS.plantillas
    ).map(buildTemplate_),
    routeCatalog: buildRouteCatalog_(
      readEnabledRows_(
        spreadsheet.getSheetByName(TARIFF_CONFIG.SHEETS.ROUTES),
        TARIFF_CONFIG.HEADERS.catalogo_rutas
      )
    )
  };
}

function readEnabledRows_(sheet, headers) {
  if (!sheet || sheet.getLastRow() < 2) {
    return [];
  }

  const width = Math.max(headers.length, sheet.getLastColumn());
  const values = sheet.getRange(1, 1, sheet.getLastRow(), width).getDisplayValues();
  const actualHeaders = values.shift().map(function(header) {
    return String(header).trim();
  });

  return values
    .filter(function(row) {
      return row.some(function(value) {
        return String(value).trim() !== '';
      });
    })
    .map(function(row) {
      return rowToObject_(actualHeaders, row);
    })
    .filter(function(row) {
      return parseBoolean_(row.enabled);
    });
}

function buildActor_(row) {
  const datasetType = normalizeDatasetType_(row.dataset_type);
  const mailboxSide = normalizeMailboxSide_(row.mailbox_side);
  if (!row.actor_id || !row.name || !datasetType || !mailboxSide || !row.parser_id) {
    throw new Error('Invalid actor configuration: ' + JSON.stringify(row));
  }

  return {
    id: String(row.actor_id).trim(),
    name: String(row.name).trim(),
    datasetType: datasetType,
    mailboxSide: mailboxSide,
    emailRules: splitList_(row.email_match),
    parserId: String(row.parser_id).trim(),
    defaultCurrency: normalizeCurrency_(row.default_currency || 'USD')
  };
}

function buildTemplate_(row) {
  if (!row.parser_id) {
    throw new Error('A parser_id is required in plantillas.');
  }

  return {
    id: String(row.parser_id).trim(),
    fileTypes: splitList_(row.file_types).map(function(value) {
      return value.toLowerCase().replace(/^\./, '');
    }),
    filenameRegex: String(row.filename_regex || '.*').trim(),
    sheetName: String(row.sheet_name || '').trim(),
    headerRow: positiveInteger_(row.header_row, 1),
    columnMap: parseJsonObject_(row.column_map_json, 'column_map_json'),
    transforms: parseJsonObject_(row.transforms_json || '{}', 'transforms_json')
  };
}

function buildRouteCatalog_(rows) {
  const catalog = {
    byAlias: {},
    byNetwork: {}
  };

  rows.forEach(function(row) {
    const actorId = String(row.actor_id || '*').trim() || '*';
    const route = String(row.route || '').trim();
    const alias = String(row.route_alias || route).trim();
    const mcc = normalizeNetworkCode_(row.mcc);
    const mnc = normalizeNetworkCode_(row.mnc);
    const entry = {
      actorId: actorId,
      alias: alias,
      country: String(row.country || '').trim(),
      route: route,
      mcc: mcc,
      mnc: mnc,
      operator: String(row.operator || '').trim()
    };

    if (alias) {
      catalog.byAlias[routeCatalogKey_(actorId, alias)] = entry;
    }
    if (route && normalizeTextKey_(route) !== normalizeTextKey_(alias)) {
      catalog.byAlias[routeCatalogKey_(actorId, route)] = entry;
    }
    if (mcc && mnc) {
      catalog.byNetwork[networkCatalogKey_(actorId, mcc, mnc)] = entry;
    }
  });

  return catalog;
}

function loadProcessedImportKeys_(spreadsheet) {
  const sheet = spreadsheet.getSheetByName(TARIFF_CONFIG.SHEETS.HISTORY);
  if (!sheet || sheet.getLastRow() < 2) {
    return {};
  }

  const headers = TARIFF_CONFIG.HEADERS.historial_importaciones;
  const values = sheet.getRange(2, 1, sheet.getLastRow() - 1, headers.length).getDisplayValues();
  const processed = {};
  values.forEach(function(row) {
    const item = rowToObject_(headers, row);
    if (TERMINAL_IMPORT_STATUSES.indexOf(String(item.status).trim().toUpperCase()) !== -1) {
      processed[importKey_(item.message_id, item.attachment_name, item.attachment_hash)] = true;
    }
  });
  return processed;
}

function upsertRates_(spreadsheet, records) {
  if (!records.length) {
    return 0;
  }

  const grouped = {
    costos: [],
    ventas: []
  };
  records.forEach(function(record) {
    grouped[record.datasetType].push(record);
  });

  return upsertRateSheet_(
    spreadsheet.getSheetByName(TARIFF_CONFIG.SHEETS.COSTS),
    grouped.costos
  ) + upsertRateSheet_(
    spreadsheet.getSheetByName(TARIFF_CONFIG.SHEETS.SALES),
    grouped.ventas
  );
}

function upsertRateSheet_(sheet, records) {
  if (!records.length) {
    return 0;
  }

  const headers = TARIFF_CONFIG.HEADERS.currentRates;
  const existingValues = sheet.getLastRow() > 1
    ? sheet.getRange(2, 1, sheet.getLastRow() - 1, headers.length).getValues()
    : [];
  const byKey = {};

  existingValues.forEach(function(row, index) {
    const item = rowToObject_(headers, row);
    if (item.record_key) {
      byKey[String(item.record_key)] = {
        index: index,
        messageAt: toTimestamp_(item.source_message_at)
      };
    }
  });

  let published = 0;
  records.forEach(function(record) {
    const existing = byKey[record.recordKey];
    const incomingTimestamp = record.sourceMessageAt.getTime();
    if (existing && incomingTimestamp < existing.messageAt) {
      return;
    }

    const row = rateRecordToRow_(record);
    if (existing) {
      existingValues[existing.index] = row;
      existing.messageAt = incomingTimestamp;
    } else {
      byKey[record.recordKey] = {
        index: existingValues.length,
        messageAt: incomingTimestamp
      };
      existingValues.push(row);
    }
    published += 1;
  });

  if (existingValues.length) {
    sheet.getRange(2, 1, existingValues.length, headers.length).setValues(existingValues);
  }
  return published;
}

function rateRecordToRow_(record) {
  return [
    record.recordKey,
    record.datasetType,
    record.actorId,
    record.actorName,
    record.country,
    record.route,
    record.mcc,
    record.mnc,
    record.operator,
    record.rate,
    record.currency,
    record.effectiveFrom || '',
    record.sourceMessageId,
    record.sourceMessageAt,
    record.sourceAttachment,
    record.attachmentHash,
    new Date()
  ];
}

function appendHistory_(spreadsheet, item) {
  spreadsheet.getSheetByName(TARIFF_CONFIG.SHEETS.HISTORY).appendRow([
    new Date(),
    item.messageId || '',
    item.messageAt || '',
    item.mailboxSide || '',
    item.actorId || '',
    item.actorName || '',
    item.datasetType || '',
    item.attachmentName || '',
    item.attachmentHash || '',
    item.status || '',
    item.rowsRead || 0,
    item.rowsPublished || 0,
    item.rowsRejected || 0,
    item.details || ''
  ]);
}

function appendErrors_(spreadsheet, errors) {
  if (!errors.length) {
    return;
  }

  const rows = errors.map(function(error) {
    return [
      new Date(),
      error.messageId || '',
      error.actorId || '',
      error.attachmentName || '',
      error.rowNumber || '',
      error.code || 'INVALID_ROW',
      error.message || '',
      safeJsonStringify_(error.raw || {})
    ];
  });

  spreadsheet.getSheetByName(TARIFF_CONFIG.SHEETS.ERRORS)
    .getRange(
      spreadsheet.getSheetByName(TARIFF_CONFIG.SHEETS.ERRORS).getLastRow() + 1,
      1,
      rows.length,
      TARIFF_CONFIG.HEADERS.errores.length
    )
    .setValues(rows);
}

function getCurrentRatesForApi_(spreadsheet, sheetName, actorField) {
  const sheet = spreadsheet.getSheetByName(sheetName);
  if (!sheet || sheet.getLastRow() < 2) {
    return [];
  }

  const headers = TARIFF_CONFIG.HEADERS.currentRates;
  return sheet.getRange(2, 1, sheet.getLastRow() - 1, headers.length)
    .getValues()
    .map(function(row) {
      const item = rowToObject_(headers, row);
      const rate = parseRate_(item.rate, {});
      const output = {
        Pais: item.country,
        Ruta: item.route,
        MCC: item.mcc,
        MNC: item.mnc,
        Operador: item.operator,
        Tarifa: Number.isFinite(rate) ? rate : 0,
        Moneda: item.currency
      };
      output[actorField] = item.actor_name;
      return output;
    });
}

function rowToObject_(headers, row) {
  const output = {};
  headers.forEach(function(header, index) {
    output[header] = row[index] === undefined ? '' : row[index];
  });
  return output;
}

function parseBoolean_(value) {
  return ['true', '1', 'yes', 'si', 's\u00ed', 'x'].indexOf(
    String(value || '').trim().toLowerCase()
  ) !== -1;
}

function splitList_(value) {
  return String(value || '')
    .split(/[,;\n]+/)
    .map(function(item) {
      return item.trim();
    })
    .filter(Boolean);
}

function parseJsonObject_(value, fieldName) {
  try {
    const parsed = JSON.parse(String(value || '{}'));
    if (!parsed || Array.isArray(parsed) || typeof parsed !== 'object') {
      throw new Error('JSON value must be an object.');
    }
    return parsed;
  } catch (error) {
    throw new Error('Invalid ' + fieldName + ': ' + error.message);
  }
}

function normalizeDatasetType_(value) {
  const normalized = normalizeTextKey_(value);
  if (['costos', 'costo', 'compras', 'compra'].indexOf(normalized) !== -1) {
    return 'costos';
  }
  if (['ventas', 'venta'].indexOf(normalized) !== -1) {
    return 'ventas';
  }
  return '';
}

function normalizeMailboxSide_(value) {
  const normalized = normalizeTextKey_(value);
  if (['received', 'recibido', 'recibidos', 'inbox'].indexOf(normalized) !== -1) {
    return 'received';
  }
  if (['sent', 'enviado', 'enviados'].indexOf(normalized) !== -1) {
    return 'sent';
  }
  if (normalized === 'both' || normalized === 'ambos') {
    return 'both';
  }
  return '';
}

function routeCatalogKey_(actorId, route) {
  return String(actorId || '*').trim() + '|' + normalizeTextKey_(route);
}

function networkCatalogKey_(actorId, mcc, mnc) {
  return String(actorId || '*').trim() + '|' + normalizeNetworkCode_(mcc) +
    '|' + normalizeNetworkCode_(mnc);
}

function importKey_(messageId, attachmentName, attachmentHash) {
  return [messageId, attachmentName, attachmentHash].join('|');
}

function toTimestamp_(value) {
  if (value instanceof Date) {
    return value.getTime();
  }
  const parsed = new Date(value).getTime();
  return Number.isFinite(parsed) ? parsed : 0;
}

function safeJsonStringify_(value) {
  try {
    const text = JSON.stringify(value);
    return text.length > 45000 ? text.slice(0, 45000) : text;
  } catch (error) {
    return '{"error":"Could not serialize row"}';
  }
}
