function parseAttachment_(attachment, template) {
  const extension = getFileExtension_(attachment.getName());
  if (template.fileTypes.length && template.fileTypes.indexOf(extension) === -1) {
    throw tariffError_(
      'UNSUPPORTED_FILE_TYPE',
      'File type .' + extension + ' is not enabled for parser ' + template.id + '.'
    );
  }

  if (!filenameMatches_(attachment.getName(), template.filenameRegex)) {
    throw tariffError_(
      'FILENAME_MISMATCH',
      'Attachment name does not match parser pattern ' + template.filenameRegex + '.'
    );
  }

  let table;
  if (extension === 'csv') {
    table = parseCsvAttachment_(attachment, template);
  } else if (extension === 'xlsx' || extension === 'xls') {
    table = parseExcelAttachment_(attachment, template);
  } else {
    throw tariffError_(
      'UNSUPPORTED_FILE_TYPE',
      'Only CSV, XLSX and XLS attachments are supported automatically.'
    );
  }

  return tableToObjects_(table, template.headerRow);
}

function parseCsvAttachment_(attachment, template) {
  const transforms = template.transforms || {};
  const encoding = String(transforms.encoding || 'UTF-8');
  const text = attachment.getDataAsString(encoding).replace(/^\uFEFF/, '');
  const firstLine = text.split(/\r?\n/, 1)[0] || '';
  const delimiter = transforms.delimiter
    ? String(transforms.delimiter).replace('\\t', '\t')
    : detectDelimiter_(firstLine);
  return Utilities.parseCsv(text, delimiter);
}

function parseExcelAttachment_(attachment, template) {
  let convertedFileId = '';
  try {
    const resource = {
      name: 'tmp_tariff_' + new Date().getTime() + '_' + attachment.getName(),
      mimeType: MimeType.GOOGLE_SHEETS
    };
    const converted = Drive.Files.create(resource, attachment.copyBlob(), {
      fields: 'id'
    });
    convertedFileId = converted.id;

    const spreadsheet = SpreadsheetApp.openById(convertedFileId);
    const sheet = template.sheetName
      ? spreadsheet.getSheetByName(template.sheetName)
      : spreadsheet.getSheets()[0];
    if (!sheet) {
      throw tariffError_(
        'SHEET_NOT_FOUND',
        'Sheet "' + template.sheetName + '" was not found in ' + attachment.getName() + '.'
      );
    }
    return sheet.getDataRange().getDisplayValues();
  } catch (error) {
    if (error.code) {
      throw error;
    }
    throw tariffError_(
      'EXCEL_CONVERSION_FAILED',
      'Could not convert Excel attachment: ' + error.message
    );
  } finally {
    if (convertedFileId) {
      try {
        DriveApp.getFileById(convertedFileId).setTrashed(true);
      } catch (cleanupError) {
        console.warn('Could not trash temporary converted file: ' + cleanupError.message);
      }
    }
  }
}

function tableToObjects_(table, headerRow) {
  if (!table || table.length < headerRow) {
    throw tariffError_('EMPTY_FILE', 'The attachment has no usable header row.');
  }

  const headers = table[headerRow - 1].map(function(header) {
    return String(header || '').trim();
  });
  const normalizedHeaders = headers.map(normalizeTextKey_);
  if (!normalizedHeaders.some(Boolean)) {
    throw tariffError_('EMPTY_HEADER', 'The configured header row is empty.');
  }

  return table.slice(headerRow)
    .map(function(row, index) {
      const values = {};
      headers.forEach(function(header, columnIndex) {
        values[header] = row[columnIndex] === undefined ? '' : row[columnIndex];
      });
      return {
        rowNumber: headerRow + index + 1,
        values: values,
        normalizedHeaders: normalizedHeaders,
        headers: headers
      };
    })
    .filter(function(row) {
      return Object.keys(row.values).some(function(key) {
        return String(row.values[key] || '').trim() !== '';
      });
    });
}

function normalizeParsedRows_(parsedRows, context) {
  const valid = [];
  const errors = [];

  parsedRows.forEach(function(parsedRow) {
    try {
      valid.push(normalizeParsedRow_(parsedRow, context));
    } catch (error) {
      errors.push({
        messageId: context.messageId,
        actorId: context.actor.id,
        attachmentName: context.attachmentName,
        rowNumber: parsedRow.rowNumber,
        code: error.code || 'INVALID_ROW',
        message: error.message,
        raw: parsedRow.values
      });
    }
  });

  return {
    valid: deduplicateIncomingRecords_(valid),
    errors: errors
  };
}

function normalizeParsedRow_(parsedRow, context) {
  const template = context.template;
  const transforms = template.transforms || {};
  const defaults = transforms.defaults || {};
  const raw = {};

  CANONICAL_COLUMNS.forEach(function(canonical) {
    const mappedValue = extractMappedValue_(
      parsedRow,
      template.columnMap[canonical]
    );
    raw[canonical] = mappedValue === '' || mappedValue === null ||
      mappedValue === undefined
      ? (defaults[canonical] === undefined ? '' : defaults[canonical])
      : mappedValue;
  });

  raw.route = applyRouteTransforms_(raw.route, transforms);
  raw.currency = normalizeCurrency_(raw.currency || context.actor.defaultCurrency);
  raw.mcc = normalizeNetworkCode_(raw.mcc);
  raw.mnc = normalizeNetworkCode_(raw.mnc);

  const catalogEntry = resolveRouteCatalogEntry_(
    context.routeCatalog,
    context.actor.id,
    raw.route,
    raw.mcc,
    raw.mnc
  );
  if (catalogEntry) {
    raw.country = catalogEntry.country || raw.country;
    raw.route = catalogEntry.route || raw.route;
    raw.mcc = catalogEntry.mcc || raw.mcc;
    raw.mnc = catalogEntry.mnc || raw.mnc;
    raw.operator = catalogEntry.operator || raw.operator;
  }

  const rate = parseRate_(raw.rate, transforms);
  const missing = [];
  ['country', 'route', 'mcc', 'mnc'].forEach(function(field) {
    if (!String(raw[field] || '').trim()) {
      missing.push(field);
    }
  });
  if (!Number.isFinite(rate)) {
    missing.push('rate');
  }
  if (missing.length) {
    throw tariffError_(
      'MISSING_REQUIRED_FIELDS',
      'Missing or invalid fields: ' + missing.join(', ') + '.'
    );
  }

  const recordKey = [
    context.actor.datasetType,
    context.actor.id,
    normalizeTextKey_(raw.route),
    raw.mcc,
    raw.mnc
  ].join('|');

  return {
    recordKey: recordKey,
    datasetType: context.actor.datasetType,
    actorId: context.actor.id,
    actorName: context.actor.name,
    country: String(raw.country).trim(),
    route: String(raw.route).trim(),
    mcc: raw.mcc,
    mnc: raw.mnc,
    operator: String(raw.operator || '').trim(),
    rate: rate,
    currency: raw.currency,
    effectiveFrom: normalizeDateValue_(raw.effective_from),
    sourceMessageId: context.messageId,
    sourceMessageAt: context.messageAt,
    sourceAttachment: context.attachmentName,
    attachmentHash: context.attachmentHash
  };
}

function extractMappedValue_(parsedRow, aliases) {
  if (aliases === undefined || aliases === null || aliases === '') {
    return '';
  }

  const aliasList = Array.isArray(aliases) ? aliases : [aliases];
  const normalizedToActual = {};
  parsedRow.headers.forEach(function(header) {
    normalizedToActual[normalizeTextKey_(header)] = header;
  });

  for (let index = 0; index < aliasList.length; index += 1) {
    const actualHeader = normalizedToActual[normalizeTextKey_(aliasList[index])];
    if (actualHeader !== undefined) {
      return parsedRow.values[actualHeader];
    }
  }
  return '';
}

function parseRate_(value, transforms) {
  let text = String(value === null || value === undefined ? '' : value).trim();
  if (!text) {
    return NaN;
  }

  text = text.replace(/[^\d,.\-]/g, '');
  const decimalSeparator = transforms.decimal_separator;
  const thousandsSeparator = transforms.thousands_separator;

  if (thousandsSeparator) {
    text = text.split(String(thousandsSeparator)).join('');
  }
  if (decimalSeparator) {
    text = text.split(String(decimalSeparator)).join('.');
  } else if (text.indexOf(',') !== -1 && text.indexOf('.') === -1) {
    text = text.replace(',', '.');
  } else if (text.indexOf(',') !== -1 && text.indexOf('.') !== -1) {
    if (text.lastIndexOf(',') > text.lastIndexOf('.')) {
      text = text.replace(/\./g, '').replace(',', '.');
    } else {
      text = text.replace(/,/g, '');
    }
  }

  const parsed = parseFloat(text);
  const multiplier = Number(transforms.rate_multiplier === undefined
    ? 1
    : transforms.rate_multiplier);
  return Number.isFinite(parsed) && Number.isFinite(multiplier)
    ? parsed * multiplier
    : NaN;
}

function resolveRouteCatalogEntry_(catalog, actorId, route, mcc, mnc) {
  if (route) {
    const actorMatch = catalog.byAlias[routeCatalogKey_(actorId, route)];
    const globalMatch = catalog.byAlias[routeCatalogKey_('*', route)];
    if (actorMatch || globalMatch) {
      return actorMatch || globalMatch;
    }
  }

  if (mcc && mnc) {
    const actorNetwork = catalog.byNetwork[networkCatalogKey_(actorId, mcc, mnc)];
    const globalNetwork = catalog.byNetwork[networkCatalogKey_('*', mcc, mnc)];
    return actorNetwork || globalNetwork || null;
  }
  return null;
}

function deduplicateIncomingRecords_(records) {
  const byKey = {};
  records.forEach(function(record) {
    byKey[record.recordKey] = record;
  });
  return Object.keys(byKey).map(function(key) {
    return byKey[key];
  });
}

function applyRouteTransforms_(route, transforms) {
  let value = String(route || '').trim();
  if (transforms.route_prefix) {
    value = String(transforms.route_prefix) + value;
  }
  if (transforms.route_suffix) {
    value += String(transforms.route_suffix);
  }
  return value.trim();
}

function normalizeCurrency_(value) {
  const normalized = String(value || 'USD').trim().toUpperCase();
  if (normalized === '$' || normalized === 'US$' || normalized === 'DOLAR' ||
      normalized === 'DOLLAR') {
    return 'USD';
  }
  if (normalized === '\u20ac' || normalized === 'EURO') {
    return 'EUR';
  }
  return normalized || 'USD';
}

function normalizeNetworkCode_(value) {
  return String(value === null || value === undefined ? '' : value)
    .trim()
    .replace(/\.0+$/, '');
}

function normalizeDateValue_(value) {
  if (!value) {
    return '';
  }
  if (value instanceof Date) {
    return Utilities.formatDate(value, Session.getScriptTimeZone(), 'yyyy-MM-dd');
  }
  return String(value).trim();
}

function normalizeTextKey_(value) {
  return String(value === null || value === undefined ? '' : value)
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function detectDelimiter_(firstLine) {
  const delimiters = [',', ';', '\t', '|'];
  let selected = ',';
  let maxCount = -1;
  delimiters.forEach(function(delimiter) {
    const count = firstLine.split(delimiter).length - 1;
    if (count > maxCount) {
      selected = delimiter;
      maxCount = count;
    }
  });
  return selected;
}

function filenameMatches_(filename, regexText) {
  try {
    return new RegExp(regexText || '.*', 'i').test(filename);
  } catch (error) {
    throw tariffError_('INVALID_FILENAME_REGEX', error.message);
  }
}

function getFileExtension_(filename) {
  const parts = String(filename || '').toLowerCase().split('.');
  return parts.length > 1 ? parts.pop() : '';
}

function tariffError_(code, message) {
  const error = new Error(message);
  error.code = code;
  return error;
}
