const TARIFF_CONFIG = Object.freeze({
  SPREADSHEET_PROPERTY: 'TARIFF_SPREADSHEET_ID',
  SCAN_DAYS_PROPERTY: 'TARIFF_SCAN_DAYS',
  MAX_THREADS_PROPERTY: 'TARIFF_MAX_THREADS',
  MAX_ATTACHMENTS_PROPERTY: 'TARIFF_MAX_ATTACHMENTS',
  DEFAULT_SCAN_DAYS: 30,
  DEFAULT_MAX_THREADS: 100,
  DEFAULT_MAX_ATTACHMENTS: 40,
  TRIGGER_HANDLER: 'scanTariffMailbox',
  SHEETS: Object.freeze({
    ACTORS: 'actores',
    TEMPLATES: 'plantillas',
    ROUTES: 'catalogo_rutas',
    COSTS: 'costos_actuales',
    SALES: 'ventas_actuales',
    HISTORY: 'historial_importaciones',
    ERRORS: 'errores'
  }),
  HEADERS: Object.freeze({
    actores: [
      'actor_id',
      'name',
      'dataset_type',
      'mailbox_side',
      'email_match',
      'parser_id',
      'default_currency',
      'enabled'
    ],
    plantillas: [
      'parser_id',
      'file_types',
      'filename_regex',
      'sheet_name',
      'header_row',
      'column_map_json',
      'transforms_json',
      'enabled'
    ],
    catalogo_rutas: [
      'actor_id',
      'route_alias',
      'country',
      'route',
      'mcc',
      'mnc',
      'operator',
      'enabled'
    ],
    currentRates: [
      'record_key',
      'dataset_type',
      'actor_id',
      'actor_name',
      'country',
      'route',
      'mcc',
      'mnc',
      'operator',
      'rate',
      'currency',
      'effective_from',
      'source_message_id',
      'source_message_at',
      'source_attachment',
      'attachment_hash',
      'updated_at'
    ],
    historial_importaciones: [
      'processed_at',
      'message_id',
      'message_at',
      'mailbox_side',
      'actor_id',
      'actor_name',
      'dataset_type',
      'attachment_name',
      'attachment_hash',
      'status',
      'rows_read',
      'rows_published',
      'rows_rejected',
      'details'
    ],
    errores: [
      'created_at',
      'message_id',
      'actor_id',
      'attachment_name',
      'row_number',
      'error_code',
      'error_message',
      'raw_json'
    ]
  })
});

const CANONICAL_COLUMNS = Object.freeze([
  'country',
  'route',
  'mcc',
  'mnc',
  'operator',
  'rate',
  'currency',
  'effective_from'
]);

const TERMINAL_IMPORT_STATUSES = Object.freeze([
  'SUCCESS',
  'PARTIAL',
  'REJECTED',
  'UNSUPPORTED',
  'ERROR'
]);
