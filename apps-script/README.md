# Automatizacion de tarifas Telvoice

Este proyecto de Google Apps Script procesa adjuntos de Gmail y publica la API
`{ costos, ventas, meta }` que consume `Destinos.html`.

## Flujo

1. Revisa cada 5 minutos correos recibidos y enviados con adjuntos.
2. Identifica al proveedor o cliente por remitente/destinatario.
3. Convierte archivos `csv`, `xlsx` o `xls` a filas normalizadas.
4. Completa rutas con el catalogo cuando el archivo no incluye todos los datos.
5. Actualiza la tarifa vigente con la regla "ultimo correo gana".
6. Registra cada importacion y cada fila rechazada.

Los adjuntos Word no se publican automaticamente. Quedan registrados en
`errores` con estado `UNSUPPORTED`.

Los mensajes que no coinciden con un actor habilitado se ignoran. Esto evita
que adjuntos personales, facturas u otros documentos de la cuenta entren al
proceso de tarifas.

## Instalacion

1. Crear un proyecto independiente en Google Apps Script.
2. Copiar el contenido de esta carpeta o desplegarlo con `clasp`.
3. Confirmar que el servicio avanzado de Google Drive esta habilitado.
4. Ejecutar `createTariffDatabase()` desde el editor.
5. Autorizar Gmail, Drive, Sheets y los triggers cuando Google lo solicite.
6. Abrir la URL que devuelve la ejecucion y configurar las hojas.
7. Ejecutar `runTariffUnitTests()` y luego `scanTariffMailbox()`.
8. Desplegar como aplicacion web, ejecutando como propietario y con acceso
   publico.
9. Poner la URL `/exec` del despliegue en `js/config.js`.

Para usar una hoja existente, ejecutar:

```javascript
initializeTariffAutomation('SPREADSHEET_ID');
```

## Despliegue con clasp

Instalar y autenticar `clasp`, copiar `.clasp.json.example` a `.clasp.json`,
reemplazar el `scriptId` y ejecutar:

```powershell
clasp push
```

No versionar `.clasp.json`, ya que identifica el proyecto real.

El servicio nativo `GmailApp` solicita el scope completo
`https://mail.google.com/`. La implementacion no modifica correos ni etiquetas:
solo busca mensajes y lee sus adjuntos, pero Google exige ese scope para estos
metodos del servicio Apps Script.

## Hojas de configuracion

### actores

Cada fila identifica un proveedor o cliente.

- `actor_id`: identificador estable, sin reutilizar.
- `name`: nombre que se muestra en la pagina.
- `dataset_type`: `costos` para proveedores o `ventas` para clientes.
- `mailbox_side`: `received`, `sent` o `both`.
- `email_match`: uno o varios correos separados por coma. Tambien admite
  dominios como `@proveedor.com`.
- `parser_id`: referencia a una fila habilitada de `plantillas`.
- `default_currency`: moneda usada cuando el archivo no trae una.
- `enabled`: `TRUE` para activar.

### plantillas

Cada formato distinto debe tener su propio `parser_id`.

- `file_types`: `csv,xlsx,xls`.
- `filename_regex`: expresion regular para validar o distinguir adjuntos.
- `sheet_name`: hoja del Excel; vacio usa la primera.
- `header_row`: numero de fila, comenzando en 1.
- `column_map_json`: aliases de columnas hacia el esquema canonico.
- `transforms_json`: defaults, encoding, delimitador, separadores,
  multiplicador y prefijo/sufijo.

Ejemplo de `column_map_json`:

```json
{
  "country": ["Country", "Pais"],
  "route": ["Route", "Destination"],
  "mcc": ["MCC"],
  "mnc": ["MNC"],
  "operator": ["Operator", "Network"],
  "rate": ["Rate", "Price"],
  "currency": ["Currency"],
  "effective_from": ["Effective Date"]
}
```

Ejemplo de `transforms_json`:

```json
{
  "defaults": {"currency": "USD"},
  "encoding": "UTF-8",
  "delimiter": ";",
  "decimal_separator": ",",
  "thousands_separator": ".",
  "rate_multiplier": 1
}
```

### catalogo_rutas

Permite completar nombres de pais, ruta, MCC, MNC y operador. `actor_id` puede
ser un actor concreto o `*` para compartir una equivalencia global.

### resultados y auditoria

- `costos_actuales`: snapshot publicado de proveedores.
- `ventas_actuales`: snapshot publicado de clientes.
- `historial_importaciones`: deduplicacion y auditoria por adjunto.
- `errores`: errores de archivo o filas rechazadas.

## Operacion

- `scanTariffMailbox()`: ejecucion normal del proceso.
- `reprocessTariffMessage(messageId, mailboxSide)`: fuerza un correo ya visto.
- `setTariffRuntimeOptions(30, 100, 40)`: dias, hilos y adjuntos maximos por
  ejecucion.
- `installFiveMinuteTrigger()`: reinstala el trigger.
- `removeTariffTriggers()`: elimina triggers del procesador.
