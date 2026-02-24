import os
import io
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta

# 1. Configuración Segura de Credenciales (Vienen de GitHub Secrets)
USUARIO = os.environ.get('SMS_USER')
CLAVE = os.environ.get('SMS_PASS')

if not USUARIO or not CLAVE:
    print("❌ Error: Faltan las credenciales en las variables de entorno.")
    exit(1)

# URLs del sistema
url_inicio = 'http://65.108.69.39:5660/'
url_login = 'http://65.108.69.39:5660/Home/CheckLogin'
url_descarga = 'http://65.108.69.39:5660/DLRWholesaleReport/DownloadExcel'

# Ruta del archivo histórico en tu repositorio
ruta_archivo_local = 'datos/reporte_actual.xlsx'

# Iniciamos la sesión
session = requests.Session()
print("🤖 Iniciando proceso de actualización automática...")

try:
    # --- PASO 1: LOGIN NINJA ---
    print("⏳ Entrando al sistema y obteniendo token de seguridad...")
    respuesta_inicio = session.get(url_inicio)
    soup = BeautifulSoup(respuesta_inicio.text, 'html.parser')
    
    token_input = soup.find('input', {'name': '__RequestVerificationToken'})
    if not token_input:
        raise Exception("No se pudo obtener el token de seguridad inicial.")
        
    token_secreto = token_input['value']
    
    datos_login = {
        'Username': USUARIO,
        'UserKey': CLAVE,
        'RememberMe': 'true',
        '__RequestVerificationToken': token_secreto
    }
    
    cabeceras = {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'X-Requested-With': 'XMLHttpRequest',
        'RequestVerificationToken': token_secreto
    }
    
    respuesta_login = session.post(url_login, data=datos_login, headers=cabeceras)
    if respuesta_login.status_code != 200:
        raise Exception(f"Fallo el Login. Código HTTP: {respuesta_login.status_code}")
    
    print("✅ ¡Login exitoso!")

    # --- PASO 2: CALCULAR FECHA DE AYER ---
    # Calculamos el día de ayer
    ayer = datetime.now() - timedelta(days=1)
    fecha_inicio = ayer.strftime('%Y-%m-%d 00:00:00')
    fecha_fin = ayer.strftime('%Y-%m-%d 23:59:59')
    print(f"📅 Solicitando reporte del día: {ayer.strftime('%Y-%m-%d')}")

    # --- PASO 3: DESCARGAR EL REPORTE ---
    parametros_descarga = {
        'StartDate': fecha_inicio,
        'EndDate': fecha_fin,
        'SenderID': '', 'DLRStatus': '', 'PhoneNumber': '', 'SMSID': '',
        'VendorSMSID': '', 'CountryID': '', 'VendorAccountID': '',
        'CustomerSMPPAccountID': '', 'ErrorDescription': '', 'MCC': '',
        'MNC': '', 'ExcludeCountryID': '', 'ExcludeCustomerSMPPAccountID': '',
        'CustomerId': ''
    }
    
    print("📥 Descargando Excel desde el servidor...")
    respuesta_excel = session.get(url_descarga, params=parametros_descarga)
    
    if respuesta_excel.status_code != 200 or len(respuesta_excel.content) < 1000:
        raise Exception("El archivo descargado está vacío o dio error.")

    # --- PASO 4: PROCESAR Y UNIR LOS DATOS CON PANDAS ---
    print("⚙️ Uniendo datos nuevos con el historial...")
    
    # Leemos el Excel descargado (desde la memoria, sin guardarlo en disco aún)
    df_nuevos = pd.read_excel(io.BytesIO(respuesta_excel.content))
    
    if df_nuevos.empty:
        print("⚠️ El reporte de ayer no contiene datos (0 SMS). No hay nada que actualizar.")
        exit(0)

    # Leemos el archivo actual (si existe)
    try:
        df_actual = pd.read_excel(ruta_archivo_local, sheet_name=0) # Lee la primera hoja siempre
    except FileNotFoundError:
        print("⚠️ No se encontró archivo local previo. Creando uno nuevo.")
        df_actual = pd.DataFrame()

    # Unimos ambos DataFrames
    df_final = pd.concat([df_actual, df_nuevos], ignore_index=True)
    
    # Opcional pero recomendado: Eliminar filas duplicadas exactas
    df_final = df_final.drop_duplicates()

    # --- PASO 5: GUARDAR EL ARCHIVO FINAL ---
    print("💾 Guardando el archivo actualizado...")
    with pd.ExcelWriter(ruta_archivo_local, engine='openpyxl') as writer:
        df_final.to_excel(writer, sheet_name='DLRSheet', index=False)
        
    print("🚀 ¡Proceso finalizado con éxito! El archivo está listo para el dashboard.")

except Exception as e:
    print(f"❌ ERROR CRÍTICO: {e}")
    exit(1) # Le avisa a GitHub Actions que el script falló
