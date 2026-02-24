import os
import io
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta

# 1. Credenciales Secretas
USUARIO = os.environ.get('SMS_USER')
CLAVE = os.environ.get('SMS_PASS')

if not USUARIO or not CLAVE:
    print("❌ Error: Faltan las credenciales en GitHub Secrets.")
    exit(1)

url_inicio = 'http://65.108.69.39:5660/'
url_login = 'http://65.108.69.39:5660/Home/CheckLogin'
url_descarga = 'http://65.108.69.39:5660/DLRWholesaleReport/DownloadExcel'
ruta_archivo_local = 'datos/reporte_actual.xlsx'

# Iniciamos sesión y LE PONEMOS EL DISFRAZ DE CHROME 🥸
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept-Language': 'es-ES,es;q=0.9',
    'Connection': 'keep-alive'
})

print("🤖 Iniciando proceso con disfraz de navegador...")

try:
    # --- PASO 1: ROBAR EL TOKEN ---
    print("⏳ Entrando a la web principal para obtener token...")
    respuesta_inicio = session.get(url_inicio)
    soup = BeautifulSoup(respuesta_inicio.text, 'html.parser')
    
    token_input = soup.find('input', {'name': '__RequestVerificationToken'})
    if not token_input:
        raise Exception("No se encontró el token de seguridad. ¿La web está caída?")
        
    token_secreto = token_input['value']
    
    # --- PASO 2: LOGIN ---
    datos_login = {
        'Username': USUARIO,
        'UserKey': CLAVE,
        'RememberMe': 'true',
        '__RequestVerificationToken': token_secreto
    }
    
    cabeceras_login = {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'X-Requested-With': 'XMLHttpRequest',
        'RequestVerificationToken': token_secreto,
        'Origin': 'http://65.108.69.39:5660',
        'Referer': 'http://65.108.69.39:5660/'
    }
    
    print("🔑 Enviando credenciales...")
    respuesta_login = session.post(url_login, data=datos_login, headers=cabeceras_login)
    
    # Imprimimos lo que dice el servidor para estar 100% seguros de que nos dejó pasar
    print("🔍 Servidor dice sobre el login:", respuesta_login.text[:100])
    
    if "true" not in respuesta_login.text.lower() and "success" not in respuesta_login.text.lower():
        if respuesta_login.status_code != 200 and respuesta_login.status_code != 302:
            raise Exception("El login parece haber fallado. Revisa credenciales.")

    # --- PASO 3: CALCULAR FECHA ---
    ayer = datetime.now() - timedelta(days=1)
    fecha_inicio = ayer.strftime('%Y-%m-%d 00:00:00')
    fecha_fin = ayer.strftime('%Y-%m-%d 23:59:59')
    print(f"📅 Solicitando reporte del día: {ayer.strftime('%Y-%m-%d')}")

    # --- PASO 4: DESCARGAR ---
    parametros_descarga = {
        'StartDate': fecha_inicio,
        'EndDate': fecha_fin,
        'SenderID': '', 'DLRStatus': '', 'PhoneNumber': '', 'SMSID': '',
        'VendorSMSID': '', 'CountryID': '', 'VendorAccountID': '',
        'CustomerSMPPAccountID': '', 'ErrorDescription': '', 'MCC': '',
        'MNC': '', 'ExcludeCountryID': '', 'ExcludeCustomerSMPPAccountID': '',
        'CustomerId': ''
    }
    
    print("📥 Descargando Excel...")
    respuesta_excel = session.get(url_descarga, params=parametros_descarga)
    
    print("🔍 Espiando descarga:", respuesta_excel.text[:150])
    
    if "Log In | aSMSC" in respuesta_excel.text:
        raise Exception("El servidor nos devolvió a la pantalla de Login. Autenticación fallida.")

    # --- PASO 5: PANDAS ---
    print("⚙️ Uniendo datos...")
    # Si es HTML disfrazado usamos read_html, si es Excel read_excel
    try:
        df_nuevos = pd.read_excel(io.BytesIO(respuesta_excel.content))
    except ValueError:
        print("⚠️ Formato Excel nativo falló, intentando leer como tabla web...")
        df_nuevos = pd.read_html(io.StringIO(respuesta_excel.text))[0]

    if df_nuevos.empty:
        print("⚠️ El reporte de ayer no contiene datos. Fin del proceso.")
        exit(0)

    try:
        df_actual = pd.read_excel(ruta_archivo_local, sheet_name=0)
    except FileNotFoundError:
        df_actual = pd.DataFrame()

    df_final = pd.concat([df_actual, df_nuevos], ignore_index=True)
    df_final = df_final.drop_duplicates()

    print("💾 Guardando el archivo...")
    with pd.ExcelWriter(ruta_archivo_local, engine='openpyxl') as writer:
        df_final.to_excel(writer, sheet_name='DLRSheet', index=False)
        
    print("🚀 ¡Éxito total! Datos listos.")

except Exception as e:
    print(f"❌ ERROR CRÍTICO: {e}")
    exit(1)
