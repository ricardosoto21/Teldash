import os, io, requests, pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# --- CONFIGURACIÓN ---
USUARIO = os.environ.get('SMS_USER')
CLAVE = os.environ.get('SMS_PASS')
ruta_excel = 'datos/reporte_actual.xlsx'

url_inicio = 'http://65.108.69.39:5660/'
url_login = 'http://65.108.69.39:5660/Home/CheckLogin'
url_descarga = 'http://65.108.69.39:5660/DLRWholesaleReport/DownloadExcel'

# Disfraz de navegador para evitar bloqueos
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
})

def login():
    print("⏳ Entrando al sistema...", flush=True)
    r = session.get(url_inicio)
    token = BeautifulSoup(r.text, 'html.parser').find('input', {'name': '__RequestVerificationToken'})['value']
    
    payload = {
        'Username': USUARIO, 
        'UserKey': CLAVE, 
        'RememberMe': 'true', 
        '__RequestVerificationToken': token
    }
    
    headers_login = {
        'X-Requested-With': 'XMLHttpRequest',
        'RequestVerificationToken': token,
        'Referer': url_inicio
    }
    
    res = session.post(url_login, data=payload, headers=headers_login)
    if "UserID" in res.text and "10" in res.text: # Ajustado según tu respuesta exitosa previa
        print("✅ Login exitoso!", flush=True)
    else:
        print(f"⚠️ Aviso login: {res.text[:100]}", flush=True)

def ejecutar_actualizacion():
    if not os.path.exists('datos'): os.makedirs('datos')
    
    login()
    
    # 1. Definir rango de AYER
    ayer = datetime.now() - timedelta(days=1)
    f_inicio = ayer.strftime('%Y-%m-%d 00:00:00')
    f_fin = ayer.strftime('%Y-%m-%d 23:59:59')
    print(f"📅 Procesando fecha: {ayer.strftime('%Y-%m-%d')}", flush=True)
    
    # 2. Descargar datos
    params = {
        'StartDate': f_inicio, 'EndDate': f_fin,
        'SenderID': '', 'DLRStatus': '', 'PhoneNumber': '', 'SMSID': '',
        'VendorSMSID': '', 'CountryID': '', 'VendorAccountID': '',
        'CustomerSMPPAccountID': '', 'ErrorDescription': '', 'MCC': '',
        'MNC': '', 'ExcludeCountryID': '', 'ExcludeCustomerSMPPAccountID': '',
        'CustomerId': ''
    }
    
    print("📥 Descargando reporte diario...", flush=True)
    r = session.get(url_descarga, params=params)
    
    if "PK" not in r.text[:10]:
        print("❌ El servidor no entregó un Excel válido. Fin del proceso.", flush=True)
        return

    # 3. Procesar y RESUMIR datos nuevos
    df_nuevos_crudos = pd.read_excel(io.BytesIO(r.content))
    if df_nuevos_crudos.empty:
        print("⚪ No hubo tráfico ayer. Nada que agregar.", flush=True)
        return
    
    print(f"⚙️ Resumiendo {len(df_nuevos_crudos)} filas nuevas...", flush=True)
    df_nuevos_crudos['SubmitDate'] = pd.to_datetime(df_nuevos_crudos['SubmitDate']).dt.date
    
    df_resumido = df_nuevos_crudos.groupby(['SubmitDate', 'CompanyName', 'CountryRealName', 'DLRStatus']).agg({
        'MessageParts': 'sum',
        'ClientCost': 'sum',
        'TerminationCost': 'sum'
    }).reset_index()

    # 4. Unir con el histórico de GitHub
    try:
        df_historico = pd.read_excel(ruta_excel)
        print(f"📚 Histórico cargado ({len(df_historico)} filas).", flush=True)
        df_final = pd.concat([df_historico, df_resumido], ignore_index=True)
    except FileNotFoundError:
        print("🆕 No se encontró histórico, creando archivo nuevo.", flush=True)
        df_final = df_resumido

    # 5. Limpiar duplicados y guardar
    df_final = df_final.drop_duplicates()
    df_final.to_excel(ruta_excel, index=False)
    print(f"🚀 ¡Éxito! Archivo actualizado. Total filas: {len(df_final)}", flush=True)

if __name__ == "__main__":
    ejecutar_actualizacion()
