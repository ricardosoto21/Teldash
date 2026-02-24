import os, io, requests, pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# --- CONFIGURACIÓN ---
USUARIO = os.environ.get('SMS_USER')
CLAVE = os.environ.get('SMS_PASS')
DIAS_ATRAS = 730  # 2 años aprox (365 * 2)

url_inicio = 'http://65.108.69.39:5660/'
url_login = 'http://65.108.69.39:5660/Home/CheckLogin'
url_descarga = 'http://65.108.69.39:5660/DLRWholesaleReport/DownloadExcel'

session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})

def login():
    print("⏳ Iniciando sesión...")
    r = session.get(url_inicio)
    soup = BeautifulSoup(r.text, 'html.parser')
    token = soup.find('input', {'name': '__RequestVerificationToken'})['value']
    payload = {'Username': USUARIO, 'UserKey': CLAVE, 'RememberMe': 'true', '__RequestVerificationToken': token}
    h = {'RequestVerificationToken': token, 'X-Requested-With': 'XMLHttpRequest', 'Referer': url_inicio}
    session.post(url_login, data=payload, headers=h)
    print("✅ Sesión establecida.")

def descargar_y_resumir(f_inicio, f_fin):
    params = {
        'StartDate': f_inicio, 'EndDate': f_fin,
        'SenderID': '', 'DLRStatus': '', 'PhoneNumber': '', 'SMSID': '',
        'VendorSMSID': '', 'CountryID': '', 'VendorAccountID': '',
        'CustomerSMPPAccountID': '', 'ErrorDescription': '', 'MCC': '',
        'MNC': '', 'ExcludeCountryID': '', 'ExcludeCustomerSMPPAccountID': '',
        'CustomerId': ''
    }
    r = session.get(url_descarga, params=params)
    
    if "PK" not in r.text[:10]:
        return pd.DataFrame()

    try:
        df = pd.read_excel(io.BytesIO(r.content))
        if df.empty: return pd.DataFrame()

        df['SubmitDate'] = pd.to_datetime(df['SubmitDate']).dt.date
        
        resumen = df.groupby(['SubmitDate', 'CompanyName', 'CountryRealName', 'DLRStatus']).agg({
            'MessageParts': 'sum',
            'ClientCost': 'sum',
            'TerminationCost': 'sum'
        }).reset_index()
        
        return resumen
    except Exception as e:
        print(f"⚠️ Error en periodo {f_inicio}: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    if not os.path.exists('datos'): os.makedirs('datos')
    login()
    
    # Empezamos desde el 31 de diciembre de 2025 para saltarnos febrero y enero que ya tienes/procesaste
    fecha_actual_proceso = datetime(2025, 12, 31, 23, 59, 59)
    fecha_limite = fecha_actual_proceso - timedelta(days=DIAS_ATRAS)
    
    all_data = []

    # Intentamos mantener lo que ya tienes de Febrero 2026
    try:
        df_actual = pd.read_excel('datos/reporte_actual.xlsx')
        all_data.append(df_actual)
        print("📁 Manteniendo datos de Febrero 2026 encontrados en el archivo.")
    except:
        print("info: Creando archivo desde cero.")

    print(f"🚀 Iniciando saltos de 7 días hacia el pasado...")

    while fecha_actual_proceso > fecha_limite:
        f_fin = fecha_actual_proceso
        f_ini = fecha_actual_proceso - timedelta(days=6) # 7 días contando el actual
        
        # Ajustar para que no pida hora exacta de inicio si no es necesario, pero mantenemos el formato
        ini_str = f_ini.replace(hour=0, minute=0, second=0).strftime('%Y-%m-%d %H:%M:%S')
        fin_str = f_fin.strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"📅 Rango: {ini_str} al {f_fin.strftime('%Y-%m-%d')}")
        
        df_semana = descargar_y_resumir(ini_str, fin_str)
        
        if not df_semana.empty:
            all_data.append(df_semana)
            print(f"   ✅ {len(df_semana)} filas agregadas.")
        
        # Movemos el cursor al día anterior al bloque procesado
        fecha_actual_proceso = f_ini - timedelta(seconds=1)

    if all_data:
        df_final = pd.concat(all_data, ignore_index=True)
        df_final = df_final.drop_duplicates()
        df_final.to_excel('datos/reporte_actual.xlsx', index=False)
        print(f"\n🏆 ¡PROCESO COMPLETADO! Histórico de 2 años resumido y guardado.")
    else:
        print("\n❌ No se pudo recuperar información.")
