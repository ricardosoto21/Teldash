import os, io, requests, pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time

# --- CONFIGURACIÓN ---
USUARIO = os.environ.get('SMS_USER')
CLAVE = os.environ.get('SMS_PASS')
DIAS_ATRAS = 365 
RUTA_EXCEL = 'datos/reporte_actual.xlsx'

# URLs del servidor
URL_INICIO = 'http://65.108.69.39:5660/'
URL_LOGIN = 'http://65.108.69.39:5660/Home/CheckLogin'
URL_DESCARGA = 'http://65.108.69.39:5660/DLRWholesaleReport/DownloadExcel'

session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})

cache_tasas = {}

def obtener_tasa_diaria(fecha_obj, moneda):
    """Consulta tasas reales: Frankfurter (EUR) y Mindicador (CLP)."""
    if not moneda or moneda == 'USD' or pd.isna(moneda): return 1.0
    
    fecha_str = fecha_obj.strftime('%Y-%m-%d')
    key = f"{fecha_str}_{moneda}"
    if key in cache_tasas: return cache_tasas[key]
    
    try:
        if moneda == 'EUR':
            url = f"https://api.frankfurter.app/{fecha_str}?from=EUR&to=USD"
            res = requests.get(url, timeout=5).json()
            tasa = res['rates']['USD']
        elif moneda == 'CLP':
            fecha_clp = fecha_obj.strftime('%d-%m-%Y')
            url = f"https://mindicador.cl/api/dolar/{fecha_clp}"
            res = requests.get(url, timeout=5).json()
            tasa = 1 / res['serie'][0]['valor']
        else:
            tasa = 1.0
        cache_tasas[key] = tasa
        return tasa
    except:
        fallbacks = {'EUR': 1.08, 'CLP': 0.0011}
        return fallbacks.get(moneda, 1.0)

def convertir_y_agrupar(df):
    """Convierte a USD y agrupa manteniendo TODAS las dimensiones clave."""
    if df.empty: return df
    
    # Aseguramos que existan las columnas de moneda originales
    if 'CurrencyCode' not in df.columns: df['CurrencyCode'] = 'USD'
    if 'TerminationCurrencyCode' not in df.columns: df['TerminationCurrencyCode'] = 'USD'

    print("   💱 Convirtiendo cobros y pagos a USD...", flush=True)
    
    def aplicar_cambio(row):
        fecha = pd.to_datetime(row['SubmitDate'])
        # Tasa para lo que tú cobras
        tasa_client = obtener_tasa_diaria(fecha, row['CurrencyCode'])
        # Tasa para lo que a ti te cobran
        tasa_vendor = obtener_tasa_diaria(fecha, row['TerminationCurrencyCode'])
        
        return pd.Series([
            row['ClientCost'] * tasa_client, 
            row['TerminationCost'] * tasa_vendor
        ])

    df[['ClientCostUSD', 'TerminationCostUSD']] = df.apply(aplicar_cambio, axis=1)
    
    # Agrupamos sin perder las columnas de moneda ni detalles operativos
    df['SubmitDate'] = pd.to_datetime(df['SubmitDate']).dt.date
    dimensiones = [
        'SubmitDate', 'CompanyName', 'SMPPAccountName', 'SMPPUsername', 
        'MCC', 'MNC', 'OperatorName', 'DLRStatus', 'ErrorDescription', 
        'VendorAccountName', 'SenderID', 'CountryRealName', 'CurrencyCode', 
        'TerminationCurrencyCode', 'SMSSource', 'SMSType', 'MessageType', 'ErrorCode'
    ]
    
    columnas_agrupar = [c for c in dimensiones if c in df.columns]
    
    return df.groupby(columnas_agrupar).agg({
        'MessageParts': 'sum',
        'ClientCost': 'sum',
        'TerminationCost': 'sum',
        'ClientCostUSD': 'sum',
        'TerminationCostUSD': 'sum'
    }).reset_index()

def login():
    print("⏳ Iniciando sesión...", flush=True)
    r = session.get(URL_INICIO)
    token = BeautifulSoup(r.text, 'html.parser').find('input', {'name': '__RequestVerificationToken'})['value']
    payload = {'Username': USUARIO, 'UserKey': CLAVE, 'RememberMe': 'true', '__RequestVerificationToken': token}
    session.post(URL_LOGIN, data=payload, headers={'RequestVerificationToken': token, 'X-Requested-With': 'XMLHttpRequest'})
    print("✅ Conexión establecida.", flush=True)

if __name__ == "__main__":
    if not os.path.exists('datos'): os.makedirs('datos')
    
    # IMPORTANTE: No cargamos el archivo anterior porque tiene errores de formato.
    # Empezaremos de cero para que toda la base de datos sea perfecta.
    login()
    
    all_data = []
    fecha_cursor = datetime.now() # Empezamos desde hoy para recuperar TODO febrero bien
    fecha_limite = fecha_cursor - timedelta(days=DIAS_ATRAS)
    
    print(f"🚀 Reconstruyendo base de datos ({DIAS_ATRAS} días)...", flush=True)

    while fecha_cursor > fecha_limite:
        f_fin = fecha_cursor
        f_ini = fecha_cursor - timedelta(days=6)
        
        ini_str = f_ini.strftime('%Y-%m-%d 00:00:00')
        fin_str = f_fin.strftime('%Y-%m-%d 23:59:59')
        
        print(f"📅 Rango: {f_ini.strftime('%Y-%m-%d')} al {f_fin.strftime('%Y-%m-%d')}... ", end="", flush=True)
        r = session.get(URL_DESCARGA, params={'StartDate': ini_str, 'EndDate': fin_str})
        
        if "PK" in r.text[:10]:
            df_semana = pd.read_excel(io.BytesIO(r.content))
            if not df_semana.empty:
                df_resumido = convertir_y_agrupar(df_semana)
                all_data.append(df_resumido)
                print(f" ✅ Ok.", flush=True)
            else:
                print(" ⚪ Sin datos.", flush=True)
        else:
            print(" ❌ Error servidor.", flush=True)
        
        fecha_cursor = f_ini - timedelta(seconds=1)
        time.sleep(1)

    if all_data:
        df_final = pd.concat(all_data, ignore_index=True)
        df_final.to_excel(RUTA_EXCEL, index=False)
        print(f"🏆 ¡PROCESO EXITOSO! Base de datos reconstruida con TODAS las columnas.")
