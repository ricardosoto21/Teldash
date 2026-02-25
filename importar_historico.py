import os, io, requests, pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time

# --- CONFIGURACIÓN ---
USUARIO = os.environ.get('SMS_USER')
CLAVE = os.environ.get('SMS_PASS')
DIAS_ATRAS = 365 
RUTA_EXCEL = 'datos/reporte_actual.xlsx'

# URLs
URL_INICIO = 'http://65.108.69.39:5660/'
URL_LOGIN = 'http://65.108.69.39:5660/Home/CheckLogin'
URL_DESCARGA = 'http://65.108.69.39:5660/DLRWholesaleReport/DownloadExcel'

session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})

cache_tasas = {}

def obtener_tasa_diaria(fecha_obj, moneda):
    if not moneda or moneda == 'USD' or pd.isna(moneda): return 1.0
    fecha_str = fecha_obj.strftime('%Y-%m-%d')
    key = f"{fecha_str}_{moneda}"
    if key in cache_tasas: return cache_tasas[key]
    
    try:
        if moneda == 'EUR':
            url = f"https://api.frankfurter.app/{fecha_str}?from=EUR&to=USD"
            res = requests.get(url, timeout=10).json()
            tasa = res['rates']['USD']
        elif moneda == 'CLP':
            url = f"https://mindicador.cl/api/dolar/{fecha_obj.strftime('%d-%m-%Y')}"
            res = requests.get(url, timeout=10).json()
            tasa = 1 / res['serie'][0]['valor']
        else: tasa = 1.0
        cache_tasas[key] = tasa
        return tasa
    except:
        return {'EUR': 1.08, 'CLP': 0.0011}.get(moneda, 1.0)

def convertir_y_agrupar_optimizado(df):
    if df.empty: return df
    
    # 1. Identificar pares ÚNICOS de Fecha/Moneda para no repetir llamadas a la API
    df['TempDate'] = pd.to_datetime(df['SubmitDate']).dt.date
    combinaciones = df[['TempDate', 'CurrencyCode', 'TerminationCurrencyCode']].drop_duplicates()
    
    rates_map = {}
    print(f"   💱 Calculando tasas para {len(combinaciones)} combinaciones únicas...", end="", flush=True)
    
    for _, combo in combinaciones.iterrows():
        d = combo['TempDate']
        # Tasa Client
        if (d, combo['CurrencyCode']) not in rates_map:
            rates_map[(d, combo['CurrencyCode'])] = obtener_tasa_diaria(d, combo['CurrencyCode'])
        # Tasa Vendor
        if (d, combo['TerminationCurrencyCode']) not in rates_map:
            rates_map[(d, combo['TerminationCurrencyCode'])] = obtener_tasa_diaria(d, combo['TerminationCurrencyCode'])

    # 2. Mapeo instantáneo (en lugar de row-by-row apply)
    df['ClientCostUSD'] = df.apply(lambda x: x['ClientCost'] * rates_map[(x['TempDate'], x['CurrencyCode'])], axis=1)
    df['TerminationCostUSD'] = df.apply(lambda x: x['TerminationCost'] * rates_map[(x['TempDate'], x['TerminationCurrencyCode'])], axis=1)

    # 3. Agrupación final
    dimensiones = ['TempDate', 'CompanyName', 'SMPPAccountName', 'SMPPUsername', 'MCC', 'MNC', 'OperatorName', 'DLRStatus', 'ErrorDescription', 'VendorAccountName', 'SenderID', 'CountryRealName', 'CurrencyCode', 'TerminationCurrencyCode', 'SMSSource', 'SMSType', 'MessageType', 'ErrorCode']
    cols = [c for c in dimensiones if c in df.columns]
    
    resumen = df.groupby(cols).agg({
        'MessageParts': 'sum', 'ClientCost': 'sum', 'TerminationCost': 'sum',
        'ClientCostUSD': 'sum', 'TerminationCostUSD': 'sum'
    }).reset_index()
    
    return resumen.rename(columns={'TempDate': 'SubmitDate'})

def login():
    print("⏳ Iniciando sesión...", flush=True)
    r = session.get(URL_INICIO, timeout=20)
    token = BeautifulSoup(r.text, 'html.parser').find('input', {'name': '__RequestVerificationToken'})['value']
    session.post(URL_LOGIN, data={'Username': USUARIO, 'UserKey': CLAVE, 'RememberMe': 'true', '__RequestVerificationToken': token}, 
                 headers={'RequestVerificationToken': token, 'X-Requested-With': 'XMLHttpRequest'}, timeout=20)
    print("✅ Conexión establecida.", flush=True)

if __name__ == "__main__":
    if not os.path.exists('datos'): os.makedirs('datos')
    login()
    all_data = []
    fecha_cursor = datetime.now()
    fecha_limite = fecha_cursor - timedelta(days=DIAS_ATRAS)

    while fecha_cursor > fecha_limite:
        f_fin = fecha_cursor
        f_ini = fecha_cursor - timedelta(days=6)
        ini_str = f_ini.strftime('%Y-%m-%d 00:00:00')
        fin_str = f_fin.strftime('%Y-%m-%d 23:59:59')
        
        print(f"📅 Rango: {f_ini.strftime('%Y-%m-%d')} al {f_fin.strftime('%Y-%m-%d')}...", flush=True)
        try:
            # AGREGAMOS TIMEOUT AQUÍ PARA QUE NO SE CUELGUE (300 segundos = 5 minutos max)
            r = session.get(URL_DESCARGA, params={'StartDate': ini_str, 'EndDate': fin_str}, timeout=300)
            
            if "PK" in r.text[:10]:
                df_semana = pd.read_excel(io.BytesIO(r.content))
                if not df_semana.empty:
                    all_data.append(convertir_y_agrupar_optimizado(df_semana))
                    print(f" ✅ Procesado.", flush=True)
                else: print(" ⚪ Sin datos.", flush=True)
            else: print(" ❌ Respuesta inválida del servidor.", flush=True)
        except Exception as e:
            print(f" ⚠️ Salto por error o timeout: {e}", flush=True)
        
        fecha_cursor = f_ini - timedelta(seconds=1)
        time.sleep(2)

    if all_data:
        pd.concat(all_data, ignore_index=True).to_excel(RUTA_EXCEL, index=False)
        print(f"🏆 ¡PROCESO EXITOSO!")
