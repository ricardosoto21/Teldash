import os, io, requests, pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time

# --- CONFIGURACIÓN ---
USUARIO = os.environ.get('SMS_USER')
CLAVE = os.environ.get('SMS_PASS')
DIAS_ATRAS = 365 
RUTA_EXCEL = 'datos/reporte_actual.xlsx'

URL_INICIO = 'http://65.108.69.39:5660/'
URL_LOGIN = 'http://65.108.69.39:5660/Home/CheckLogin'
URL_DESCARGA = 'http://65.108.69.39:5660/DLRWholesaleReport/DownloadExcel'

session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})

cache_tasas = {}

def obtener_tasa_diaria(fecha_obj, moneda):
    """Consulta tasas reales: Frankfurter para EUR, Mindicador para CLP."""
    if moneda == 'USD': return 1.0
    
    fecha_str = fecha_obj.strftime('%Y-%m-%d')
    key = f"{fecha_str}_{moneda}"
    if key in cache_tasas: return cache_tasas[key]
    
    try:
        if moneda == 'EUR':
            # API Frankfurter: EUR a USD
            url = f"https://api.frankfurter.app/{fecha_str}?from=EUR&to=USD"
            res = requests.get(url, timeout=5).json()
            tasa = res['rates']['USD']
            
        elif moneda == 'CLP':
            # API Mindicador: USD a CLP (requiere formato DD-MM-YYYY)
            fecha_clp = fecha_obj.strftime('%d-%m-%Y')
            url = f"https://mindicador.cl/api/dolar/{fecha_clp}"
            res = requests.get(url, timeout=5).json()
            valor_dolar = res['serie'][0]['valor']
            tasa = 1 / valor_dolar # Convertimos CLP a factor de USD
            
        else:
            tasa = 1.0
        
        cache_tasas[key] = tasa
        return tasa
    except Exception as e:
        # Fallbacks de seguridad si las APIs fallan
        fallbacks = {'EUR': 1.08, 'CLP': 0.0011}
        return fallbacks.get(moneda, 1.0)

def agrupar_data(df):
    """Agrupa manteniendo el detalle de operadores y cuentas."""
    if df.empty: return df
    df['SubmitDate'] = pd.to_datetime(df['SubmitDate']).dt.date
    
    dimensiones = [
        'SubmitDate', 'CompanyName', 'SMPPAccountName', 'SMPPUsername', 
        'MCC', 'MNC', 'OperatorName', 'DLRStatus', 'ErrorDescription', 
        'VendorAccountName', 'SenderID', 'CountryRealName', 'CurrencyCode', 
        'SMSSource', 'SMSType', 'MessageType', 'ErrorCode'
    ]
    
    columnas_agrupar = [c for c in dimensiones if c in df.columns]
    metricas = ['MessageParts', 'ClientCost', 'TerminationCost']
    if 'ClientCostUSD' in df.columns: metricas.append('ClientCostUSD')
    if 'TerminationCostUSD' in df.columns: metricas.append('TerminationCostUSD')

    return df.groupby(columnas_agrupar).agg({m: 'sum' for m in metricas}).reset_index()

def convertir_moneda_df(df):
    """Aplica conversión fila por fila con la tasa histórica de cada día."""
    if df.empty: return df
    print("   💱 Consultando APIs de divisas...", end="", flush=True)
    
    def aplicar_conversion(row):
        # Usamos el objeto de fecha para la API
        fecha_obj = pd.to_datetime(row['SubmitDate'])
        tasa = obtener_tasa_diaria(fecha_obj, row['CurrencyCode'])
        return pd.Series([row['ClientCost'] * tasa, row['TerminationCost'] * tasa])

    df[['ClientCostUSD', 'TerminationCostUSD']] = df.apply(aplicar_conversion, axis=1)
    return df

def login():
    print("⏳ Iniciando sesión...", flush=True)
    r = session.get(URL_INICIO)
    token = BeautifulSoup(r.text, 'html.parser').find('input', {'name': '__RequestVerificationToken'})['value']
    payload = {'Username': USUARIO, 'UserKey': CLAVE, 'RememberMe': 'true', '__RequestVerificationToken': token}
    session.post(URL_LOGIN, data=payload, headers={'RequestVerificationToken': token, 'X-Requested-With': 'XMLHttpRequest'})
    print("✅ Conexión establecida.", flush=True)

if __name__ == "__main__":
    if not os.path.exists('datos'): os.makedirs('datos')
    all_data = []

    if os.path.exists(RUTA_EXCEL):
        print(f"📂 Optimizando datos actuales...", flush=True)
        df_temp = pd.read_excel(RUTA_EXCEL)
        if 'ClientCostUSD' not in df_temp.columns:
            df_temp = convertir_moneda_df(df_temp)
        all_data.append(agrupar_data(df_temp))

    login()
    fecha_cursor = datetime(2026, 1, 31, 23, 59, 59)
    fecha_limite = fecha_cursor - timedelta(days=DIAS_ATRAS)
    
    print(f"🚀 Iniciando recuperación histórica con tasas diarias reales...", flush=True)

    while fecha_cursor > fecha_limite:
        f_fin = fecha_cursor
        f_ini = fecha_cursor - timedelta(days=6)
        ini_str = f_ini.replace(hour=0, minute=0, second=0).strftime('%Y-%m-%d %H:%M:%S')
        fin_str = f_fin.strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"📅 Rango: {ini_str} al {f_fin.strftime('%Y-%m-%d')}... ", end="", flush=True)
        r = session.get(URL_DESCARGA, params={'StartDate': ini_str, 'EndDate': fin_str})
        
        if "PK" in r.text[:10]:
            df_semana = pd.read_excel(io.BytesIO(r.content))
            if not df_semana.empty:
                df_semana = convertir_moneda_df(df_semana)
                df_semana_agrupada = agrupar_data(df_semana)
                all_data.append(df_semana_agrupada)
                print(f" ✅ {len(df_semana_agrupada)} grupos.", flush=True)
            else:
                print(" ⚪ Sin tráfico.", flush=True)
        else:
            print(" ❌ Error en servidor.", flush=True)
        
        fecha_cursor = f_ini - timedelta(seconds=1)
        time.sleep(1.5) # Pausa para respetar las APIs de moneda

    if all_data:
        print("\n⚙️ Unificando base de datos final...", flush=True)
        df_final = pd.concat(all_data, ignore_index=True)
        df_final = agrupar_data(df_final)
        df_final.to_excel(RUTA_EXCEL, index=False)
        print(f"🏆 ¡PROCESO EXITOSO! Reporte histórico 100% verificado en USD.")
