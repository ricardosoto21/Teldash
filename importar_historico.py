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
    if not moneda or moneda == 'USD' or pd.isna(moneda) or str(moneda).strip() == "": return 1.0
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
    
    # 0. Mapeo flexible de columnas (por si el servidor cambia los nombres)
    # Buscamos columnas de moneda
    col_client_cur = 'CurrencyCode' if 'CurrencyCode' in df.columns else ('ClientCurrency' if 'ClientCurrency' in df.columns else None)
    col_vendor_cur = 'TerminationCurrencyCode' if 'TerminationCurrencyCode' in df.columns else ('VendorCurrency' if 'VendorCurrency' in df.columns else None)
    
    # Si no existen, creamos las columnas como USD por defecto
    if not col_client_cur: 
        df['CurrencyCode'] = 'USD'
        col_client_cur = 'CurrencyCode'
    if not col_vendor_cur:
        df['TerminationCurrencyCode'] = 'USD'
        col_vendor_cur = 'TerminationCurrencyCode'

    df['TempDate'] = pd.to_datetime(df['SubmitDate']).dt.date
    
    # 1. Identificar combinaciones únicas para consultar APIs una sola vez
    combinaciones = df[['TempDate', col_client_cur, col_vendor_cur]].drop_duplicates()
    rates_map = {}
    
    print(f" (Tasas: {len(combinaciones)} comb.) ", end="", flush=True)
    
    for _, row in combinaciones.iterrows():
        d = row['TempDate']
        c_cur = row[col_client_cur]
        v_cur = row[col_vendor_cur]
        if (d, c_cur) not in rates_map: rates_map[(d, c_cur)] = obtener_tasa_diaria(d, c_cur)
        if (d, v_cur) not in rates_map: rates_map[(d, v_cur)] = obtener_tasa_diaria(d, v_cur)

    # 2. Aplicar conversión masiva
    df['ClientCostUSD'] = df.apply(lambda x: x['ClientCost'] * rates_map[(x['TempDate'], x[col_client_cur])], axis=1)
    df['TerminationCostUSD'] = df.apply(lambda x: x['TerminationCost'] * rates_map[(x['TempDate'], x[col_vendor_cur])], axis=1)

    # 3. Agrupación final con todas las dimensiones operativas
    dimensiones = ['TempDate', 'CompanyName', 'SMPPAccountName', 'SMPPUsername', 'MCC', 'MNC', 'OperatorName', 'DLRStatus', 'ErrorDescription', 'VendorAccountName', 'SenderID', 'CountryRealName', col_client_cur, col_vendor_cur, 'SMSSource', 'SMSType', 'MessageType', 'ErrorCode']
    cols_actuales = [c for c in dimensiones if c in df.columns]
    
    resumen = df.groupby(cols_actuales).agg({
        'MessageParts': 'sum', 'ClientCost': 'sum', 'TerminationCost': 'sum',
        'ClientCostUSD': 'sum', 'TerminationCostUSD': 'sum'
    }).reset_index()
    
    # Estandarizamos nombres para el dashboard
    resumen = resumen.rename(columns={'TempDate': 'SubmitDate', col_client_cur: 'CurrencyCode', col_vendor_cur: 'TerminationCurrencyCode'})
    return resumen

def login():
    print("⏳ Iniciando sesión...", flush=True)
    r = session.get(URL_INICIO, timeout=20)
    soup = BeautifulSoup(r.text, 'html.parser')
    token = soup.find('input', {'name': '__RequestVerificationToken'})['value']
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
        
        print(f"📅 Rango: {f_ini.strftime('%Y-%m-%d')} al {f_fin.strftime('%Y-%m-%d')}... ", end="", flush=True)
        try:
            r = session.get(URL_DESCARGA, params={'StartDate': ini_str, 'EndDate': fin_str}, timeout=300)
            
            if "PK" in r.text[:10]:
                df_semana = pd.read_excel(io.BytesIO(r.content))
                if not df_semana.empty:
                    # LOG DE COLUMNAS PARA DEPURAR SI FALLA
                    # print(f" (Cols: {list(df_semana.columns)[:5]}...) ", end="")
                    df_res = convertir_y_agrupar_optimizado(df_semana)
                    all_data.append(df_res)
                    print(f"✅ Ok.", flush=True)
                else: print("⚪ Sin datos.", flush=True)
            else: print("❌ Respuesta no Excel.", flush=True)
        except Exception as e:
            print(f"⚠️ Salto por error: {e}", flush=True)
        
        fecha_cursor = f_ini - timedelta(seconds=1)
        time.sleep(2)

    if all_data:
        print("\n⚙️ Unificando y guardando...", flush=True)
        pd.concat(all_data, ignore_index=True).to_excel(RUTA_EXCEL, index=False)
        print(f"🏆 ¡PROCESO EXITOSO!")
