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
    if not moneda or pd.isna(moneda) or str(moneda).strip() == "" or moneda == 'USD': return 1.0
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
    
    # --- LA SOLUCIÓN: Estandarizar el nombre del Operador ---
    if 'Operator' in df.columns and 'OperatorName' not in df.columns:
        df = df.rename(columns={'Operator': 'OperatorName'})
    # Si por alguna razón viene completamente vacía o sin operador, la creamos para que no falle
    if 'OperatorName' not in df.columns:
        df['OperatorName'] = 'Desconocido'
    # --------------------------------------------------------

    df['SubmitDate'] = pd.to_datetime(df['SubmitDate']).dt.date
    
    col_client_cur = 'CurrencyCode' if 'CurrencyCode' in df.columns else ('ClientCurrency' if 'ClientCurrency' in df.columns else None)
    col_vendor_cur = 'TerminationCurrencyCode' if 'TerminationCurrencyCode' in df.columns else ('VendorCurrency' if 'VendorCurrency' in df.columns else None)
    if not col_client_cur: df['CurrencyCode'] = 'USD'; col_client_cur = 'CurrencyCode'
    if not col_vendor_cur: df['TerminationCurrencyCode'] = 'USD'; col_vendor_cur = 'TerminationCurrencyCode'

    # Limpieza DLRDelay
    if 'DLRDelay' in df.columns:
        df['DLRDelay'] = df['DLRDelay'].astype(str).str.extract(r'(\d+)').astype(float).fillna(0)
    else:
        df['DLRDelay'] = 0

    df['ClientCostUSD'] = df.apply(lambda x: x['ClientCost'] * obtener_tasa_diaria(x['SubmitDate'], x[col_client_cur]), axis=1)
    df['TerminationCostUSD'] = df.apply(lambda x: x['TerminationCost'] * obtener_tasa_diaria(x['SubmitDate'], x[col_vendor_cur]), axis=1)

    # Agrupación estricta para el Dashboard (elimina solapamientos internos)
    resumen = df.groupby(['SubmitDate', 'CompanyName', 'CountryRealName', 'OperatorName', 'DLRStatus']).agg({
        'MessageParts': 'sum', 
        'ClientCostUSD': 'sum', 
        'TerminationCostUSD': 'sum',
        'DLRDelay': 'mean'
    }).reset_index()
    
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
        print("\n⚙️ Unificando y limpiando solapamientos finales...", flush=True)
        df_completo = pd.concat(all_data, ignore_index=True)
        
        # Agrupación final maestra (Evita cualquier fila duplicada si los rangos de fecha se cruzaron)
        df_maestro = df_completo.groupby(['SubmitDate', 'CompanyName', 'CountryRealName', 'OperatorName', 'DLRStatus']).agg({
            'MessageParts': 'sum', 'ClientCostUSD': 'sum', 'TerminationCostUSD': 'sum', 'DLRDelay': 'mean'
        }).reset_index()
        
        df_maestro.to_excel(RUTA_EXCEL, index=False)
        print(f"🏆 ¡PROCESO EXITOSO! Base de datos comprimida a {len(df_maestro)} filas.")
