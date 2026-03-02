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

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
})

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

def login():
    print("⏳ Entrando al sistema...", flush=True)
    r = session.get(url_inicio)
    token = BeautifulSoup(r.text, 'html.parser').find('input', {'name': '__RequestVerificationToken'})['value']
    session.post(url_login, data={'Username': USUARIO, 'UserKey': CLAVE, 'RememberMe': 'true', '__RequestVerificationToken': token}, 
                 headers={'RequestVerificationToken': token, 'X-Requested-With': 'XMLHttpRequest', 'Referer': url_inicio})

def ejecutar_actualizacion():
    if not os.path.exists('datos'): os.makedirs('datos')
    login()
    
    ayer = datetime.now() - timedelta(days=1)
    f_inicio = ayer.strftime('%Y-%m-%d 00:00:00')
    f_fin = ayer.strftime('%Y-%m-%d 23:59:59')
    print(f"📅 Procesando fecha: {ayer.strftime('%Y-%m-%d')}", flush=True)
    
    params = {'StartDate': f_inicio, 'EndDate': f_fin, 'SenderID': '', 'DLRStatus': '', 'PhoneNumber': '', 'SMSID': '', 'VendorSMSID': '', 'CountryID': '', 'VendorAccountID': '', 'CustomerSMPPAccountID': '', 'ErrorDescription': '', 'MCC': '', 'MNC': '', 'ExcludeCountryID': '', 'ExcludeCustomerSMPPAccountID': '', 'CustomerId': ''}
    
    r = session.get(url_descarga, params=params)
    if "PK" not in r.text[:10]:
        print("❌ El servidor no entregó un Excel válido. Fin del proceso.", flush=True)
        return

    df = pd.read_excel(io.BytesIO(r.content))
    if df.empty:
        print("⚪ No hubo tráfico ayer. Nada que agregar.", flush=True)
        return
    
    # 1. Preparar Columnas Base
    df['SubmitDate'] = pd.to_datetime(df['SubmitDate']).dt.date
    col_client_cur = 'CurrencyCode' if 'CurrencyCode' in df.columns else ('ClientCurrency' if 'ClientCurrency' in df.columns else None)
    col_vendor_cur = 'TerminationCurrencyCode' if 'TerminationCurrencyCode' in df.columns else ('VendorCurrency' if 'VendorCurrency' in df.columns else None)
    if not col_client_cur: df['CurrencyCode'] = 'USD'; col_client_cur = 'CurrencyCode'
    if not col_vendor_cur: df['TerminationCurrencyCode'] = 'USD'; col_vendor_cur = 'TerminationCurrencyCode'

    # 2. Limpiar DLRDelay (Extraer solo los números)
    if 'DLRDelay' in df.columns:
        df['DLRDelay'] = df['DLRDelay'].astype(str).str.extract(r'(\d+)').astype(float).fillna(0)
    else:
        df['DLRDelay'] = 0

    # 3. Conversión de Monedas a USD
    df['ClientCostUSD'] = df.apply(lambda x: x['ClientCost'] * obtener_tasa_diaria(x['SubmitDate'], x[col_client_cur]), axis=1)
    df['TerminationCostUSD'] = df.apply(lambda x: x['TerminationCost'] * obtener_tasa_diaria(x['SubmitDate'], x[col_vendor_cur]), axis=1)

    # 4. Agrupación Optimizada (Evita solapamientos y mantiene filtros vivos)
    df_resumido = df.groupby(['SubmitDate', 'CompanyName', 'CountryRealName', 'OperatorName', 'DLRStatus']).agg({
        'MessageParts': 'sum',
        'ClientCostUSD': 'sum',
        'TerminationCostUSD': 'sum',
        'DLRDelay': 'mean' # Promedio matemático del día para ese grupo
    }).reset_index()

    # 5. Unir con el histórico de GitHub y Eliminar cualquier solapamiento
    try:
        df_historico = pd.read_excel(ruta_excel)
        df_historico['SubmitDate'] = pd.to_datetime(df_historico['SubmitDate']).dt.date
        df_final = pd.concat([df_historico, df_resumido], ignore_index=True)
        
        # Volvemos a agrupar todo el documento por si hubo corridas duplicadas del bot
        df_final = df_final.groupby(['SubmitDate', 'CompanyName', 'CountryRealName', 'OperatorName', 'DLRStatus']).agg({
            'MessageParts': 'sum', 'ClientCostUSD': 'sum', 'TerminationCostUSD': 'sum', 'DLRDelay': 'mean'
        }).reset_index()
    except FileNotFoundError:
        df_final = df_resumido

    df_final.to_excel(ruta_excel, index=False)
    print(f"🚀 ¡Éxito! Archivo actualizado. Total filas: {len(df_final)}", flush=True)

if __name__ == "__main__":
    ejecutar_actualizacion()
