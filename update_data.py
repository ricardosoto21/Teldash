import os, io, requests, pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# --- CONFIGURACIÓN ---
USUARIO = os.environ.get('SMS_USER')
CLAVE = os.environ.get('SMS_PASS')
RUTA_EXCEL = 'datos/reporte_actual.xlsx'

# 🚨 LISTA OFICIAL DE DIMENSIONES (Garantiza paridad)
DIMENSIONES = [
    'SubmitDate', 'CompanyName', 'SMPPAccountName', 'SMPPUsername', 'MCC', 'MNC', 
    'OperatorName', 'DLRStatus', 'ErrorDescription', 'VendorAccountName', 'SenderID', 
    'CountryRealName', 'CurrencyCode', 'TerminationCurrencyCode', 'SMSSource', 
    'SMSType', 'MessageType', 'ErrorCode'
]

METRICAS = {
    'MessageParts': 'sum', 'ClientCost': 'sum', 'TerminationCost': 'sum',
    'ClientCostUSD': 'sum', 'TerminationCostUSD': 'sum', 'DLRDelay': 'mean'
}

session = requests.Session()
cache_tasas = {}

def obtener_tasa_diaria(fecha_str, moneda):
    if not moneda or moneda == 'USD' or pd.isna(moneda): return 1.0
    key = f"{fecha_str}_{moneda}"
    if key in cache_tasas: return cache_tasas[key]
    try:
        if moneda == 'EUR':
            url = f"https://api.frankfurter.app/{fecha_str}?from=EUR&to=USD"
            res = requests.get(url, timeout=10).json()
            tasa = res['rates']['USD']
        elif moneda == 'CLP':
            f_obj = datetime.strptime(fecha_str, '%Y-%m-%d')
            url = f"https://mindicador.cl/api/dolar/{f_obj.strftime('%d-%m-%Y')}"
            res = requests.get(url, timeout=10).json()
            tasa = 1 / res['serie'][0]['valor']
        else: tasa = 1.0
        cache_tasas[key] = tasa
        return tasa
    except:
        return {'EUR': 1.08, 'CLP': 0.0011}.get(moneda, 1.0)

def login():
    r = session.get('http://65.108.69.39:5660/')
    token = BeautifulSoup(r.text, 'html.parser').find('input', {'name': '__RequestVerificationToken'})['value']
    payload = {'Username': USUARIO, 'UserKey': CLAVE, 'RememberMe': 'true', '__RequestVerificationToken': token}
    session.post('http://65.108.69.39:5660/Home/CheckLogin', data=payload, headers={'RequestVerificationToken': token, 'X-Requested-With': 'XMLHttpRequest'})

def update():
    if not os.path.exists('datos'): os.makedirs('datos')
    login()
    
    ayer_date_obj = datetime.now() - timedelta(days=1)
    ayer_str = ayer_date_obj.strftime('%Y-%m-%d')
    print(f"📡 Descargando tráfico agrupado de AYER ({ayer_str})...")
    
    params = {'StartDate': f"{ayer_str} 00:00:00", 'EndDate': f"{ayer_str} 23:59:59"}
    r = session.get('http://65.108.69.39:5660/DLRWholesaleReport/DownloadExcel', params=params)
    
    if "PK" in r.text[:10]:
        df = pd.read_excel(io.BytesIO(r.content))
        if df.empty:
            print("⚪ No hubo tráfico ayer.")
            return

        # 1. 🎯 Renombramos para no perder la moneda original
        renombramientos = {
            'Operator': 'OperatorName',
            'ClientCurrency': 'CurrencyCode',
            'VendorCurrency': 'TerminationCurrencyCode',
            'TerminationCurrency': 'TerminationCurrencyCode'
        }
        df = df.rename(columns=renombramientos)
        
        if 'CurrencyCode' not in df.columns: df['CurrencyCode'] = 'USD'
        if 'TerminationCurrencyCode' not in df.columns: df['TerminationCurrencyCode'] = 'USD'
        df['CurrencyCode'] = df['CurrencyCode'].fillna('USD')
        df['TerminationCurrencyCode'] = df['TerminationCurrencyCode'].fillna('USD')
        
        def aplicar_conversion(row):
            t_client = obtener_tasa_diaria(ayer_str, row['CurrencyCode'])
            t_vendor = obtener_tasa_diaria(ayer_str, row['TerminationCurrencyCode'])
            return pd.Series([row.get('ClientCost', 0) * t_client, row.get('TerminationCost', 0) * t_vendor])

        df[['ClientCostUSD', 'TerminationCostUSD']] = df.apply(aplicar_conversion, axis=1)
        df['SubmitDate'] = pd.to_datetime(df['SubmitDate']).dt.date
        # --- (Luego de esto sigue el código forzando las dimensiones con for col in DIMENSIONES...) ---
        
        # 2. 🛡️ FORZAR COLUMNAS PARA PARIDAD
        for col in DIMENSIONES:
            if col not in df.columns: df[col] = "N/A" # Crea la columna si el proveedor no la envió
            
        for col in METRICAS.keys():
            if col not in df.columns: df[col] = 0.0

        # Agrupación con dimensiones garantizadas
        resumen_ayer = df.groupby(DIMENSIONES).agg(METRICAS).reset_index()

        # 3. Cirugía segura en el histórico
        if os.path.exists(RUTA_EXCEL):
            df_hist = pd.read_excel(RUTA_EXCEL)
            df_hist['SubmitDate'] = pd.to_datetime(df_hist['SubmitDate']).dt.date
            
            # Eliminamos solo el día de ayer por si acaso
            df_hist = df_hist[df_hist['SubmitDate'] != ayer_date_obj.date()]
            
            df_final = pd.concat([df_hist, resumen_ayer], ignore_index=True)
        else:
            df_final = resumen_ayer

        df_final.to_excel(RUTA_EXCEL, index=False)
        print(f"✅ ¡Éxito! Ayer se agregaron {len(resumen_ayer)} grupos de datos.")
    else:
        print("❌ Error: El servidor no entregó un archivo Excel válido.")

if __name__ == '__main__':
    update()
