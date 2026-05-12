import os, io, requests, pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

# --- CONFIGURACIÓN ---
USUARIO = os.environ.get('SMS_USER')
CLAVE = os.environ.get('SMS_PASS')
RUTA_EXCEL = 'datos/reporte_actual.xlsx'

# 🎯 DÍAS A RECUPERAR
DIAS_FALTANTES = ['2026-05-07', '2026-05-08', '2026-05-09', '2026-05-10', '2026-05-11']

# 🚨 LISTA OFICIAL DE DIMENSIONES (Garantiza paridad exacta)
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

def recuperar():
    login()
    print("⏳ Leyendo base de datos histórica...")
    if not os.path.exists(RUTA_EXCEL):
        print("❌ Error: No se encontró el histórico para inyectar los datos.")
        return
        
    df_hist = pd.read_excel(RUTA_EXCEL)
    df_hist['SubmitDate'] = pd.to_datetime(df_hist['SubmitDate']).dt.date
    nuevos_datos = []
    
    for dia in DIAS_FALTANTES:
        print(f"📥 Recuperando datos del {dia}...")
        params = {'StartDate': f"{dia} 00:00:00", 'EndDate': f"{dia} 23:59:59"}
        r = session.get('http://65.108.69.39:5660/DLRWholesaleReport/DownloadExcel', params=params)
        
        if "PK" in r.text[:10]:
            df = pd.read_excel(io.BytesIO(r.content))
            if df.empty:
                print(f"⚪ Sin tráfico para el {dia}")
                continue
            
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
                t_client = obtener_tasa_diaria(dia, row['CurrencyCode'])
                t_vendor = obtener_tasa_diaria(dia, row['TerminationCurrencyCode'])
                return pd.Series([row.get('ClientCost', 0) * t_client, row.get('TerminationCost', 0) * t_vendor])

            df[['ClientCostUSD', 'TerminationCostUSD']] = df.apply(aplicar_conversion, axis=1)
            df['SubmitDate'] = pd.to_datetime(df['SubmitDate']).dt.date
            
            # 2. 🛡️ FORZAR COLUMNAS PARA PARIDAD EXACTA
            for col in DIMENSIONES:
                if col not in df.columns: df[col] = "N/A"
                
            for col in METRICAS.keys():
                if col not in df.columns: df[col] = 0.0
            
            resumen = df.groupby(DIMENSIONES).agg(METRICAS).reset_index()
            nuevos_datos.append(resumen)
            print(f"✅ {dia} procesado: {len(resumen)} grupos generados.")
        else:
            print(f"❌ Error descargando datos del {dia}. El archivo no es Excel válido.")
            print(f"🔍 ESTO FUE LO QUE RESPONDIÓ EL SERVIDOR:\n{r.text[:300]}")
    
    if nuevos_datos:
        print("⚙️ Uniendo los datos recuperados al archivo principal...")
        
        # 🚨 CIRUGÍA SEGURA
        dias_a_borrar = [pd.to_datetime(d).date() for d in DIAS_FALTANTES]
        df_hist = df_hist[~df_hist['SubmitDate'].isin(dias_a_borrar)]
        
        df_final = pd.concat([df_hist] + nuevos_datos, ignore_index=True)
        df_final.to_excel(RUTA_EXCEL, index=False)
        print("🏆 ¡Rescate Exitoso! Archivo guardado, con paridad de columnas verificada.")
    else:
        print("⚠️ No se generaron datos nuevos.")

if __name__ == '__main__':
    recuperar()
