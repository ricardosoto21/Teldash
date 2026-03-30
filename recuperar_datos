import os, io, requests, pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

# --- CONFIGURACIÓN ---
USUARIO = os.environ.get('SMS_USER')
CLAVE = os.environ.get('SMS_PASS')
RUTA_EXCEL = 'datos/reporte_actual.xlsx'

# 🎯 AQUÍ PONES LOS DÍAS EXACTOS QUE QUIERES RECUPERAR (Formato YYYY-MM-DD)
DIAS_FALTANTES = ['2026-03-24', '2026-03-25']

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
    df_hist = pd.read_excel(RUTA_EXCEL)
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
            
            c_cur = 'CurrencyCode' if 'CurrencyCode' in df.columns else 'ClientCurrency'
            v_cur = 'TerminationCurrencyCode' if 'TerminationCurrencyCode' in df.columns else 'VendorCurrency'
            
            def aplicar_conversion(row):
                t_client = obtener_tasa_diaria(dia, row.get(c_cur, 'USD'))
                t_vendor = obtener_tasa_diaria(dia, row.get(v_cur, 'USD'))
                return pd.Series([row['ClientCost'] * t_client, row['TerminationCost'] * t_vendor])

            df[['ClientCostUSD', 'TerminationCostUSD']] = df.apply(aplicar_conversion, axis=1)
            
            df['SubmitDate'] = pd.to_datetime(df['SubmitDate']).dt.date
            dimensiones = ['SubmitDate', 'CompanyName', 'SMPPAccountName', 'SMPPUsername', 'MCC', 'MNC', 'OperatorName', 'DLRStatus', 'ErrorDescription', 'VendorAccountName', 'SenderID', 'CountryRealName', 'CurrencyCode', 'TerminationCurrencyCode', 'SMSSource', 'SMSType', 'MessageType', 'ErrorCode']
            cols_agrupar = [c for c in dimensiones if c in df.columns]
            
            resumen = df.groupby(cols_agrupar).agg({
                'MessageParts': 'sum', 'ClientCost': 'sum', 'TerminationCost': 'sum',
                'ClientCostUSD': 'sum', 'TerminationCostUSD': 'sum', 'DLRDelay': 'mean'
            }).reset_index()
            
            nuevos_datos.append(resumen)
            print(f"✅ {dia} procesado: {len(resumen)} grupos generados.")
    
    if nuevos_datos:
        print("⚙️ Uniendo los datos recuperados al archivo principal...")
        df_final = pd.concat([df_hist] + nuevos_datos, ignore_index=True)
        # Limpieza por si acaso alguna fila de ese día ya existía a medias
        df_final = df_final.drop_duplicates(subset=cols_agrupar, keep='last')
        df_final.to_excel(RUTA_EXCEL, index=False)
        print("🏆 ¡Rescate Exitoso! Archivo guardado.")
    else:
        print("⚠️ No se generaron datos nuevos.")

if __name__ == '__main__':
    recuperar()
