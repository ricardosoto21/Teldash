import os, io, requests, pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time

# --- CONFIGURACIÓN ---
USUARIO = os.environ.get('SMS_USER')
CLAVE = os.environ.get('SMS_PASS')
DIAS_ATRAS = 365  # Puedes subirlo a 730 después de probar un año
RUTA_EXCEL = 'datos/reporte_actual.xlsx'

URL_INICIO = 'http://65.108.69.39:5660/'
URL_LOGIN = 'http://65.108.69.39:5660/Home/CheckLogin'
URL_DESCARGA = 'http://65.108.69.39:5660/DLRWholesaleReport/DownloadExcel'

session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})

# --- CACHE DE MONEDAS PARA NO SATURAR APIS ---
cache_tasas = {}

def obtener_tasa_diaria(fecha_str, moneda):
    """Obtiene la tasa de cambio para una fecha específica."""
    if moneda == 'USD': return 1.0
    key = f"{fecha_str}_{moneda}"
    if key in cache_tasas: return cache_tasas[key]
    
    try:
        if moneda == 'EUR':
            url = f"https://api.frankfurter.app/{fecha_str}?from=EUR&to=USD"
            res = requests.get(url, timeout=5).json()
            tasa = res['rates']['USD']
        elif moneda == 'CLP':
            # Para CLP usamos un valor de referencia histórico si la API falla 
            # (o puedes conectar a otra API específica de CLP)
            tasa = 0.0011 # Valor base aproximado
        else:
            tasa = 1.0
        
        cache_tasas[key] = tasa
        return tasa
    except:
        # Fallback en caso de error de red o API
        fallback = {'EUR': 1.08, 'CLP': 0.0011}
        return fallback.get(moneda, 1.0)

def agrupar_data(df):
    """Agrupa los datos manteniendo el máximo detalle operativo."""
    if df.empty: return df
    
    # Convertir fecha a objeto date (sin hora) para agrupar por día
    df['SubmitDate'] = pd.to_datetime(df['SubmitDate']).dt.date
    
    # Dimensiones a mantener (según tu lista de columnas)
    dimensiones = [
        'SubmitDate', 'CompanyName', 'SMPPAccountName', 'SMPPUsername', 
        'MCC', 'MNC', 'OperatorName', 'DLRStatus', 'ErrorDescription', 
        'VendorAccountName', 'SenderID', 'CountryRealName', 'CurrencyCode', 
        'SMSSource', 'SMSType', 'MessageType', 'ErrorCode'
    ]
    
    # Solo usamos las columnas que existan en el archivo descargado
    columnas_agrupar = [c for c in dimensiones if c in df.columns]
    
    # Columnas numéricas a sumar
    metricas = ['MessageParts', 'ClientCost', 'TerminationCost']
    if 'ClientCostUSD' in df.columns: metricas.append('ClientCostUSD')
    if 'TerminationCostUSD' in df.columns: metricas.append('TerminationCostUSD')

    resumen = df.groupby(columnas_agrupar).agg({m: 'sum' for m in metricas}).reset_index()
    return resumen

def convertir_moneda_df(df):
    """Aplica la conversión de moneda fila por fila usando la fecha de cada SMS."""
    if df.empty: return df
    
    def aplicar_conversion(row):
        fecha = str(row['SubmitDate'])
        tasa = obtener_tasa_diaria(fecha, row['CurrencyCode'])
        return pd.Series([row['ClientCost'] * tasa, row['TerminationCost'] * tasa])

    df[['ClientCostUSD', 'TerminationCostUSD']] = df.apply(aplicar_conversion, axis=1)
    return df

def login():
    print("⏳ Iniciando sesión en el servidor...", flush=True)
    r = session.get(URL_INICIO)
    token = BeautifulSoup(r.text, 'html.parser').find('input', {'name': '__RequestVerificationToken'})['value']
    payload = {'Username': USUARIO, 'UserKey': CLAVE, 'RememberMe': 'true', '__RequestVerificationToken': token}
    session.post(URL_LOGIN, data=payload, headers={'RequestVerificationToken': token, 'X-Requested-With': 'XMLHttpRequest'})
    print("✅ Conexión establecida.", flush=True)

# --- INICIO DEL PROCESO ---
if __name__ == "__main__":
    if not os.path.exists('datos'): os.makedirs('datos')
    
    all_data = []

    # 1. CARGAR Y RE-AGRUPAR LO EXISTENTE (Febrero 2026)
    if os.path.exists(RUTA_EXCEL):
        print(f"📂 Cargando datos actuales de {RUTA_EXCEL} para optimizarlos...", flush=True)
        df_temp = pd.read_excel(RUTA_EXCEL)
        # Si no tiene columnas USD, las calculamos (usando tasa de hoy para lo actual)
        if 'ClientCostUSD' not in df_temp.columns:
            df_temp = convertir_moneda_df(df_temp)
        all_data.append(agrupar_data(df_temp))

    # 2. LOGIN Y DESCARGA HISTÓRICA
    login()
    
    # Empezamos desde el 31 de enero de 2026 hacia atrás
    fecha_cursor = datetime(2026, 1, 31, 23, 59, 59)
    fecha_limite = fecha_cursor - timedelta(days=DIAS_ATRAS)
    
    print(f"🚀 Iniciando viaje al pasado ({DIAS_ATRAS} días)...", flush=True)

    while fecha_cursor > fecha_limite:
        f_fin = fecha_cursor
        f_ini = fecha_cursor - timedelta(days=6)
        
        ini_str = f_ini.replace(hour=0, minute=0, second=0).strftime('%Y-%m-%d %H:%M:%S')
        fin_str = f_fin.strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"📅 Rango: {ini_str} al {f_fin.strftime('%Y-%m-%d')}... ", end="", flush=True)
        
        params = {'StartDate': ini_str, 'EndDate': fin_str}
        r = session.get(URL_DESCARGA, params=params)
        
        if "PK" in r.text[:10]:
            df_semana = pd.read_excel(io.BytesIO(r.content))
            if not df_semana.empty:
                # Procesar: Convertir -> Agrupar
                df_semana = convertir_moneda_df(df_semana)
                df_semana_agrupada = agrupar_data(df_semana)
                all_data.append(df_semana_agrupada)
                print(f"✅ {len(df_semana_agrupada)} grupos de datos.", flush=True)
            else:
                print("⚪ Vacío.", flush=True)
        else:
            print("❌ Error descarga.", flush=True)
        
        # Retroceder 7 días
        fecha_cursor = f_ini - timedelta(seconds=1)
        time.sleep(1) # Pausa breve para no saturar al proveedor

    # 3. UNIFICACIÓN FINAL
    if all_data:
        print("\n⚙️ Consolidando toda la información...", flush=True)
        df_final = pd.concat(all_data, ignore_index=True)
        # Volvemos a agrupar por si hay solapamiento de días
        df_final = agrupar_data(df_final)
        df_final.to_excel(RUTA_EXCEL, index=False)
        print(f"🏆 ¡PROCESO COMPLETADO! Archivo optimizado con {len(df_final)} filas totales.")
    else:
        print("\n❌ No se pudo recuperar ningún dato.")
