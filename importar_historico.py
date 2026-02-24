import os, io, requests, pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# --- CONFIGURACIÓN ---
USUARIO = os.environ.get('SMS_USER')
CLAVE = os.environ.get('SMS_PASS')
MESES_ATRAS = 24  # ¿Cuántos meses quieres recuperar? (Ej: 2 años)
# ---------------------

url_inicio = 'http://65.108.69.39:5660/'
url_login = 'http://65.108.69.39:5660/Home/CheckLogin'
url_descarga = 'http://65.108.69.39:5660/DLRWholesaleReport/DownloadExcel'

session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})

def login():
    r = session.get(url_inicio)
    token = BeautifulSoup(r.text, 'html.parser').find('input', {'name': '__RequestVerificationToken'})['value']
    payload = {'Username': USUARIO, 'UserKey': CLAVE, 'RememberMe': 'true', '__RequestVerificationToken': token}
    h = {'RequestVerificationToken': token, 'X-Requested-With': 'XMLHttpRequest'}
    session.post(url_login, data=payload, headers=h)
    print("✅ Sesión iniciada.")

def descargar_y_resumir(f_inicio, f_fin):
    params = {'StartDate': f_inicio, 'EndDate': f_fin}
    r = session.get(url_descarga, params=params)
    
    if "PK" not in r.text[:10]: # Si no es un Excel
        print(f"⚠️ No hay datos o error en periodo {f_inicio}")
        return pd.DataFrame()

    df = pd.read_excel(io.BytesIO(r.content))
    
    # --- LA MAGIA DEL RESUMEN ---
    # 1. Limpiamos la fecha para que sea solo Año-Mes-Día
    df['SubmitDate'] = pd.to_datetime(df['SubmitDate']).dt.date
    
    # 2. Agrupamos y sumamos
    resumen = df.groupby(['SubmitDate', 'CompanyName', 'CountryRealName', 'DLRStatus']).agg({
        'MessageParts': 'sum',
        'ClientCost': 'sum',
        'TerminationCost': 'sum'
    }).reset_index()
    
    return resumen

# --- PROCESO PRINCIPAL ---
login()
hoy = datetime.now()
lista_resumenes = []

for i in range(MESES_ATRAS):
    inicio_mes = (hoy - relativedelta(months=i)).replace(day=1, hour=0, minute=0, second=0)
    fin_mes = (inicio_mes + relativedelta(months=1)) - timedelta(seconds=1)
    
    print(f"⏳ Procesando: {inicio_mes.strftime('%B %Y')}...")
    
    df_mes = descargar_y_resumir(inicio_mes.strftime('%Y-%m-%d %H:%M:%S'), fin_mes.strftime('%Y-%m-%d %H:%M:%S'))
    if not df_mes.empty:
        lista_resumenes.append(df_mes)

# Unimos todo el histórico
if lista_resumenes:
    final_df = pd.concat(lista_resumenes, ignore_index=True)
    final_df.to_excel('datos/reporte_actual.xlsx', index=False)
    print(f"🏆 ¡Misión cumplida! Se procesaron {MESES_ATRAS} meses y se guardaron en datos/reporte_actual.xlsx")
else:
    print("❌ No se pudo recuperar ningún dato.")
