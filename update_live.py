import os, io, requests, pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# --- CONFIGURACIÓN (Usa tus mismos secretos) ---
USUARIO = os.environ.get('SMS_USER')
CLAVE = os.environ.get('SMS_PASS')
RUTA_LIVE = 'datos/live_traffic.xlsx'

URL_BASE = 'http://65.108.69.39:5660'
URL_LOGIN = f'{URL_BASE}/Home/CheckLogin'
URL_DESCARGA = f'{URL_BASE}/DLRWholesaleReport/DownloadExcel'

session = requests.Session()

def login():
    print("⏳ Conectando al servidor para tráfico vivo...")
    r = session.get(URL_BASE)
    soup = BeautifulSoup(r.text, 'html.parser')
    token = soup.find('input', {'name': '__RequestVerificationToken'})['value']
    
    payload = {
        'Username': USUARIO, 
        'UserKey': CLAVE, 
        'RememberMe': 'true', 
        '__RequestVerificationToken': token
    }
    session.post(URL_LOGIN, data=payload, headers={'RequestVerificationToken': token, 'X-Requested-With': 'XMLHttpRequest'})

def update_live():
    if not os.path.exists('datos'): os.makedirs('datos')
    login()
    
    ahora = datetime.now()
    hace_12h = ahora - timedelta(hours=12)
    
    print(f"📡 Descargando tráfico desde {hace_12h.strftime('%H:%M')} hasta ahora...")
    
    params = {
        'StartDate': hace_12h.strftime('%Y-%m-%d %H:%M:%S'),
        'EndDate': ahora.strftime('%Y-%m-%d %H:%M:%S')
    }
    
    r = session.get(URL_DESCARGA, params=params)
    
    if "PK" in r.text[:10]: # Verifica que sea un archivo Excel válido (ZIP/Excel format)
        df = pd.read_excel(io.BytesIO(r.content))
        if not df.empty:
            # Ordenamos por lo más nuevo y limitamos a 3000 filas para que el Dashboard no pese
            df_live = df.sort_values('SubmitDate', ascending=False).head(300000)
            df_live.to_excel(RUTA_LIVE, index=False)
            print(f"✅ Live Traffic actualizado: {len(df_live)} registros guardados en {RUTA_LIVE}")
        else:
            print("⚪ No hay tráfico en las últimas 12 horas.")
    else:
        print("❌ Error: El servidor no entregó un archivo Excel. Revisa credenciales.")

if __name__ == "__main__":
    update_live()
