import io
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from bs4 import BeautifulSoup


USUARIO = os.environ.get("SMS_USER")
CLAVE = os.environ.get("SMS_PASS")
RUTA_LIVE = "datos/live_traffic.xlsx"
RUTA_LIVE_TODAY = "datos/live_today.xlsx"

URL_BASE = "http://65.108.69.39:5660"
URL_LOGIN = f"{URL_BASE}/Home/CheckLogin"
URL_DESCARGA = f"{URL_BASE}/DLRWholesaleReport/DownloadExcel"

session = requests.Session()
session.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36"
        )
    }
)


def validar_credenciales():
    if not USUARIO or not CLAVE:
        raise RuntimeError("Faltan SMS_USER o SMS_PASS en el entorno.")


def login():
    validar_credenciales()
    print("Conectando al servidor para trafico live...")

    r = session.get(URL_BASE, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    token_input = soup.find("input", {"name": "__RequestVerificationToken"})
    if token_input is None or not token_input.get("value"):
        raise RuntimeError("No se encontro el token de login en la pagina del servidor.")

    token = token_input["value"]
    payload = {
        "Username": USUARIO,
        "UserKey": CLAVE,
        "RememberMe": "true",
        "__RequestVerificationToken": token,
    }

    res = session.post(
        URL_LOGIN,
        data=payload,
        headers={"RequestVerificationToken": token, "X-Requested-With": "XMLHttpRequest"},
        timeout=30,
    )
    res.raise_for_status()
    print("Login live enviado correctamente.")


def descargar_reporte(start_date, end_date, label):
    print(f"Descargando {label} desde {start_date:%Y-%m-%d %H:%M:%S} hasta {end_date:%Y-%m-%d %H:%M:%S}...")

    params = {
        "StartDate": start_date.strftime("%Y-%m-%d %H:%M:%S"),
        "EndDate": end_date.strftime("%Y-%m-%d %H:%M:%S"),
    }

    r = session.get(URL_DESCARGA, params=params, timeout=120)
    r.raise_for_status()

    if r.content[:2] != b"PK":
        preview = r.text[:180].replace("\n", " ").replace("\r", " ")
        raise RuntimeError(f"El servidor no entrego un Excel valido para {label}: {preview}")

    return pd.read_excel(io.BytesIO(r.content))


def guardar_excel(df, ruta, label):
    if df.empty:
        df.to_excel(ruta, index=False)
        print(f"No hay trafico para {label}. Se guardo un Excel vacio para evitar datos obsoletos.")
        return

    df_live = df.sort_values("SubmitDate", ascending=False).head(300000)
    df_live.to_excel(ruta, index=False)
    print(f"{label} actualizado: {len(df_live)} registros guardados en {ruta}")


def update_live():
    os.makedirs("datos", exist_ok=True)
    login()

    ahora = datetime.now()
    hace_12h = ahora - timedelta(hours=12)
    inicio_dia = ahora.replace(hour=0, minute=0, second=0, microsecond=0)

    df_12h = descargar_reporte(hace_12h, ahora, "trafico live ultimas 12 horas")
    guardar_excel(df_12h, RUTA_LIVE, "Live Traffic 12h")

    try:
        df_today = descargar_reporte(inicio_dia, ahora, "trafico live del dia en curso")
        guardar_excel(df_today, RUTA_LIVE_TODAY, "Live Traffic diario")
    except Exception as exc:
        pd.DataFrame().to_excel(RUTA_LIVE_TODAY, index=False)
        print(f"No se pudo actualizar Live Traffic diario. Se guardo un Excel vacio para no publicar datos obsoletos: {exc}")


if __name__ == "__main__":
    update_live()
