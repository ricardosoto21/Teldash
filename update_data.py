import io
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from bs4 import BeautifulSoup


USUARIO = os.environ.get("SMS_USER")
CLAVE = os.environ.get("SMS_PASS")
RUTA_EXCEL = "datos/reporte_actual.xlsx"

URL_BASE = "http://65.108.69.39:5660"
URL_LOGIN = f"{URL_BASE}/Home/CheckLogin"
URL_DESCARGA = f"{URL_BASE}/DLRWholesaleReport/DownloadExcel"

DEFAULT_BACKFILL_DAYS = 7

DIMENSIONES = [
    "SubmitDate",
    "CompanyName",
    "SMPPAccountName",
    "SMPPUsername",
    "MCC",
    "MNC",
    "OperatorName",
    "DLRStatus",
    "ErrorDescription",
    "VendorAccountName",
    "SenderID",
    "CountryRealName",
    "CurrencyCode",
    "TerminationCurrencyCode",
    "SMSSource",
    "SMSType",
    "MessageType",
    "ErrorCode",
]

METRICAS = {
    "MessageParts": "sum",
    "ClientCost": "sum",
    "TerminationCost": "sum",
    "ClientCostUSD": "sum",
    "TerminationCostUSD": "sum",
    "DLRDelay": "mean",
}

RENOMBRAMIENTOS = {
    "Operator": "OperatorName",
    "ClientCurrency": "CurrencyCode",
    "VendorCurrency": "TerminationCurrencyCode",
    "TerminationCurrency": "TerminationCurrencyCode",
}

PAISES_INVALIDOS = {"", "N/A", "NA", "NAN", "NONE", "NULL"}

session = requests.Session()
session.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36"
        )
    }
)
cache_tasas = {}


def obtener_tasa_diaria(fecha_str, moneda):
    if not moneda or moneda == "USD" or pd.isna(moneda):
        return 1.0

    key = f"{fecha_str}_{moneda}"
    if key in cache_tasas:
        return cache_tasas[key]

    try:
        if moneda == "EUR":
            url = f"https://api.frankfurter.app/{fecha_str}?from=EUR&to=USD"
            res = requests.get(url, timeout=10).json()
            tasa = res["rates"]["USD"]
        elif moneda == "CLP":
            f_obj = datetime.strptime(fecha_str, "%Y-%m-%d")
            url = f"https://mindicador.cl/api/dolar/{f_obj.strftime('%d-%m-%Y')}"
            res = requests.get(url, timeout=10).json()
            tasa = 1 / res["serie"][0]["valor"]
        else:
            tasa = 1.0
        cache_tasas[key] = tasa
        return tasa
    except Exception:
        return {"EUR": 1.08, "CLP": 0.0011}.get(moneda, 1.0)


def validar_credenciales():
    if not USUARIO or not CLAVE:
        raise RuntimeError("Faltan SMS_USER o SMS_PASS en el entorno.")


def login():
    validar_credenciales()
    print("Conectando al servidor SMS...")

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
    print("Login enviado correctamente.")


def normalizar_fecha(fecha):
    if isinstance(fecha, datetime):
        return fecha.date()
    if hasattr(fecha, "year") and hasattr(fecha, "month") and hasattr(fecha, "day"):
        return fecha
    return datetime.strptime(str(fecha), "%Y-%m-%d").date()


def obtener_dias_objetivo():
    target_days = os.environ.get("TARGET_DAYS", "").strip()
    if target_days:
        return [normalizar_fecha(d.strip()) for d in target_days.split(",") if d.strip()]

    backfill_days = int(os.environ.get("BACKFILL_DAYS", DEFAULT_BACKFILL_DAYS))
    if backfill_days < 1:
        backfill_days = 1

    ayer = datetime.now().date() - timedelta(days=1)
    return [ayer - timedelta(days=offset) for offset in range(backfill_days - 1, -1, -1)]


def descargar_excel_dia(fecha):
    fecha_str = normalizar_fecha(fecha).strftime("%Y-%m-%d")
    print(f"Descargando trafico agrupado de {fecha_str}...")

    params = {"StartDate": f"{fecha_str} 00:00:00", "EndDate": f"{fecha_str} 23:59:59"}
    r = session.get(URL_DESCARGA, params=params, timeout=120)
    r.raise_for_status()

    if r.content[:2] != b"PK":
        preview = r.text[:180].replace("\n", " ").replace("\r", " ")
        raise RuntimeError(f"El servidor no entrego un Excel valido para {fecha_str}: {preview}")

    return pd.read_excel(io.BytesIO(r.content))


def preparar_resumen_dia(df, fecha):
    fecha_str = normalizar_fecha(fecha).strftime("%Y-%m-%d")
    if df.empty:
        print(f"Sin trafico para {fecha_str}.")
        return pd.DataFrame(columns=DIMENSIONES + list(METRICAS.keys()))

    df = df.rename(columns=RENOMBRAMIENTOS)
    if "CountryRealName" not in df.columns:
        df["CountryRealName"] = pd.NA

    pais = df["CountryRealName"]
    pais_normalizado = pais.astype("string").str.strip()
    pais_invalido = pais.isna() | pais_normalizado.str.upper().isin(PAISES_INVALIDOS)
    filas_sin_pais = int(pais_invalido.sum())
    if filas_sin_pais:
        print(f"{fecha_str}: {filas_sin_pais} filas sin pais real fueron excluidas del historico.")
        df = df[~pais_invalido].copy()

    if df.empty:
        print(f"{fecha_str}: no quedaron filas con pais real despues de limpiar el reporte.")
        return pd.DataFrame(columns=DIMENSIONES + list(METRICAS.keys()))

    df["CountryRealName"] = df["CountryRealName"].astype("string").str.strip().astype(str)

    if "CurrencyCode" not in df.columns:
        df["CurrencyCode"] = "USD"
    if "TerminationCurrencyCode" not in df.columns:
        df["TerminationCurrencyCode"] = "USD"

    df["CurrencyCode"] = df["CurrencyCode"].fillna("USD")
    df["TerminationCurrencyCode"] = df["TerminationCurrencyCode"].fillna("USD")
    for money_col in ("ClientCost", "TerminationCost"):
        if money_col not in df.columns:
            df[money_col] = 0.0
        df[money_col] = pd.to_numeric(df[money_col], errors="coerce").fillna(0.0)

    def aplicar_conversion(row):
        t_client = obtener_tasa_diaria(fecha_str, row["CurrencyCode"])
        t_vendor = obtener_tasa_diaria(fecha_str, row["TerminationCurrencyCode"])
        return pd.Series(
            [
                row.get("ClientCost", 0) * t_client,
                row.get("TerminationCost", 0) * t_vendor,
            ]
        )

    df[["ClientCostUSD", "TerminationCostUSD"]] = df.apply(aplicar_conversion, axis=1)
    df["SubmitDate"] = pd.to_datetime(df["SubmitDate"]).dt.date

    for col in DIMENSIONES:
        if col not in df.columns:
            df[col] = "N/A"
        if col == "CountryRealName":
            df[col] = df[col].astype("string").str.strip().astype(str)
        else:
            df[col] = df[col].fillna("N/A")

    for col in METRICAS.keys():
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    resumen = df.groupby(DIMENSIONES).agg(METRICAS).reset_index()
    print(f"{fecha_str} procesado: {len(resumen)} grupos.")
    return resumen


def descargar_resumen_dia(fecha):
    df = descargar_excel_dia(fecha)
    return preparar_resumen_dia(df, fecha)


def cargar_historico():
    if not os.path.exists(RUTA_EXCEL):
        return pd.DataFrame()

    df_hist = pd.read_excel(RUTA_EXCEL)
    if not df_hist.empty and "SubmitDate" in df_hist.columns:
        df_hist["SubmitDate"] = pd.to_datetime(df_hist["SubmitDate"]).dt.date
    return df_hist


def guardar_historico(df_hist, resumenes):
    resumenes_con_datos = [resumen for _, resumen in resumenes if not resumen.empty]
    if not resumenes_con_datos:
        print("No hay datos nuevos para guardar.")
        return False

    dias_a_reemplazar = {
        normalizar_fecha(dia)
        for dia, resumen in resumenes
        if not resumen.empty
    }

    if df_hist.empty:
        df_final = pd.concat(resumenes_con_datos, ignore_index=True)
    else:
        df_hist = df_hist[~df_hist["SubmitDate"].isin(dias_a_reemplazar)]
        df_final = pd.concat([df_hist] + resumenes_con_datos, ignore_index=True)

    if "SubmitDate" in df_final.columns:
        df_final = df_final.sort_values("SubmitDate").reset_index(drop=True)

    os.makedirs(os.path.dirname(RUTA_EXCEL), exist_ok=True)
    df_final.to_excel(RUTA_EXCEL, index=False)
    print(f"Historico actualizado. Dias reemplazados: {len(dias_a_reemplazar)}.")
    return True


def update():
    dias = obtener_dias_objetivo()
    print("Dias objetivo: " + ", ".join(d.strftime("%Y-%m-%d") for d in dias))

    login()
    df_hist = cargar_historico()

    resumenes = []
    errores = []
    for dia in dias:
        try:
            resumenes.append((dia, descargar_resumen_dia(dia)))
        except Exception as exc:
            errores.append((dia, exc))
            print(f"ERROR procesando {dia.strftime('%Y-%m-%d')}: {exc}")

    if not resumenes:
        detalle = "; ".join(f"{d.strftime('%Y-%m-%d')}: {e}" for d, e in errores)
        raise RuntimeError(f"No se pudo procesar ningun dia. {detalle}")

    guardado = guardar_historico(df_hist, resumenes)

    if errores:
        print("Advertencia: algunos dias no pudieron procesarse y se reintentaran en el proximo backfill.")
        for dia, exc in errores:
            print(f"- {dia.strftime('%Y-%m-%d')}: {exc}")

    if guardado:
        print("Actualizacion finalizada correctamente.")
    else:
        print("Actualizacion finalizada sin cambios.")


if __name__ == "__main__":
    update()
