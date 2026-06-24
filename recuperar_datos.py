import os
from datetime import timedelta

import pandas as pd

from update_data import (
    RUTA_EXCEL,
    cargar_historico,
    descargar_resumen_dia,
    guardar_historico,
    login,
    normalizar_fecha,
)


DEFAULT_RESCUE_LOOKBACK_DAYS = 14


def obtener_dias_desde_env():
    raw = os.environ.get("DIAS_FALTANTES", "").strip() or os.environ.get("TARGET_DAYS", "").strip()
    if not raw:
        return []
    return [normalizar_fecha(d.strip()) for d in raw.split(",") if d.strip()]


def detectar_huecos_recientes(df_hist):
    if df_hist.empty or "SubmitDate" not in df_hist.columns:
        raise RuntimeError(f"No se encontro historico valido en {RUTA_EXCEL}.")

    fechas = pd.to_datetime(df_hist["SubmitDate"], errors="coerce").dropna().dt.date
    if fechas.empty:
        raise RuntimeError(f"No hay fechas validas en {RUTA_EXCEL}.")

    max_date = max(fechas)
    lookback_days = int(os.environ.get("RESCUE_LOOKBACK_DAYS", DEFAULT_RESCUE_LOOKBACK_DAYS))
    if lookback_days < 1:
        lookback_days = 1

    start_date = max_date - timedelta(days=lookback_days - 1)
    fechas_en_ventana = {d for d in fechas if start_date <= d <= max_date}

    faltantes = []
    current = start_date
    while current <= max_date:
        if current not in fechas_en_ventana:
            faltantes.append(current)
        current += timedelta(days=1)

    return faltantes


def obtener_dias_rescate(df_hist):
    dias_env = obtener_dias_desde_env()
    if dias_env:
        return dias_env
    return detectar_huecos_recientes(df_hist)


def recuperar():
    df_hist = cargar_historico()
    dias = obtener_dias_rescate(df_hist)

    if not dias:
        print("No se detectaron dias faltantes recientes para rescatar.")
        return

    print("Dias de rescate: " + ", ".join(d.strftime("%Y-%m-%d") for d in dias))
    login()

    resumenes = []
    errores = []
    for dia in dias:
        try:
            resumenes.append((dia, descargar_resumen_dia(dia)))
        except Exception as exc:
            errores.append((dia, exc))
            print(f"ERROR recuperando {dia.strftime('%Y-%m-%d')}: {exc}")

    if not resumenes:
        detalle = "; ".join(f"{d.strftime('%Y-%m-%d')}: {e}" for d, e in errores)
        raise RuntimeError(f"No se pudo recuperar ningun dia. {detalle}")

    guardado = guardar_historico(df_hist, resumenes)

    if errores:
        print("Advertencia: algunos dias no pudieron recuperarse.")
        for dia, exc in errores:
            print(f"- {dia.strftime('%Y-%m-%d')}: {exc}")

    if guardado:
        print("Rescate finalizado correctamente.")
    else:
        print("Rescate finalizado sin cambios.")


if __name__ == "__main__":
    recuperar()
