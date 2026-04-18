"""
Cliente de logging para Agente Kowen.
Registra eventos, errores y resultados de rutinas en la hoja LOG de Google Sheets.
"""

from datetime import datetime, timedelta
from sheets_client import (
    _ensure_log_tab, _read_sheet, _append_sheet, TAB_LOG,
)


def log_event(tipo, accion, detalle="", resultado="", origen="sistema"):
    """
    Registra un evento en la hoja LOG.

    Args:
        tipo: RUTINA, IMPORT, MATCH, ERROR, CORRECCION, DRIVIN
        accion: Descripcion breve de la accion
        detalle: Informacion adicional
        resultado: Resultado (OK, ERROR, cantidad, etc.)
        origen: Quien lo hizo (sistema, dashboard, scheduler, usuario)
    """
    try:
        _ensure_log_tab()
        now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        _append_sheet(TAB_LOG, [[now, tipo, accion, detalle, resultado, origen]])
    except Exception:
        pass  # El log nunca debe romper la operacion principal


def log_rutina(resultado):
    """Registra el resultado completo de una rutina diaria."""
    bsale_pend = len(resultado.get("bsale_pendientes", []))
    resumen = (
        f"Ayer: {resultado['entregados_ayer']} entregados, "
        f"{resultado['movidos_a_hoy']} movidos. "
        f"Hoy: +{resultado['planilla_importados']} planilla, "
        f"+{resultado.get('cactus_importados', 0)} Cactus, "
        f"{resultado['codigos_asignados']} codigos, "
        f"{resultado.get('drivin_subidos', 0)} a drivin, "
        f"{bsale_pend} Bsale sin planilla."
    )
    errores = "; ".join(resultado.get("errores", []))
    log_event(
        "RUTINA", "rutina_diaria",
        detalle=f"{resultado['fecha_ayer']} -> {resultado['fecha_hoy']}",
        resultado=f"{resumen} | Errores: {errores}" if errores else resumen,
        origen="scheduler",
    )


def log_match_manual(direccion, codigo_elegido, candidatos=""):
    """Registra cuando el usuario elige manualmente un codigo driv.in."""
    log_event(
        "MATCH", "match_manual",
        detalle=f"{direccion} -> {codigo_elegido}",
        resultado=f"Candidatos: {candidatos}" if candidatos else "Asignacion directa",
        origen="dashboard",
    )


def log_error(accion, error, detalle=""):
    """Registra un error."""
    log_event("ERROR", accion, detalle=detalle, resultado=str(error), origen="sistema")


def get_errores_recurrentes(dias=7):
    """
    Analiza el log y detecta errores que se repiten.

    Returns:
        Lista de dicts con {accion, conteo, ultimo_error, ejemplo}.
    """
    try:
        _ensure_log_tab()
        rows = _read_sheet(TAB_LOG)
        if len(rows) < 2:
            return []

        limite = datetime.now() - timedelta(days=dias)

        errores = {}
        for row in rows[1:]:
            if len(row) < 5:
                continue
            fecha_str = row[0] if row[0] else ""
            tipo = row[1] if len(row) > 1 else ""
            accion = row[2] if len(row) > 2 else ""
            detalle = row[3] if len(row) > 3 else ""
            resultado = row[4] if len(row) > 4 else ""

            if tipo != "ERROR":
                continue

            try:
                fecha = datetime.strptime(fecha_str, "%d/%m/%Y %H:%M:%S")
                if fecha < limite:
                    continue
            except (ValueError, TypeError):
                continue

            key = accion
            if key not in errores:
                errores[key] = {"accion": accion, "conteo": 0, "ultimo_error": "", "ejemplo": ""}
            errores[key]["conteo"] += 1
            errores[key]["ultimo_error"] = fecha_str
            errores[key]["ejemplo"] = detalle or resultado

        return [e for e in errores.values() if e["conteo"] >= 2]
    except Exception:
        return []
