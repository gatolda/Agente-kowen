"""
Reportes operativos diarios para el dashboard.
Lee de la planilla Pedidos 2026 (OPERACION DIARIA y PAGOS).
"""

from datetime import datetime, date
import sheets_client

PRECIO_BOTELLON = 2990


def _parse_fecha(s):
    """Parsea 'DD/MM/YYYY' a date. Devuelve None si falla."""
    if not s:
        return None
    try:
        return datetime.strptime(s.strip(), "%d/%m/%Y").date()
    except (ValueError, AttributeError):
        return None


def _monto_pedido(p):
    """Monto estimado del pedido (Cant x $2990). Si ya esta pagado, usa el real."""
    try:
        cant = int(p.get("Cant", 0) or 0)
    except (ValueError, TypeError):
        cant = 0
    if (p.get("Estado Pago", "") or "").upper() == "PAGADO":
        try:
            ef = int((p.get("Efectivo", "") or "0").replace(".", "").replace(",", "") or 0)
        except (ValueError, TypeError):
            ef = 0
        try:
            tr = int((p.get("Transferencia", "") or "0").replace(".", "").replace(",", "") or 0)
        except (ValueError, TypeError):
            tr = 0
        if ef + tr > 0:
            return ef + tr
    return cant * PRECIO_BOTELLON


def _estado(p):
    return (p.get("Estado Pedido", "") or "").upper().strip()


def _pago_estado(p):
    return (p.get("Estado Pago", "") or "").upper().strip()


def get_kpis(fecha_str):
    """
    KPIs para un dia: pendientes, en camino, entregados, rechazados, botellones.
    fecha_str: "DD/MM/YYYY"
    """
    pedidos = sheets_client.get_pedidos(fecha_str)
    kpis = {
        "pendientes": 0,
        "en_camino": 0,
        "entregados": 0,
        "rechazados": 0,
        "botellones": 0,
        "kowen": 0,
        "cactus": 0,
        "total": len(pedidos),
    }
    for p in pedidos:
        est = _estado(p)
        if est == "PENDIENTE":
            kpis["pendientes"] += 1
        elif est == "EN CAMINO":
            kpis["en_camino"] += 1
        elif est == "ENTREGADO":
            kpis["entregados"] += 1
            try:
                cant = int(p.get("Cant", 0) or 0)
            except (ValueError, TypeError):
                cant = 0
            kpis["botellones"] += cant
            marca = (p.get("Marca", "") or "").lower()
            if "cactus" in marca:
                kpis["cactus"] += cant
            else:
                kpis["kowen"] += cant
        elif est == "NO ENTREGADO":
            kpis["rechazados"] += 1
    return kpis


def get_sin_cobrar(hoy=None):
    """
    Pedidos entregados aun sin pagar, agrupados por atraso.

    Args:
        hoy: date de referencia (default: hoy).

    Returns:
        Dict con 3 listas: 'hoy', 'ayer', 'atrasados' (cada una ordenada).
        Cada pedido lleva campos extra: 'atraso_dias', 'monto_estimado'.
    """
    if hoy is None:
        hoy = date.today()

    pedidos = sheets_client.get_pedidos()
    sin_cobrar = {"hoy": [], "ayer": [], "atrasados": []}

    for p in pedidos:
        if _estado(p) != "ENTREGADO":
            continue
        if _pago_estado(p) == "PAGADO":
            continue

        fecha = _parse_fecha(p.get("Fecha", ""))
        if not fecha:
            continue

        atraso = (hoy - fecha).days
        if atraso < 0:
            continue

        p_view = dict(p)
        p_view["atraso_dias"] = atraso
        p_view["monto_estimado"] = _monto_pedido(p)

        if atraso == 0:
            sin_cobrar["hoy"].append(p_view)
        elif atraso == 1:
            sin_cobrar["ayer"].append(p_view)
        else:
            sin_cobrar["atrasados"].append(p_view)

    # Ordenar: atrasados por mayor atraso primero; hoy/ayer por numero
    sin_cobrar["atrasados"].sort(key=lambda x: -x["atraso_dias"])
    sin_cobrar["ayer"].sort(key=lambda x: str(x.get("#", "")))
    sin_cobrar["hoy"].sort(key=lambda x: str(x.get("#", "")))

    return sin_cobrar


def get_entregas_por_repartidor(fecha_str):
    """
    Estadisticas por repartidor para un dia.

    Returns:
        Lista de dicts con: repartidor, asignados, entregados, rechazados,
        en_ruta, pct_cumplimiento.
    """
    pedidos = sheets_client.get_pedidos(fecha_str)
    stats = {}
    for p in pedidos:
        rep = (p.get("Repartidor", "") or "Sin asignar").strip() or "Sin asignar"
        if rep not in stats:
            stats[rep] = {"repartidor": rep, "asignados": 0, "entregados": 0,
                          "rechazados": 0, "en_ruta": 0}
        stats[rep]["asignados"] += 1
        est = _estado(p)
        if est == "ENTREGADO":
            stats[rep]["entregados"] += 1
        elif est == "NO ENTREGADO":
            stats[rep]["rechazados"] += 1
        elif est in ("EN CAMINO", "PENDIENTE"):
            stats[rep]["en_ruta"] += 1

    result = list(stats.values())
    for s in result:
        s["pct_cumplimiento"] = (
            round(s["entregados"] / s["asignados"] * 100) if s["asignados"] else 0
        )
    result.sort(key=lambda x: -x["entregados"])
    return result


def get_pagos_recibidos(fecha_str):
    """
    Pagos registrados en un dia.
    fecha_str: "DD/MM/YYYY"
    """
    return sheets_client.get_pagos(fecha_str)
