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


def _iso_date(fecha_ddmmyyyy):
    """'DD/MM/YYYY' -> 'YYYY-MM-DD'. Retorna None si falla."""
    parts = (fecha_ddmmyyyy or "").split("/")
    if len(parts) != 3:
        return None
    try:
        return f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
    except Exception:
        return None


def get_ruta_del_dia(fecha_str):
    """
    Devuelve la ruta operativa del dia cruzando OPERACION DIARIA con el
    scenario drivin del dia (solo lectura, no modifica nada).

    Returns:
        Dict con:
          - pedidos: lista de dicts con campos
                (numero, fecha, direccion, depto, comuna, cliente, telefono,
                 cantidad, marca, codigo_drivin, repartidor, estado, estado_pago,
                 forma_pago, monto, observaciones, origen, en_drivin)
            ordenados: entregados+pagados al final, resto por estado.
          - stats: {total, pendientes, en_camino, entregados, no_entregados,
                    pagados, por_cobrar, completos}
          - scenario: {existe, description, token, status} del plan drivin
          - solo_en_drivin: pedidos presentes en el scenario sin fila en
                            OPERACION DIARIA (address_code, description, units)
    """
    pedidos = sheets_client.get_pedidos(fecha_str)

    # Intentar leer scenario drivin del dia (best-effort; si falla, seguimos)
    scenario_info = {"existe": False, "description": "", "token": "", "status": ""}
    drivin_codes = set()
    solo_en_drivin = []
    api_date = _iso_date(fecha_str)
    if api_date:
        try:
            import drivin_client
            scenarios = drivin_client.get_scenarios_by_date(api_date).get("response", []) or []
            # Preferimos el scenario que termina en "API" (creado por el agente),
            # si no hay cualquier scenario del dia.
            chosen = None
            for s in scenarios:
                if str(s.get("description", "")).endswith("API"):
                    chosen = s
                    break
            if not chosen and scenarios:
                chosen = scenarios[0]

            if chosen:
                token = chosen.get("token") or chosen.get("scenario_token", "")
                scenario_info = {
                    "existe": True,
                    "description": chosen.get("description", ""),
                    "token": token,
                    "status": chosen.get("status", ""),
                }
                if token:
                    orders = drivin_client.get_orders(token).get("response", []) or []
                    for o in orders:
                        code = (o.get("code", "") or "").strip()
                        if code:
                            drivin_codes.add(code)

                    # Codigos que no aparecen en pedidos planilla
                    planilla_codes = {
                        (p.get("Codigo Drivin", "") or "").strip() for p in pedidos
                        if (p.get("Codigo Drivin", "") or "").strip()
                    }
                    for o in orders:
                        code = (o.get("code", "") or "").strip()
                        if not code or code in planilla_codes:
                            continue
                        nested = (o.get("orders") or [{}])[0]
                        solo_en_drivin.append({
                            "codigo_drivin": code,
                            "direccion": o.get("address_1", ""),
                            "comuna": o.get("area_level_3", ""),
                            "descripcion": nested.get("description", ""),
                            "cantidad": nested.get("units_1", 0) or 0,
                        })
        except Exception:
            # Si drivin no responde, el reporte sigue funcionando con planilla sola
            pass

    # Armar vista unificada
    out = []
    for p in pedidos:
        codigo = (p.get("Codigo Drivin", "") or "").strip()
        out.append({
            "numero": p.get("#", ""),
            "fecha": p.get("Fecha", ""),
            "direccion": p.get("Direccion", ""),
            "depto": p.get("Depto", ""),
            "comuna": p.get("Comuna", ""),
            "cliente": p.get("Cliente", ""),
            "telefono": p.get("Telefono", ""),
            "cantidad": p.get("Cant", ""),
            "marca": p.get("Marca", ""),
            "codigo_drivin": codigo,
            "repartidor": p.get("Repartidor", ""),
            "estado": _estado(p) or "PENDIENTE",
            "estado_pago": _pago_estado(p) or "PENDIENTE",
            "forma_pago": p.get("Forma Pago", ""),
            "monto": _monto_pedido(p),
            "observaciones": p.get("Observaciones", ""),
            "canal": p.get("Canal", ""),
            "origen": "planilla",
            "en_drivin": bool(codigo and codigo in drivin_codes),
        })

    # Orden: PENDIENTE -> EN CAMINO -> NO ENTREGADO -> ENTREGADO(pendiente pago) -> COMPLETO
    def _orden_key(r):
        est = r["estado"]
        pag = r["estado_pago"]
        completo = est == "ENTREGADO" and pag == "PAGADO"
        if completo:
            return (4, r["numero"])
        if est == "ENTREGADO":
            return (3, r["numero"])
        if est == "NO ENTREGADO":
            return (2, r["numero"])
        if est == "EN CAMINO":
            return (1, r["numero"])
        return (0, r["numero"])
    out.sort(key=_orden_key)

    def _cant_of(r):
        try:
            return int(str(r.get("cantidad", 0) or 0).strip() or 0)
        except (ValueError, TypeError):
            return 0

    bot_total = sum(_cant_of(r) for r in out)
    bot_entregados = sum(_cant_of(r) for r in out if r["estado"] == "ENTREGADO")
    bot_cobrados = sum(_cant_of(r) for r in out if r["estado_pago"] == "PAGADO")
    # Por cobrar = botellones ya entregados que aun no estan pagados.
    # (No usar entregados - cobrados: puede ser negativo si hay pagos adelantados)
    bot_por_cobrar_real = sum(
        _cant_of(r) for r in out
        if r["estado"] == "ENTREGADO" and r["estado_pago"] != "PAGADO"
    )

    # Stats
    stats = {
        "total": len(out),
        "pendientes": sum(1 for r in out if r["estado"] == "PENDIENTE"),
        "en_camino": sum(1 for r in out if r["estado"] == "EN CAMINO"),
        "entregados": sum(1 for r in out if r["estado"] == "ENTREGADO"),
        "no_entregados": sum(1 for r in out if r["estado"] == "NO ENTREGADO"),
        "pagados": sum(1 for r in out if r["estado_pago"] == "PAGADO"),
        "por_cobrar": sum(
            r["monto"] for r in out
            if r["estado"] == "ENTREGADO" and r["estado_pago"] != "PAGADO"
        ),
        "completos": sum(
            1 for r in out
            if r["estado"] == "ENTREGADO" and r["estado_pago"] == "PAGADO"
        ),
        "botellones_total": bot_total,
        "botellones_entregados": bot_entregados,
        "botellones_cobrados": bot_cobrados,
        "botellones_por_cobrar": bot_por_cobrar_real,
    }
    stats["pct_entregados"] = (
        round(stats["entregados"] / stats["total"] * 100) if stats["total"] else 0
    )
    stats["pct_completos"] = (
        round(stats["completos"] / stats["total"] * 100) if stats["total"] else 0
    )
    stats["pct_bot_entregados"] = (
        round(bot_entregados / bot_total * 100) if bot_total else 0
    )
    stats["pct_bot_cobrados"] = (
        round(bot_cobrados / bot_total * 100) if bot_total else 0
    )

    return {
        "pedidos": out,
        "stats": stats,
        "scenario": scenario_info,
        "solo_en_drivin": solo_en_drivin,
    }
