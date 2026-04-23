"""
Scheduler del Agente Kowen.
Automatiza la operacion durante horario laboral (Lun-Vie 8:00-19:00).

Tareas:
  - 08:00  Rutina diaria completa (importar, asignar codigos, subir plan)
  - Cada 15 min  Importar pedidos nuevos (Bsale + planillas)
  - Cada 30 min  Verificar estados contra driv.in (PODs)
  - 18:30  Resumen de cierre del dia

Uso:
    python scheduler.py              # Ejecuta la rutina una vez (para probar)
    python scheduler.py --daemon     # Corre en segundo plano con todas las tareas
    python scheduler.py --status     # Muestra estado actual
"""

import os
import sys
import time
import logging
from datetime import datetime

import config
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
import observability

# --- Logging ---

LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "agente_kowen.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("kowen")


# --- Configuracion ---

HORA_INICIO = 8    # Inicio jornada laboral
HORA_FIN = 19      # Fin jornada laboral
HORA_CIERRE = 18   # Hora del resumen de cierre (18:30)
INTERVALO_IMPORTAR = 15   # Minutos entre importaciones
INTERVALO_VERIFICAR = 30  # Minutos entre verificaciones driv.in
INTERVALO_EMAILS = 30     # Minutos entre lecturas de Gmail
DIAS_LABORALES = {0, 1, 2, 3, 4}  # Lunes=0 a Viernes=4


# --- Dedup de alertas (evita spamear Telegram con la misma alerta) ---

_alertas_enviadas = {
    "fecha": None,
    "huerfanos": set(),           # pedido# ya alertados como huerfanos
    "sin_pedido": set(),          # keys de filas PAGOS sin pedido ya alertadas
    "planes_sin_despachar": set(),  # nombres de plan ya alertados
    "estancados": set(),          # pedido# ya alertados como estancados
}


def _reset_alertas_si_nuevo_dia():
    """Limpia el set de alertas al cambiar de dia."""
    hoy = config.now().date()
    if _alertas_enviadas["fecha"] != hoy:
        _alertas_enviadas["huerfanos"].clear()
        _alertas_enviadas["sin_pedido"].clear()
        _alertas_enviadas["planes_sin_despachar"].clear()
        _alertas_enviadas["estancados"].clear()
        _alertas_enviadas["fecha"] = hoy


def es_horario_laboral():
    """Verifica si estamos en horario laboral."""
    ahora = config.now()
    return (ahora.weekday() in DIAS_LABORALES
            and HORA_INICIO <= ahora.hour < HORA_FIN)


# --- Notificaciones Telegram ---

def notificar(mensaje):
    """Envia notificacion por Telegram si esta configurado."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    try:
        import requests
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": mensaje,
            "parse_mode": "Markdown",
        }, timeout=10)
    except Exception as e:
        log.warning(f"Error enviando Telegram: {e}")


# --- Tarea 1: Rutina diaria (08:00) ---

def ejecutar_rutina():
    """Ejecuta la rutina diaria completa."""
    import operations

    hoy = config.now().strftime("%d/%m/%Y")
    log.info(f"=== RUTINA DIARIA - {hoy} ===")

    try:
        resultado = operations.rutina_diaria(fecha_hoy=hoy)
    except Exception as e:
        log.error(f"Error critico en rutina diaria: {e}", exc_info=True)
        observability.capture_exception(e)
        observability.ping_healthcheck(fail=True, msg=f"rutina_diaria fallo: {e}")
        notificar(f"*Error en rutina diaria:*\n{e}")
        return None

    # Log del resultado
    bsale_pend = resultado.get("bsale_pendientes", [])
    log.info(f"Ayer ({resultado['fecha_ayer']}): "
             f"{resultado['entregados_ayer']} entregados, "
             f"{resultado['movidos_a_hoy']} movidos")
    log.info(f"Hoy ({resultado['fecha_hoy']}): "
             f"Planilla +{resultado['planilla_importados']}, "
             f"Cactus +{resultado.get('cactus_importados', 0)}, "
             f"Codigos {resultado['codigos_asignados']}, "
             f"driv.in {resultado.get('drivin_subidos', 0)}, "
             f"Bsale sin planilla: {len(bsale_pend)}")

    if resultado["errores"]:
        for err in resultado["errores"]:
            log.warning(f"  Advertencia: {err}")

    # Notificar por Telegram
    import sheets_client
    pedidos_hoy = sheets_client.get_pedidos(hoy)
    total = len(pedidos_hoy)
    total_bot = sum(int(p.get("Cant", 0) or 0) for p in pedidos_hoy)

    msg = (
        f"*Rutina diaria completada*\n\n"
        f"*{hoy}*: {total} pedidos, {total_bot} botellones\n"
        f"Planilla: +{resultado['planilla_importados']}\n"
        f"Cactus: +{resultado.get('cactus_importados', 0)}\n"
        f"Codigos: {resultado['codigos_asignados']}\n"
        f"driv.in: {resultado.get('drivin_subidos', 0)} subidos"
    )

    # Alertar de pedidos Bsale que no estan en la planilla
    if bsale_pend:
        msg += f"\n\n*{len(bsale_pend)} pedidos Bsale sin planilla:*"
        for p in bsale_pend[:8]:
            dir_str = p.get("direccion", "")[:30]
            msg += f"\n- #{p.get('pedido_nro', '')} {dir_str} ({p.get('cantidad', 0)})"
        if len(bsale_pend) > 8:
            msg += f"\n...y {len(bsale_pend) - 8} mas"
        msg += "\n_Pasar a la planilla para operar._"

    if resultado["errores"]:
        msg += "\n\n*Advertencias:*\n" + "\n".join(f"- {e}" for e in resultado["errores"])

    notificar(msg)
    observability.ping_healthcheck(msg=f"rutina {hoy} ok")
    log.info("=== FIN RUTINA ===")

    return resultado


# --- Tarea 2: Importar pedidos nuevos (cada 15 min) ---

def importar_nuevos():
    """
    Importa pedidos nuevos desde las planillas (fuentes de verdad operativas).

    Nota: NO se importa Bsale automaticamente. La regla del negocio es que
    la planilla Kowen (PRIMER TURNO) manda — los pedidos web se pasan
    manualmente a la planilla antes de ser operativos. Para alertar de
    pedidos Bsale pendientes de validacion, ver check_bsale_pendientes()
    y el paso 4 de la rutina diaria.
    """
    import operations

    hoy = config.now().strftime("%d/%m/%Y")
    total_importados = 0

    # Planilla Kowen
    try:
        count = operations.sync_from_planilla_reparto(fecha=hoy)
        if count > 0:
            log.info(f"Planilla Kowen: +{count} pedidos nuevos")
            total_importados += count
    except Exception as e:
        log.warning(f"Error importando planilla Kowen: {e}")

    # Planilla Cactus
    try:
        count = operations.sync_from_planilla_cactus(fecha=hoy)
        if count > 0:
            log.info(f"Planilla Cactus: +{count} pedidos nuevos")
            total_importados += count
    except Exception as e:
        log.warning(f"Error importando planilla Cactus: {e}")

    # Auto-asignar codigos a los nuevos
    if total_importados > 0:
        try:
            import address_matcher
            pedidos_hoy = sheets_client.get_pedidos(hoy)
            sin_codigo = [p for p in pedidos_hoy
                          if not p.get("Codigo Drivin", "").strip()
                          and p.get("Estado Pedido") == "PENDIENTE"]

            if sin_codigo:
                addresses = address_matcher.load_cache()
                updates = []
                for p in sin_codigo:
                    nro = p.get("#", "")
                    if not nro or not nro.isdigit():
                        continue
                    code, confidence = address_matcher.auto_match(
                        p.get("Direccion", ""), p.get("Depto", ""),
                        p.get("Comuna", ""), addresses,
                    )
                    if confidence == "auto" and code:
                        updates.append((int(nro), {"codigo_drivin": code}))

                if updates:
                    sheets_client.update_pedidos_batch(updates)
                    log.info(f"Codigos asignados: {len(updates)}")
        except Exception as e:
            log.warning(f"Error asignando codigos: {e}")

        # Notificar
        notificar(f"*{total_importados} pedidos nuevos importados*\n{hoy}")

    return total_importados


# --- Tarea 3: Verificar estados driv.in (cada 30 min) ---

def verificar_estados():
    """Verifica estados de pedidos contra driv.in."""
    import operations

    _reset_alertas_si_nuevo_dia()
    hoy = config.now().strftime("%d/%m/%Y")

    try:
        v = operations.verify_orders_drivin(fecha=hoy, auto_update=True)
    except Exception as e:
        log.warning(f"Error verificando driv.in: {e}")
        return None

    if v["actualizados"] > 0:
        log.info(f"driv.in: {v['actualizados']} pedidos actualizados "
                 f"({v['entregados_detectados']} entregados, "
                 f"{v['no_entregados_detectados']} no entregados, "
                 f"{v['en_camino_detectados']} en camino)")

        # Notificar cambios importantes
        msg = f"*Actualizacion driv.in*\n"
        if v["entregados_detectados"]:
            msg += f"Entregados: +{v['entregados_detectados']}\n"
        if v["no_entregados_detectados"]:
            msg += f"No entregados: {v['no_entregados_detectados']}\n"
        if v["en_camino_detectados"]:
            msg += f"En camino: {v['en_camino_detectados']}\n"
        efectivo = v.get("efectivo_detectado", 0)
        if efectivo:
            msg += f"Efectivo (POD): {efectivo}\n"
        for d in v["detalle"][:5]:
            msg += f"  #{d['numero']} {d['direccion'][:25]}: {d['estado_nuevo']}\n"
        notificar(msg)

    # Alertar pedidos estancados NUEVOS (no alertados hoy aun)
    estancados_nuevos = [
        e for e in v.get("estancados", [])
        if str(e.get("numero", "")) not in _alertas_enviadas["estancados"]
    ]
    # Solo alertar a partir de las 10:00 para no molestar temprano
    if config.now().hour >= 10 and estancados_nuevos:
        msg = f"*{len(estancados_nuevos)} pedidos estancados nuevos:*\n"
        for e in estancados_nuevos[:10]:
            msg += f"  #{e['numero']} {e['direccion'][:25]} ({e['dias']}d)\n"
        notificar(msg)
        log.warning(f"{len(estancados_nuevos)} estancados nuevos detectados")
        for e in estancados_nuevos:
            _alertas_enviadas["estancados"].add(str(e.get("numero", "")))

    # Alertar planes creados pero no despachados (cada plan solo una vez al dia)
    planes_nuevos = [
        p for p in v.get("planes_sin_despachar", [])
        if p.get("plan", "") not in _alertas_enviadas["planes_sin_despachar"]
    ]
    if planes_nuevos:
        msg = f"*{len(planes_nuevos)} plan(es) sin despachar:*\n"
        for p in planes_nuevos[:5]:
            msg += f"  {p['plan']} ({p.get('status', '')})\n"
        msg += "\n_Revisar si falta iniciar rutas en driv.in._"
        notificar(msg)
        log.warning(f"{len(planes_nuevos)} planes sin despachar detectados")
        for p in planes_nuevos:
            _alertas_enviadas["planes_sin_despachar"].add(p.get("plan", ""))

    return v


# --- Tarea 3.5: Procesar emails (cada 30 min) ---

def _alertar_reconciliacion(rec):
    """
    Emite alertas Telegram sobre el resultado de reconciliar_pagos.
    Usa dedup por dia para no spamear — cada huerfano/sin_pedido se alerta
    solo la primera vez que se detecta en el dia.
    """
    huerfanos = rec.get("huerfanos", [])
    sin_pedido = rec.get("sin_pedido", [])

    # Huerfanos: pedidos marcados PAGADO sin fila en PAGOS (ingresos manuales sospechosos)
    huerfanos_nuevos = [
        h for h in huerfanos
        if str(h.get("numero", "")) not in _alertas_enviadas["huerfanos"]
    ]
    if huerfanos_nuevos:
        msg = f"*{len(huerfanos_nuevos)} pedido(s) PAGADO sin fila en PAGOS:*\n"
        for h in huerfanos_nuevos[:10]:
            cliente = (h.get("cliente", "") or "")[:25]
            msg += (f"  #{h['numero']} {cliente} "
                    f"({h.get('forma_pago', '') or 's/forma'} - {h.get('fecha', '')})\n")
        msg += "\n_Revisar si son pagos validos o errores._"
        notificar(msg)
        for h in huerfanos_nuevos:
            _alertas_enviadas["huerfanos"].add(str(h.get("numero", "")))

    # Filas PAGOS con pedido# que no existe en OPERACION DIARIA
    sin_pedido_keys = {
        f"{s.get('pedido_num', '')}-{s.get('pago_fecha', '')}" for s in sin_pedido
    }
    sin_pedido_nuevos_keys = sin_pedido_keys - _alertas_enviadas["sin_pedido"]
    if sin_pedido_nuevos_keys:
        nuevos = [
            s for s in sin_pedido
            if f"{s.get('pedido_num', '')}-{s.get('pago_fecha', '')}" in sin_pedido_nuevos_keys
        ]
        msg = f"*{len(nuevos)} fila(s) PAGOS apuntan a pedido inexistente:*\n"
        for s in nuevos[:10]:
            msg += (f"  #{s['pedido_num']} ${s.get('pago_monto', '')} "
                    f"({s.get('pago_fecha', '')})\n")
        msg += "\n_Pedido# mal escrito en PAGOS._"
        notificar(msg)
        _alertas_enviadas["sin_pedido"].update(sin_pedido_nuevos_keys)


def procesar_emails():
    """Lee emails no leidos, clasifica y concilia pagos."""
    import payments
    import operations

    _reset_alertas_si_nuevo_dia()

    try:
        r = payments.procesar_emails_no_leidos(max_emails=30)
    except Exception as e:
        log.warning(f"Error procesando emails: {e}")
        return None

    # Reconciliar PAGOS <-> OPERACION DIARIA por si hay confirmaciones manuales pendientes
    try:
        rec = operations.reconciliar_pagos()
        if rec["actualizados"] > 0:
            log.info(f"Reconciliacion pagos: {rec['actualizados']} pedidos actualizados")
        _alertar_reconciliacion(rec)
    except Exception as e:
        log.warning(f"Error reconciliando pagos: {e}")

    if r["total"] == 0:
        return r

    cats = r.get("por_categoria", {})
    por_confirmar = r.get("pagos_por_confirmar", [])
    alertas = r.get("alertas", [])
    dup = r.get("duplicados", 0)

    log.info(f"Emails: {r['total']} leidos | "
             f"por confirmar: {len(por_confirmar)}, "
             f"duplicados: {dup}, "
             f"alertas: {len(alertas)}")

    if por_confirmar or alertas:
        msg = "*Emails procesados*\n"
        resumen_cats = ", ".join(f"{k}:{v}" for k, v in cats.items())
        if resumen_cats:
            msg += f"\n{resumen_cats}\n"

        if por_confirmar:
            msg += f"\n*Pagos por confirmar ({len(por_confirmar)}):*"
            for p in por_confirmar[:5]:
                pago = p["pago"]
                top = p["candidatos"][0] if p["candidatos"] else None
                sugerencia = (
                    f"-> #{top['numero']} {top['cliente'][:20]} ({top['score']}%)"
                    if top else "(sin candidatos)"
                )
                msg += (f"\n- {pago.get('remitente_nombre', '')[:25]} "
                        f"${pago.get('monto', '')} {sugerencia}")
            msg += "\n\nConfirmar en el dashboard."

        if alertas:
            msg += f"\n\n*Alertas ({len(alertas)}):*"
            for a in alertas[:5]:
                msg += f"\n- [{a['categoria']}] {a['subject'][:40]}"

        notificar(msg)

    return r


# --- Tarea 4: Resumen de cierre (18:30) ---

def resumen_cierre():
    """Genera y envia resumen de cierre del dia."""
    import operations

    hoy = config.now().strftime("%d/%m/%Y")

    try:
        r = operations.resumen_dia(hoy)
    except Exception as e:
        log.error(f"Error generando resumen: {e}")
        return

    log.info(f"=== CIERRE DEL DIA {hoy} ===")
    log.info(f"Total: {r['total_pedidos']} pedidos, {r['total_botellones']} botellones")
    log.info(f"Entregados: {r['entregados']}, Pendientes: {r['pendientes']}, "
             f"No entregados: {r['no_entregados']}")

    tasa = 0
    if r["total_pedidos"] > 0:
        tasa = round(r["entregados"] / r["total_pedidos"] * 100)

    msg = (
        f"*Cierre del dia {hoy}*\n\n"
        f"Total: {r['total_pedidos']} pedidos, {r['total_botellones']} botellones\n"
        f"Entregados: {r['entregados']} ({tasa}%)\n"
        f"Pendientes: {r['pendientes']}\n"
        f"En camino: {r['en_camino']}\n"
        f"No entregados: {r['no_entregados']}\n\n"
        f"Pagados: {r['pagados']} | Por cobrar: {r['por_cobrar']}"
    )
    notificar(msg)


# --- Loop principal ---

def daemon():
    """Loop principal del scheduler."""
    log.info("=" * 50)
    log.info("Agente Kowen - Scheduler iniciado")
    log.info(f"Horario: Lun-Vie {HORA_INICIO}:00 - {HORA_FIN}:00")
    log.info(f"Importar cada {INTERVALO_IMPORTAR} min")
    log.info(f"Verificar cada {INTERVALO_VERIFICAR} min")
    log.info("=" * 50)

    notificar("*Agente Kowen iniciado*\nScheduler corriendo.")

    rutina_hecha_hoy = None
    cierre_hecho_hoy = None
    ultima_importacion = None
    ultima_verificacion = None
    ultima_emails = None

    while True:
        ahora = config.now()
        hoy = ahora.date()

        # --- Rutina diaria (una vez al dia, a las 8:00+) ---
        if (es_horario_laboral()
                and ahora.hour >= HORA_INICIO
                and rutina_hecha_hoy != hoy):
            log.info("Ejecutando rutina diaria...")
            ejecutar_rutina()
            rutina_hecha_hoy = hoy
            ultima_importacion = ahora
            ultima_verificacion = ahora

        # --- Importar pedidos nuevos (cada 15 min) ---
        elif (es_horario_laboral()
              and rutina_hecha_hoy == hoy
              and (ultima_importacion is None
                   or (ahora - ultima_importacion).seconds >= INTERVALO_IMPORTAR * 60)):
            importar_nuevos()
            ultima_importacion = ahora

        # --- Verificar estados driv.in (cada 30 min) ---
        if (es_horario_laboral()
                and rutina_hecha_hoy == hoy
                and (ultima_verificacion is None
                     or (ahora - ultima_verificacion).seconds >= INTERVALO_VERIFICAR * 60)):
            verificar_estados()
            ultima_verificacion = ahora

        # --- Procesar emails / pagos (cada 30 min) ---
        if (es_horario_laboral()
                and rutina_hecha_hoy == hoy
                and (ultima_emails is None
                     or (ahora - ultima_emails).seconds >= INTERVALO_EMAILS * 60)):
            procesar_emails()
            ultima_emails = ahora

        # --- Resumen de cierre (18:30, una vez al dia) ---
        if (ahora.weekday() in DIAS_LABORALES
                and ahora.hour == HORA_CIERRE
                and ahora.minute >= 30
                and cierre_hecho_hoy != hoy):
            resumen_cierre()
            cierre_hecho_hoy = hoy

        # Dormir 60 segundos
        time.sleep(60)


# --- Status ---

def mostrar_status():
    """Muestra el estado actual del sistema."""
    import operations
    import sheets_client

    hoy = config.now().strftime("%d/%m/%Y")
    print(f"\n=== Estado del sistema - {hoy} ===\n")

    pedidos = sheets_client.get_pedidos(hoy)
    if not pedidos:
        print("No hay pedidos para hoy.")
        return

    total_bot = sum(int(p.get("Cant", 0) or 0) for p in pedidos)
    entregados = sum(1 for p in pedidos if p.get("Estado Pedido") == "ENTREGADO")
    pendientes = sum(1 for p in pedidos if p.get("Estado Pedido") == "PENDIENTE")
    en_camino = sum(1 for p in pedidos if p.get("Estado Pedido") == "EN CAMINO")
    con_codigo = sum(1 for p in pedidos if p.get("Codigo Drivin", "").strip())
    con_plan = sum(1 for p in pedidos if p.get("Plan Drivin", "").strip())

    print(f"Pedidos: {len(pedidos)} | Botellones: {total_bot}")
    print(f"Entregados: {entregados} | Pendientes: {pendientes} | En camino: {en_camino}")
    print(f"Con codigo: {con_codigo} | Con plan: {con_plan}")
    print(f"\nHorario laboral: {'SI' if es_horario_laboral() else 'NO'}")
    print(f"Hora actual: {config.now().strftime('%H:%M')}")


# --- Entry point ---

if __name__ == "__main__":
    observability.init_sentry(component="scheduler")
    if "--daemon" in sys.argv:
        daemon()
    elif "--status" in sys.argv:
        mostrar_status()
    else:
        resultado = ejecutar_rutina()
        if resultado:
            print("\nResumen:")
            for k, v in resultado.items():
                if k != "errores":
                    print(f"  {k}: {v}")
            if resultado["errores"]:
                print("  Errores:")
                for e in resultado["errores"]:
                    print(f"    - {e}")
