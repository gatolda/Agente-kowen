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
from dotenv import load_dotenv

load_dotenv()

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


def es_horario_laboral():
    """Verifica si estamos en horario laboral."""
    ahora = datetime.now()
    return (ahora.weekday() in DIAS_LABORALES
            and HORA_INICIO <= ahora.hour < HORA_FIN)


# --- Notificaciones Telegram ---

def notificar(mensaje):
    """Envia notificacion por Telegram si esta configurado."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        return

    try:
        import requests
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        requests.post(url, json={
            "chat_id": chat_id,
            "text": mensaje,
            "parse_mode": "Markdown",
        }, timeout=10)
    except Exception as e:
        log.warning(f"Error enviando Telegram: {e}")


# --- Tarea 1: Rutina diaria (08:00) ---

def ejecutar_rutina():
    """Ejecuta la rutina diaria completa."""
    import operations

    hoy = datetime.now().strftime("%d/%m/%Y")
    log.info(f"=== RUTINA DIARIA - {hoy} ===")

    try:
        resultado = operations.rutina_diaria(fecha_hoy=hoy)
    except Exception as e:
        log.error(f"Error critico en rutina diaria: {e}", exc_info=True)
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

    hoy = datetime.now().strftime("%d/%m/%Y")
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

    hoy = datetime.now().strftime("%d/%m/%Y")

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
        for d in v["detalle"][:5]:
            msg += f"  #{d['numero']} {d['direccion'][:25]}: {d['estado_nuevo']}\n"
        notificar(msg)

    # Alertar pedidos estancados (solo una vez al dia, a las 10:00)
    if datetime.now().hour == 10 and v["estancados"]:
        msg = f"*{len(v['estancados'])} pedidos estancados:*\n"
        for e in v["estancados"][:10]:
            msg += f"  #{e['numero']} {e['direccion'][:25]} ({e['dias']}d)\n"
        notificar(msg)
        log.warning(f"{len(v['estancados'])} pedidos estancados detectados")

    return v


# --- Tarea 3.5: Procesar emails (cada 30 min) ---

def procesar_emails():
    """Lee emails no leidos, clasifica y concilia pagos."""
    import payments

    try:
        r = payments.procesar_emails_no_leidos(max_emails=30, marcar_leidos=False)
    except Exception as e:
        log.warning(f"Error procesando emails: {e}")
        return None

    if r["total"] == 0:
        return r

    cats = r.get("por_categoria", {})
    conciliados = r.get("pagos_conciliados", [])
    sugeridos = r.get("pagos_sugeridos", [])
    sin_match = r.get("pagos_sin_match", [])
    alertas = r.get("alertas", [])

    log.info(f"Emails: {r['total']} leidos | "
             f"pagos auto: {len(conciliados)}, "
             f"sugeridos: {len(sugeridos)}, "
             f"sin match: {len(sin_match)}, "
             f"alertas: {len(alertas)}")

    # Notificar solo si hay pagos conciliados o cosas para revisar
    if conciliados or sugeridos or sin_match or alertas:
        msg = "*Emails procesados*\n"
        resumen_cats = ", ".join(f"{k}:{v}" for k, v in cats.items())
        if resumen_cats:
            msg += f"\n{resumen_cats}\n"

        if conciliados:
            msg += f"\n*Pagos conciliados auto ({len(conciliados)}):*"
            for p in conciliados[:5]:
                msg += f"\n- #{p['pedido']} {p['cliente'][:25]} ${p['monto']}"

        if sugeridos:
            msg += f"\n\n*Pagos para revisar ({len(sugeridos)}):*"
            for p in sugeridos[:5]:
                pago = p["pago"]
                msg += (f"\n- {pago.get('remitente_nombre', '')[:25]} "
                        f"${pago.get('monto', '')} -> "
                        f"{len(p['candidatos'])} candidatos")

        if sin_match:
            msg += f"\n\n*Pagos sin match ({len(sin_match)}):*"
            for p in sin_match[:5]:
                pago = p["pago"]
                msg += (f"\n- {pago.get('remitente_nombre', '')[:25]} "
                        f"${pago.get('monto', '')}")

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

    hoy = datetime.now().strftime("%d/%m/%Y")

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
        ahora = datetime.now()
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

    hoy = datetime.now().strftime("%d/%m/%Y")
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
    print(f"Hora actual: {datetime.now().strftime('%H:%M')}")


# --- Entry point ---

if __name__ == "__main__":
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
