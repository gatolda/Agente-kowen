"""
Scheduler del Agente Kowen.
Ejecuta la rutina diaria automaticamente a las 8:00 AM.

Uso:
    python scheduler.py              # Ejecuta la rutina una vez (para probar)
    python scheduler.py --daemon     # Corre en segundo plano, ejecuta cada dia a las 8 AM
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# Configurar logging
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


def ejecutar_rutina():
    """Ejecuta la rutina diaria completa."""
    import sheets_client

    hoy = datetime.now().strftime("%d/%m/%Y")
    log.info(f"=== RUTINA DIARIA - {hoy} ===")

    try:
        resultado = sheets_client.rutina_diaria(fecha_hoy=hoy)
    except Exception as e:
        log.error(f"Error critico en rutina diaria: {e}", exc_info=True)
        return

    # Log del resultado
    log.info(f"Pedidos ayer ({resultado['fecha_ayer']}):")
    log.info(f"  Entregados: {resultado['entregados_ayer']}")
    log.info(f"  No entregados: {resultado['no_entregados_ayer']}")
    log.info(f"  Movidos a hoy: {resultado['movidos_a_hoy']}")
    log.info(f"  Duplicados eliminados: {resultado['duplicados_eliminados']}")
    log.info(f"Importaciones hoy ({resultado['fecha_hoy']}):")
    log.info(f"  Desde Bsale: {resultado['bsale_importados']}")
    log.info(f"  Desde planilla: {resultado['planilla_importados']}")
    log.info(f"  Codigos asignados: {resultado['codigos_asignados']}")

    log.info(f"Plan driv.in:")
    log.info(f"  Plan: {resultado.get('drivin_plan', 'N/A')}")
    log.info(f"  Subidos: {resultado.get('drivin_subidos', 0)}")

    if resultado["errores"]:
        for err in resultado["errores"]:
            log.warning(f"  Advertencia: {err}")

    # Resumen final
    import sheets_client as sc
    pedidos_hoy = sc.get_pedidos(hoy)
    total = len(pedidos_hoy)
    pendientes = sum(1 for p in pedidos_hoy if p.get("Estado Pedido") == "PENDIENTE")
    con_codigo = sum(1 for p in pedidos_hoy if p.get("Codigo Drivin", "").strip())
    sin_codigo = total - con_codigo

    log.info(f"Estado final: {total} pedidos para hoy, {pendientes} pendientes, {sin_codigo} sin codigo drivin")
    log.info("=== FIN RUTINA ===")

    return resultado


def daemon():
    """Corre como demonio, ejecuta la rutina cada dia a las 8:00 AM."""
    HORA_EJECUCION = 8  # 8 AM

    log.info("Agente Kowen - Scheduler iniciado")
    log.info(f"Rutina programada para las {HORA_EJECUCION}:00 cada dia")

    ultima_ejecucion = None

    while True:
        ahora = datetime.now()
        hoy = ahora.date()

        # Ejecutar si es la hora y no se ha ejecutado hoy
        if ahora.hour >= HORA_EJECUCION and ultima_ejecucion != hoy:
            log.info("Hora de ejecutar rutina diaria...")
            ejecutar_rutina()
            ultima_ejecucion = hoy

        # Dormir 5 minutos
        time.sleep(300)


if __name__ == "__main__":
    if "--daemon" in sys.argv:
        daemon()
    else:
        # Ejecucion unica (para probar)
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
