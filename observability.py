"""
Observabilidad del Agente Kowen: Sentry (errores) + healthcheck (liveness).

Uso:
    from observability import init_sentry, ping_healthcheck

    init_sentry(component="scheduler")     # al arranque del entrypoint
    ...
    ping_healthcheck()                     # al final de rutina diaria
    ping_healthcheck(fail=True, msg=...)   # si algo fallo critico
"""

import logging
import config

log = logging.getLogger("kowen")

_sentry_ready = False


def init_sentry(component="unknown"):
    """Inicializa Sentry si SENTRY_DSN esta definido. Idempotente."""
    global _sentry_ready
    if _sentry_ready or not config.SENTRY_DSN:
        return

    try:
        import sentry_sdk
        sentry_sdk.init(
            dsn=config.SENTRY_DSN,
            environment=config.SENTRY_ENVIRONMENT,
            traces_sample_rate=0.0,
            send_default_pii=False,
        )
        sentry_sdk.set_tag("component", component)
        _sentry_ready = True
        log.info(f"Sentry inicializado ({component}, env={config.SENTRY_ENVIRONMENT})")
    except Exception as e:
        log.warning(f"No se pudo inicializar Sentry: {e}")


def capture_exception(e):
    """Envia excepcion a Sentry si esta configurado."""
    if not _sentry_ready:
        return
    try:
        import sentry_sdk
        sentry_sdk.capture_exception(e)
    except Exception:
        pass


def ping_healthcheck(fail=False, msg=""):
    """
    Pings al servicio de healthcheck externo (ej. healthchecks.io).
    - fail=False: envia /<url>  (OK)
    - fail=True:  envia /<url>/fail (alerta)
    Es best-effort: si no hay URL configurada o falla el request, no lanza.
    """
    if not config.HEALTHCHECK_URL:
        return

    try:
        import requests
        url = config.HEALTHCHECK_URL.rstrip("/")
        if fail:
            url = url + "/fail"
        requests.post(url, data=msg[:1000] if msg else "", timeout=5)
    except Exception as e:
        log.warning(f"Error pingeando healthcheck: {e}")
