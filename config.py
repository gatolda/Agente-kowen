"""
Configuración centralizada del Agente Kowen.
Todas las variables de entorno se cargan y validan aquí.
Importar desde este módulo en lugar de llamar os.getenv() directo.
"""

import os
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()


class ConfigError(RuntimeError):
    """Error de configuración: variable requerida faltante."""


def _required(name):
    val = os.getenv(name)
    if not val:
        raise ConfigError(
            f"Variable de entorno requerida '{name}' no está definida. "
            f"Revisá tu archivo .env (ver .env.example)."
        )
    return val


def _optional(name, default=None):
    return os.getenv(name, default)


# Zona horaria del negocio (Chile)
TZ_NAME = _optional("TZ", "America/Santiago")
TZ = ZoneInfo(TZ_NAME)


def now():
    """Retorna datetime actual en zona horaria del negocio (Chile)."""
    return datetime.now(TZ)


def today():
    """Retorna fecha actual en zona horaria del negocio (Chile)."""
    return now().date()

# Google Sheets - planilla unificada Pedidos 2026
SPREADSHEET_ID = _optional(
    "GOOGLE_SHEETS_KOWEN_ID",
    "11cG1jArLtQrfmAqX-Qqsfx3Eqkns3Z80Ff9rCk2WwQU",
)

# Planillas fuente (transicionales)
PLANILLA_REPARTO_ID = _optional(
    "PLANILLA_REPARTO_ID",
    "1jNTWO2hkkRBlEamXrQ6BGy28tlAj7ei1Qyyt559mvds",
)
PLANILLA_CACTUS_ID = _optional(
    "PLANILLA_CACTUS_ID",
    "1w5Klrcbq7-B6HUBCADeIkmv5_r4vhGnsYv7EuJQqxgU",
)

# APIs externas
DRIVIN_API_KEY = _optional("DRIVIN_API_KEY")
BSALE_API_TOKEN = _optional("BSALE_API_TOKEN")
BSALE_PEDIDO_WEB_TYPE_ID = int(_optional("BSALE_PEDIDO_WEB_TYPE_ID", "32"))
ANTHROPIC_API_KEY = _optional("ANTHROPIC_API_KEY")

# Telegram
TELEGRAM_BOT_TOKEN = _optional("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = _optional("TELEGRAM_CHAT_ID")

# Google auth (alternativas)
GOOGLE_SA_JSON = _optional("GOOGLE_SA_JSON")

# Observabilidad
SENTRY_DSN = _optional("SENTRY_DSN")
SENTRY_ENVIRONMENT = _optional("SENTRY_ENVIRONMENT", "production")
HEALTHCHECK_URL = _optional("HEALTHCHECK_URL")


def validate_critical():
    """
    Valida que las env vars críticas estén presentes.
    Llamar al inicio de entrypoints (scheduler, streamlit, cli, bot).
    Lanza ConfigError si falta algo crítico.
    """
    missing = []
    if not DRIVIN_API_KEY:
        missing.append("DRIVIN_API_KEY")
    if not SPREADSHEET_ID:
        missing.append("GOOGLE_SHEETS_KOWEN_ID")
    if not PLANILLA_REPARTO_ID:
        missing.append("PLANILLA_REPARTO_ID")
    if missing:
        raise ConfigError(
            f"Variables críticas faltantes: {', '.join(missing)}. "
            f"Sin estas el sistema no puede operar."
        )
