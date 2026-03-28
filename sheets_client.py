"""
Cliente de Google Sheets para Agente Kowen.
Guarda los correos clasificados en hojas separadas por categoria.
"""

import os
import gspread
from google.oauth2.credentials import Credentials

TOKEN_PATH = "token.json"

# ID del spreadsheet (cambiar por el tuyo)
SPREADSHEET_ID = os.getenv(
    "GOOGLE_SHEETS_ID", "1QO_Ib_bzQzBVZ0Gn7Y39upS02Yo7-My8Ve_Jp9EscsM"
)

# Mapeo de categoria a nombre de hoja en Google Sheets
SHEET_MAP = {
    "pagos": "Pagos",
    "pedidos": "Pedidos",
    "cotizaciones": "Cotizaciones",
    "sii": "SII",
    "servicios": "Servicios",
    "spam": "Spam",
}

# Columnas por categoria
COLUMNS_PAGOS = [
    "fecha_correo",
    "hora_correo",
    "remitente",
    "asunto",
    "texto_clave",
    "gmailMessageId",
    "threadId",
    "nombre",
    "banco",
    "monto",
    "fecha_transferencia",
    "numero_operacion",
]

COLUMNS_PEDIDOS = [
    "fecha_correo",
    "hora_correo",
    "remitente",
    "asunto",
    "resumen",
    "gmailMessageId",
    "threadId",
    "cliente",
    "telefono",
    "direccion",
    "productos",
    "total",
    "origen",
]

COLUMNS_DEFAULT = [
    "fecha_correo",
    "hora_correo",
    "remitente",
    "asunto",
    "resumen",
    "gmailMessageId",
    "threadId",
]


def get_sheets_client():
    """Autentica con Google Sheets usando el mismo token de Gmail."""
    if not os.path.exists(TOKEN_PATH):
        raise FileNotFoundError(
            "No se encontro token.json. Ejecuta primero la autenticacion de Gmail."
        )

    creds = Credentials.from_authorized_user_file(TOKEN_PATH)
    return gspread.authorize(creds)


def _get_or_create_worksheet(spreadsheet, sheet_name, columns):
    """Obtiene una hoja o la crea con los headers si no existe."""
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=sheet_name, rows=1000, cols=len(columns)
        )
        worksheet.append_row(columns)
    return worksheet


def save_to_sheets(category, data):
    """
    Guarda datos de un correo clasificado en la hoja correspondiente.

    Args:
        category: Categoria del correo (pagos, pedidos, etc.)
        data: Diccionario con los campos a guardar
    """
    sheet_name = SHEET_MAP.get(category)
    if not sheet_name:
        return f"Categoria '{category}' no tiene hoja configurada."

    # Seleccionar columnas segun categoria
    if category == "pagos":
        columns = COLUMNS_PAGOS
    elif category == "pedidos":
        columns = COLUMNS_PEDIDOS
    else:
        columns = COLUMNS_DEFAULT

    gc = get_sheets_client()
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    worksheet = _get_or_create_worksheet(spreadsheet, sheet_name, columns)

    # Construir fila en orden de columnas
    row = [str(data.get(col, "")) for col in columns]
    worksheet.append_row(row, value_input_option="USER_ENTERED")

    return f"Guardado en hoja '{sheet_name}': {data.get('asunto', '')[:50]}"
