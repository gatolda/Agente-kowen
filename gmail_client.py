"""
Cliente de Gmail API para Agente Kowen.
Maneja autenticacion OAuth2 y operaciones sobre correos.
"""

import os
import base64
import re
from email.utils import parsedate_to_datetime
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.labels",
    "https://www.googleapis.com/auth/spreadsheets",
]

TOKEN_PATH = "token.json"
CREDENTIALS_PATH = "credentials.json"


def get_gmail_service():
    """Autentica y retorna el servicio de Gmail API."""
    creds = None

    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(CREDENTIALS_PATH):
                raise FileNotFoundError(
                    f"No se encontro '{CREDENTIALS_PATH}'. "
                    "Descargalo desde Google Cloud Console > APIs > Credenciales."
                )
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_PATH, "w") as token:
            token.write(creds.to_json())

    return build("gmail", "v1", credentials=creds)


def _extract_text_from_payload(payload):
    """Extrae texto plano del payload de un mensaje de Gmail."""
    parts = payload.get("parts", [])
    body = payload.get("body", {})

    # Mensaje simple sin partes
    if not parts and body.get("data"):
        return base64.urlsafe_b64decode(body["data"]).decode("utf-8", errors="replace")

    # Mensaje con partes (multipart)
    text = ""
    for part in parts:
        mime_type = part.get("mimeType", "")
        if mime_type == "text/plain" and part.get("body", {}).get("data"):
            text += base64.urlsafe_b64decode(part["body"]["data"]).decode(
                "utf-8", errors="replace"
            )
        elif mime_type.startswith("multipart/"):
            text += _extract_text_from_payload(part)

    return text


def _get_header(headers, name):
    """Obtiene un header especifico del mensaje."""
    for h in headers:
        if h["name"].lower() == name.lower():
            return h["value"]
    return ""


def get_unread_emails(service, max_results=20):
    """Obtiene correos no leidos del inbox (sin etiqueta kowen/procesado)."""
    query = "in:inbox is:unread -label:kowen-procesado"

    results = (
        service.users()
        .messages()
        .list(userId="me", q=query, maxResults=max_results)
        .execute()
    )

    messages = results.get("messages", [])
    emails = []

    for msg_ref in messages:
        msg = (
            service.users()
            .messages()
            .get(userId="me", id=msg_ref["id"], format="full")
            .execute()
        )

        headers = msg.get("payload", {}).get("headers", [])
        body_text = _extract_text_from_payload(msg.get("payload", {}))

        # Limpiar texto excesivo
        if len(body_text) > 3000:
            body_text = body_text[:3000] + "... [truncado]"

        # Limpiar espacios multiples y lineas vacias
        body_text = re.sub(r"\n{3,}", "\n\n", body_text)
        body_text = re.sub(r" {2,}", " ", body_text)

        date_str = _get_header(headers, "Date")
        try:
            dt = parsedate_to_datetime(date_str)
            fecha = dt.strftime("%Y-%m-%d")
            hora = dt.strftime("%H:%M")
        except Exception:
            fecha = ""
            hora = ""

        emails.append(
            {
                "id": msg["id"],
                "threadId": msg["threadId"],
                "fecha": fecha,
                "hora": hora,
                "remitente": _get_header(headers, "From"),
                "asunto": _get_header(headers, "Subject"),
                "cuerpo": body_text.strip(),
            }
        )

    return emails


# Mapeo de categorias a nombres de etiqueta en Gmail
LABEL_MAP = {
    "pagos": "kowen/pagos",
    "pedidos": "kowen/pedidos",
    "cotizaciones": "kowen/cotizaciones",
    "sii": "kowen/sii",
    "servicios": "kowen/servicios",
    "spam": "kowen/spam",
}


def _get_or_create_label(service, label_name):
    """Obtiene el ID de una etiqueta, o la crea si no existe."""
    results = service.users().labels().list(userId="me").execute()
    labels = results.get("labels", [])

    for label in labels:
        if label["name"] == label_name:
            return label["id"]

    # Crear la etiqueta
    body = {
        "name": label_name,
        "labelListVisibility": "labelShow",
        "messageListVisibility": "show",
    }
    created = service.users().labels().create(userId="me", body=body).execute()
    return created["id"]


def add_label(service, message_id, category):
    """Agrega una etiqueta de categoria a un mensaje."""
    label_name = LABEL_MAP.get(category)
    if not label_name:
        return f"Categoria '{category}' no reconocida. Usar: {list(LABEL_MAP.keys())}"

    label_id = _get_or_create_label(service, label_name)

    # Tambien agregar etiqueta de "procesado"
    processed_label_id = _get_or_create_label(service, "kowen-procesado")

    service.users().messages().modify(
        userId="me",
        id=message_id,
        body={"addLabelIds": [label_id, processed_label_id]},
    ).execute()

    return f"Etiqueta '{label_name}' agregada al mensaje {message_id}"


def mark_as_read(service, message_id):
    """Marca un mensaje como leido."""
    service.users().messages().modify(
        userId="me",
        id=message_id,
        body={"removeLabelIds": ["UNREAD"]},
    ).execute()
    return f"Mensaje {message_id} marcado como leido"
