"""
Cliente de Gmail para Agente Kowen.
Lee correos no leidos de la cuenta copiacorreoskowen@gmail.com.

Autenticacion:
- Usa credentials.json (mismo OAuth client que Sheets)
- Guarda token en gmail_token.json (separado del token.json de Sheets)
- En el primer run abre browser -> loguearse con copiacorreoskowen@gmail.com

Requisitos:
- API de Gmail habilitada en GCP project "prueba-2-kowen"
  https://console.cloud.google.com/apis/library/gmail.googleapis.com
"""

import base64
import os
from email.utils import parseaddr
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]
TOKEN_FILE = "gmail_token.json"
CREDENTIALS_FILE = "credentials.json"

_service = None


def _get_service():
    """Obtiene o crea el servicio Gmail autenticado."""
    global _service
    if _service:
        return _service

    creds = None
    token_path = os.path.join(os.path.dirname(__file__), TOKEN_FILE)
    creds_path = os.path.join(os.path.dirname(__file__), CREDENTIALS_FILE)

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, "w") as f:
            f.write(creds.to_json())

    _service = build("gmail", "v1", credentials=creds)
    return _service


def _decode_body(part):
    """Decodifica el body de un part del mensaje."""
    data = part.get("body", {}).get("data", "")
    if not data:
        return ""
    try:
        return base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
    except Exception:
        return ""


def _extract_body(payload):
    """Extrae el texto plano del body recorriendo parts."""
    mime = payload.get("mimeType", "")
    if mime == "text/plain":
        return _decode_body(payload)
    if mime == "text/html":
        # Fallback si no hay text/plain
        return _decode_body(payload)
    parts = payload.get("parts", [])
    if not parts:
        return _decode_body(payload)
    # Preferir text/plain
    for p in parts:
        if p.get("mimeType") == "text/plain":
            txt = _extract_body(p)
            if txt:
                return txt
    # Si no hay plano, buscar HTML
    for p in parts:
        if p.get("mimeType") == "text/html":
            txt = _extract_body(p)
            if txt:
                return txt
    # Recursivo para multipart anidado
    for p in parts:
        txt = _extract_body(p)
        if txt:
            return txt
    return ""


def _get_header(headers, name):
    """Obtiene el valor de un header por nombre (case-insensitive)."""
    name_lower = name.lower()
    for h in headers:
        if h.get("name", "").lower() == name_lower:
            return h.get("value", "")
    return ""


def list_unread(max_results=50, label_ids=None):
    """
    Lista mensajes no leidos.

    Args:
        max_results: Maximo de mensajes a devolver.
        label_ids: Lista de labels a filtrar (default: ["UNREAD", "INBOX"]).

    Returns:
        Lista de dicts con {id, thread_id}.
    """
    service = _get_service()
    labels = label_ids if label_ids else ["UNREAD", "INBOX"]
    result = service.users().messages().list(
        userId="me", labelIds=labels, maxResults=max_results,
    ).execute()
    messages = result.get("messages", [])
    return [{"id": m["id"], "thread_id": m["threadId"]} for m in messages]


def get_message(message_id):
    """
    Obtiene un mensaje completo con headers y body.

    Returns:
        Dict con {id, thread_id, from, from_email, to, subject, date, snippet, body}.
    """
    service = _get_service()
    msg = service.users().messages().get(
        userId="me", id=message_id, format="full",
    ).execute()

    payload = msg.get("payload", {})
    headers = payload.get("headers", [])

    from_raw = _get_header(headers, "From")
    from_name, from_email = parseaddr(from_raw)

    return {
        "id": msg["id"],
        "thread_id": msg["threadId"],
        "from": from_raw,
        "from_name": from_name,
        "from_email": from_email,
        "to": _get_header(headers, "To"),
        "subject": _get_header(headers, "Subject"),
        "date": _get_header(headers, "Date"),
        "snippet": msg.get("snippet", ""),
        "body": _extract_body(payload),
        "label_ids": msg.get("labelIds", []),
    }


def get_unread_messages(max_results=50):
    """
    Lista y obtiene todos los mensajes no leidos (con contenido completo).

    Returns:
        Lista de dicts de mensajes.
    """
    ids = list_unread(max_results=max_results)
    return [get_message(m["id"]) for m in ids]


def mark_as_read(message_id):
    """Marca un mensaje como leido (quita el label UNREAD)."""
    service = _get_service()
    service.users().messages().modify(
        userId="me", id=message_id,
        body={"removeLabelIds": ["UNREAD"]},
    ).execute()


def add_label(message_id, label_id):
    """Agrega un label a un mensaje."""
    service = _get_service()
    service.users().messages().modify(
        userId="me", id=message_id,
        body={"addLabelIds": [label_id]},
    ).execute()


def get_or_create_label(name):
    """Obtiene el id de un label; lo crea si no existe."""
    service = _get_service()
    result = service.users().labels().list(userId="me").execute()
    for lbl in result.get("labels", []):
        if lbl["name"] == name:
            return lbl["id"]
    created = service.users().labels().create(
        userId="me",
        body={"name": name, "labelListVisibility": "labelShow",
              "messageListVisibility": "show"},
    ).execute()
    return created["id"]


def test_connection():
    """Prueba la conexion a Gmail."""
    try:
        service = _get_service()
        profile = service.users().getProfile(userId="me").execute()
        return {
            "ok": True,
            "email": profile.get("emailAddress", ""),
            "total_messages": profile.get("messagesTotal", 0),
        }
    except Exception as e:
        return {"ok": False, "message": str(e)}
