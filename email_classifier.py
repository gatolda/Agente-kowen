"""
Clasificador de emails con Claude.
Categorias: pagos, pedidos, cotizaciones, sii, servicios, spam.

Para emails "pagos" ademas extrae: monto, banco, referencia, fecha, remitente.
"""

import json
import re
from anthropic import Anthropic

from config import ANTHROPIC_API_KEY

MODEL = "claude-haiku-4-5-20251001"

CATEGORIAS = ["pagos", "pedidos", "cotizaciones", "sii", "servicios", "spam"]

_client = None


def _get_client():
    global _client
    if _client:
        return _client
    if not ANTHROPIC_API_KEY:
        raise ValueError("ANTHROPIC_API_KEY no configurada en .env")
    _client = Anthropic(api_key=ANTHROPIC_API_KEY)
    return _client


CLASSIFY_PROMPT = """Clasifica el siguiente email en UNA de estas categorias:

- pagos: comprobantes de transferencia, notificaciones bancarias de deposito recibido, pagos Webpay, boletas pagadas
- pedidos: cliente pide botellones de agua (nuevo pedido, re-pedido, cambio de direccion)
- cotizaciones: cliente pide precios, cotizacion para empresa/casa
- sii: Servicio de Impuestos Internos (facturas electronicas, folios, notificaciones SII)
- servicios: proveedores, facturas de luz/agua/internet, mantencion, insumos
- spam: publicidad, newsletters, phishing, notificaciones irrelevantes

Email:
De: {from_}
Asunto: {subject}
Cuerpo:
{body}

Responde SOLO con un JSON valido (sin markdown, sin explicaciones), con este formato:
{{"categoria": "pagos|pedidos|cotizaciones|sii|servicios|spam", "confianza": "alta|media|baja", "razon": "breve explicacion"}}"""


EXTRACT_PAGO_PROMPT = """Este email es un comprobante de pago o transferencia bancaria. Extraé los datos.

Email:
De: {from_}
Asunto: {subject}
Cuerpo:
{body}

Responde SOLO con un JSON valido (sin markdown), con este formato:
{{
  "monto": "numero entero en CLP sin puntos ni $ (ej: 15000)",
  "banco": "Banco de Chile|BancoEstado|Santander|BCI|Scotiabank|Webpay|Itau|Security|otro",
  "medio": "transferencia|webpay|deposito|otro",
  "referencia": "numero de operacion/comprobante/folio",
  "fecha": "YYYY-MM-DD",
  "remitente_nombre": "nombre de la persona/empresa que pago",
  "remitente_rut": "RUT si aparece, sino vacio",
  "glosa": "descripcion/motivo si aparece"
}}

Si un campo no se puede extraer, ponelo como string vacio "". No inventes datos."""


def _call_claude(prompt, max_tokens=500):
    """Llama a Claude y devuelve el texto de la respuesta."""
    client = _get_client()
    msg = client.messages.create(
        model=MODEL,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    return msg.content[0].text.strip()


def _parse_json(text):
    """Parsea JSON de la respuesta, tolerando bloques markdown."""
    # Quitar fences de markdown si los hay
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Intentar extraer el primer objeto JSON
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            return json.loads(match.group(0))
        raise


def classify_email(email):
    """
    Clasifica un email en una de las 6 categorias.

    Args:
        email: Dict con {from, subject, body} (de gmail_client.get_message).

    Returns:
        Dict con {categoria, confianza, razon}.
    """
    body = email.get("body", "") or email.get("snippet", "")
    # Truncar body para ahorrar tokens
    if len(body) > 4000:
        body = body[:4000] + "... [truncado]"

    prompt = CLASSIFY_PROMPT.format(
        from_=email.get("from", ""),
        subject=email.get("subject", ""),
        body=body,
    )
    try:
        response = _call_claude(prompt, max_tokens=300)
        result = _parse_json(response)
        cat = result.get("categoria", "spam").lower()
        if cat not in CATEGORIAS:
            cat = "spam"
        return {
            "categoria": cat,
            "confianza": result.get("confianza", "baja"),
            "razon": result.get("razon", ""),
        }
    except Exception as e:
        return {"categoria": "spam", "confianza": "baja", "razon": f"error: {e}"}


def extract_pago(email):
    """
    Extrae los datos de un email clasificado como 'pagos'.

    Returns:
        Dict con monto, banco, medio, referencia, fecha, remitente_nombre, remitente_rut, glosa.
    """
    body = email.get("body", "") or email.get("snippet", "")
    if len(body) > 4000:
        body = body[:4000] + "... [truncado]"

    prompt = EXTRACT_PAGO_PROMPT.format(
        from_=email.get("from", ""),
        subject=email.get("subject", ""),
        body=body,
    )
    try:
        response = _call_claude(prompt, max_tokens=500)
        return _parse_json(response)
    except Exception as e:
        return {
            "monto": "", "banco": "", "medio": "", "referencia": "",
            "fecha": "", "remitente_nombre": "", "remitente_rut": "",
            "glosa": f"error extraccion: {e}",
        }


def classify_and_extract(email):
    """
    Clasifica el email y, si es pago, extrae los datos.

    Returns:
        Dict con {categoria, confianza, razon, datos} (datos solo si es pago).
    """
    cls = classify_email(email)
    if cls["categoria"] == "pagos":
        cls["datos"] = extract_pago(email)
    return cls
