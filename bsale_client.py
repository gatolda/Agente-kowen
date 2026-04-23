"""
Cliente de Bsale API para Agente Kowen.
Obtiene pedidos web (ventas desde la tienda online).
"""

import logging
import requests
from datetime import datetime, timezone

from config import BSALE_API_TOKEN, BSALE_PEDIDO_WEB_TYPE_ID

log = logging.getLogger("kowen.bsale")

BASE_URL = "https://api.bsale.cl/v1"
PEDIDO_WEB_TYPE_ID = BSALE_PEDIDO_WEB_TYPE_ID
TIMEOUT = 30  # segundos


def _get_headers():
    """Retorna headers de autenticacion."""
    if not BSALE_API_TOKEN:
        raise ValueError("BSALE_API_TOKEN no configurada en .env")
    return {"access_token": BSALE_API_TOKEN}


def _request(endpoint, params=None):
    """Ejecuta un GET a la API de Bsale."""
    url = f"{BASE_URL}/{endpoint}"
    response = requests.get(url, headers=_get_headers(), params=params, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


def get_web_orders(since_number=0):
    """
    Obtiene pedidos web posteriores a un numero dado.

    Args:
        since_number: Numero de pedido desde el cual buscar (exclusivo).

    Returns:
        Lista de pedidos con datos normalizados.
    """
    # Bsale ordena por ID internamente. Usamos offset para ir al final.
    total = _request("documents.json", {
        "documenttypeid": PEDIDO_WEB_TYPE_ID,
        "limit": 1,
    })["count"]

    # Buscar desde los ultimos registros
    orders = []
    batch_size = 50
    offset = max(0, total - batch_size)

    while offset >= 0:
        data = _request("documents.json", {
            "documenttypeid": PEDIDO_WEB_TYPE_ID,
            "limit": batch_size,
            "offset": offset,
        })

        items = data.get("items", [])
        found_older = False

        for item in items:
            if item["number"] > since_number:
                # Saltar anulados
                if item.get("state", 0) != 0:
                    continue
                order = _parse_order(item)
                if order and order["estado"] != "anulado":
                    # Saltar pedidos con mas de 10 dias de antiguedad
                    try:
                        order_date = datetime.strptime(order["fecha"], "%Y-%m-%d")
                        if (datetime.now() - order_date).days > 10:
                            continue
                    except (ValueError, TypeError):
                        pass
                    orders.append(order)
            else:
                found_older = True

        if found_older or offset == 0:
            break
        offset = max(0, offset - batch_size)

    # Ordenar por numero
    orders.sort(key=lambda x: x["pedido_nro"])

    # Eliminar duplicados
    seen = set()
    unique = []
    for o in orders:
        if o["pedido_nro"] not in seen:
            seen.add(o["pedido_nro"])
            unique.append(o)

    return unique


def _parse_order(item):
    """Parsea un documento de Bsale a formato normalizado."""
    date = datetime.fromtimestamp(
        item["emissionDate"], tz=timezone.utc
    ).strftime("%Y-%m-%d")

    address_raw = item.get("address", "")
    municipality = item.get("municipality", "")
    city = item.get("city", "")

    # Parsear direccion y depto
    direccion, depto = _parse_address(address_raw)

    # Estado
    estado = "activo" if item["state"] == 0 else "anulado"

    # Obtener detalle (cantidad) y cliente
    doc_id = item["id"]
    client_id = item.get("client", {}).get("id")

    cantidad, descripcion_producto = _get_order_detail(doc_id)
    cliente_nombre, cliente_email, cliente_telefono = _get_client_info(client_id)

    # Detectar marca
    marca = "Cactus" if "cactus" in descripcion_producto.lower() else "Kowen"

    return {
        "pedido_nro": item["number"],
        "fecha": date,
        "cliente": cliente_nombre,
        "email": cliente_email,
        "telefono": cliente_telefono,
        "direccion": direccion,
        "depto": depto,
        "comuna": municipality,
        "ciudad": city or municipality,
        "cantidad": cantidad,
        "precio_unit": 2990,
        "total": item["totalAmount"],
        "estado": estado,
        "marca": marca,
        "doc_id": doc_id,
    }


def _parse_address(address_raw):
    """Separa direccion y departamento."""
    if not address_raw:
        return "", ""

    if "; depto/of." in address_raw:
        parts = address_raw.split("; depto/of.")
        direccion = parts[0].strip()
        depto = parts[1].strip() if len(parts) > 1 else ""
    elif ", depto/of." in address_raw.lower():
        idx = address_raw.lower().index(", depto/of.")
        direccion = address_raw[:idx].strip()
        depto = address_raw[idx + 11:].strip()
    else:
        direccion = address_raw.strip()
        depto = ""

    return direccion, depto


def _get_order_detail(doc_id):
    """Obtiene cantidad y descripcion del producto de un documento."""
    try:
        data = _request(f"documents/{doc_id}/details.json")
        items = data.get("items", [])
        if items:
            cantidad = items[0].get("quantity", 0)
            # Obtener descripcion del variante
            variant_href = items[0].get("variant", {}).get("href", "")
            descripcion = ""
            if variant_href:
                try:
                    resp = requests.get(variant_href, headers=_get_headers(), timeout=TIMEOUT)
                    if resp.ok:
                        descripcion = resp.json().get("description", "")
                except Exception as e:
                    log.warning("Fallo obtener variante Bsale %s: %s", variant_href, e)
            return int(cantidad), descripcion
    except Exception as e:
        log.warning("Fallo obtener items de orden Bsale: %s", e)
    return 0, ""


def _get_client_info(client_id):
    """Obtiene informacion del cliente."""
    if not client_id:
        return "", "", ""
    try:
        data = _request(f"clients/{client_id}.json")
        nombre = f"{data.get('firstName', '')} {data.get('lastName', '')}".strip()
        email = data.get("email", "")
        telefono = data.get("phone", "")
        return nombre, email, telefono
    except Exception:
        return "", "", ""


def get_order_by_number(number):
    """Busca un pedido web especifico por numero."""
    data = _request("documents.json", {
        "documenttypeid": PEDIDO_WEB_TYPE_ID,
        "number": number,
    })
    items = data.get("items", [])
    if items:
        return _parse_order(items[0])
    return None


def test_connection():
    """Prueba la conexion a la API de Bsale."""
    try:
        data = _request("documents.json", {"limit": 1})
        count = data.get("count", 0)
        return {"ok": True, "message": f"Conexion exitosa. {count} documentos en Bsale."}
    except requests.exceptions.HTTPError as e:
        return {"ok": False, "message": f"Error HTTP: {e.response.status_code}"}
    except Exception as e:
        return {"ok": False, "message": f"Error: {e}"}
