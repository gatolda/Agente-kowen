"""
Cliente de Google Sheets para Agente Kowen.
Lee y escribe datos en la planilla "Pedidos 2026".
Tabs: OPERACION DIARIA, CLIENTES, PAGOS, LOG.
"""

import logging
import os
from datetime import datetime
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

from config import SPREADSHEET_ID, GOOGLE_SA_JSON

log = logging.getLogger("kowen.sheets")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Nombres de las hojas
TAB_OPERACION = "OPERACION DIARIA"
TAB_CLIENTES = "CLIENTES"
TAB_PAGOS = "PAGOS"
TAB_LOG = "LOG"

# Columnas de OPERACION DIARIA
OP_COLS = [
    "#", "Fecha", "Direccion", "Depto", "Comuna", "Codigo Drivin", "Cant",
    "Marca", "Documento", "Repartidor", "Vuelta", "Zona", "Estado Pedido",
    "Canal", "Observaciones", "Com. Chofer", "Cliente", "Telefono", "Email",
    "Efectivo", "Transferencia", "Forma Pago", "Estado Pago", "Fecha Pago",
    "Aliado", "Plan Drivin", "Pedido Bsale",
]

_service = None


def _get_service():
    """Obtiene o crea el servicio de Google Sheets autenticado.

    Soporta 3 modos de autenticacion:
    1. Service Account JSON file (para cloud/CI)
    2. Service Account desde variable de entorno GOOGLE_SA_JSON (para GitHub Actions)
    3. OAuth desktop flow con token.json (para desarrollo local)
    """
    global _service
    if _service:
        return _service

    creds = None
    sa_file = os.path.join(os.path.dirname(__file__), "service_account.json")

    # Modo 1: Service Account desde archivo
    if os.path.exists(sa_file):
        from google.oauth2.service_account import Credentials as SACredentials
        creds = SACredentials.from_service_account_file(sa_file, scopes=SCOPES)

    # Modo 2: Service Account desde variable de entorno (GitHub Actions)
    elif GOOGLE_SA_JSON:
        import json
        from google.oauth2.service_account import Credentials as SACredentials
        sa_info = json.loads(GOOGLE_SA_JSON)
        creds = SACredentials.from_service_account_info(sa_info, scopes=SCOPES)

    # Modo 3: OAuth desktop flow (desarrollo local)
    else:
        if os.path.exists("token.json"):
            creds = Credentials.from_authorized_user_file("token.json", SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
                creds = flow.run_local_server(port=0)
            with open("token.json", "w") as f:
                f.write(creds.to_json())

    _service = build("sheets", "v4", credentials=creds)
    return _service


def _retry(func, max_retries=4):
    """Ejecuta una funcion con retry + backoff exponencial para manejar rate limits.

    Detecta rate limits/errores transitorios por:
    - HttpError.resp.status en [429, 500, 502, 503, 504]
    - Strings "429", "500", "503", "RATE_LIMIT", "Quota exceeded" en el mensaje
    """
    import time
    _RETRYABLE_STATUS = {429, 500, 502, 503, 504}
    _RETRYABLE_STRS = ("429", "500", "502", "503", "504", "RATE_LIMIT",
                       "Quota exceeded", "backend error")
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            is_retryable = False
            # Chequeo por status code HTTP (googleapiclient.errors.HttpError)
            status = getattr(getattr(e, "resp", None), "status", None)
            if status in _RETRYABLE_STATUS:
                is_retryable = True
            else:
                error_str = str(e)
                if any(s in error_str for s in _RETRYABLE_STRS):
                    is_retryable = True

            if is_retryable and attempt < max_retries - 1:
                wait = 2 ** attempt  # 1s, 2s, 4s, 8s
                time.sleep(wait)
                continue
            raise


def _read_sheet(tab, range_suffix=""):
    """Lee datos de una hoja."""
    service = _get_service()
    range_str = f"'{tab}'{range_suffix}" if range_suffix else f"'{tab}'"
    result = _retry(lambda: service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=range_str,
    ).execute())
    return result.get("values", [])


def _write_sheet(tab, range_suffix, values, input_option="USER_ENTERED"):
    """Escribe datos en una hoja."""
    service = _get_service()
    range_str = f"'{tab}'!{range_suffix}"
    _retry(lambda: service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=range_str,
        valueInputOption=input_option,
        body={"values": values},
    ).execute())


def _append_sheet(tab, values, input_option="USER_ENTERED"):
    """Agrega filas al final de una hoja."""
    service = _get_service()
    _retry(lambda: service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{tab}'!A:A",
        valueInputOption=input_option,
        insertDataOption="INSERT_ROWS",
        body={"values": values},
    ).execute())


# ===== UTILIDADES =====

def _normalize_address(direccion):
    """
    Normaliza una direccion para deteccion de duplicados.
    Extrae nombre de calle simplificado + numero principal.
    Ejemplos:
        "Av. Las Condes 9460 of 1503" -> "condes 9460"
        "avenida nueva providencia 1881" -> "providencia 1881"
        "Las Condes 9460" -> "condes 9460"
        "Antonia Lopez de Bello 068" -> "bello 068"
    """
    import re
    from unidecode import unidecode
    text = unidecode(direccion).lower().strip()

    # Quitar prefijos comunes
    for prefix in ["avenida ", "av. ", "av ", "calle ", "pasaje ", "psje ",
                    "paseo ", "nueva ", "camino ", "del "]:
        text = text.replace(prefix, "")

    # Quitar sufijos despues del numero (depto, of, piso, casa, local, etc.)
    text = re.sub(r"\b(depto|dpto|dep|of|oficina|piso|casa|local|subte|torre)\b.*", "", text)
    # Quitar parentesis y su contenido (incluso si no cierra)
    text = re.sub(r"\(.*?\)", "", text)
    text = re.sub(r"\(.*", "", text)
    # Quitar comas y lo que sigue (comuna)
    text = re.sub(r",.*", "", text)

    # Limpiar caracteres especiales
    text = re.sub(r"[^a-z0-9 ]", "", text)
    words = text.split()

    # Buscar el numero principal de la calle
    number = None
    number_idx = None
    for i, w in enumerate(words):
        if w.isdigit() and len(w) >= 2 and number is None:
            number = w
            number_idx = i
            break

    if not number:
        return " ".join(words)

    # Tomar la palabra significativa justo antes del numero
    words_before = words[:number_idx]
    if words_before:
        significant = [w for w in words_before if len(w) > 2]
        last_word = significant[-1] if significant else words_before[-1]
        return f"{last_word} {number}"

    return number


# ===== OPERACION DIARIA =====

def get_pedidos(fecha=None):
    """
    Obtiene pedidos de OPERACION DIARIA.

    Args:
        fecha: Filtrar por fecha (str "DD/MM/YYYY"). None = todos.

    Returns:
        Lista de dicts con los datos de cada pedido.
    """
    rows = _read_sheet(TAB_OPERACION)
    if len(rows) < 2:
        return []

    headers = rows[0]
    pedidos = []
    for row in rows[1:]:
        row_padded = row + [""] * (len(headers) - len(row))
        pedido = dict(zip(headers, row_padded))
        if fecha:
            if pedido.get("Fecha", "") == fecha:
                pedidos.append(pedido)
        else:
            pedidos.append(pedido)

    return pedidos


def get_next_number():
    """Obtiene el proximo numero de pedido (#)."""
    rows = _read_sheet(TAB_OPERACION, "!A:A")
    if len(rows) < 2:
        return 1
    last = 0
    for row in rows[1:]:
        if row and row[0].isdigit():
            last = int(row[0])
    return last + 1


def add_pedido(pedido):
    """
    Agrega un pedido a OPERACION DIARIA.

    Args:
        pedido: Dict con campos del pedido (fecha, direccion, depto, comuna,
            codigo_drivin, cant, marca, etc.)

    Returns:
        Numero asignado al pedido.
    """
    num = get_next_number()
    row = _pedido_to_row(num, pedido)
    _append_sheet(TAB_OPERACION, [row])
    return num


def add_pedidos(pedidos):
    """
    Agrega multiples pedidos de una vez.

    Args:
        pedidos: Lista de dicts con campos de pedido.

    Returns:
        Lista de numeros asignados.
    """
    start_num = get_next_number()
    rows = []
    nums = []

    for i, pedido in enumerate(pedidos):
        num = start_num + i
        nums.append(num)
        rows.append(_pedido_to_row(num, pedido))

    _append_sheet(TAB_OPERACION, rows)
    return nums


def _pedido_to_row(num, pedido):
    """Convierte un dict de pedido a fila de spreadsheet."""
    return [
        str(num),
        pedido.get("fecha", datetime.now().strftime("%d/%m/%Y")),
        pedido.get("direccion", ""),
        pedido.get("depto", ""),
        pedido.get("comuna", ""),
        pedido.get("codigo_drivin", ""),
        str(pedido.get("cant", pedido.get("cantidad", ""))),
        pedido.get("marca", "KOWEN"),
        pedido.get("documento", ""),
        pedido.get("repartidor", ""),
        pedido.get("vuelta", ""),
        pedido.get("zona", ""),
        pedido.get("estado_pedido", "PENDIENTE"),
        pedido.get("canal", ""),
        pedido.get("observaciones", ""),
        pedido.get("com_chofer", ""),
        pedido.get("cliente", ""),
        pedido.get("telefono", ""),
        pedido.get("email", ""),
        pedido.get("efectivo", ""),
        pedido.get("transferencia", ""),
        pedido.get("forma_pago", ""),
        pedido.get("estado_pago", "PENDIENTE"),
        pedido.get("fecha_pago", ""),
        pedido.get("aliado", ""),
        pedido.get("plan_drivin", ""),
        pedido.get("pedido_bsale", ""),
    ]


def delete_pedidos_batch(row_numbers):
    """
    Elimina varios pedidos en UNA sola llamada a Sheets API.
    Args:
        row_numbers: Lista de numeros de pedido (#) a eliminar.
    Returns:
        Cantidad eliminada.
    """
    if not row_numbers:
        return 0
    targets = set(str(n).strip() for n in row_numbers if str(n).strip())
    rows = _read_sheet(TAB_OPERACION, "!A:A")
    rows_to_del = []
    for i, row in enumerate(rows):
        if i == 0:
            continue
        if row and str(row[0]).strip() in targets:
            rows_to_del.append(i)  # 0-based sheet row index

    if not rows_to_del:
        return 0

    # Obtener sheetId real de la tab OPERACION DIARIA
    service = _get_service()
    meta = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    sheet_id = None
    for s in meta.get("sheets", []):
        if s["properties"]["title"] == TAB_OPERACION:
            sheet_id = s["properties"]["sheetId"]
            break
    if sheet_id is None:
        raise RuntimeError(f"Tab {TAB_OPERACION} no encontrada")

    # Borrar de mayor a menor para no desplazar indices
    rows_to_del.sort(reverse=True)
    requests = [{
        "deleteDimension": {
            "range": {
                "sheetId": sheet_id,
                "dimension": "ROWS",
                "startIndex": r,
                "endIndex": r + 1,
            }
        }
    } for r in rows_to_del]

    _retry(lambda: service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"requests": requests},
    ).execute())
    return len(rows_to_del)


def delete_pedido(row_number):
    """
    Elimina un pedido de OPERACION DIARIA.

    Args:
        row_number: Numero de pedido (#) a eliminar.
    """
    rows = _read_sheet(TAB_OPERACION, "!A:A")
    target_row = None
    for i, row in enumerate(rows):
        if row and row[0] == str(row_number):
            target_row = i
            break

    if target_row is None:
        raise ValueError(f"Pedido #{row_number} no encontrado")

    service = _get_service()
    meta = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    sheet_id = None
    for s in meta["sheets"]:
        if s["properties"]["title"] == TAB_OPERACION:
            sheet_id = s["properties"]["sheetId"]
            break

    _retry(lambda: service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"requests": [{
            "deleteDimension": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": target_row,
                    "endIndex": target_row + 1,
                }
            }
        }]}
    ).execute())


# Mapeo de campo a indice de columna
_FIELD_TO_COL = {
    "fecha": 1, "direccion": 2, "depto": 3, "comuna": 4,
    "codigo_drivin": 5, "cant": 6, "cantidad": 6, "marca": 7,
    "documento": 8, "repartidor": 9, "vuelta": 10, "zona": 11,
    "estado_pedido": 12, "canal": 13, "observaciones": 14,
    "com_chofer": 15, "cliente": 16, "telefono": 17, "email": 18,
    "efectivo": 19, "transferencia": 20, "forma_pago": 21,
    "estado_pago": 22, "fecha_pago": 23, "aliado": 24,
    "plan_drivin": 25, "pedido_bsale": 26,
}


def _col_letter(idx):
    """Convierte indice (0-based) a letra de columna."""
    if idx < 26:
        return chr(65 + idx)
    return "A" + chr(65 + idx - 26)


def update_pedido(row_number, updates):
    """
    Actualiza campos de un pedido existente.

    Args:
        row_number: Numero de pedido (#) a actualizar.
        updates: Dict con campos a actualizar (ej: {"estado_pedido": "ENTREGADO"}).
    """
    rows = _read_sheet(TAB_OPERACION, "!A:A")
    target_row = None
    for i, row in enumerate(rows):
        if row and row[0] == str(row_number):
            target_row = i + 1
            break

    if not target_row:
        raise ValueError(f"Pedido #{row_number} no encontrado")

    service = _get_service()
    for field, value in updates.items():
        col_idx = _FIELD_TO_COL.get(field)
        if col_idx is None:
            continue
        cell = f"'{TAB_OPERACION}'!{_col_letter(col_idx)}{target_row}"
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=cell,
            valueInputOption="USER_ENTERED",
            body={"values": [[str(value)]]},
        ).execute()


def update_pedidos_batch(updates_list):
    """
    Actualiza multiples pedidos en un solo request.

    Args:
        updates_list: Lista de (row_number, updates_dict).
    """
    rows = _read_sheet(TAB_OPERACION, "!A:A")
    num_to_row = {}
    for i, row in enumerate(rows):
        if row and row[0].isdigit():
            num_to_row[int(row[0])] = i + 1

    data = []
    for row_number, updates in updates_list:
        target_row = num_to_row.get(row_number)
        if not target_row:
            continue
        for field, value in updates.items():
            col_idx = _FIELD_TO_COL.get(field)
            if col_idx is None:
                continue
            data.append({
                "range": f"'{TAB_OPERACION}'!{_col_letter(col_idx)}{target_row}",
                "values": [[str(value)]],
            })

    if data:
        service = _get_service()
        _retry(lambda: service.spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"valueInputOption": "USER_ENTERED", "data": data},
        ).execute())


# ===== CLIENTES =====

def get_clientes():
    """Obtiene todos los clientes."""
    rows = _read_sheet(TAB_CLIENTES)
    if len(rows) < 2:
        return []
    headers = rows[0]
    return [
        dict(zip(headers, row + [""] * (len(headers) - len(row))))
        for row in rows[1:]
    ]


def find_cliente(nombre):
    """Busca un cliente por nombre (parcial, case-insensitive)."""
    clientes = get_clientes()
    nombre_lower = nombre.lower()
    return [c for c in clientes if nombre_lower in c.get("Nombre", "").lower()]


def add_cliente(cliente):
    """
    Agrega un cliente nuevo.

    Args:
        cliente: Dict con campos: nombre, telefono, email, direccion,
            depto, comuna, codigo_drivin, marca, precio_especial.
    """
    row = [
        cliente.get("nombre", ""),
        cliente.get("telefono", ""),
        cliente.get("email", ""),
        cliente.get("direccion", ""),
        cliente.get("depto", ""),
        cliente.get("comuna", ""),
        cliente.get("codigo_drivin", ""),
        cliente.get("marca", "KOWEN"),
        str(cliente.get("precio_especial", "")),
        str(cliente.get("total_pedidos", 0)),
        cliente.get("ultimo_pedido", ""),
        cliente.get("estado", "Activo"),
    ]
    _append_sheet(TAB_CLIENTES, [row])


def get_clientes_indexed():
    """
    Como get_clientes(), pero cada dict incluye un campo '_row' con el indice
    1-based del row en la hoja. Util para evitar relectura en updates masivos.
    """
    rows = _read_sheet(TAB_CLIENTES)
    if len(rows) < 2:
        return []
    headers = rows[0]
    out = []
    for i, row in enumerate(rows[1:], start=2):
        d = dict(zip(headers, row + [""] * (len(headers) - len(row))))
        d["_row"] = i
        out.append(d)
    return out


def update_cliente(nombre, updates, row_idx=None):
    """
    Actualiza un cliente.

    Args:
        nombre: Nombre del cliente (para buscar la fila, si row_idx es None).
        updates: Dict con campos a actualizar.
        row_idx: Fila 1-based en la hoja. Si se provee, evita la re-lectura
            (mucho mas rapido, crucial cuando se actualizan muchos clientes
            en batch — Sheets API tiene limite 60 reads/min).
    """
    field_to_col = {
        "nombre": 0, "telefono": 1, "email": 2, "direccion": 3,
        "depto": 4, "comuna": 5, "codigo_drivin": 6, "marca": 7,
        "precio_especial": 8, "total_pedidos": 9, "ultimo_pedido": 10,
        "estado": 11,
    }

    target_row = row_idx
    if target_row is None:
        rows = _read_sheet(TAB_CLIENTES)
        for i, row in enumerate(rows):
            if i == 0:
                continue
            if row and row[0].lower() == nombre.lower():
                target_row = i + 1
                break
        if not target_row:
            raise ValueError(f"Cliente '{nombre}' no encontrado")

    # Batch: 1 llamada con todos los campos en vez de N llamadas
    data = []
    for field, value in updates.items():
        col_idx = field_to_col.get(field)
        if col_idx is None:
            continue
        data.append({
            "range": f"'{TAB_CLIENTES}'!{chr(65 + col_idx)}{target_row}",
            "values": [[str(value)]],
        })

    if not data:
        return

    service = _get_service()
    _retry(lambda: service.spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"valueInputOption": "USER_ENTERED", "data": data},
    ).execute())


# ===== PAGOS =====

def get_pagos(fecha=None):
    """Obtiene registros de pago, opcionalmente filtrados por fecha."""
    rows = _read_sheet(TAB_PAGOS)
    if len(rows) < 2:
        return []
    headers = rows[0]
    pagos = []
    for row in rows[1:]:
        pago = dict(zip(headers, row + [""] * (len(headers) - len(row))))
        if fecha:
            if pago.get("Fecha", "") == fecha:
                pagos.append(pago)
        else:
            pagos.append(pago)
    return pagos


_pagos_header_checked = False


def _ensure_pagos_email_id_header():
    """Garantiza que la hoja PAGOS tenga el header 'Email ID' en columna H."""
    global _pagos_header_checked
    if _pagos_header_checked:
        return
    try:
        rows = _read_sheet(TAB_PAGOS, "!A1:H1")
        header = rows[0] if rows else []
        if len(header) < 8 or not header[7].strip():
            service = _get_service()
            service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{TAB_PAGOS}'!H1",
                valueInputOption="USER_ENTERED",
                body={"values": [["Email ID"]]},
            ).execute()
    except Exception as e:
        log.warning("No se pudo asegurar header 'Email ID' en PAGOS: %s", e)
    _pagos_header_checked = True


def add_pago(pago):
    """
    Registra un pago.

    Args:
        pago: Dict con campos: fecha, monto, medio, referencia,
            pedido_vinculado, cliente, estado, email_id.
    """
    _ensure_pagos_email_id_header()
    row = [
        pago.get("fecha", datetime.now().strftime("%d/%m/%Y")),
        str(pago.get("monto", "")),
        pago.get("medio", ""),
        pago.get("referencia", ""),
        str(pago.get("pedido_vinculado", "")),
        pago.get("cliente", ""),
        pago.get("estado", "PENDIENTE"),
        pago.get("email_id", ""),
    ]
    _append_sheet(TAB_PAGOS, [row])


def get_pago_email_ids():
    """Devuelve el set de email_id ya registrados en PAGOS (para dedup)."""
    rows = _read_sheet(TAB_PAGOS)
    if len(rows) < 2:
        return set()
    headers = rows[0]
    try:
        idx = headers.index("Email ID")
    except ValueError:
        # Backfill: si la hoja aun no tiene columna, asumir ultima
        idx = 7
    ids = set()
    for row in rows[1:]:
        if idx < len(row) and row[idx].strip():
            ids.add(row[idx].strip())
    return ids


# ===== LOG (helper usado por log_client.py) =====

def _ensure_log_tab():
    """Crea la hoja LOG si no existe."""
    service = _get_service()
    meta = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    tabs = [s["properties"]["title"] for s in meta["sheets"]]
    if TAB_LOG not in tabs:
        _retry(lambda: service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": TAB_LOG}}}]},
        ).execute())
        _write_sheet(TAB_LOG, "A1:F1", [[
            "Fecha/Hora", "Tipo", "Accion", "Detalle", "Resultado", "Origen",
        ]])


# ===== TEST =====

def test_connection():
    """Prueba la conexion a Google Sheets."""
    try:
        rows = _read_sheet(TAB_OPERACION, "!A1:A1")
        return {"ok": True, "message": "Conexion exitosa a planilla Pedidos 2026."}
    except Exception as e:
        return {"ok": False, "message": f"Error: {e}"}


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    print("=== Test Google Sheets Client ===\n")

    result = test_connection()
    print(f"Conexion: {result['message']}")

    if result["ok"]:
        pedidos = get_pedidos()
        print(f"Pedidos en planilla: {len(pedidos)}")

        clientes = get_clientes()
        print(f"Clientes en planilla: {len(clientes)}")

        pagos = get_pagos()
        print(f"Pagos en planilla: {len(pagos)}")
