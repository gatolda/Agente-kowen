"""
Cliente de Google Sheets para Agente Kowen.
Lee y escribe datos en la planilla "Pedidos 2026".
Tabs: OPERACION DIARIA, CLIENTES, PAGOS.
"""

import os
from datetime import datetime
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
SPREADSHEET_ID = os.getenv(
    "GOOGLE_SHEETS_KOWEN_ID",
    "11cG1jArLtQrfmAqX-Qqsfx3Eqkns3Z80Ff9rCk2WwQU",
)

# Nombres de las hojas
TAB_OPERACION = "OPERACION DIARIA"
TAB_CLIENTES = "CLIENTES"
TAB_PAGOS = "PAGOS"

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
    elif os.getenv("GOOGLE_SA_JSON"):
        import json
        from google.oauth2.service_account import Credentials as SACredentials
        sa_info = json.loads(os.getenv("GOOGLE_SA_JSON"))
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


def _read_sheet(tab, range_suffix=""):
    """Lee datos de una hoja."""
    service = _get_service()
    range_str = f"'{tab}'{range_suffix}" if range_suffix else f"'{tab}'"
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=range_str,
    ).execute()
    return result.get("values", [])


def _write_sheet(tab, range_suffix, values, input_option="USER_ENTERED"):
    """Escribe datos en una hoja."""
    service = _get_service()
    range_str = f"'{tab}'!{range_suffix}"
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=range_str,
        valueInputOption=input_option,
        body={"values": values},
    ).execute()


def _append_sheet(tab, values, input_option="USER_ENTERED"):
    """Agrega filas al final de una hoja."""
    service = _get_service()
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{tab}'!A:A",
        valueInputOption=input_option,
        insertDataOption="INSERT_ROWS",
        body={"values": values},
    ).execute()


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

    # Obtener sheet ID
    service = _get_service()
    meta = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    sheet_id = None
    for s in meta["sheets"]:
        if s["properties"]["title"] == TAB_OPERACION:
            sheet_id = s["properties"]["sheetId"]
            break

    service.spreadsheets().batchUpdate(
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
    ).execute()


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
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"valueInputOption": "USER_ENTERED", "data": data},
        ).execute()


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
        "0",
        "",
        "Activo",
    ]
    _append_sheet(TAB_CLIENTES, [row])


def update_cliente(nombre, updates):
    """Actualiza un cliente por nombre exacto."""
    field_to_col = {
        "nombre": 0, "telefono": 1, "email": 2, "direccion": 3,
        "depto": 4, "comuna": 5, "codigo_drivin": 6, "marca": 7,
        "precio_especial": 8, "total_pedidos": 9, "ultimo_pedido": 10,
        "estado": 11,
    }

    rows = _read_sheet(TAB_CLIENTES)
    target_row = None
    for i, row in enumerate(rows):
        if i == 0:
            continue
        if row and row[0].lower() == nombre.lower():
            target_row = i + 1
            break

    if not target_row:
        raise ValueError(f"Cliente '{nombre}' no encontrado")

    service = _get_service()
    for field, value in updates.items():
        col_idx = field_to_col.get(field)
        if col_idx is None:
            continue
        cell = f"'{TAB_CLIENTES}'!{chr(65 + col_idx)}{target_row}"
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=cell,
            valueInputOption="USER_ENTERED",
            body={"values": [[str(value)]]},
        ).execute()


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


def add_pago(pago):
    """
    Registra un pago.

    Args:
        pago: Dict con campos: fecha, monto, medio, referencia,
            pedido_vinculado, cliente, estado.
    """
    row = [
        pago.get("fecha", datetime.now().strftime("%d/%m/%Y")),
        str(pago.get("monto", "")),
        pago.get("medio", ""),
        pago.get("referencia", ""),
        str(pago.get("pedido_vinculado", "")),
        pago.get("cliente", ""),
        pago.get("estado", "PENDIENTE"),
    ]
    _append_sheet(TAB_PAGOS, [row])


# ===== SINCRONIZACION =====

def check_bsale_orders(orders):
    """
    Revisa pedidos de Bsale y marca cuales ya existen en el sistema (ultimos 7 dias).

    Args:
        orders: Lista de pedidos de bsale_client.get_web_orders().

    Returns:
        Lista de dicts con campo extra 'existe' (bool) y 'motivo' (str).
    """
    from unidecode import unidecode

    existing = get_pedidos()

    # Set de numeros Bsale existentes
    existing_bsale = {p.get("Pedido Bsale", "") for p in existing if p.get("Pedido Bsale")}

    # Set de direccion normalizada + cantidad (ultimos 7 dias)
    from datetime import timedelta
    hoy = datetime.now()
    existing_dirs = set()
    for p in existing:
        fecha_str = p.get("Fecha", "")
        if fecha_str:
            try:
                parts = fecha_str.split("/")
                fecha_p = datetime(int(parts[2]), int(parts[1]), int(parts[0]))
                if (hoy - fecha_p).days <= 7:
                    dir_norm = unidecode(p.get("Direccion", "")).lower().strip()
                    cant = str(p.get("Cant", "")).strip()
                    existing_dirs.add((dir_norm, cant))
            except (ValueError, IndexError):
                pass

    # Verificar entregas en driv.in (ultimos 10 dias)
    delivered_addrs = set()
    try:
        import drivin_client
        today = hoy.strftime("%Y-%m-%d")
        ten_ago = (hoy - timedelta(days=10)).strftime("%Y-%m-%d")
        # Consultar en tramos de 3 dias para evitar error 403
        from datetime import date as _date
        d = hoy - timedelta(days=10)
        while d < hoy:
            d_end = min(d + timedelta(days=3), hoy)
            try:
                pods = drivin_client.get_pods(d.strftime("%Y-%m-%d"), d_end.strftime("%Y-%m-%d"))
                for pod in pods.get("response", []):
                    ords = pod.get("orders", [])
                    if ords and ords[0].get("status") == "approved":
                        delivered_addrs.add(unidecode(pod.get("address_name", "")).lower().strip())
            except Exception:
                pass
            d = d_end
    except Exception:
        pass

    result = []
    for order in orders:
        bsale_nro = str(order["pedido_nro"])
        dir_norm = unidecode(order.get("direccion", "")).lower().strip()
        cant = str(order.get("cantidad", "")).strip()

        existe = False
        motivo = ""

        if bsale_nro in existing_bsale:
            existe = True
            motivo = "Nro Bsale ya registrado"
        elif (dir_norm, cant) in existing_dirs:
            existe = True
            motivo = "Direccion + cantidad coincide (ultimos 7 dias)"
        elif any(dir_norm[:15] in d or d[:15] in dir_norm for d in delivered_addrs):
            existe = True
            motivo = "Ya entregado en driv.in"

        result.append({**order, "existe": existe, "motivo": motivo})

    return result


def sync_from_bsale(orders, fecha_destino=None):
    """
    Importa pedidos de Bsale a OPERACION DIARIA evitando duplicados.

    Args:
        orders: Lista de pedidos de bsale_client.get_web_orders().
        fecha_destino: Fecha para los pedidos (DD/MM/YYYY). None = usa la fecha del pedido.

    Returns:
        Cantidad de pedidos agregados.
    """
    checked = check_bsale_orders(orders)
    nuevos_raw = [o for o in checked if not o["existe"]]

    nuevos = []
    for order in nuevos_raw:
        fecha = fecha_destino
        if not fecha:
            fecha = order.get("fecha", "")
            if fecha and "-" in fecha:
                parts = fecha.split("-")
                fecha = f"{parts[2]}/{parts[1]}/{parts[0]}"

        nuevos.append({
            "fecha": fecha or datetime.now().strftime("%d/%m/%Y"),
            "direccion": order.get("direccion", ""),
            "depto": order.get("depto", ""),
            "comuna": order.get("comuna", ""),
            "cant": order.get("cantidad", 0),
            "marca": order.get("marca", "KOWEN").upper(),
            "cliente": order.get("cliente", ""),
            "telefono": order.get("telefono", ""),
            "email": order.get("email", ""),
            "canal": "WEB",
            "estado_pedido": "PENDIENTE",
            "estado_pago": "PENDIENTE",
            "pedido_bsale": str(order["pedido_nro"]),
        })

    if nuevos:
        add_pedidos(nuevos)

    return len(nuevos)


def import_from_drivin(scenario_token, fecha=None):
    """
    Importa pedidos desde un escenario de driv.in a OPERACION DIARIA.
    Util cuando la ruta ya esta hecha en driv.in y queremos poblar la planilla.

    Args:
        scenario_token: Token del escenario.
        fecha: Fecha para los pedidos (DD/MM/YYYY). None = hoy.

    Returns:
        Cantidad de pedidos importados.
    """
    import drivin_client

    if not fecha:
        fecha = datetime.now().strftime("%d/%m/%Y")

    orders_data = drivin_client.get_orders(scenario_token)
    response = orders_data.get("response", [])

    if not response:
        return 0

    # Mapear vehiculos a conductores
    driver_map = {}
    try:
        results = drivin_client.get_results(scenario_token)
        for route in results.get("response", []):
            driver_info = route.get("driver_name", route.get("driver", {}))
            if isinstance(driver_info, dict):
                driver_name = driver_info.get("full_name", "")
            else:
                driver_name = str(driver_info)
            vehicle = route.get("vehicle_code", "")
            if vehicle and driver_name:
                driver_map[vehicle] = driver_name
    except Exception:
        pass

    # Evitar duplicados
    existing = get_pedidos(fecha)
    existing_codes = {p.get("Codigo Drivin", "") for p in existing if p.get("Codigo Drivin")}

    nuevos = []
    for client in response:
        code = client.get("code", "")
        address = client.get("address_1", "")
        address2 = client.get("address_2", "").strip()
        comuna = client.get("area_level_3", "")

        for order in client.get("orders", []):
            order_code = order.get("code", "")
            units = int(order.get("units_1", 0))
            description = order.get("description", "Kowen")
            vehicle_code = order.get("vehicle_code", "")
            trip_number = order.get("trip_number", 1)
            name = order.get("name", "")

            if code in existing_codes:
                continue

            # Marca
            marca = "KOWEN"
            if "cactus" in description.lower():
                marca = "CACTUS"

            # Aliado
            aliado = ""
            for al in ("Bernardino", "Puragua Ivan", "Pulmahue"):
                if al.lower() in description.lower():
                    aliado = al
                    break

            # Repartidor desde vehiculo
            repartidor = driver_map.get(vehicle_code, "")

            # Vuelta
            vuelta = ""
            if trip_number == 2:
                vuelta = "2a"
            elif trip_number == 3:
                vuelta = "3a"
            elif trip_number == 1:
                vuelta = "1a"

            # Depto desde address2 o name
            depto = address2 if address2 and address2 != " " else ""
            if not depto and name and name != address:
                # Extraer parte extra del name que no esta en address
                extra = name.replace(address, "").strip()
                if extra:
                    depto = extra

            nuevos.append({
                "fecha": fecha,
                "direccion": address,
                "depto": depto,
                "comuna": comuna,
                "codigo_drivin": code,
                "cant": units,
                "marca": marca,
                "repartidor": repartidor,
                "vuelta": vuelta,
                "estado_pedido": "PENDIENTE",
                "estado_pago": "PENDIENTE",
                "aliado": aliado,
                "canal": "MANUAL",
                "plan_drivin": "",
            })

            existing_codes.add(code)

    if nuevos:
        add_pedidos(nuevos)

    return len(nuevos)


def sync_from_planilla_reparto(fecha=None):
    """
    Importa pedidos desde la planilla reparto (PRIMER TURNO) a OPERACION DIARIA.
    Evita duplicados comparando direccion + fecha + cantidad.

    Args:
        fecha: Fecha a importar (DD/MM/YYYY). None = hoy.

    Returns:
        Cantidad de pedidos importados.
    """
    if not fecha:
        fecha = datetime.now().strftime("%d/%m/%Y")

    PLANILLA_REPARTO_ID = "1jNTWO2hkkRBlEamXrQ6BGy28tlAj7ei1Qyyt559mvds"

    service = _get_service()

    # Leer toda la hoja PRIMER TURNO
    result = service.spreadsheets().values().get(
        spreadsheetId=PLANILLA_REPARTO_ID,
        range="'PRIMER TURNO'!A:N",
    ).execute()
    rows = result.get("values", [])

    # Headers en fila 2 (index 1):
    # A=Fecha, B=direccion, C=Repartidor, D=ESTADO PEDIDO, E=com. repartidor,
    # F=observaciones, G=Nombre, H=cantidad, I=Cliente,
    # J=EFECTIVO, K=TRANS./CHEQUE, L=forma de pago, M=Pago por Transferencia, N=FECHA PAGO

    # Filtrar filas de la fecha solicitada con datos reales
    pedidos_reparto = []
    for row in rows[2:]:  # Saltar fila vacia y headers
        if len(row) < 9:
            continue
        row_fecha = row[0].strip() if row[0] else ""
        row_dir = row[1].strip() if len(row) > 1 else ""

        if row_fecha != fecha:
            continue
        if not row_dir or row_dir == "DIRECCION":
            continue

        # Parsear direccion y depto
        direccion = row_dir
        depto = ""
        for sep in [" Dep/Ofi. ", " dpto ", " Dpto ", " OF ", " Of ", " piso "]:
            if sep in direccion:
                parts = direccion.split(sep, 1)
                direccion = parts[0].strip()
                depto = parts[1].strip()
                break

        # Parsear comuna (puede estar al final despues de coma)
        comuna = ""
        if "," in direccion:
            parts = direccion.rsplit(",", 1)
            direccion = parts[0].strip()
            comuna = parts[1].strip()

        repartidor_raw = row[2].strip() if len(row) > 2 else ""
        estado_raw = row[3].strip() if len(row) > 3 else ""
        com_chofer = row[4].strip() if len(row) > 4 else ""
        observaciones = row[5].strip() if len(row) > 5 else ""
        nombre = row[6].strip() if len(row) > 6 else ""
        cantidad = row[7].strip() if len(row) > 7 else "0"
        marca_raw = row[8].strip() if len(row) > 8 else "KOWEN"

        # Parsear pago
        efectivo = row[9].strip() if len(row) > 9 else ""
        transferencia = row[10].strip() if len(row) > 10 else ""
        forma_pago_raw = row[11].strip() if len(row) > 11 else ""
        pago_nombre = row[12].strip() if len(row) > 12 else ""
        fecha_pago = row[13].strip() if len(row) > 13 else ""

        # Mapear repartidor
        repartidor_map = {
            "REPARTIDOR1": "Angel Salas", "REPARTIC": "Angel Salas",
            "DOMI": "Angel Salas", "LEO": "Leo Carreño",
            "SEBA": "Sebastian Ramirez", "NICO": "Nicolas Muñoz",
            "DANIEL": "Daniel Araya", "YHOEL": "Yhoel Del Campo",
            "BACK UP": "",
        }
        repartidor = repartidor_map.get(repartidor_raw.upper(), repartidor_raw)

        # Mapear estado
        estado_map = {
            "ENTREGADO": "ENTREGADO", "PENDIENTE": "PENDIENTE",
            "EN CAMINO": "EN CAMINO", "2a Vuelta": "PENDIENTE",
            "3a Vuelta": "PENDIENTE", "NO ENTREGADO": "NO ENTREGADO",
            "---------------": "PENDIENTE",
        }
        estado = estado_map.get(estado_raw, "PENDIENTE")

        # Vuelta
        vuelta = ""
        if "2a Vuelta" in estado_raw:
            vuelta = "2a"
        elif "3a Vuelta" in estado_raw:
            vuelta = "3a"

        # Marca
        marca = marca_raw.upper() if marca_raw else "KOWEN"
        # Detectar aliados
        aliado = ""
        if marca in ("BERNARDINO", "PURAGUA IVAN", "PULMAHUE"):
            aliado = marca
            marca = "KOWEN"

        # Forma de pago
        forma_pago = ""
        estado_pago = "PENDIENTE"
        if forma_pago_raw and forma_pago_raw != "-----------":
            if "TRANS" in forma_pago_raw.upper() or "CHEQ" in forma_pago_raw.upper():
                forma_pago = "Transferencia"
            elif "EFECT" in forma_pago_raw.upper():
                forma_pago = "Efectivo"
            elif "PLAN EMPRESA" in forma_pago_raw.upper():
                forma_pago = "Transferencia"
                observaciones = (observaciones + " PLAN EMPRESA").strip()
        if efectivo and efectivo not in ("", "0", "$0"):
            forma_pago = "Efectivo"
            estado_pago = "PAGADO"
        if transferencia and transferencia not in ("", "0", "$0"):
            forma_pago = "Transferencia"
            estado_pago = "PAGADO"

        pedidos_reparto.append({
            "fecha": fecha,
            "direccion": direccion,
            "depto": depto,
            "comuna": comuna,
            "cant": cantidad,
            "marca": marca,
            "repartidor": repartidor,
            "estado_pedido": estado,
            "vuelta": vuelta,
            "observaciones": observaciones,
            "com_chofer": com_chofer,
            "cliente": nombre,
            "efectivo": efectivo,
            "transferencia": transferencia,
            "forma_pago": forma_pago,
            "estado_pago": estado_pago,
            "fecha_pago": fecha_pago,
            "aliado": aliado,
            "canal": "MANUAL",
        })

    if not pedidos_reparto:
        return 0

    # Evitar duplicados: comparar direccion + fecha existentes
    existing = get_pedidos(fecha)
    existing_dirs = set()
    for p in existing:
        key = (p.get("Direccion", "").lower().strip(), p.get("Cant", ""))
        existing_dirs.add(key)

    nuevos = []
    for p in pedidos_reparto:
        key = (p["direccion"].lower().strip(), str(p["cant"]))
        if key not in existing_dirs:
            nuevos.append(p)

    if nuevos:
        add_pedidos(nuevos)

    return len(nuevos)


def sync_from_drivin(fecha=None, plan_name=""):
    """
    Actualiza estado, repartidor y comentarios desde driv.in usando PODs.

    Args:
        fecha: Fecha a sincronizar (DD/MM/YYYY). None = hoy.
        plan_name: Nombre del plan (referencia).

    Returns:
        Cantidad de pedidos actualizados.
    """
    import drivin_client

    if not fecha:
        fecha = datetime.now().strftime("%d/%m/%Y")

    # Convertir fecha a YYYY-MM-DD para la API
    parts = fecha.split("/")
    api_date = f"{parts[2]}-{parts[1]}-{parts[0]}"

    # Obtener PODs del dia
    pods_data = drivin_client.get_pods(api_date, api_date)
    pods = pods_data.get("response", [])

    if not pods:
        return 0

    # Leer pedidos existentes de esa fecha
    existing = get_pedidos(fecha)
    if not existing:
        return 0

    # Mapear address_code a pedido
    code_to_pedido = {}
    for p in existing:
        code = p.get("Codigo Drivin", "")
        if code:
            code_to_pedido[code] = p

    estado_map = {
        "approved": "ENTREGADO",
        "delivered": "ENTREGADO",
        "rejected": "NO ENTREGADO",
        "not_delivered": "NO ENTREGADO",
        "pending": "EN CAMINO",
        "in-transit": "EN CAMINO",
    }

    updates = []
    seen = set()

    for pod in pods:
        address_code = pod.get("address_code", "")
        if not address_code or address_code in seen:
            continue
        seen.add(address_code)

        pedido = code_to_pedido.get(address_code)
        if not pedido:
            continue

        pedido_num = pedido.get("#", "")
        if not pedido_num or not pedido_num.isdigit():
            continue

        driver = pod.get("driver_name", "")
        vehicle = pod.get("vehicle_code", "")
        comment = pod.get("comment", "")
        trip = pod.get("trip_number", 1)

        # Estado del pedido (viene en orders dentro del POD)
        estado = ""
        order_comment = ""
        for order in pod.get("orders", []):
            status = order.get("status", "")
            if status:
                estado = estado_map.get(status, "")
            oc = order.get("comment", "")
            if oc:
                order_comment = oc

        # Determinar si la ruta ya inicio
        route_started = pod.get("route_is_started", False)
        route_finished = pod.get("route_is_finished", False)

        if not estado:
            if route_finished:
                estado = "NO ENTREGADO"
            elif route_started:
                estado = "EN CAMINO"
            else:
                estado = "PENDIENTE"

        # Vuelta
        vuelta = ""
        if trip == 2:
            vuelta = "2a"
        elif trip == 3:
            vuelta = "3a"
        elif trip == 1:
            vuelta = "1a"

        upd = {}
        if estado:
            upd["estado_pedido"] = estado
        if driver:
            upd["repartidor"] = driver
        if comment or order_comment:
            upd["com_chofer"] = comment or order_comment
        if vuelta:
            upd["vuelta"] = vuelta
        if plan_name:
            upd["plan_drivin"] = plan_name

        if upd:
            updates.append((int(pedido_num), upd))

    if updates:
        update_pedidos_batch(updates)

    return len(updates)


def sync_to_planilla_reparto(fecha=None):
    """
    Sincroniza pedidos de OPERACION DIARIA hacia la planilla reparto (PRIMER TURNO).
    Actualiza filas existentes por direccion o agrega nuevas.

    Args:
        fecha: Fecha a sincronizar (DD/MM/YYYY). None = hoy.

    Returns:
        Cantidad de filas actualizadas/agregadas.
    """
    if not fecha:
        fecha = datetime.now().strftime("%d/%m/%Y")

    PLANILLA_REPARTO_ID = "1jNTWO2hkkRBlEamXrQ6BGy28tlAj7ei1Qyyt559mvds"

    # Leer pedidos de nuestro sistema
    pedidos = get_pedidos(fecha)
    if not pedidos:
        return 0

    service = _get_service()

    # Leer planilla reparto para encontrar filas existentes de esa fecha
    result = service.spreadsheets().values().get(
        spreadsheetId=PLANILLA_REPARTO_ID,
        range="'PRIMER TURNO'!A:N",
    ).execute()
    rows = result.get("values", [])

    # Mapear filas existentes por direccion normalizada
    from unidecode import unidecode
    existing_rows = {}  # dir_normalizada -> row_index (1-based)
    for i, row in enumerate(rows):
        if i < 2:
            continue
        if len(row) < 2:
            continue
        row_fecha = row[0].strip() if row[0] else ""
        row_dir = row[1].strip() if len(row) > 1 else ""
        if row_fecha == fecha and row_dir and row_dir != "DIRECCION":
            key = unidecode(row_dir).lower().strip()
            existing_rows[key] = i + 1  # 1-based for Sheets

    # Mapear repartidor inverso
    rep_map_inv = {
        "Angel Salas": "REPARTIC", "Leo Carreño": "LEO",
        "Sebastian Ramirez": "SEBA", "Nicolas Muñoz": "NICO",
        "Daniel Araya": "DANIEL", "Yhoel Del Campo": "BACK UP",
    }

    # Mapear estado inverso
    estado_map_inv = {
        "ENTREGADO": "ENTREGADO ", "PENDIENTE": "PENDIENTE",
        "EN CAMINO": "EN CAMINO ", "NO ENTREGADO": "NO ENTREGADO",
    }

    batch_data = []
    new_rows = []
    count = 0

    for p in pedidos:
        dir_completa = p.get("Direccion", "")
        depto = p.get("Depto", "")
        if depto:
            dir_display = f"{dir_completa} {depto}"
        else:
            dir_display = dir_completa
        comuna = p.get("Comuna", "")
        if comuna:
            dir_display = f"{dir_display}, {comuna}"

        repartidor = rep_map_inv.get(p.get("Repartidor", ""), p.get("Repartidor", ""))
        estado = estado_map_inv.get(p.get("Estado Pedido", ""), p.get("Estado Pedido", ""))
        vuelta = p.get("Vuelta", "")
        if vuelta:
            estado = f"{vuelta} Vuelta"

        # Buscar fila existente
        key = unidecode(dir_completa).lower().strip()
        existing_row = existing_rows.get(key)

        row_data = [
            fecha,           # A: Fecha
            dir_display,     # B: direccion
            repartidor,      # C: Repartidor
            estado,          # D: ESTADO PEDIDO
            p.get("Com. Chofer", ""),  # E: com. repartidor
            p.get("Observaciones", ""),  # F: observaciones
            "",              # G: Nombre
            str(p.get("Cant", "")),  # H: cantidad
            p.get("Marca", "KOWEN"),  # I: Cliente (marca)
            p.get("Efectivo", ""),     # J: EFECTIVO
            p.get("Transferencia", ""),  # K: TRANS
            p.get("Forma Pago", ""),   # L: forma de pago
            "",              # M: Pago por Transferencia
            p.get("Fecha Pago", ""),   # N: FECHA PAGO
        ]

        if existing_row:
            # Actualizar fila existente
            batch_data.append({
                "range": f"'PRIMER TURNO'!A{existing_row}:N{existing_row}",
                "values": [row_data],
            })
        else:
            new_rows.append(row_data)

        count += 1

    # Batch update filas existentes
    if batch_data:
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=PLANILLA_REPARTO_ID,
            body={"valueInputOption": "USER_ENTERED", "data": batch_data},
        ).execute()

    # Append filas nuevas
    if new_rows:
        service.spreadsheets().values().append(
            spreadsheetId=PLANILLA_REPARTO_ID,
            range="'PRIMER TURNO'!A:A",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": new_rows},
        ).execute()

    return count


# ===== RUTINA DIARIA =====

def rutina_diaria(fecha_hoy=None):
    """
    Rutina automatica diaria del agente:
    1. Revisa pedidos de ayer en driv.in (PODs)
    2. Marca entregados como ENTREGADO
    3. Mueve no entregados/pendientes a hoy
    4. Importa pedidos nuevos de Bsale
    5. Importa pedidos de planilla reparto
    6. Busca codigos driv.in para pedidos sin codigo

    Args:
        fecha_hoy: Fecha de hoy (DD/MM/YYYY). None = hoy.

    Returns:
        Dict con resumen de acciones realizadas.
    """
    import drivin_client
    from unidecode import unidecode
    from datetime import timedelta

    if not fecha_hoy:
        fecha_hoy = datetime.now().strftime("%d/%m/%Y")

    # Calcular fecha de ayer
    parts = fecha_hoy.split("/")
    hoy_dt = datetime(int(parts[2]), int(parts[1]), int(parts[0]))
    ayer_dt = hoy_dt - timedelta(days=1)
    fecha_ayer = ayer_dt.strftime("%d/%m/%Y")
    ayer_api = ayer_dt.strftime("%Y-%m-%d")
    hoy_api = hoy_dt.strftime("%Y-%m-%d")

    resultado = {
        "fecha_ayer": fecha_ayer,
        "fecha_hoy": fecha_hoy,
        "entregados_ayer": 0,
        "no_entregados_ayer": 0,
        "movidos_a_hoy": 0,
        "duplicados_eliminados": 0,
        "bsale_importados": 0,
        "planilla_importados": 0,
        "codigos_asignados": 0,
        "errores": [],
    }

    # --- PASO 1: Revisar PODs de ayer ---
    pedidos_ayer = get_pedidos(fecha_ayer)
    if not pedidos_ayer:
        resultado["errores"].append("No hay pedidos de ayer para revisar")
    else:
        # Consultar driv.in PODs
        try:
            pods_data = drivin_client.get_pods(ayer_api, hoy_api)
            pods = pods_data.get("response", [])
        except Exception as e:
            pods = []
            resultado["errores"].append(f"Error consultando PODs: {e}")

        # Mapear PODs por address_code
        pod_status = {}  # code -> status
        for pod in pods:
            code = pod.get("address_code", "")
            orders = pod.get("orders", [])
            if code and orders:
                pod_status[code] = orders[0].get("status", "pending")

        # Clasificar pedidos de ayer
        entregados = []
        no_entregados = []
        pendientes_sin_pod = []

        for p in pedidos_ayer:
            nro = p.get("#", "")
            if not nro or not nro.isdigit():
                continue
            nro = int(nro)
            codigo = p.get("Codigo Drivin", "")
            estado = p.get("Estado Pedido", "")

            if estado == "ENTREGADO":
                continue  # Ya marcado

            status_drivin = pod_status.get(codigo, "")

            if status_drivin == "approved":
                entregados.append(nro)
            elif status_drivin == "rejected":
                no_entregados.append(nro)
            elif estado in ("PENDIENTE", "EN CAMINO"):
                pendientes_sin_pod.append(nro)

        # --- PASO 2: Marcar entregados ---
        if entregados:
            updates = [(n, {"estado_pedido": "ENTREGADO"}) for n in entregados]
            update_pedidos_batch(updates)
            resultado["entregados_ayer"] = len(entregados)

        # --- PASO 3: Marcar y mover no entregados ---
        mover = no_entregados + pendientes_sin_pod
        resultado["no_entregados_ayer"] = len(no_entregados)

        if mover:
            updates = [(n, {
                "fecha": fecha_hoy,
                "estado_pedido": "PENDIENTE",
                "plan_drivin": "",
            }) for n in mover]
            update_pedidos_batch(updates)
            resultado["movidos_a_hoy"] = len(mover)

        # --- Detectar duplicados (misma direccion en entregados y pendientes) ---
        pedidos_ayer_refresh = get_pedidos(fecha_ayer)
        dirs_entregadas = set()
        for p in pedidos_ayer_refresh:
            if p.get("Estado Pedido") == "ENTREGADO":
                dirs_entregadas.add(unidecode(p.get("Direccion", "")).lower().strip())

        # Revisar los movidos a hoy si alguno coincide con entregado (duplicado)
        pedidos_hoy_pre = get_pedidos(fecha_hoy)
        for p in pedidos_hoy_pre:
            dir_norm = unidecode(p.get("Direccion", "")).lower().strip()
            nro = p.get("#", "")
            if dir_norm in dirs_entregadas and nro and nro.isdigit():
                try:
                    delete_pedido(int(nro))
                    resultado["duplicados_eliminados"] += 1
                except Exception:
                    pass

    # --- PASO 4: Importar pedidos web de Bsale ---
    try:
        import bsale_client
        # Obtener ultimo numero Bsale en el sistema
        all_pedidos = get_pedidos()
        bsale_nums = [int(p.get("Pedido Bsale", "0") or "0") for p in all_pedidos]
        since = max(bsale_nums) if bsale_nums else 0

        orders = bsale_client.get_web_orders(since_number=since)
        if orders:
            count = sync_from_bsale(orders, fecha_destino=fecha_hoy)
            resultado["bsale_importados"] = count
    except Exception as e:
        resultado["errores"].append(f"Error importando Bsale: {e}")

    # --- PASO 5: Importar desde planilla reparto ---
    try:
        count = sync_from_planilla_reparto(fecha=fecha_hoy)
        resultado["planilla_importados"] = count
    except Exception as e:
        resultado["errores"].append(f"Error importando planilla: {e}")

    # --- PASO 6: Asignar codigos driv.in ---
    try:
        import address_matcher
        pedidos_hoy = get_pedidos(fecha_hoy)
        sin_codigo = [p for p in pedidos_hoy if not p.get("Codigo Drivin", "").strip()]

        if sin_codigo:
            addresses = address_matcher.load_cache()
            updates = []
            for p in sin_codigo:
                nro = p.get("#", "")
                if not nro or not nro.isdigit():
                    continue
                direccion = p.get("Direccion", "")
                depto = p.get("Depto", "")
                comuna = p.get("Comuna", "")
                code, confidence = address_matcher.auto_match(
                    direccion, depto, comuna, addresses
                )
                if confidence == "auto" and code:
                    updates.append((int(nro), {"codigo_drivin": code}))

            if updates:
                update_pedidos_batch(updates)
                resultado["codigos_asignados"] = len(updates)
    except Exception as e:
        resultado["errores"].append(f"Error asignando codigos: {e}")

    # --- PASO 7: Crear plan en driv.in y subir pedidos ---
    try:
        pedidos_hoy = get_pedidos(fecha_hoy)
        con_codigo = [p for p in pedidos_hoy
                      if p.get("Codigo Drivin", "").strip()
                      and p.get("Estado Pedido") == "PENDIENTE"
                      and not p.get("Plan Drivin", "").strip()]

        if con_codigo:
            # Convertir fecha para API
            parts_h = fecha_hoy.split("/")
            api_date = f"{parts_h[2]}-{parts_h[1]}-{parts_h[0]}"
            plan_name = f"{fecha_hoy}API"
            suffix = f"{parts_h[1]}{parts_h[0]}"

            # Verificar si ya existe un plan para hoy
            scenarios = drivin_client.get_scenarios_by_date(api_date)
            existing = scenarios.get("response", [])
            scenario_token = None

            for s in existing:
                if s.get("description") == plan_name:
                    scenario_token = s.get("token", s.get("scenario_token", ""))
                    break

            # Armar clients
            clients = []
            for p in con_codigo:
                code = p.get("Codigo Drivin", "")
                marca = p.get("Marca", "KOWEN")
                cant = int(p.get("Cant", 0) or 0)
                desc = f"{marca} - Retiro" if cant == 0 else marca
                order_code = f"{code}-{suffix}"
                clients.append({
                    "code": code,
                    "orders": [{"code": order_code, "description": desc, "units_1": cant}]
                })

            if scenario_token:
                # Agregar a plan existente
                result = drivin_client.create_orders(
                    clients=clients, scenario_token=scenario_token
                )
            else:
                # Crear plan nuevo con pedidos
                result = drivin_client.create_scenario(
                    description=plan_name,
                    date=api_date,
                    clients=clients,
                )
                resp = result.get("response", {})
                scenario_token = resp.get("scenario_token", resp.get("token", ""))

            added = result.get("response", result).get("added", [])
            resultado["drivin_plan"] = plan_name
            resultado["drivin_subidos"] = len(added)
            resultado["drivin_token"] = scenario_token

            # Marcar pedidos con el plan
            updates = []
            for p in con_codigo:
                nro = p.get("#", "")
                if nro and nro.isdigit():
                    updates.append((int(nro), {"plan_drivin": plan_name}))
            if updates:
                update_pedidos_batch(updates)
        else:
            resultado["drivin_plan"] = ""
            resultado["drivin_subidos"] = 0
            resultado["drivin_token"] = ""
    except Exception as e:
        resultado["errores"].append(f"Error subiendo a driv.in: {e}")

    return resultado


# ===== RESUMEN =====

def resumen_dia(fecha=None):
    """
    Genera un resumen del dia.

    Args:
        fecha: Fecha a resumir (DD/MM/YYYY). None = hoy.

    Returns:
        Dict con totales y conteos.
    """
    if not fecha:
        fecha = datetime.now().strftime("%d/%m/%Y")

    pedidos = get_pedidos(fecha)

    total_botellones = sum(int(p.get("Cant", 0) or 0) for p in pedidos)
    entregados = sum(1 for p in pedidos if p.get("Estado Pedido") == "ENTREGADO")
    pendientes = sum(1 for p in pedidos if p.get("Estado Pedido") == "PENDIENTE")
    en_camino = sum(1 for p in pedidos if p.get("Estado Pedido") == "EN CAMINO")
    no_entregados = sum(1 for p in pedidos if p.get("Estado Pedido") == "NO ENTREGADO")
    pagados = sum(1 for p in pedidos if p.get("Estado Pago") == "PAGADO")
    por_cobrar = sum(1 for p in pedidos if p.get("Estado Pago") in ("PENDIENTE", "POR CONFIRMAR"))

    return {
        "fecha": fecha,
        "total_pedidos": len(pedidos),
        "total_botellones": total_botellones,
        "entregados": entregados,
        "pendientes": pendientes,
        "en_camino": en_camino,
        "no_entregados": no_entregados,
        "pagados": pagados,
        "por_cobrar": por_cobrar,
    }


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

        resumen = resumen_dia()
        print(f"\nResumen de hoy ({resumen['fecha']}):")
        print(f"  Pedidos: {resumen['total_pedidos']}")
        print(f"  Botellones: {resumen['total_botellones']}")
