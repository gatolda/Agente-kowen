"""
Operaciones de negocio del Agente Kowen.
Sincronizacion, importacion, rutina diaria y resumen.
"""

from datetime import datetime, timedelta
from unidecode import unidecode

from sheets_client import (
    get_pedidos, add_pedidos, update_pedido, update_pedidos_batch,
    delete_pedido, get_clientes, _get_service, _read_sheet, _write_sheet,
    _append_sheet, _retry, _normalize_address, _pedido_to_row,
    OP_COLS, TAB_OPERACION, SPREADSHEET_ID,
)

PLANILLA_REPARTO_ID = "1jNTWO2hkkRBlEamXrQ6BGy28tlAj7ei1Qyyt559mvds"


# ===== SINCRONIZACION =====

def _get_client_frequency(existing):
    """
    Calcula la frecuencia de pedidos por direccion normalizada.
    Retorna dict: dir_norm -> dias promedio entre pedidos.
    """
    from collections import defaultdict
    fechas_por_dir = defaultdict(list)
    for p in existing:
        fecha_str = p.get("Fecha", "")
        if not fecha_str:
            continue
        try:
            parts = fecha_str.split("/")
            fecha_p = datetime(int(parts[2]), int(parts[1]), int(parts[0]))
            dir_norm = _normalize_address(p.get("Direccion", ""))
            depto = unidecode(p.get("Depto", "")).lower().strip()
            key = (dir_norm, depto)
            fechas_por_dir[key].append(fecha_p)
        except (ValueError, IndexError):
            pass

    frecuencias = {}
    for key, fechas in fechas_por_dir.items():
        if len(fechas) < 2:
            continue
        fechas.sort()
        diffs = [(fechas[i+1] - fechas[i]).days for i in range(len(fechas)-1)]
        diffs = [d for d in diffs if d > 0]  # Ignorar mismo dia
        if diffs:
            frecuencias[key] = sum(diffs) / len(diffs)

    return frecuencias


def check_bsale_orders(orders):
    """
    Revisa pedidos de Bsale y marca cuales ya existen en el sistema.
    Detecta duplicados por:
    1. Numero de pedido Bsale
    2. Pedidos activos (PENDIENTE/EN CAMINO) en la misma direccion+depto
    3. Frecuencia del cliente (permite pedidos recurrentes si corresponde)

    Args:
        orders: Lista de pedidos de bsale_client.get_web_orders().

    Returns:
        Lista de dicts con campo extra 'existe' (bool) y 'motivo' (str).
    """
    existing = get_pedidos()

    # Set de numeros Bsale existentes
    existing_bsale = {p.get("Pedido Bsale", "") for p in existing if p.get("Pedido Bsale")}

    # Pedidos activos (PENDIENTE o EN CAMINO) por direccion+depto
    active_dirs = set()
    for p in existing:
        estado = p.get("Estado Pedido", "")
        if estado in ("PENDIENTE", "EN CAMINO"):
            dir_norm = _normalize_address(p.get("Direccion", ""))
            depto = unidecode(p.get("Depto", "")).lower().strip()
            active_dirs.add((dir_norm, depto))

    # Frecuencia de pedidos por cliente (para clientes recurrentes)
    frecuencias = _get_client_frequency(existing)

    # Ultima fecha de entrega por direccion
    ultima_entrega = {}
    for p in existing:
        if p.get("Estado Pedido") == "ENTREGADO":
            fecha_str = p.get("Fecha", "")
            if not fecha_str:
                continue
            try:
                parts = fecha_str.split("/")
                fecha_p = datetime(int(parts[2]), int(parts[1]), int(parts[0]))
                dir_norm = _normalize_address(p.get("Direccion", ""))
                depto = unidecode(p.get("Depto", "")).lower().strip()
                key = (dir_norm, depto)
                if key not in ultima_entrega or fecha_p > ultima_entrega[key]:
                    ultima_entrega[key] = fecha_p
            except (ValueError, IndexError):
                pass

    hoy = datetime.now()

    result = []
    for order in orders:
        bsale_nro = str(order["pedido_nro"])
        dir_norm = _normalize_address(order.get("direccion", ""))
        depto = unidecode(order.get("depto", "")).lower().strip()
        key = (dir_norm, depto)

        existe = False
        motivo = ""

        if bsale_nro in existing_bsale:
            existe = True
            motivo = "Nro Bsale ya registrado"
        elif key in active_dirs:
            # Hay un pedido activo para esta direccion — es duplicado
            existe = True
            motivo = "Ya tiene pedido activo (PENDIENTE/EN CAMINO)"
        elif key in ultima_entrega:
            # Ya fue entregado — verificar si es cliente recurrente
            dias_desde_entrega = (hoy - ultima_entrega[key]).days
            freq = frecuencias.get(key)
            if freq and dias_desde_entrega >= freq * 0.7:
                # Cliente recurrente y ya paso suficiente tiempo — permitir
                existe = False
            elif dias_desde_entrega <= 2:
                # Entregado hace menos de 2 dias y no es recurrente conocido
                existe = True
                motivo = f"Entregado hace {dias_desde_entrega} dia(s)"

        result.append({**order, "existe": existe, "motivo": motivo})

    return result


def sync_from_bsale(orders, fecha_destino=None):
    """
    Importa pedidos de Bsale a OPERACION DIARIA evitando duplicados.
    Usa el numero Bsale como ID unico — si ya existe, no lo importa.

    Args:
        orders: Lista de pedidos de bsale_client.get_web_orders().
        fecha_destino: Fecha fija para todos (DD/MM/YYYY).
                       None = usa la fecha del pedido Bsale convertida a DD/MM/YYYY.

    Returns:
        Cantidad de pedidos agregados.
    """
    checked = check_bsale_orders(orders)
    nuevos_raw = [o for o in checked if not o["existe"]]

    nuevos = []
    for order in nuevos_raw:
        if fecha_destino:
            fecha = fecha_destino
        else:
            # Convertir fecha Bsale (YYYY-MM-DD) a DD/MM/YYYY
            fecha_bsale = order.get("fecha", "")
            if fecha_bsale and "-" in fecha_bsale:
                parts = fecha_bsale.split("-")
                fecha = f"{parts[2]}/{parts[1]}/{parts[0]}"
            else:
                fecha = datetime.now().strftime("%d/%m/%Y")

        nuevos.append({
            "fecha": fecha,
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
    Evita duplicados comparando direccion + depto + cantidad.

    Args:
        fecha: Fecha a importar (DD/MM/YYYY). None = hoy.

    Returns:
        Cantidad de pedidos importados.
    """
    if not fecha:
        fecha = datetime.now().strftime("%d/%m/%Y")

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
        if len(row) < 2:
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

    # Evitar duplicados: comparar conteos por direccion
    # Si la planilla tiene 2 pedidos a "san antonio 113" y nosotros solo 1, importar el faltante
    from collections import Counter
    from unidecode import unidecode as _ud
    existing = get_pedidos(fecha)

    existing_counts = Counter()
    for p in existing:
        dir_key = (
            _normalize_address(p.get("Direccion", "")),
            _ud(p.get("Depto", "")).lower().strip(),
        )
        existing_counts[dir_key] += 1

    # Contar cuantos hay en la planilla por dir_key
    planilla_counts = Counter()
    for p in pedidos_reparto:
        dir_key = (
            _normalize_address(p["direccion"]),
            _ud(p.get("depto", "")).lower().strip(),
        )
        planilla_counts[dir_key] += 1

    # Importar solo los que faltan (planilla tiene mas que existentes)
    import_counts = Counter()
    nuevos = []
    for p in pedidos_reparto:
        dir_key = (
            _normalize_address(p["direccion"]),
            _ud(p.get("depto", "")).lower().strip(),
        )
        import_counts[dir_key] += 1
        # Importar si aun no alcanzamos el conteo existente
        if import_counts[dir_key] > existing_counts[dir_key]:
            nuevos.append(p)

    if nuevos:
        add_pedidos(nuevos)

    return len(nuevos)


PLANILLA_CACTUS_ID = "1w5Klrcbq7-B6HUBCADeIkmv5_r4vhGnsYv7EuJQqxgU"


def sync_from_planilla_cactus(fecha=None):
    """
    Importa pedidos desde la planilla Cactus (Enero 2023) a OPERACION DIARIA.
    La planilla Cactus tiene un header con la fecha y pedidos debajo sin fecha.
    Estructura: B=Direccion, C=Comuna, D=Nombre, E=Repartidor, F=Estado, G=Cant, H=Comentario

    Args:
        fecha: Fecha a importar (DD/MM/YYYY). None = hoy.

    Returns:
        Cantidad de pedidos importados.
    """
    if not fecha:
        fecha = datetime.now().strftime("%d/%m/%Y")

    service = _get_service()

    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=PLANILLA_CACTUS_ID,
            range="'Enero 2023'!A:M",
        ).execute()
    except Exception:
        return 0

    rows = result.get("values", [])
    if not rows:
        return 0

    # Buscar el bloque de hoy: fila con la fecha como header, pedidos debajo
    header_idx = None
    for i, row in enumerate(rows):
        if not row:
            continue
        cell = str(row[0]).strip()
        if cell == fecha:
            header_idx = i
            break

    if header_idx is None:
        return 0

    # Leer pedidos debajo del header hasta la proxima seccion de resumen
    pedidos_cactus = []
    repartidor_map = {
        "REPARTIDOR 1": "Angel Salas", "REPARTIC": "Angel Salas",
        "DOMI": "Angel Salas", "LEO": "Leo Carreño",
        "SEBA": "Sebastian Ramirez", "NICO": "Nicolas Muñoz",
        "DANIEL": "Daniel Araya", "YHOEL": "Yhoel Del Campo",
        "BACK UP": "", "PULMA": "",
    }
    estado_map = {
        "ENTREGADO": "ENTREGADO", "PENDIENTE": "PENDIENTE",
        "EN CAMINO": "EN CAMINO", "2DA VUELTA": "PENDIENTE",
        "3RA VUELTA": "PENDIENTE", "NO ENTREGADO": "NO ENTREGADO",
    }

    for row in rows[header_idx + 1:]:
        if not row or len(row) < 2:
            continue

        # Detectar seccion de resumen (CARGA 1, TOTAL, etc.)
        cell_b = str(row[1]).strip() if len(row) > 1 else ""
        if cell_b.startswith("CARGA ") or cell_b == "TOTAL CARGAS" or cell_b == "CARGA DISPONIBLE":
            break

        # Saltar filas vacias o con #N/A en direccion
        if not cell_b or cell_b == "#N/A" or cell_b == "Direccion":
            continue

        direccion = cell_b
        comuna = str(row[2]).strip() if len(row) > 2 else ""
        if comuna == "#N/A":
            comuna = ""
        nombre = str(row[3]).strip() if len(row) > 3 else ""
        if nombre == "#N/A":
            nombre = ""
        repartidor_raw = str(row[4]).strip() if len(row) > 4 else ""
        estado_raw = str(row[5]).strip() if len(row) > 5 else "PENDIENTE"
        cantidad = str(row[6]).strip() if len(row) > 6 else "0"
        comentario = str(row[7]).strip() if len(row) > 7 else ""

        # Parsear depto de la direccion
        depto = ""
        for sep in ["depto ", "Dpto ", "Depto ", " dp ", " OF ", " Of ", " of ", " piso "]:
            lower_dir = direccion.lower()
            lower_sep = sep.lower()
            idx = lower_dir.find(lower_sep)
            if idx >= 0:
                depto = direccion[idx + len(sep):].strip()
                direccion = direccion[:idx].strip()
                break

        # Parsear comuna de la direccion
        if not comuna and "," in direccion:
            parts = direccion.rsplit(",", 1)
            direccion = parts[0].strip()
            comuna = parts[1].strip()

        repartidor = repartidor_map.get(repartidor_raw.upper(), repartidor_raw)
        estado = estado_map.get(estado_raw.upper(), "PENDIENTE")
        vuelta = ""
        if "2DA" in estado_raw.upper():
            vuelta = "2a"
        elif "3RA" in estado_raw.upper():
            vuelta = "3a"

        # Pago
        efectivo = ""
        transferencia = ""
        forma_pago = ""
        estado_pago = "PENDIENTE"
        pago_raw = str(row[9]).strip() if len(row) > 9 else ""
        forma_pago_raw = str(row[10]).strip() if len(row) > 10 else ""

        if pago_raw and pago_raw not in ("", "$0", "0"):
            if "TRANS" in forma_pago_raw.upper():
                transferencia = pago_raw
                forma_pago = "Transferencia"
                estado_pago = "PAGADO"
            elif "EFECT" in forma_pago_raw.upper():
                efectivo = pago_raw
                forma_pago = "Efectivo"
                estado_pago = "PAGADO"

        if not cantidad or cantidad == "0":
            continue

        pedidos_cactus.append({
            "fecha": fecha,
            "direccion": direccion,
            "depto": depto,
            "comuna": comuna,
            "cant": cantidad,
            "marca": "CACTUS",
            "repartidor": repartidor,
            "estado_pedido": estado,
            "vuelta": vuelta,
            "observaciones": comentario,
            "com_chofer": "",
            "cliente": nombre,
            "efectivo": efectivo,
            "transferencia": transferencia,
            "forma_pago": forma_pago,
            "estado_pago": estado_pago,
            "fecha_pago": "",
            "aliado": "",
            "canal": "Planilla Cactus",
        })

    if not pedidos_cactus:
        return 0

    # Evitar duplicados por conteo
    from collections import Counter
    from unidecode import unidecode as _ud
    existing = get_pedidos(fecha)

    existing_counts = Counter()
    for p in existing:
        dir_key = (
            _normalize_address(p.get("Direccion", "")),
            _ud(p.get("Depto", "")).lower().strip(),
        )
        existing_counts[dir_key] += 1

    import_counts = Counter()
    nuevos = []
    for p in pedidos_cactus:
        dir_key = (
            _normalize_address(p["direccion"]),
            _ud(p.get("depto", "")).lower().strip(),
        )
        import_counts[dir_key] += 1
        if import_counts[dir_key] > existing_counts[dir_key]:
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


# ===== VERIFICACION CONTRA DRIV.IN =====

def verify_orders_drivin(fecha=None, days_back=7, auto_update=True):
    """
    Verifica el estado real de pedidos PENDIENTE contra driv.in.

    Logica:
    1. Para pedidos con codigo driv.in y plan asignado:
       - Consulta el escenario en driv.in para ver si fue ejecutado
       - Consulta PODs para ver si fueron entregados/rechazados
    2. Para pedidos PENDIENTE sin codigo:
       - Busca por direccion en PODs recientes
    3. Pedidos PENDIENTE por mas de N dias sin actividad → reporta como estancados

    Args:
        fecha: Fecha base (DD/MM/YYYY). None = hoy.
        days_back: Dias hacia atras para buscar PODs.
        auto_update: Si True, actualiza automaticamente los estados en Sheets.

    Returns:
        Dict con resultados de la verificacion.
    """
    import drivin_client

    if not fecha:
        fecha = datetime.now().strftime("%d/%m/%Y")

    parts = fecha.split("/")
    hoy_dt = datetime(int(parts[2]), int(parts[1]), int(parts[0]))
    hoy_api = hoy_dt.strftime("%Y-%m-%d")
    desde_dt = hoy_dt - timedelta(days=days_back)
    desde_api = desde_dt.strftime("%Y-%m-%d")

    resultado = {
        "fecha": fecha,
        "total_verificados": 0,
        "actualizados": 0,
        "entregados_detectados": 0,
        "no_entregados_detectados": 0,
        "en_camino_detectados": 0,
        "estancados": [],       # Pedidos PENDIENTE sin actividad
        "planes_sin_despachar": [],  # Planes creados pero no iniciados
        "detalle": [],          # Lista de cambios realizados
    }

    # --- Obtener todos los pedidos PENDIENTE o EN CAMINO ---
    all_pedidos = get_pedidos()
    pendientes = [
        p for p in all_pedidos
        if p.get("Estado Pedido", "") in ("PENDIENTE", "EN CAMINO", "")
    ]

    if not pendientes:
        return resultado

    resultado["total_verificados"] = len(pendientes)

    # --- Recopilar planes unicos ---
    planes = set()
    for p in pendientes:
        plan = p.get("Plan Drivin", "").strip()
        if plan:
            planes.add(plan)

    # --- Verificar estado de cada plan en driv.in ---
    plan_status = {}  # plan_name -> {token, status, routes_started, routes_finished}
    for plan_name in planes:
        # Extraer fecha del plan (formato DD/MM/YYYYAPI)
        try:
            plan_fecha = plan_name.replace("API", "").strip()
            pf = plan_fecha.split("/")
            plan_api_date = f"{pf[2]}-{pf[1]}-{pf[0]}"
        except (ValueError, IndexError):
            continue

        try:
            scenarios = drivin_client.get_scenarios_by_date(plan_api_date)
            for s in scenarios.get("response", []):
                if s.get("description") == plan_name:
                    token = s.get("token", "")
                    status = s.get("status", "")

                    route_info = {"started": False, "finished": False}
                    if token and status in ("Started", "Finished"):
                        try:
                            routes = drivin_client.get_routes(scenario_token=token)
                            for r in routes.get("response", []):
                                if r.get("is_started"):
                                    route_info["started"] = True
                                if r.get("is_finished"):
                                    route_info["finished"] = True
                        except Exception:
                            pass

                    plan_status[plan_name] = {
                        "token": token,
                        "status": status,
                        "started": route_info["started"],
                        "finished": route_info["finished"],
                    }

                    if status in ("Ready", "Optimized") and not route_info["started"]:
                        resultado["planes_sin_despachar"].append({
                            "plan": plan_name,
                            "status": status,
                            "token": token,
                        })
                    break
        except Exception:
            pass

    # --- Obtener PODs del rango ---
    all_pods = []
    try:
        pods_data = drivin_client.get_pods(desde_api, hoy_api)
        all_pods = pods_data.get("response", [])
    except Exception:
        # Intentar en tramos si falla
        try:
            mid_dt = desde_dt + timedelta(days=days_back // 2)
            mid_api = mid_dt.strftime("%Y-%m-%d")
            p1 = drivin_client.get_pods(desde_api, mid_api)
            p2 = drivin_client.get_pods(mid_api, hoy_api)
            all_pods = p1.get("response", []) + p2.get("response", [])
        except Exception:
            pass

    # Indexar PODs por address_code y por direccion normalizada
    pods_by_code = {}
    pods_by_addr = {}
    for pod in all_pods:
        addr_code = pod.get("address_code", "")
        if addr_code:
            pods_by_code[addr_code] = pod
        addr_1 = pod.get("address_1", "") or ""
        addr_norm = _normalize_address(addr_1)
        if addr_norm:
            pods_by_addr[addr_norm] = pod

    estado_map = {
        "approved": "ENTREGADO",
        "delivered": "ENTREGADO",
        "rejected": "NO ENTREGADO",
        "not_delivered": "NO ENTREGADO",
        "pending": "EN CAMINO",
        "in-transit": "EN CAMINO",
    }

    updates = []

    for p in pendientes:
        nro = p.get("#", "")
        if not nro or not nro.isdigit():
            continue

        nro_int = int(nro)
        codigo = p.get("Codigo Drivin", "").strip()
        plan = p.get("Plan Drivin", "").strip()
        fecha_pedido = p.get("Fecha", "")
        direccion = p.get("Direccion", "")
        dir_norm = _normalize_address(direccion)
        estado_actual = p.get("Estado Pedido", "")

        # Buscar POD por codigo o por direccion
        pod = None
        if codigo:
            pod = pods_by_code.get(codigo)
        if not pod and dir_norm:
            pod = pods_by_addr.get(dir_norm)

        nuevo_estado = None
        driver = None
        comment = None

        if pod:
            # Extraer estado del POD
            for order in pod.get("orders", []):
                status = order.get("status", "")
                if status:
                    nuevo_estado = estado_map.get(status)
                    break

            if not nuevo_estado:
                if pod.get("route_is_finished"):
                    nuevo_estado = "NO ENTREGADO"
                elif pod.get("route_is_started"):
                    nuevo_estado = "EN CAMINO"

            driver = pod.get("driver_name", "")
            comment = ""
            for order in pod.get("orders", []):
                c = order.get("comment", "")
                if c:
                    comment = c
                    break

        # Si tiene plan, verificar estado del plan
        if not pod and plan and plan in plan_status:
            ps = plan_status[plan]
            if ps["finished"]:
                # Plan terminado pero sin POD → NO ENTREGADO
                nuevo_estado = "NO ENTREGADO"
            elif ps["started"]:
                nuevo_estado = "EN CAMINO"

        # Detectar estancados
        if not nuevo_estado and fecha_pedido:
            try:
                fp = fecha_pedido.split("/")
                fecha_dt = datetime(int(fp[2]), int(fp[1]), int(fp[0]))
                dias = (hoy_dt - fecha_dt).days
                if dias >= 2:
                    resultado["estancados"].append({
                        "numero": nro,
                        "direccion": direccion,
                        "fecha": fecha_pedido,
                        "dias": dias,
                        "codigo": codigo,
                        "plan": plan,
                    })
            except (ValueError, IndexError):
                pass

        # Aplicar actualizacion si hay cambio
        if nuevo_estado and nuevo_estado != estado_actual:
            upd = {"estado_pedido": nuevo_estado}
            if driver:
                upd["repartidor"] = driver
            if comment:
                upd["com_chofer"] = comment

            if nuevo_estado == "ENTREGADO":
                resultado["entregados_detectados"] += 1
            elif nuevo_estado == "NO ENTREGADO":
                resultado["no_entregados_detectados"] += 1
            elif nuevo_estado == "EN CAMINO":
                resultado["en_camino_detectados"] += 1

            resultado["detalle"].append({
                "numero": nro,
                "direccion": direccion,
                "estado_anterior": estado_actual or "PENDIENTE",
                "estado_nuevo": nuevo_estado,
                "fuente": "POD" if pod else "plan",
            })

            if auto_update:
                updates.append((nro_int, upd))

    if updates:
        update_pedidos_batch(updates)
        resultado["actualizados"] = len(updates)

    return resultado


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
    from log_client import log_rutina, log_error

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
        "cactus_importados": 0,
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

        # --- Detectar duplicados (misma direccion+depto en entregados y pendientes) ---
        pedidos_ayer_refresh = get_pedidos(fecha_ayer)
        dirs_entregadas = set()
        for p in pedidos_ayer_refresh:
            if p.get("Estado Pedido") == "ENTREGADO":
                dir_key = (
                    _normalize_address(p.get("Direccion", "")),
                    unidecode(p.get("Depto", "")).lower().strip(),
                )
                dirs_entregadas.add(dir_key)

        # Revisar los movidos a hoy si alguno coincide con entregado (duplicado)
        pedidos_hoy_pre = get_pedidos(fecha_hoy)
        for p in pedidos_hoy_pre:
            dir_key = (
                _normalize_address(p.get("Direccion", "")),
                unidecode(p.get("Depto", "")).lower().strip(),
            )
            nro = p.get("#", "")
            if dir_key in dirs_entregadas and nro and nro.isdigit():
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
            # Filtrar: solo pedidos de hoy o ayer (no arrastrar viejos)
            parts_h = fecha_hoy.split("/")
            hoy_iso = f"{parts_h[2]}-{parts_h[1]}-{parts_h[0]}"
            ayer_iso = ayer_dt.strftime("%Y-%m-%d")
            orders = [o for o in orders if o.get("fecha", "") in (hoy_iso, ayer_iso)]

            if orders:
                # Usar la fecha real del pedido Bsale (no forzar a hoy)
                count = sync_from_bsale(orders, fecha_destino=None)
                resultado["bsale_importados"] = count
    except Exception as e:
        resultado["errores"].append(f"Error importando Bsale: {e}")

    # --- PASO 5: Importar desde planilla reparto ---
    try:
        count = sync_from_planilla_reparto(fecha=fecha_hoy)
        resultado["planilla_importados"] = count
    except Exception as e:
        resultado["errores"].append(f"Error importando planilla Kowen: {e}")

    # --- PASO 5b: Importar desde planilla Cactus ---
    try:
        count_c = sync_from_planilla_cactus(fecha=fecha_hoy)
        resultado["cactus_importados"] = count_c
    except Exception as e:
        resultado["errores"].append(f"Error importando planilla Cactus: {e}")

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
                result = drivin_client.create_orders(
                    clients=clients, scenario_token=scenario_token
                )
            else:
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

    # --- PASO 8: Verificar pedidos contra driv.in ---
    try:
        verificacion = verify_orders_drivin(fecha=fecha_hoy, auto_update=True)
        resultado["verificacion"] = {
            "total_verificados": verificacion["total_verificados"],
            "actualizados": verificacion["actualizados"],
            "entregados_detectados": verificacion["entregados_detectados"],
            "estancados": len(verificacion["estancados"]),
            "planes_sin_despachar": len(verificacion["planes_sin_despachar"]),
        }
    except Exception as e:
        resultado["errores"].append(f"Error verificando contra driv.in: {e}")

    # --- Registrar en log ---
    try:
        log_rutina(resultado)
        for err in resultado.get("errores", []):
            log_error("rutina_diaria", err, detalle=fecha_hoy)
    except Exception:
        pass

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
