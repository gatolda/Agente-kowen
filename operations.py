"""
Operaciones de negocio del Agente Kowen.
Sincronizacion, importacion, rutina diaria y resumen.
"""

import logging
import os
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from unidecode import unidecode

import config
from config import PLANILLA_REPARTO_ID, PLANILLA_CACTUS_ID

log = logging.getLogger("kowen.operations")

# Lock de sincronizacion: scheduler (15 min), rutina diaria (08:00), CLI y
# botones del dashboard pueden disparar la misma importacion concurrentemente.
# Un lock de archivo evita duplicados y race conditions entre procesos distintos.
_LOCK_DIR = os.path.join(os.path.dirname(__file__), "logs")
_LOCK_STALE_SECONDS = 300  # 5 min: si el lock es mas viejo, se considera muerto


@contextmanager
def _sync_lock(name):
    """
    Lock de archivo para evitar ejecuciones concurrentes de la misma operacion
    entre procesos (scheduler, CLI, Streamlit, rutina diaria).

    Si el lock esta tomado y fresco, yielda False (el caller debe saltarse).
    Si esta libre o stale, lo toma y yielda True.
    """
    os.makedirs(_LOCK_DIR, exist_ok=True)
    path = os.path.join(_LOCK_DIR, f"{name}.lock")
    acquired = False
    try:
        if os.path.exists(path):
            age = time.time() - os.path.getmtime(path)
            if age < _LOCK_STALE_SECONDS:
                log.info("Lock %s tomado hace %.0fs, saltando.", name, age)
                yield False
                return
            log.warning("Lock %s stale (%.0fs), tomando control.", name, age)
            try:
                os.remove(path)
            except OSError:
                pass
        with open(path, "w", encoding="utf-8") as f:
            f.write(f"{os.getpid()} {datetime.now().isoformat()}")
        acquired = True
        yield True
    finally:
        if acquired:
            try:
                os.remove(path)
            except OSError as e:
                log.warning("No se pudo borrar lock %s: %s", path, e)
from sheets_client import (
    get_pedidos, add_pedidos, update_pedido, update_pedidos_batch,
    delete_pedido, get_clientes, _get_service, _read_sheet, _write_sheet,
    _append_sheet, _retry, _normalize_address, _pedido_to_row,
    OP_COLS, TAB_OPERACION, SPREADSHEET_ID,
)

# Mapeo de status de driv.in (PODs / orders) a estados del sistema.
DRIVIN_STATUS_MAP = {
    "approved": "ENTREGADO",
    "delivered": "ENTREGADO",
    "rejected": "NO ENTREGADO",
    "not_delivered": "NO ENTREGADO",
    "pending": "EN CAMINO",
    "in-transit": "EN CAMINO",
}


def _parse_cash_from_comment(comment):
    """
    Extrae monto de efectivo de un comentario de POD.
    Ejemplos soportados: "pago $5000", "efectivo 10.000", "$2990",
    "pagaron 5990 en efectivo", "5.980 cash".

    Retorna int con el monto o None si no encuentra.
    """
    if not comment:
        return None
    import re
    text = unidecode(comment).lower()
    # Ignorar si menciona transferencia/webpay/deposito — no es efectivo
    for neg in ("transfer", "webpay", "deposito", "tarjeta", "cheque"):
        if neg in text:
            return None
    # Extraer numeros con posibles separadores de miles
    # Acepta: 5000, 5.000, 5,000, $5000, $5.000
    matches = re.findall(r"\$?\s*(\d{1,3}(?:[.,]\d{3})+|\d{3,6})", text)
    if not matches:
        return None
    # Tomar el mayor valor (asumimos es el total del pago)
    valores = []
    for m in matches:
        limpio = m.replace(".", "").replace(",", "")
        if limpio.isdigit():
            v = int(limpio)
            # Filtrar rangos razonables: 1000 a 500000 CLP
            if 1000 <= v <= 500000:
                valores.append(v)
    if not valores:
        return None
    return max(valores)


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

    hoy = config.now().replace(tzinfo=None)

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


def crear_direccion_drivin(pedido_num, codigo_sugerido=None):
    """
    Crea una nueva direccion en driv.in para un pedido de OPERACION DIARIA
    que no tiene codigo driv.in asignado, la agrega al cache local y asigna
    el codigo al pedido.

    Regla del negocio: el codigo usa la numeracion de la direccion
    (ej: San Isidro 538 -> "SI 538" usando iniciales + numero).

    Args:
        pedido_num: Numero de fila (#) del pedido en OPERACION DIARIA.
        codigo_sugerido: Codigo drivin. Si None, se genera a partir
                         de las iniciales + numero de calle.

    Returns:
        Dict con {"code", "status", "pedido_num"} o error.
    """
    import address_matcher
    import drivin_client
    import csv

    all_p = get_pedidos()
    pedido = next((p for p in all_p if p.get("#") == str(pedido_num)), None)
    if not pedido:
        return {"status": "error", "message": f"Pedido #{pedido_num} no existe"}

    direccion = pedido.get("Direccion", "").strip()
    depto = pedido.get("Depto", "").strip()
    comuna = pedido.get("Comuna", "").strip() or "Santiago"
    cliente = pedido.get("Cliente", "").strip()
    telefono = pedido.get("Telefono", "").strip()
    email = pedido.get("Email", "").strip()

    if not direccion:
        return {"status": "error", "message": f"Pedido #{pedido_num} sin direccion"}

    # Generar codigo sugerido: iniciales + numero
    if not codigo_sugerido:
        numero = address_matcher.extract_street_number(direccion)
        if not numero:
            return {"status": "error",
                    "message": f"No se pudo extraer numero de: {direccion}"}
        nombre_calle = direccion.replace(numero, "").strip()
        tokens = [t for t in nombre_calle.split()
                  if t.lower() not in ("avenida", "av", "av.", "calle", "pasaje")]
        iniciales = "".join(t[0].upper() for t in tokens if t)[:3] or "X"
        codigo_sugerido = f"{iniciales} {numero}"

    # Crear en driv.in (deja que drivin geocodifique)
    try:
        resp = drivin_client.create_address(
            code=codigo_sugerido,
            address1=direccion,
            address2=depto,
            city=comuna,
            name=cliente or direccion,
            contact_name=cliente,
            phone=telefono,
            email=email,
        )
    except Exception as e:
        return {"status": "error", "message": f"Error driv.in: {e}",
                "code": codigo_sugerido}

    # Apendear al cache CSV
    try:
        with open(address_matcher.CACHE_FILE, "a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                codigo_sugerido,
                cliente or direccion,
                direccion,
                depto,
                comuna,
                "", "",  # lat, lng (se llenaran en proximo refresh)
            ])
    except Exception as e:
        log.warning("Fallo cache CSV al agregar %s: %s", codigo_sugerido, e)

    # Asignar codigo al pedido
    update_pedido(int(pedido_num), {"codigo_drivin": codigo_sugerido})

    return {
        "status": "ok",
        "code": codigo_sugerido,
        "pedido_num": pedido_num,
        "direccion": direccion,
        "drivin_response": resp,
    }


def check_bsale_pendientes(fecha=None):
    """
    Revisa pedidos Bsale recientes que aun NO estan reflejados en la planilla
    Kowen (PRIMER TURNO) ni en OPERACION DIARIA.

    La regla del negocio (feedback_planilla_manda): la planilla Kowen manda.
    Los pedidos web de Bsale deben pasarse manualmente a la planilla antes
    de ser operativos. Esta funcion NO importa — solo alerta al equipo de
    los pedidos Bsale de hoy/ayer que siguen pendientes de validacion manual.

    Args:
        fecha: Fecha base (DD/MM/YYYY). None = hoy.

    Returns:
        Dict con:
          - pendientes: [{"pedido_nro", "fecha", "cliente", "direccion", "depto",
                          "comuna", "cantidad", "marca"}]
          - total_revisados: int
          - ya_en_sistema: int  (ya sea en planilla o en OPERACION DIARIA)
    """
    import bsale_client

    if not fecha:
        fecha = config.now().strftime("%d/%m/%Y")

    parts = fecha.split("/")
    hoy_dt = datetime(int(parts[2]), int(parts[1]), int(parts[0]))
    ayer_dt = hoy_dt - timedelta(days=1)
    hoy_iso = hoy_dt.strftime("%Y-%m-%d")
    ayer_iso = ayer_dt.strftime("%Y-%m-%d")
    fecha_ayer = ayer_dt.strftime("%d/%m/%Y")

    # Ultimo numero Bsale ya registrado
    all_pedidos = get_pedidos()
    bsale_nums = [int(p.get("Pedido Bsale", "0") or "0") for p in all_pedidos]
    since = max(bsale_nums) if bsale_nums else 0

    try:
        orders = bsale_client.get_web_orders(since_number=since)
    except Exception:
        orders = []

    # Filtrar solo pedidos activos de hoy/ayer
    orders = [
        o for o in orders
        if o.get("estado", "activo") == "activo"
        and o.get("fecha", "") in (hoy_iso, ayer_iso)
    ]

    resultado = {
        "pendientes": [],
        "total_revisados": len(orders),
        "ya_en_sistema": 0,
    }

    if not orders:
        return resultado

    # Set de numeros Bsale existentes en OPERACION DIARIA
    existing_bsale = {
        str(p.get("Pedido Bsale", ""))
        for p in all_pedidos if p.get("Pedido Bsale")
    }

    # Set de direcciones normalizadas de OPERACION DIARIA para hoy/ayer
    # (si la planilla ya paso el pedido a OPERACION DIARIA, la direccion estara)
    dirs_en_sistema = set()
    for p in all_pedidos:
        if p.get("Fecha") in (fecha, fecha_ayer):
            dir_norm = _normalize_address(p.get("Direccion", ""))
            if dir_norm:
                dirs_en_sistema.add(dir_norm)

    # Leer planilla Kowen para hoy y ayer (fuente de verdad operativa)
    dirs_en_planilla = set()
    try:
        service = _get_service()
        result = service.spreadsheets().values().get(
            spreadsheetId=PLANILLA_REPARTO_ID,
            range="'PRIMER TURNO'!A:B",
        ).execute()
        for row in result.get("values", []):
            if len(row) < 2:
                continue
            row_fecha = row[0].strip() if row[0] else ""
            row_dir = row[1].strip() if len(row) > 1 else ""
            if row_fecha in (fecha, fecha_ayer) and row_dir and row_dir != "DIRECCION":
                # Normalizar quitando posible depto/comuna en la misma celda
                base_dir = row_dir
                for sep in [" Dep/Ofi. ", " dpto ", " Dpto ", " OF ", " Of ", " piso "]:
                    if sep in base_dir:
                        base_dir = base_dir.split(sep, 1)[0].strip()
                        break
                if "," in base_dir:
                    base_dir = base_dir.rsplit(",", 1)[0].strip()
                dir_norm = _normalize_address(base_dir)
                if dir_norm:
                    dirs_en_planilla.add(dir_norm)
    except Exception:
        # Si no podemos leer la planilla, igual reportamos pendientes por numero Bsale
        pass

    for o in orders:
        bsale_nro = str(o.get("pedido_nro", ""))
        dir_norm = _normalize_address(o.get("direccion", ""))

        if bsale_nro in existing_bsale or dir_norm in dirs_en_sistema:
            resultado["ya_en_sistema"] += 1
            continue

        if dir_norm in dirs_en_planilla:
            # Esta en la planilla pero aun no pasado a OPERACION DIARIA
            # El paso sync_from_planilla_reparto lo incorporara → no es pendiente
            resultado["ya_en_sistema"] += 1
            continue

        resultado["pendientes"].append({
            "pedido_nro": bsale_nro,
            "fecha": o.get("fecha", ""),
            "cliente": o.get("cliente", ""),
            "direccion": o.get("direccion", ""),
            "depto": o.get("depto", ""),
            "comuna": o.get("comuna", ""),
            "cantidad": o.get("cantidad", 0),
            "marca": o.get("marca", "KOWEN"),
            "telefono": o.get("telefono", ""),
            "email": o.get("email", ""),
        })

    return resultado


def sugerir_codigo_bsale(pedido_bsale):
    """
    Dada una fila del output de check_bsale_pendientes(), sugiere un codigo
    drivin usando address_matcher.auto_match.

    Returns:
        Dict con {codigo, confianza, candidatos}.
            confianza: 'auto' | 'memory' | 'ambiguous' | 'none'
            candidatos: lista de Match si ambiguous, [] en otros casos
    """
    import address_matcher
    addrs = address_matcher.load_cache()
    res, conf = address_matcher.auto_match(
        direccion=pedido_bsale.get("direccion", ""),
        depto=pedido_bsale.get("depto", ""),
        comuna=pedido_bsale.get("comuna", ""),
        addresses=addrs,
    )
    if conf == "ambiguous":
        return {"codigo": "", "confianza": conf, "candidatos": res or []}
    return {"codigo": res or "", "confianza": conf, "candidatos": []}


def importar_bsale_a_operacion(pedido_bsale, codigo_drivin, fecha_destino=None,
                                 subir_a_drivin=True):
    """
    Importa un pedido Bsale a OPERACION DIARIA con el codigo drivin dado.
    Si subir_a_drivin y existe un scenario para la fecha, tambien agrega el
    pedido al plan drivin del dia.

    Args:
        pedido_bsale: Dict del check_bsale_pendientes (pedido_nro, direccion,
            depto, comuna, cantidad, marca, cliente, telefono, email, fecha).
        codigo_drivin: Codigo (puede ser el sugerido o uno corregido).
        fecha_destino: DD/MM/YYYY. Default: hoy.
        subir_a_drivin: Si True y hay scenario para la fecha, sube el order.

    Returns:
        Dict con {numero, subido_drivin, scenario_token, motivo_no_subido}.
    """
    import sheets_client
    import drivin_client
    import address_matcher
    from log_client import log_match_manual

    if not fecha_destino:
        fecha_destino = config.now().strftime("%d/%m/%Y")

    marca = (pedido_bsale.get("marca", "KOWEN") or "KOWEN").upper()
    cantidad = pedido_bsale.get("cantidad", 0) or 0

    # Guardar aprendizaje del match manual si el usuario corrigio
    sugerencia = sugerir_codigo_bsale(pedido_bsale)
    if (codigo_drivin and codigo_drivin != sugerencia.get("codigo")
            and sugerencia.get("confianza") in ("ambiguous", "none")):
        try:
            address_matcher.save_memory_entry(
                pedido_bsale.get("direccion", ""), codigo_drivin
            )
            log_match_manual(pedido_bsale.get("direccion", ""), codigo_drivin)
        except Exception as e:
            log.warning("Fallo guardar match manual: %s", e)

    # 1. Agregar a OPERACION DIARIA
    num = sheets_client.add_pedido({
        "fecha": fecha_destino,
        "direccion": pedido_bsale.get("direccion", ""),
        "depto": pedido_bsale.get("depto", ""),
        "comuna": pedido_bsale.get("comuna", ""),
        "codigo_drivin": codigo_drivin,
        "cant": cantidad,
        "marca": marca,
        "documento": "Boleta",
        "canal": "WEB",
        "cliente": pedido_bsale.get("cliente", ""),
        "telefono": pedido_bsale.get("telefono", ""),
        "email": pedido_bsale.get("email", ""),
        "observaciones": "",
        "pedido_bsale": str(pedido_bsale.get("pedido_nro", "")),
        "estado_pedido": "PENDIENTE",
        "estado_pago": "PENDIENTE",
    })

    # 2. Intentar subir al scenario drivin del dia (si existe y hay codigo)
    subido = False
    scenario_token = ""
    motivo = ""

    if not subir_a_drivin:
        motivo = "subir_a_drivin=False"
    elif not codigo_drivin:
        motivo = "sin codigo drivin"
    else:
        try:
            parts = fecha_destino.split("/")
            api_date = f"{parts[2]}-{parts[1]}-{parts[0]}"
            suffix = f"{parts[1]}{parts[0]}"
            plan_name = f"{fecha_destino}API"

            scenarios = drivin_client.get_scenarios_by_date(api_date).get("response", [])
            for s in scenarios:
                if s.get("description") == plan_name:
                    scenario_token = s.get("token", s.get("scenario_token", ""))
                    break

            if scenario_token:
                desc = f"{marca} - Retiro" if cantidad == 0 else marca
                clients = [{
                    "code": codigo_drivin,
                    "orders": [{
                        "code": f"{codigo_drivin}-{suffix}",
                        "description": desc,
                        "units_1": cantidad,
                    }],
                }]
                drivin_client.create_orders(clients=clients, scenario_token=scenario_token)
                subido = True
                # Marcar el pedido con el plan
                sheets_client.update_pedidos_batch([(num, {"plan_drivin": plan_name})])
            else:
                motivo = f"no existe scenario {plan_name} (rutina diaria lo creara en la proxima corrida)"
        except Exception as e:
            motivo = f"error subiendo a drivin: {e}"
            log.warning("Fallo subir pedido Bsale #%s a drivin: %s",
                        pedido_bsale.get("pedido_nro", ""), e)

    return {
        "numero": num,
        "subido_drivin": subido,
        "scenario_token": scenario_token,
        "motivo_no_subido": motivo,
    }


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
                fecha = config.now().strftime("%d/%m/%Y")

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


def sync_from_planilla_reparto(fecha=None):
    """
    Importa pedidos desde la planilla reparto (PRIMER TURNO) a OPERACION DIARIA.
    Evita duplicados comparando direccion + depto + cantidad.

    Protegido por _sync_lock — si otro proceso esta importando, retorna 0 sin
    reintentar (el scheduler volvera a intentar en el proximo ciclo).

    Args:
        fecha: Fecha a importar (DD/MM/YYYY). None = hoy.

    Returns:
        Cantidad de pedidos importados (0 si lock tomado o no hay nuevos).
    """
    with _sync_lock("sync_planilla_reparto") as acquired:
        if not acquired:
            return 0
        return _sync_from_planilla_reparto_impl(fecha)


def _sync_from_planilla_reparto_impl(fecha=None):
    """Implementacion real de sync_from_planilla_reparto (llamar solo con lock)."""
    if not fecha:
        fecha = config.now().strftime("%d/%m/%Y")

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


def sync_from_planilla_cactus(fecha=None):
    """
    Importa pedidos desde la planilla Cactus (Enero 2023) a OPERACION DIARIA.
    La planilla Cactus tiene un header con la fecha y pedidos debajo sin fecha.
    Estructura: B=Direccion, C=Comuna, D=Nombre, E=Repartidor, F=Estado, G=Cant, H=Comentario

    Protegido por _sync_lock — si otro proceso esta importando, retorna 0.

    Args:
        fecha: Fecha a importar (DD/MM/YYYY). None = hoy.

    Returns:
        Cantidad de pedidos importados (0 si lock tomado o no hay nuevos).
    """
    with _sync_lock("sync_planilla_cactus") as acquired:
        if not acquired:
            return 0
        return _sync_from_planilla_cactus_impl(fecha)


def _sync_from_planilla_cactus_impl(fecha=None):
    """Implementacion real de sync_from_planilla_cactus (llamar solo con lock)."""
    if not fecha:
        fecha = config.now().strftime("%d/%m/%Y")

    service = _get_service()

    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=PLANILLA_CACTUS_ID,
            range="'Enero 2023'!A:M",
        ).execute()
    except Exception as e:
        log.warning("Fallo leer planilla Cactus: %s", e)
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
        fecha = config.now().strftime("%d/%m/%Y")

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

    estado_map = DRIVIN_STATUS_MAP

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
        fecha = config.now().strftime("%d/%m/%Y")

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
        fecha = config.now().strftime("%d/%m/%Y")

    parts = fecha.split("/")
    hoy_dt = datetime(int(parts[2]), int(parts[1]), int(parts[0]))
    hoy_api = hoy_dt.strftime("%Y-%m-%d")

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

    # Ampliar days_back dinamicamente al pedido pendiente mas antiguo, para
    # detectar PODs de pedidos estancados de semanas atras.
    max_dias_pendiente = days_back
    for p in pendientes:
        fp_str = p.get("Fecha", "")
        if not fp_str:
            continue
        try:
            fp = fp_str.split("/")
            fecha_p = datetime(int(fp[2]), int(fp[1]), int(fp[0]))
            dias = (hoy_dt - fecha_p).days
            if dias > max_dias_pendiente:
                max_dias_pendiente = dias
        except (ValueError, IndexError):
            pass
    days_back = min(max_dias_pendiente + 1, 60)  # cap en 60 dias por seguridad
    desde_dt = hoy_dt - timedelta(days=days_back)
    desde_api = desde_dt.strftime("%Y-%m-%d")

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
                        except Exception as e:
                            log.warning("No se pudo obtener routes del plan %s: %s", plan_name, e)

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
        except Exception as e:
            log.warning("No se pudo verificar status del plan %s: %s", plan_name, e)

    # --- Obtener PODs del rango (en tramos <=5 dias, la API da 403 con rangos mas largos) ---
    all_pods = []
    MAX_TRAMO = 5  # dias
    tramo_inicio = desde_dt
    while tramo_inicio < hoy_dt:
        tramo_fin = min(tramo_inicio + timedelta(days=MAX_TRAMO - 1), hoy_dt)
        try:
            tramo_data = drivin_client.get_pods(
                tramo_inicio.strftime("%Y-%m-%d"),
                tramo_fin.strftime("%Y-%m-%d"),
            )
            all_pods.extend(tramo_data.get("response", []))
        except Exception as e:
            log.warning(
                "Fallo get_pods tramo %s..%s: %s",
                tramo_inicio.strftime("%Y-%m-%d"), tramo_fin.strftime("%Y-%m-%d"), e,
            )
        tramo_inicio = tramo_fin + timedelta(days=1)

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

    estado_map = DRIVIN_STATUS_MAP

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

            # Detectar pago en efectivo desde comentario POD
            if (nuevo_estado == "ENTREGADO"
                    and comment
                    and p.get("Estado Pago", "").upper() != "PAGADO"):
                monto_efectivo = _parse_cash_from_comment(comment)
                if monto_efectivo:
                    upd["efectivo"] = str(monto_efectivo)
                    upd["estado_pago"] = "PAGADO"
                    upd["forma_pago"] = "Efectivo"
                    upd["fecha_pago"] = fecha_pedido or fecha
                    resultado.setdefault("efectivo_detectado", 0)
                    resultado["efectivo_detectado"] += 1
                    # Registrar en PAGOS para mantener el libro centralizado
                    try:
                        from sheets_client import add_pago
                        add_pago({
                            "fecha": fecha_pedido or fecha,
                            "monto": monto_efectivo,
                            "medio": "Efectivo",
                            "referencia": f"POD drivin {codigo or dir_norm}",
                            "pedido_vinculado": nro,
                            "cliente": p.get("Cliente", ""),
                            "estado": "CONCILIADO_POD",
                            "email_id": f"drivin-pod-{nro}-{fecha_pedido or fecha}",
                        })
                    except Exception as e:
                        log.warning("Fallo registrar pago POD para #%s: %s", nro, e)

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


# ===== RECONCILIACION PAGOS <-> OPERACION DIARIA =====

def reconciliar_pagos():
    """
    Sincroniza la tab PAGOS contra OPERACION DIARIA.

    Flujo:
    - Para cada fila de PAGOS con pedido_vinculado y estado CONCILIADO_*,
      asegura que el pedido en OPERACION DIARIA tenga Estado Pago = PAGADO.
    - Si el pedido ya esta PAGADO no toca nada.
    - Detecta pedidos marcados PAGADO en OPERACION DIARIA que no tienen
      fila en PAGOS (huerfanos) para alertar.

    Returns:
        Dict con {actualizados, huerfanos, sin_pedido, errores}.
    """
    from sheets_client import get_pagos, get_pedidos as _get_pedidos

    resultado = {
        "actualizados": 0,
        "huerfanos": [],     # Pedidos PAGADO sin fila en PAGOS
        "sin_pedido": [],    # Filas PAGOS con pedido# que no existe
        "detalle": [],
    }

    try:
        pagos = get_pagos()
        pedidos = _get_pedidos()
    except Exception as e:
        log.warning("Fallo leer sheets en reconciliar_pagos: %s", e)
        return resultado

    # Indexar pedidos por #
    ped_por_num = {}
    for p in pedidos:
        nro = p.get("#", "").strip()
        if nro.isdigit():
            ped_por_num[int(nro)] = p

    # Conjunto de pedidos # que aparecen en PAGOS con estado conciliado
    pedidos_con_pago = set()

    updates = []
    for pago in pagos:
        estado = (pago.get("Estado", "") or "").upper()
        if not estado.startswith("CONCILIADO"):
            continue

        vinc_raw = str(pago.get("Pedido Vinculado", "") or pago.get("pedido_vinculado", "")).strip()
        if not vinc_raw.isdigit():
            continue

        nro = int(vinc_raw)
        pedidos_con_pago.add(nro)
        pedido = ped_por_num.get(nro)
        if not pedido:
            resultado["sin_pedido"].append({
                "pago_fecha": pago.get("Fecha", ""),
                "pago_monto": pago.get("Monto", ""),
                "pedido_num": nro,
            })
            continue

        if (pedido.get("Estado Pago", "") or "").upper() == "PAGADO":
            continue

        # Derivar forma_pago del medio
        medio = (pago.get("Medio", "") or "").lower()
        if "webpay" in medio:
            forma = "Webpay"
        elif "efectivo" in medio:
            forma = "Efectivo"
        elif "deposito" in medio:
            forma = "Deposito"
        else:
            forma = "Transferencia"

        upd = {
            "estado_pago": "PAGADO",
            "forma_pago": forma,
            "fecha_pago": pago.get("Fecha", "") or config.now().strftime("%d/%m/%Y"),
        }
        updates.append((nro, upd))
        resultado["detalle"].append({
            "numero": nro,
            "monto": pago.get("Monto", ""),
            "medio": pago.get("Medio", ""),
        })

    if updates:
        try:
            update_pedidos_batch(updates)
            resultado["actualizados"] = len(updates)
        except Exception as e:
            log.warning("Fallo batch update en reconciliar_pagos: %s", e)

    # Detectar huerfanos: pedidos PAGADO en OPERACION DIARIA sin fila en PAGOS
    for nro, p in ped_por_num.items():
        if (p.get("Estado Pago", "") or "").upper() == "PAGADO" and nro not in pedidos_con_pago:
            resultado["huerfanos"].append({
                "numero": nro,
                "cliente": p.get("Cliente", ""),
                "fecha": p.get("Fecha", ""),
                "forma_pago": p.get("Forma Pago", ""),
            })

    return resultado


# ===== DIAGNOSTICO DE SALUD (read-only, rapido) =====

def diagnostico_salud(dias_estancado=2):
    """
    Snapshot de cosas que requieren atencion humana.
    Solo lee de Sheets (no llama driv.in ni Bsale ni Gmail).

    Args:
        dias_estancado: minimo de dias desde Fecha para marcar un pedido
            PENDIENTE como estancado (default 2).

    Returns:
        Dict con listas y conteos: huerfanos, pagos_sin_pedido, estancados,
        pendientes_sin_codigo, total_pedidos, totales_por_estado.
    """
    from sheets_client import get_pedidos as _gp, get_pagos as _gpg

    resultado = {
        "huerfanos": [],           # Pedidos PAGADO sin fila en PAGOS
        "pagos_sin_pedido": [],    # Filas PAGOS con pedido# inexistente
        "estancados": [],          # Pedidos PENDIENTE con >=N dias
        "pendientes_sin_codigo": [],  # Pedidos PENDIENTE sin Codigo Drivin
        "total_pedidos": 0,
        "totales_por_estado": {},
    }

    try:
        pedidos = _gp()
        pagos = _gpg()
    except Exception as e:
        log.warning("Fallo leer Sheets en diagnostico_salud: %s", e)
        return resultado

    # Indexar pedidos por #
    ped_por_num = {}
    for p in pedidos:
        nro = (p.get("#", "") or "").strip()
        if nro.isdigit():
            ped_por_num[int(nro)] = p

    resultado["total_pedidos"] = len(ped_por_num)

    # Conteos por estado
    por_estado = {}
    for p in ped_por_num.values():
        estado = (p.get("Estado Pedido", "") or "SIN ESTADO").strip() or "SIN ESTADO"
        por_estado[estado] = por_estado.get(estado, 0) + 1
    resultado["totales_por_estado"] = por_estado

    # Set de pedidos# referenciados desde PAGOS con estado CONCILIADO*
    pedidos_con_pago = set()
    for pago in pagos:
        estado = (pago.get("Estado", "") or "").upper()
        if not estado.startswith("CONCILIADO"):
            continue
        vinc = str(pago.get("Pedido Vinculado", "") or "").strip()
        if not vinc.isdigit():
            continue
        nro = int(vinc)
        pedidos_con_pago.add(nro)
        if nro not in ped_por_num:
            resultado["pagos_sin_pedido"].append({
                "pedido_num": nro,
                "monto": pago.get("Monto", ""),
                "fecha": pago.get("Fecha", ""),
                "cliente": pago.get("Cliente", ""),
            })

    hoy = config.now().replace(tzinfo=None)
    limite_estancado = hoy - timedelta(days=dias_estancado)

    for nro, p in ped_por_num.items():
        estado_pago = (p.get("Estado Pago", "") or "").upper()
        estado_pedido = (p.get("Estado Pedido", "") or "").upper()
        codigo = (p.get("Codigo Drivin", "") or "").strip()
        fecha_str = p.get("Fecha", "")

        # Huerfanos: PAGADO pero sin fila en PAGOS
        if estado_pago == "PAGADO" and nro not in pedidos_con_pago:
            resultado["huerfanos"].append({
                "numero": nro,
                "cliente": p.get("Cliente", ""),
                "fecha": fecha_str,
                "forma_pago": p.get("Forma Pago", ""),
                "monto": p.get("Transferencia", "") or p.get("Efectivo", ""),
            })

        # Estancados: PENDIENTE con >= N dias
        if estado_pedido in ("PENDIENTE", ""):
            try:
                fp = fecha_str.split("/")
                fecha_dt = datetime(int(fp[2]), int(fp[1]), int(fp[0]))
                if fecha_dt < limite_estancado:
                    dias = (hoy - fecha_dt).days
                    resultado["estancados"].append({
                        "numero": nro,
                        "direccion": p.get("Direccion", ""),
                        "cliente": p.get("Cliente", ""),
                        "fecha": fecha_str,
                        "dias": dias,
                        "codigo": codigo,
                    })
            except (ValueError, IndexError):
                pass

            # Pendientes sin codigo (de hoy o recientes — solo alertamos <= 5 dias)
            if not codigo:
                try:
                    fp = fecha_str.split("/")
                    fecha_dt = datetime(int(fp[2]), int(fp[1]), int(fp[0]))
                    if (hoy - fecha_dt).days <= 5:
                        resultado["pendientes_sin_codigo"].append({
                            "numero": nro,
                            "direccion": p.get("Direccion", ""),
                            "comuna": p.get("Comuna", ""),
                            "fecha": fecha_str,
                        })
                except (ValueError, IndexError):
                    pass

    # Orden descendente por dias/fecha
    resultado["estancados"].sort(key=lambda x: -x["dias"])
    resultado["huerfanos"].sort(key=lambda x: x["fecha"], reverse=True)

    return resultado


# ===== POBLAR CLIENTES DESDE OPERACION DIARIA =====

def sync_clientes_from_operacion():
    """
    Deriva la tab CLIENTES a partir de OPERACION DIARIA.

    Agrupa por Codigo Drivin (preferido) o por direccion+depto normalizado.
    Por cada cliente unico, toma los datos del pedido mas reciente y
    calcula total_pedidos y ultimo_pedido.

    - Upsert: actualiza cliente existente (match por Codigo Drivin o Nombre)
      o agrega uno nuevo.

    Returns:
        Dict con {creados, actualizados, total_clientes}.
    """
    from sheets_client import get_clientes_indexed, add_cliente, update_cliente

    resultado = {"creados": 0, "actualizados": 0, "total_clientes": 0}

    try:
        pedidos = get_pedidos()
    except Exception as e:
        log.warning("Fallo leer OPERACION DIARIA para sync_clientes: %s", e)
        return resultado

    # Agrupar pedidos por clave de cliente
    grupos = {}
    for p in pedidos:
        codigo = (p.get("Codigo Drivin", "") or "").strip()
        direccion = (p.get("Direccion", "") or "").strip()
        depto = (p.get("Depto", "") or "").strip()
        if codigo:
            key = f"COD:{codigo}"
        elif direccion:
            key = f"ADDR:{_normalize_address(direccion)}|{unidecode(depto).lower().strip()}"
        else:
            continue

        # Parsear fecha para ordenar
        fecha_str = p.get("Fecha", "")
        try:
            fp = fecha_str.split("/")
            fecha_dt = datetime(int(fp[2]), int(fp[1]), int(fp[0]))
        except (ValueError, IndexError):
            fecha_dt = None

        g = grupos.setdefault(key, {"pedidos": [], "ultimo": None, "ultimo_dt": None})
        g["pedidos"].append(p)
        if fecha_dt and (g["ultimo_dt"] is None or fecha_dt > g["ultimo_dt"]):
            g["ultimo_dt"] = fecha_dt
            g["ultimo"] = p

    # Cargar clientes existentes CON indice de fila, para evitar re-lecturas
    # en cada update (Sheets API limite 60 reads/min).
    try:
        existentes = get_clientes_indexed()
    except Exception as e:
        log.warning("Fallo leer CLIENTES: %s", e)
        existentes = []

    por_codigo = {}
    por_nombre = {}
    for c in existentes:
        cod = (c.get("Codigo Drivin", "") or "").strip()
        nom = (c.get("Nombre", "") or "").strip().lower()
        if cod:
            por_codigo[cod] = c
        if nom:
            por_nombre[nom] = c

    for key, g in grupos.items():
        ultimo = g["ultimo"] or g["pedidos"][0]
        codigo = (ultimo.get("Codigo Drivin", "") or "").strip()
        nombre = (ultimo.get("Cliente", "") or "").strip()
        if not nombre:
            # Sin nombre no podemos diferenciar de otros sin-nombre → skip
            continue

        data = {
            "nombre": nombre,
            "telefono": (ultimo.get("Telefono", "") or "").strip(),
            "email": (ultimo.get("Email", "") or "").strip(),
            "direccion": (ultimo.get("Direccion", "") or "").strip(),
            "depto": (ultimo.get("Depto", "") or "").strip(),
            "comuna": (ultimo.get("Comuna", "") or "").strip(),
            "codigo_drivin": codigo,
            "marca": (ultimo.get("Marca", "") or "KOWEN").strip(),
            "total_pedidos": len(g["pedidos"]),
            "ultimo_pedido": ultimo.get("Fecha", ""),
        }

        # Buscar match: primero por codigo, luego por nombre exacto
        existente = None
        if codigo and codigo in por_codigo:
            existente = por_codigo[codigo]
        elif nombre.lower() in por_nombre:
            existente = por_nombre[nombre.lower()]

        if existente:
            # Solo actualizar si algun campo cambio
            cambios = {}
            mapeo = {
                "telefono": "Telefono",
                "email": "Email",
                "direccion": "Direccion",
                "depto": "Depto",
                "comuna": "Comuna",
                "codigo_drivin": "Codigo Drivin",
                "marca": "Marca",
                "total_pedidos": "Total Pedidos",
                "ultimo_pedido": "Ultimo Pedido",
            }
            for k_src, k_sheet in mapeo.items():
                nuevo = str(data[k_src])
                actual = str(existente.get(k_sheet, "") or "")
                if nuevo and nuevo != actual:
                    cambios[k_src] = nuevo
            if cambios:
                nombre_existente = existente.get("Nombre", "")
                if not nombre_existente:
                    # Fila CLIENTES sin nombre (matcheada por codigo). No hay forma
                    # de update_cliente sin key; saltar silenciosamente.
                    continue
                try:
                    # Pasamos _row cacheado de get_clientes_indexed para evitar
                    # que update_cliente relea la hoja (quota saver).
                    update_cliente(nombre_existente, cambios, row_idx=existente.get("_row"))
                    resultado["actualizados"] += 1
                except Exception as e:
                    log.warning("Fallo update_cliente %s: %s", nombre, e)
        else:
            try:
                add_cliente(data)
                resultado["creados"] += 1
                por_nombre[nombre.lower()] = data  # evitar duplicados en misma corrida
                if codigo:
                    por_codigo[codigo] = data
            except Exception as e:
                log.warning("Fallo add_cliente %s: %s", nombre, e)

    resultado["total_clientes"] = len(grupos)
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
        fecha_hoy = config.now().strftime("%d/%m/%Y")

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
        "bsale_importados": 0,   # deprecado: ya no se importa Bsale automaticamente
        "bsale_pendientes": [],  # pedidos Bsale sin reflejo en planilla / operacion diaria
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
                except Exception as e:
                    log.warning("Fallo eliminar duplicado pedido #%s: %s", nro, e)

    # --- PASO 4: Revisar Bsale vs planilla (alerta, NO importar) ---
    # Regla del negocio: la planilla Kowen manda. Los pedidos web se pasan
    # manualmente a la planilla antes de ser operativos. Aqui solo alertamos.
    try:
        bsale_check = check_bsale_pendientes(fecha=fecha_hoy)
        resultado["bsale_pendientes"] = bsale_check.get("pendientes", [])
    except Exception as e:
        resultado["errores"].append(f"Error revisando Bsale: {e}")

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
            "efectivo_detectado": verificacion.get("efectivo_detectado", 0),
        }
    except Exception as e:
        resultado["errores"].append(f"Error verificando contra driv.in: {e}")

    # --- PASO 9: Reconciliar PAGOS <-> OPERACION DIARIA ---
    try:
        rec = reconciliar_pagos()
        resultado["reconciliacion_pagos"] = {
            "actualizados": rec["actualizados"],
            "huerfanos": len(rec["huerfanos"]),
            "sin_pedido": len(rec["sin_pedido"]),
        }
    except Exception as e:
        resultado["errores"].append(f"Error reconciliando pagos: {e}")

    # --- PASO 10: Poblar/actualizar CLIENTES desde OPERACION DIARIA ---
    try:
        cli = sync_clientes_from_operacion()
        resultado["clientes"] = {
            "creados": cli["creados"],
            "actualizados": cli["actualizados"],
            "total": cli["total_clientes"],
        }
    except Exception as e:
        resultado["errores"].append(f"Error sincronizando clientes: {e}")

    # --- Registrar en log ---
    try:
        log_rutina(resultado)
        for err in resultado.get("errores", []):
            log_error("rutina_diaria", err, detalle=fecha_hoy)
    except Exception as e:
        log.warning("Fallo registrar rutina en LOG tab: %s", e)

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
        fecha = config.now().strftime("%d/%m/%Y")

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
