"""
Modulo de pagos para Agente Kowen.
- Lee emails de copiacorreoskowen@gmail.com
- Clasifica con Claude (pagos/pedidos/cotizaciones/sii/servicios/spam)
- Para emails de pago, extrae datos y hace match con pedidos de OPERACION DIARIA
- Match: nombre → fecha → monto
"""

from datetime import datetime, timedelta
from unidecode import unidecode

import gmail_client
import email_classifier
from sheets_client import (
    get_pedidos, update_pedido, add_pago, TAB_OPERACION, OP_COLS,
    _read_sheet, _write_sheet, _retry, _get_service, SPREADSHEET_ID,
)


# ===== NORMALIZACION =====

def _normalize_name(s):
    """Normaliza un nombre para comparacion: sin acentos, lowercase, sin espacios extra."""
    if not s:
        return ""
    s = unidecode(s).lower().strip()
    # Quitar puntuacion comun
    for ch in [".", ",", ";", ":", "(", ")", "\"", "'"]:
        s = s.replace(ch, " ")
    # Colapsar espacios
    return " ".join(s.split())


def _parse_fecha_dmy(s):
    """Parsea fecha en formato DD/MM/YYYY."""
    if not s:
        return None
    try:
        parts = s.split("/")
        return datetime(int(parts[2]), int(parts[1]), int(parts[0]))
    except (ValueError, IndexError):
        return None


def _parse_fecha_iso(s):
    """Parsea fecha en formato YYYY-MM-DD."""
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d")
    except (ValueError, IndexError):
        return None


def _parse_monto(s):
    """Parsea un monto a int, quitando puntos, comas, $."""
    if not s:
        return 0
    s = str(s).replace("$", "").replace(".", "").replace(",", "").strip()
    # Dejar solo digitos
    s = "".join(c for c in s if c.isdigit())
    return int(s) if s else 0


# ===== SCORING DE MATCH =====

def _score_name(name_pago, name_pedido):
    """
    Score de similaridad entre dos nombres (0-100).
    Alto si tokens del nombre del pago aparecen en el pedido.
    """
    np = _normalize_name(name_pago)
    pe = _normalize_name(name_pedido)
    if not np or not pe:
        return 0
    if np == pe:
        return 100
    tokens_pago = set(np.split())
    tokens_ped = set(pe.split())
    if not tokens_pago or not tokens_ped:
        return 0
    common = tokens_pago & tokens_ped
    # Ponderar por tokens en comun sobre el nombre mas corto
    min_tokens = min(len(tokens_pago), len(tokens_ped))
    if min_tokens == 0:
        return 0
    ratio = len(common) / min_tokens
    return int(ratio * 100)


def _score_fecha(fecha_pago_dt, fecha_pedido_dt):
    """
    Score 0-100 segun cercania de fechas.
    - Mismo dia: 100
    - 1 dia: 90
    - 2 dias: 70
    - 3-5 dias: 50
    - 6-14 dias: 20
    - >14 dias: 0
    """
    if not fecha_pago_dt or not fecha_pedido_dt:
        return 0
    dias = abs((fecha_pago_dt - fecha_pedido_dt).days)
    if dias == 0:
        return 100
    if dias == 1:
        return 90
    if dias == 2:
        return 70
    if dias <= 5:
        return 50
    if dias <= 14:
        return 20
    return 0


def _score_monto(monto_pago, monto_pedido):
    """
    Score 0-100 segun coincidencia de montos.
    - Exacto: 100
    - Dentro del 5%: 80
    - Dentro del 10%: 50
    - Mayor: 0
    Si monto_pedido es 0 (no registrado), devuelve 50 (neutral).
    """
    if monto_pago <= 0:
        return 0
    if monto_pedido <= 0:
        return 50  # No sabemos el monto del pedido
    if monto_pago == monto_pedido:
        return 100
    diff_pct = abs(monto_pago - monto_pedido) / max(monto_pago, monto_pedido)
    if diff_pct <= 0.05:
        return 80
    if diff_pct <= 0.10:
        return 50
    return 0


def _match_score(pago_data, pedido):
    """
    Calcula score total (0-100) de match entre un pago y un pedido.
    Peso: nombre 50%, fecha 30%, monto 20%.
    """
    # Nombre
    name_p = pago_data.get("remitente_nombre", "")
    name_o = pedido.get("Cliente", "")
    score_n = _score_name(name_p, name_o)

    # Fecha
    fecha_p = _parse_fecha_iso(pago_data.get("fecha", ""))
    fecha_o = _parse_fecha_dmy(pedido.get("Fecha", ""))
    score_f = _score_fecha(fecha_p, fecha_o)

    # Monto
    monto_p = _parse_monto(pago_data.get("monto", ""))
    monto_o = _parse_monto(pedido.get("Transferencia", ""))
    score_m = _score_monto(monto_p, monto_o)

    total = int(score_n * 0.5 + score_f * 0.3 + score_m * 0.2)
    return {
        "total": total,
        "nombre": score_n,
        "fecha": score_f,
        "monto": score_m,
    }


# ===== MATCHING PAGO -> PEDIDO =====

def match_pago_a_pedido(pago_data, pedidos=None, umbral_auto=75, umbral_sugerir=50):
    """
    Busca el pedido que mejor matchea con un pago.

    Args:
        pago_data: Dict con remitente_nombre, fecha, monto.
        pedidos: Lista de pedidos a considerar (default: todos pendientes de pago).
        umbral_auto: Score minimo para auto-match.
        umbral_sugerir: Score minimo para sugerir como candidato.

    Returns:
        Dict con {match: pedido | None, candidatos: [(pedido, score)], auto: bool}.
    """
    if pedidos is None:
        pedidos = [
            p for p in get_pedidos()
            if p.get("Estado Pago", "").upper() != "PAGADO"
        ]

    scored = []
    for p in pedidos:
        s = _match_score(pago_data, p)
        if s["total"] >= umbral_sugerir:
            scored.append((p, s))

    scored.sort(key=lambda x: x[1]["total"], reverse=True)

    match = None
    auto = False
    if scored and scored[0][1]["total"] >= umbral_auto:
        # Revisar que no haya empate con el segundo
        if len(scored) == 1 or scored[0][1]["total"] - scored[1][1]["total"] >= 10:
            match = scored[0][0]
            auto = True

    return {
        "match": match,
        "score": scored[0][1] if scored else None,
        "candidatos": scored[:5],
        "auto": auto,
    }


# ===== APLICAR MATCH EN SHEETS =====

def aplicar_pago(pago_data, pedido, matched_auto=True):
    """
    Aplica el pago al pedido: marca PAGADO, registra datos y graba en hoja PAGOS.

    Args:
        pago_data: Dict con datos del pago.
        pedido: Dict del pedido (de get_pedidos).
        matched_auto: Si True, el match fue automatico.
    """
    numero = pedido.get("#", "")
    if not numero:
        return False

    forma_pago = "Transferencia"
    medio = pago_data.get("medio", "").lower()
    if medio == "webpay":
        forma_pago = "Webpay"
    elif medio == "deposito":
        forma_pago = "Deposito"

    updates = {
        "Estado Pago": "PAGADO",
        "Forma Pago": forma_pago,
        "Fecha Pago": _fmt_fecha(pago_data.get("fecha", "")),
    }
    monto = _parse_monto(pago_data.get("monto", ""))
    if monto > 0:
        # Dejar el monto en Transferencia si esta vacio
        if not pedido.get("Transferencia"):
            updates["Transferencia"] = str(monto)

    update_pedido(numero, updates)

    # Registrar en hoja PAGOS
    add_pago({
        "fecha": _fmt_fecha(pago_data.get("fecha", "")),
        "monto": monto,
        "medio": pago_data.get("banco", "") or forma_pago,
        "referencia": pago_data.get("referencia", ""),
        "pedido_vinculado": numero,
        "cliente": pago_data.get("remitente_nombre", "") or pedido.get("Cliente", ""),
        "estado": "CONCILIADO_AUTO" if matched_auto else "CONCILIADO_MANUAL",
    })
    return True


def registrar_pago_sin_match(pago_data, razon=""):
    """Registra un pago en la hoja PAGOS sin vincularlo a pedido."""
    add_pago({
        "fecha": _fmt_fecha(pago_data.get("fecha", "")),
        "monto": _parse_monto(pago_data.get("monto", "")),
        "medio": pago_data.get("banco", ""),
        "referencia": pago_data.get("referencia", ""),
        "pedido_vinculado": "",
        "cliente": pago_data.get("remitente_nombre", ""),
        "estado": f"SIN_MATCH ({razon})" if razon else "SIN_MATCH",
    })


def _fmt_fecha(s):
    """Normaliza fecha a DD/MM/YYYY."""
    dt = _parse_fecha_iso(s)
    if dt:
        return dt.strftime("%d/%m/%Y")
    return s


# ===== FLUJO PRINCIPAL: PROCESAR EMAILS =====

def procesar_emails_no_leidos(max_emails=30, marcar_leidos=False):
    """
    Procesa emails no leidos:
    - Clasifica cada uno
    - Si es pago, extrae datos y busca match
    - Aplica match automatico o lo deja como sugerencia
    - Retorna resumen

    Args:
        max_emails: Maximo de emails a procesar.
        marcar_leidos: Si True, marca los emails procesados como leidos.

    Returns:
        Dict con resumen por categoria + lista de pagos procesados + alertas.
    """
    resumen = {
        "total": 0,
        "por_categoria": {},
        "pagos_conciliados": [],
        "pagos_sin_match": [],
        "pagos_sugeridos": [],  # Requieren revision manual
        "alertas": [],  # Emails de pedidos/cotizaciones/etc para revisar
        "errores": [],
    }

    try:
        emails = gmail_client.get_unread_messages(max_results=max_emails)
    except Exception as e:
        resumen["errores"].append(f"Error leyendo Gmail: {e}")
        return resumen

    resumen["total"] = len(emails)
    pedidos_pendientes = [
        p for p in get_pedidos()
        if p.get("Estado Pago", "").upper() != "PAGADO"
    ]

    for email in emails:
        try:
            result = email_classifier.classify_and_extract(email)
            cat = result["categoria"]
            resumen["por_categoria"][cat] = resumen["por_categoria"].get(cat, 0) + 1

            if cat == "pagos":
                pago_data = result.get("datos", {})
                match_result = match_pago_a_pedido(pago_data, pedidos=pedidos_pendientes)

                if match_result["auto"]:
                    pedido = match_result["match"]
                    aplicar_pago(pago_data, pedido, matched_auto=True)
                    resumen["pagos_conciliados"].append({
                        "email_id": email["id"],
                        "email_subject": email.get("subject", ""),
                        "remitente": pago_data.get("remitente_nombre", ""),
                        "monto": pago_data.get("monto", ""),
                        "pedido": pedido.get("#", ""),
                        "cliente": pedido.get("Cliente", ""),
                        "score": match_result["score"]["total"],
                    })
                    if marcar_leidos:
                        gmail_client.mark_as_read(email["id"])
                elif match_result["candidatos"]:
                    resumen["pagos_sugeridos"].append({
                        "email_id": email["id"],
                        "email_subject": email.get("subject", ""),
                        "pago": pago_data,
                        "candidatos": [
                            {
                                "numero": p.get("#", ""),
                                "cliente": p.get("Cliente", ""),
                                "fecha": p.get("Fecha", ""),
                                "monto": p.get("Transferencia", ""),
                                "score": s["total"],
                            }
                            for p, s in match_result["candidatos"]
                        ],
                    })
                else:
                    registrar_pago_sin_match(pago_data, razon="ningun pedido match")
                    resumen["pagos_sin_match"].append({
                        "email_id": email["id"],
                        "email_subject": email.get("subject", ""),
                        "pago": pago_data,
                    })
            elif cat in ("pedidos", "cotizaciones"):
                resumen["alertas"].append({
                    "email_id": email["id"],
                    "categoria": cat,
                    "from": email.get("from", ""),
                    "subject": email.get("subject", ""),
                    "snippet": email.get("snippet", "")[:200],
                })
        except Exception as e:
            resumen["errores"].append(f"Error procesando {email.get('id', '?')}: {e}")

    return resumen
