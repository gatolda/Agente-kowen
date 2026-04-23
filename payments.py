"""
Modulo de pagos para Agente Kowen.
- Lee emails de copiacorreoskowen@gmail.com
- Clasifica con Claude (pagos/pedidos/cotizaciones/sii/servicios/spam)
- Para emails de pago, extrae datos (incl. RUT) y propone match con pedidos
- Todo match propuesto se confirma manualmente desde Streamlit
- RUT memoria: una vez confirmado un RUT->Cliente, futuros pagos boostean ese match
"""

import json
import logging
import os
from datetime import datetime
from unidecode import unidecode

import gmail_client
import email_classifier
import sheets_client
from sheets_client import get_pedidos, update_pedido, add_pago

log = logging.getLogger("kowen.payments")


RUT_MEMORY_FILE = os.path.join(os.path.dirname(__file__), "rut_memory.json")


# ===== NORMALIZACION =====

def _normalize_name(s):
    """Normaliza un nombre para comparacion: sin acentos, lowercase, sin espacios extra."""
    if not s:
        return ""
    s = unidecode(s).lower().strip()
    for ch in [".", ",", ";", ":", "(", ")", "\"", "'"]:
        s = s.replace(ch, " ")
    return " ".join(s.split())


def _normalize_rut(s):
    """
    Normaliza un RUT chileno: solo digitos + verificador (K en mayuscula).
    Ejemplos: '12.345.678-9' -> '123456789', '12345678-k' -> '12345678K'.
    """
    if not s:
        return ""
    s = str(s).upper().replace(".", "").replace("-", "").replace(" ", "")
    # Dejar solo digitos y K
    return "".join(c for c in s if c.isdigit() or c == "K")


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
    s = "".join(c for c in s if c.isdigit())
    return int(s) if s else 0


def _fmt_fecha(s):
    """Normaliza fecha a DD/MM/YYYY."""
    dt = _parse_fecha_iso(s)
    if dt:
        return dt.strftime("%d/%m/%Y")
    return s


# ===== MEMORIA RUT -> CLIENTE =====

def _load_rut_memory():
    """Carga el diccionario {rut_normalizado: nombre_cliente}."""
    if not os.path.exists(RUT_MEMORY_FILE):
        return {}
    try:
        with open(RUT_MEMORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _save_rut_memory(memory):
    """Guarda el diccionario RUT->Cliente."""
    try:
        with open(RUT_MEMORY_FILE, "w", encoding="utf-8") as f:
            json.dump(memory, f, ensure_ascii=False, indent=2)
    except OSError:
        pass


def recordar_rut(rut, cliente):
    """Guarda el mapeo rut_normalizado -> cliente en la memoria."""
    rut_n = _normalize_rut(rut)
    if not rut_n or not cliente:
        return
    memory = _load_rut_memory()
    memory[rut_n] = cliente
    _save_rut_memory(memory)


# ===== SCORING =====

def _score_name(name_pago, name_pedido):
    """Score 0-100 por similaridad de nombre (tokens en comun)."""
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
    min_tokens = min(len(tokens_pago), len(tokens_ped))
    if min_tokens == 0:
        return 0
    return int(len(common) / min_tokens * 100)


def _score_fecha(fecha_pago_dt, fecha_pedido_dt):
    """Score 0-100 segun cercania de fechas."""
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
    """Score 0-100 segun coincidencia de montos."""
    if monto_pago <= 0:
        return 0
    if monto_pedido <= 0:
        return 50
    if monto_pago == monto_pedido:
        return 100
    diff_pct = abs(monto_pago - monto_pedido) / max(monto_pago, monto_pedido)
    if diff_pct <= 0.05:
        return 80
    if diff_pct <= 0.10:
        return 50
    return 0


def _score_rut(pago_rut, pedido_cliente, memory):
    """
    Si el RUT esta en la memoria:
    - apunta al mismo cliente (o tokens en comun): 100
    - apunta a OTRO cliente: 0 (penaliza)
    Si el RUT no esta en memoria: None (neutral, no cuenta).
    """
    if not pago_rut:
        return None
    rut_n = _normalize_rut(pago_rut)
    if not rut_n or rut_n not in memory:
        return None
    expected = _normalize_name(memory[rut_n])
    actual = _normalize_name(pedido_cliente)
    if not expected or not actual:
        return None
    if expected == actual:
        return 100
    tokens_e = set(expected.split())
    tokens_a = set(actual.split())
    return 100 if tokens_e & tokens_a else 0


def _match_score(pago_data, pedido, rut_memory):
    """
    Score total (0-100) de match entre pago y pedido.
    Pesos adaptativos segun RUT:
    - RUT conocido -> este cliente: RUT domina (55%), nombre 20%, fecha 15%, monto 10%
    - RUT conocido -> otro cliente: penaliza (x0.5)
    - RUT desconocido: nombre 50%, fecha 30%, monto 20% (pesos originales)
    """
    score_n = _score_name(pago_data.get("remitente_nombre", ""), pedido.get("Cliente", ""))
    score_f = _score_fecha(
        _parse_fecha_iso(pago_data.get("fecha", "")),
        _parse_fecha_dmy(pedido.get("Fecha", "")),
    )
    score_m = _score_monto(
        _parse_monto(pago_data.get("monto", "")),
        _parse_monto(pedido.get("Transferencia", "")),
    )
    score_r = _score_rut(pago_data.get("remitente_rut", ""), pedido.get("Cliente", ""), rut_memory)

    if score_r == 100:
        total = int(score_r * 0.55 + score_n * 0.2 + score_f * 0.15 + score_m * 0.1)
    elif score_r == 0:
        total = int((score_n * 0.5 + score_f * 0.3 + score_m * 0.2) * 0.5)
    else:
        total = int(score_n * 0.5 + score_f * 0.3 + score_m * 0.2)

    return {
        "total": total,
        "nombre": score_n,
        "fecha": score_f,
        "monto": score_m,
        "rut": score_r,
    }


# ===== MATCHING =====

def match_pago_a_pedido(pago_data, pedidos=None, umbral_sugerir=40):
    """
    Busca candidatos de pedidos para un pago.

    Args:
        pago_data: Dict con remitente_nombre, remitente_rut, fecha, monto.
        pedidos: Lista a considerar (default: todos no pagados).
        umbral_sugerir: Score minimo para sugerir.

    Returns:
        Dict con {candidatos: [(pedido, score)], score_top}.
    """
    if pedidos is None:
        pedidos = [
            p for p in get_pedidos()
            if p.get("Estado Pago", "").upper() != "PAGADO"
        ]

    memory = _load_rut_memory()
    scored = []
    for p in pedidos:
        s = _match_score(pago_data, p, memory)
        if s["total"] >= umbral_sugerir:
            scored.append((p, s))
    scored.sort(key=lambda x: x[1]["total"], reverse=True)

    return {
        "candidatos": scored[:5],
        "score_top": scored[0][1] if scored else None,
    }


# ===== APLICAR =====

def aplicar_pago(pago_data, pedido, email_id="", matched_auto=False):
    """
    Aplica el pago al pedido: marca PAGADO, registra datos y graba en hoja PAGOS.
    Guarda la memoria RUT->Cliente si hay RUT en el pago.
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
        "estado_pago": "PAGADO",
        "forma_pago": forma_pago,
        "fecha_pago": _fmt_fecha(pago_data.get("fecha", "")),
    }
    monto = _parse_monto(pago_data.get("monto", ""))
    if monto > 0 and not pedido.get("Transferencia"):
        updates["transferencia"] = str(monto)

    update_pedido(numero, updates)

    add_pago({
        "fecha": _fmt_fecha(pago_data.get("fecha", "")),
        "monto": monto,
        "medio": pago_data.get("banco", "") or forma_pago,
        "referencia": pago_data.get("referencia", ""),
        "pedido_vinculado": numero,
        "cliente": pago_data.get("remitente_nombre", "") or pedido.get("Cliente", ""),
        "estado": "CONCILIADO_AUTO" if matched_auto else "CONCILIADO_MANUAL",
        "email_id": email_id,
    })

    # Guardar memoria RUT -> Cliente del pedido (para futuras coincidencias)
    rut = pago_data.get("remitente_rut", "")
    cliente = pedido.get("Cliente", "")
    if rut and cliente:
        recordar_rut(rut, cliente)

    return True


def registrar_pago_sin_match(pago_data, email_id="", razon=""):
    """Registra un pago en PAGOS sin vincular a pedido."""
    add_pago({
        "fecha": _fmt_fecha(pago_data.get("fecha", "")),
        "monto": _parse_monto(pago_data.get("monto", "")),
        "medio": pago_data.get("banco", ""),
        "referencia": pago_data.get("referencia", ""),
        "pedido_vinculado": "",
        "cliente": pago_data.get("remitente_nombre", ""),
        "estado": f"SIN_MATCH ({razon})" if razon else "SIN_MATCH",
        "email_id": email_id,
    })


def confirmar_pago(email_id, pago_data, pedido):
    """
    Flujo de confirmacion desde Streamlit:
    - Aplica pago al pedido
    - Archiva email con etiqueta 'Conciliado'
    - Guarda memoria RUT
    """
    aplicar_pago(pago_data, pedido, email_id=email_id, matched_auto=False)
    if email_id:
        try:
            gmail_client.marcar_conciliado(email_id)
        except Exception as e:
            log.warning("Fallo marcar email %s como Conciliado: %s", email_id, e)


def rechazar_pago(email_id, pago_data, razon="rechazado por usuario"):
    """Registra el pago sin match y archiva el email."""
    registrar_pago_sin_match(pago_data, email_id=email_id, razon=razon)
    if email_id:
        try:
            gmail_client.marcar_conciliado(email_id)
        except Exception as e:
            log.warning("Fallo marcar email %s como Conciliado (rechazo): %s", email_id, e)


# ===== FLUJO PRINCIPAL =====

def procesar_emails_no_leidos(max_emails=30):
    """
    Lee emails no leidos, clasifica y extrae datos de pagos.
    NO aplica pagos automaticamente: todo va a 'pagos_por_confirmar'.

    Dedup: ignora emails cuyo id ya esta en PAGOS.Email ID.

    Returns:
        Dict con total, por_categoria, pagos_por_confirmar, alertas, errores, duplicados.
    """
    resumen = {
        "total": 0,
        "por_categoria": {},
        "pagos_por_confirmar": [],  # Cola unica ordenada por score descendente
        "alertas": [],
        "errores": [],
        "duplicados": 0,
    }

    try:
        emails = gmail_client.get_unread_messages(max_results=max_emails)
    except Exception as e:
        resumen["errores"].append(f"Error leyendo Gmail: {e}")
        return resumen

    resumen["total"] = len(emails)

    try:
        ya_procesados = sheets_client.get_pago_email_ids()
    except Exception as e:
        resumen["errores"].append(f"No se pudo leer PAGOS para dedup: {e}")
        ya_procesados = set()

    pedidos_pendientes = [
        p for p in get_pedidos()
        if p.get("Estado Pago", "").upper() != "PAGADO"
    ]

    for email in emails:
        try:
            if email.get("id") in ya_procesados:
                resumen["duplicados"] += 1
                continue

            result = email_classifier.classify_and_extract(email)
            cat = result["categoria"]
            resumen["por_categoria"][cat] = resumen["por_categoria"].get(cat, 0) + 1

            if cat == "pagos":
                pago_data = result.get("datos", {})
                match_result = match_pago_a_pedido(pago_data, pedidos=pedidos_pendientes)
                candidatos = match_result["candidatos"]
                top = match_result["score_top"]

                resumen["pagos_por_confirmar"].append({
                    "email_id": email["id"],
                    "email_subject": email.get("subject", ""),
                    "pago": pago_data,
                    "score_top": top["total"] if top else 0,
                    "candidatos": [
                        {
                            "numero": p.get("#", ""),
                            "cliente": p.get("Cliente", ""),
                            "fecha": p.get("Fecha", ""),
                            "monto": p.get("Transferencia", ""),
                            "score": s["total"],
                            "rut_match": s.get("rut"),
                        }
                        for p, s in candidatos
                    ],
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

    # Ordenar cola por score descendente (mas seguros arriba)
    resumen["pagos_por_confirmar"].sort(key=lambda x: -x["score_top"])

    return resumen
