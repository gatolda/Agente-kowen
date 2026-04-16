"""
Motor de matching automatico de direcciones.
Vincula direcciones de Bsale con codigos de driv.in.
"""

import csv
import os
import re
from collections import namedtuple

try:
    from unidecode import unidecode
except ImportError:
    def unidecode(s):
        return s

import drivin_client

CACHE_FILE = os.path.join(os.path.dirname(__file__), "direcciones_drivin.csv")

MatchResult = namedtuple("MatchResult", ["code", "name", "address1", "city", "score"])


def normalize(text):
    """Normaliza texto para comparacion: sin acentos, lowercase, sin prefijos comunes."""
    text = unidecode(text).lower()
    text = re.sub(r"\b(avenida|av|calle|pasaje|psje|paseo)\b", "", text)
    text = re.sub(r"[^a-z0-9 ]", "", text)
    return " ".join(text.split())


def extract_street_number(address):
    """Extrae el numero de la calle de una direccion."""
    numbers = re.findall(r"\b(\d{2,5})\b", address)
    return numbers[0] if numbers else None


def token_score(text_a, text_b):
    """Calcula similitud por overlap de tokens."""
    tokens_a = set(normalize(text_a).split())
    tokens_b = set(normalize(text_b).split())
    if not tokens_a or not tokens_b:
        return 0.0
    intersection = tokens_a & tokens_b
    return len(intersection) / max(len(tokens_a), len(tokens_b))


# --- Cache ---

MEMORY_FILE = os.path.join(os.path.dirname(__file__), "match_memory.csv")


def load_cache():
    """Carga las direcciones desde el CSV local."""
    if not os.path.exists(CACHE_FILE):
        return []

    addresses = []
    with open(CACHE_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            addresses.append(row)
    return addresses


def save_cache(addresses):
    """Guarda las direcciones en el CSV local."""
    if not addresses:
        return

    fieldnames = ["code", "name", "address1", "address2", "city", "lat", "lng"]
    with open(CACHE_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for addr in addresses:
            writer.writerow({k: addr.get(k, "") for k in fieldnames})


def refresh_cache():
    """Descarga todas las direcciones de driv.in y actualiza el cache."""
    from dotenv import load_dotenv
    load_dotenv()

    addresses = drivin_client.get_all_addresses()
    save_cache(addresses)
    return len(addresses)


# --- Memoria de correcciones ---

def load_memory():
    """Carga la memoria de correcciones manuales de match."""
    if not os.path.exists(MEMORY_FILE):
        return {}

    memory = {}
    with open(MEMORY_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = normalize(row.get("direccion", ""))
            memory[key] = {
                "code": row.get("code", ""),
                "direccion_original": row.get("direccion", ""),
                "veces_usado": int(row.get("veces_usado", 1)),
                "ultima_fecha": row.get("ultima_fecha", ""),
            }
    return memory


def save_memory_entry(direccion, code):
    """
    Guarda o actualiza una correccion manual en la memoria.
    La proxima vez que aparezca esta direccion, se usara este codigo directamente.
    """
    from datetime import datetime as _dt
    memory = load_memory()
    key = normalize(direccion)

    if key in memory:
        memory[key]["veces_usado"] += 1
        memory[key]["code"] = code
        memory[key]["ultima_fecha"] = _dt.now().strftime("%d/%m/%Y")
    else:
        memory[key] = {
            "code": code,
            "direccion_original": direccion,
            "veces_usado": 1,
            "ultima_fecha": _dt.now().strftime("%d/%m/%Y"),
        }

    # Escribir todo el archivo
    fieldnames = ["direccion", "code", "veces_usado", "ultima_fecha"]
    with open(MEMORY_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for k, v in memory.items():
            writer.writerow({
                "direccion": v["direccion_original"],
                "code": v["code"],
                "veces_usado": v["veces_usado"],
                "ultima_fecha": v["ultima_fecha"],
            })


# --- Matching ---

def find_matches(direccion, depto="", comuna="", addresses=None):
    """
    Busca coincidencias de una direccion en el cache de driv.in.

    Args:
        direccion: Direccion de la calle (ej: "Bombero Ossa 1010")
        depto: Departamento/oficina (ej: "208")
        comuna: Comuna (ej: "Santiago")
        addresses: Lista de direcciones (si None, carga del cache)

    Returns:
        Lista de MatchResult ordenada por score descendente.
    """
    if addresses is None:
        addresses = load_cache()

    if not addresses:
        return []

    street_number = extract_street_number(direccion)
    if not street_number:
        return []

    # Etapa 1: Filtrar por numero de calle
    # Regla: el numero del codigo driv.in siempre coincide con la numeracion de la direccion
    candidates = []
    number_pattern = re.compile(r"(?<!\d)" + re.escape(street_number) + r"(?!\d)")
    for addr in addresses:
        code = addr.get("code", "") or ""
        addr1 = addr.get("address1", "") or ""

        # El numero debe aparecer exacto (no como substring de otro numero)
        if number_pattern.search(addr1) or number_pattern.search(code):
            candidates.append(addr)

    if not candidates:
        return []

    # Etapa 2: Scoring por similitud de nombre de calle
    results = []
    for addr in candidates:
        addr1 = addr.get("address1", "") or ""
        name = addr.get("name", "") or ""
        code = addr.get("code", "") or ""

        # Comparar contra address1 y name, tomar el mejor
        score1 = token_score(direccion, addr1)
        score2 = token_score(direccion, name)
        score = max(score1, score2)

        # Bonus por comuna
        addr_city = addr.get("city", "") or ""
        if comuna and normalize(comuna) == normalize(addr_city):
            score += 0.1

        # Bonus si el codigo contiene el numero exacto de la direccion
        # Regla: el numero del codigo siempre coincide con la numeracion
        code_number_pattern = re.compile(r"(?<!\d)" + re.escape(street_number) + r"(?!\d)")
        if code_number_pattern.search(code):
            score += 0.15

        results.append(MatchResult(
            code=code,
            name=name,
            address1=addr1,
            city=addr_city,
            score=round(score, 3),
        ))

    # Etapa 3: Si hay depto, priorizar match exacto
    if depto:
        depto_clean = re.sub(r"[^a-z0-9]", "", depto.lower())
        for i, r in enumerate(results):
            code_clean = re.sub(r"[^a-z0-9]", "", r.code.lower())
            if depto_clean and depto_clean in code_clean:
                results[i] = r._replace(score=r.score + 0.5)

    # Filtrar score minimo y ordenar
    results = [r for r in results if r.score >= 0.3]
    results.sort(key=lambda x: x.score, reverse=True)

    return results


def auto_match(direccion, depto="", comuna="", addresses=None):
    """
    Intenta hacer match automatico. Retorna el codigo si es seguro,
    o None si necesita intervencion del usuario.

    Primero busca en la memoria de correcciones manuales.
    Si no encuentra, usa el algoritmo de matching normal.

    Returns:
        (code, confidence) donde confidence es "auto", "memory", "ambiguous" o "none"
        Si "ambiguous", code es la lista de candidatos.
    """
    # Paso 0: Buscar en memoria de correcciones manuales
    memory = load_memory()
    full_addr = f"{direccion} {depto}".strip() if depto else direccion
    mem_key = normalize(full_addr)
    # Buscar match exacto o por direccion base
    mem_entry = memory.get(mem_key) or memory.get(normalize(direccion))
    if mem_entry and mem_entry.get("code"):
        return mem_entry["code"], "memory"

    matches = find_matches(direccion, depto, comuna, addresses)

    if not matches:
        return None, "none"

    # Match seguro: el mejor candidato tiene score alto y ventaja clara
    if matches[0].score >= 0.7:
        if len(matches) == 1 or matches[0].score - matches[1].score >= 0.1:
            return matches[0].code, "auto"

    # Ambiguo: multiples candidatos similares
    return matches[:10], "ambiguous"


def match_order_interactive(direccion, depto="", comuna="", addresses=None):
    """
    Match interactivo: intenta auto-match, si falla pregunta al usuario.

    Returns:
        Codigo driv.in seleccionado o None si se omite.
    """
    result, confidence = auto_match(direccion, depto, comuna, addresses)

    full_addr = f"{direccion} {depto}".strip() if depto else direccion
    if comuna:
        full_addr += f", {comuna}"

    if confidence == "auto":
        print(f"  [OK] {full_addr} -> {result}")
        return result

    if confidence == "none":
        print(f"  [??] {full_addr} -> Sin coincidencias")
        code = input("       Ingrese codigo manualmente (o Enter para omitir): ").strip()
        return code if code else None

    # Ambiguo: mostrar opciones
    candidates = result
    print(f"  [??] {full_addr} -> Multiples coincidencias:")
    for i, match in enumerate(candidates, 1):
        print(f"       {i}. {match.code} - {match.name} ({match.city}) [score: {match.score}]")

    choice = input(f"       Elija (1-{len(candidates)}) o Enter para omitir: ").strip()
    if choice.isdigit() and 1 <= int(choice) <= len(candidates):
        return candidates[int(choice) - 1].code

    return None
