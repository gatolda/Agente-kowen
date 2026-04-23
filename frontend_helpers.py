"""
Helpers para el dashboard Streamlit.
Busqueda unificada cliente+direccion, deteccion de duplicados, etc.
"""

import unicodedata


def _norm(s):
    """Normaliza para comparacion: sin tildes, lowercase, sin espacios extras."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower().strip()


def _get(obj, key):
    """Helper para leer dict o atributo de objeto."""
    if isinstance(obj, dict):
        return obj.get(key, "")
    return getattr(obj, key, "")


def search_unified(query, clientes, addr_cache, limit=10):
    """
    Busca en CLIENTES y en el cache de direcciones driv.in.
    Devuelve lista unificada ordenada: clientes primero (match mas rico),
    luego direcciones sueltas.

    Cada resultado:
        {
            "kind": "cliente" | "direccion",
            "label": str        -> texto a mostrar en el dropdown
            "key": str          -> id unico para identificar la seleccion
            "data": dict        -> datos completos para auto-rellenar
        }
    """
    if not query or len(query.strip()) < 2:
        return []

    q = _norm(query)
    results = []

    # --- Clientes: buscar en nombre, telefono, email, direccion ---
    for c in clientes:
        nombre = c.get("Nombre", "")
        telefono = c.get("Telefono", "")
        email = c.get("Email", "")
        direccion = c.get("Direccion", "")
        comuna = c.get("Comuna", "")

        haystack = _norm(f"{nombre} {telefono} {email} {direccion} {comuna}")
        if q in haystack:
            label_extra = []
            if direccion:
                label_extra.append(direccion[:30])
            if comuna:
                label_extra.append(comuna)
            extra_str = " · ".join(label_extra)
            label = f"👤 {nombre}" + (f"  —  {extra_str}" if extra_str else "")
            results.append({
                "kind": "cliente",
                "label": label,
                "key": f"cli::{nombre}",
                "data": c,
            })
            if len(results) >= limit:
                return results

    # --- Direcciones driv.in (solo si aun hay espacio) ---
    if len(results) < limit and addr_cache:
        for a in addr_cache:
            addr = _get(a, "address1")
            name = _get(a, "name")
            code = _get(a, "code")
            city = _get(a, "city")

            haystack = _norm(f"{name} {addr} {city}")
            if q in haystack:
                label = f"📍 {addr}" + (f" — {name}" if name else "") + (f"  [{code}]" if code else "")
                results.append({
                    "kind": "direccion",
                    "label": label,
                    "key": f"dir::{code or addr}",
                    "data": {
                        "codigo_drivin": code,
                        "direccion": addr,
                        "comuna": city,
                        "nombre_ref": name,
                    },
                })
                if len(results) >= limit:
                    break

    return results


def pedidos_mismo_cliente_hoy(pedidos_hoy, cliente=None, direccion=None, codigo_drivin=None):
    """
    Devuelve la lista de pedidos del dia que ya existen para el mismo
    cliente, direccion o codigo drivin. Util para alertar duplicados.
    """
    cliente_n = _norm(cliente)
    direccion_n = _norm(direccion)
    codigo_n = _norm(codigo_drivin)

    out = []
    for p in pedidos_hoy:
        if cliente_n and _norm(p.get("Cliente", "")) == cliente_n:
            out.append(p)
            continue
        if codigo_n and _norm(p.get("Codigo Drivin", "")) == codigo_n:
            out.append(p)
            continue
        if direccion_n and _norm(p.get("Direccion", "")) == direccion_n:
            out.append(p)
    return out


def cliente_to_form_data(cliente_row):
    """
    Convierte una fila de CLIENTES al dict que espera el formulario
    'Agregar pedido manual'.
    """
    return {
        "cliente": cliente_row.get("Nombre", ""),
        "telefono": cliente_row.get("Telefono", ""),
        "email": cliente_row.get("Email", ""),
        "direccion": cliente_row.get("Direccion", ""),
        "depto": cliente_row.get("Depto", ""),
        "comuna": cliente_row.get("Comuna", ""),
        "codigo_drivin": cliente_row.get("Codigo Drivin", ""),
        "marca": cliente_row.get("Marca", "KOWEN") or "KOWEN",
    }


def direccion_to_form_data(direccion_data):
    """Convierte un resultado 'direccion' del searchbox al formato del form."""
    return {
        "cliente": "",  # sin cliente asociado aun
        "telefono": "",
        "email": "",
        "direccion": direccion_data.get("direccion", ""),
        "depto": "",
        "comuna": direccion_data.get("comuna", ""),
        "codigo_drivin": direccion_data.get("codigo_drivin", ""),
        "marca": "KOWEN",
    }
