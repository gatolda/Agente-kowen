"""
Cliente de driv.in API para Agente Kowen.
Gestiona pedidos, rutas y entregas via API REST.
"""

import os
import requests

BASE_URL = "https://external.driv.in/api/external/v2"
TIMEOUT = 30  # segundos


def _get_headers():
    """Retorna headers de autenticacion."""
    api_key = os.getenv("DRIVIN_API_KEY")
    if not api_key:
        raise ValueError("DRIVIN_API_KEY no configurada en .env")
    return {
        "X-API-Key": api_key,
        "Content-Type": "application/json",
    }


def _request(method, endpoint, params=None, json_body=None):
    """Ejecuta un request a la API de driv.in."""
    url = f"{BASE_URL}/{endpoint}"
    response = requests.request(
        method, url, headers=_get_headers(), params=params, json=json_body,
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


# --- Schemas ---

def get_schemas():
    """Obtiene los esquemas de planificacion configurados."""
    return _request("GET", "schemas")


# --- Vehiculos ---

def get_vehicles():
    """Obtiene los vehiculos registrados."""
    return _request("GET", "vehicles")


def get_fleets():
    """Obtiene los grupos de vehiculos."""
    return _request("GET", "fleets")


# --- Usuarios/Conductores ---

def get_drivers():
    """Obtiene los conductores registrados."""
    return _request("GET", "users", params={"role_name": "driver"})


# --- Direcciones ---

def get_addresses(page=1):
    """Obtiene las direcciones registradas (una pagina)."""
    return _request("GET", "addresses", params={"page": page})


def get_all_addresses():
    """Obtiene todas las direcciones paginando automaticamente."""
    all_addresses = []
    page = 1
    while True:
        result = _request("GET", "addresses", params={"page": page})
        addresses = result.get("response", [])
        if not addresses:
            break
        all_addresses.extend(addresses)
        page += 1
    return all_addresses


def create_address(code, address1, city, address2="", name="", contact_name="",
                   phone="", email="", lat=None, lng=None, country="Chile",
                   state="Region Metropolitana", address_type="Casa"):
    """
    Crea una direccion de cliente en driv.in.

    Si no se pasan lat/lng, driv.in intenta geocodificar la direccion.

    Args:
        code: Codigo unico de la direccion (ej: "SI 538").
        address1: Calle y numero (ej: "San Isidro 538").
        city: Comuna (ej: "Santiago").
        address2: Depto/oficina opcional.
        name: Nombre de la direccion/cliente.
        contact_name: Nombre del contacto.
        phone, email: Contacto.
        lat, lng: Coordenadas (opcional — si se pasan saltan el geocoding).

    Returns:
        Dict con la respuesta de la API.
    """
    addr = {
        "code": code,
        "address1": address1,
        "address2": address2 or "",
        "city": city,
        "state": state,
        "country": country,
        "name": name or address1,
        "address_type": address_type,
        "contact_name": contact_name,
        "phone": phone,
        "email": email,
        "update_all": True,
    }
    if lat is not None:
        addr["lat"] = lat
    if lng is not None:
        addr["lng"] = lng

    return _request("POST", "addresses", json_body={"addresses": [addr]})


# --- Pedidos ---

def create_orders(clients, schema_code=None, scenario_token=None):
    """
    Crea pedidos en driv.in.

    Args:
        clients: Lista de clientes con sus pedidos. Cada cliente tiene:
            - code, address, city, country, contact_name, contact_phone
            - orders: lista de {code, description, units_1}
        schema_code: Codigo del esquema (si se crea en Order Manager).
        scenario_token: Token del escenario (si se agrega a un plan existente).
    """
    params = {}
    if schema_code:
        params["schema_code"] = schema_code
    if scenario_token:
        params["token"] = scenario_token

    return _request("POST", "orders", params=params, json_body={"clients": clients})


def get_orders(scenario_token):
    """Obtiene los pedidos de un escenario."""
    return _request("GET", "orders", params={"token": scenario_token})


def delete_order(order_code):
    """Elimina un pedido por su codigo."""
    return _request("DELETE", f"orders/{order_code}")


# --- Escenarios/Planes ---

def create_scenario(description, date, clients, schema_name="Optimización", schema_code=None):
    """
    Crea un escenario (plan del dia) con pedidos.

    Args:
        description: Nombre del plan (ej: "Entregas 2026-03-31").
        date: Fecha del plan en formato "YYYY-MM-DD".
        clients: Lista de clientes con direcciones y pedidos.
        schema_name: Nombre del esquema (default: "Optimización").
        schema_code: Codigo del esquema (si tiene).
    """
    body = {
        "description": description,
        "date": date,
        "clients": clients,
    }
    if schema_code:
        body["schema_code"] = schema_code
    else:
        body["schema_name"] = schema_name
    return _request("POST", "scenarios", json_body=body)


def get_scenario_status(scenario_token):
    """Obtiene el estado de un escenario."""
    return _request("GET", f"scenarios/{scenario_token}/status")


def get_scenarios_by_date(date):
    """Obtiene los escenarios de una fecha."""
    return _request("GET", "scenarios", params={"date": date})


def optimize_scenario(scenario_token):
    """Optimiza las rutas de un escenario."""
    return _request("PUT", f"scenarios/{scenario_token}/optimize")


def approve_scenario(scenario_token):
    """Aprueba un escenario (libera rutas a conductores)."""
    return _request("PUT", f"scenarios/{scenario_token}/approve")


# --- Rutas ---

def create_route(vehicle_code, clients, scenario_token):
    """Crea una ruta asignada a un vehiculo en un escenario."""
    body = {"vehicle_code": vehicle_code, "clients": clients}
    return _request("POST", "routes", params={"token": scenario_token}, json_body=body)


def get_routes(date=None, scenario_token=None):
    """Obtiene rutas por fecha o por escenario."""
    params = {}
    if date:
        params["date"] = date
    if scenario_token:
        params["token"] = scenario_token
    return _request("GET", "routes", params=params)


def get_results(scenario_token):
    """Obtiene resultados/rutas detalladas de un escenario."""
    return _request("GET", "results", params={"token": scenario_token})


def get_unassigned(scenario_token):
    """Obtiene pedidos no asignados de un escenario."""
    return _request("GET", "unassigned", params={"token": scenario_token})


def optimize_route(vehicle_code, scenario_token):
    """Optimiza la ruta de un vehiculo."""
    return _request("PUT", f"routes/{vehicle_code}/optimize",
                    params={"token": scenario_token})


def approve_route(vehicle_code, scenario_token):
    """Aprueba la ruta de un vehiculo."""
    return _request("PUT", f"routes/{vehicle_code}/approve",
                    params={"token": scenario_token})


# --- Proof of Delivery ---

def get_pods(start_date, end_date):
    """Obtiene pruebas de entrega en un rango de fechas."""
    return _request("GET", "pods", params={
        "start_date": start_date,
        "end_date": end_date,
    })


# --- Test de conexion ---

def test_connection():
    """Prueba la conexion a la API de driv.in."""
    try:
        result = get_schemas()
        if result.get("success") or result.get("status") == "OK":
            return {"ok": True, "message": "Conexion exitosa a driv.in", "data": result}
        return {"ok": False, "message": f"Respuesta inesperada: {result}"}
    except requests.exceptions.HTTPError as e:
        return {"ok": False, "message": f"Error HTTP: {e.response.status_code} - {e.response.text}"}
    except Exception as e:
        return {"ok": False, "message": f"Error: {e}"}
