"""
CLI interactivo para Agente Kowen.
Gestiona pedidos, planes y rutas sin consumir tokens de IA.
"""

import os
import sys
from datetime import datetime, timedelta

from dotenv import load_dotenv
load_dotenv()

import bsale_client
import drivin_client
import address_matcher


# --- Estado de sesion ---

session = {
    "scenario_token": None,
    "scenario_name": None,
    "orders": [],
    "addresses_cache": None,
}


def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")


def print_header():
    today = datetime.now().strftime("%d/%m/%Y")
    print("=" * 50)
    print("  KOWEN - Gestor de Entregas")
    print(f"  Fecha: {today}")
    if session["scenario_token"]:
        print(f"  Plan activo: {session['scenario_name']}")
    print("=" * 50)
    print()


def pause():
    input("\nPresione Enter para continuar...")


# === OPCION 1: Consultar pedidos web ===

def consultar_pedidos():
    print("\n--- Consultar pedidos web (Bsale) ---\n")

    default = 3483
    since = input(f"Desde que numero de pedido? [{default}]: ").strip()
    since_number = int(since) if since else default

    print(f"\nBuscando pedidos posteriores al #{since_number}...\n")

    try:
        orders = bsale_client.get_web_orders(since_number)
    except Exception as e:
        print(f"Error al consultar Bsale: {e}")
        return

    if not orders:
        print("No se encontraron pedidos nuevos.")
        return

    # Filtrar activos y anulados
    activos = [o for o in orders if o["estado"] == "activo"]
    anulados = [o for o in orders if o["estado"] == "anulado"]

    print(f"{'Nro':<6} {'Fecha':<12} {'Cliente':<25} {'Direccion':<35} {'Cant':>4} {'Total':>8} {'Marca':<8} {'Estado'}")
    print("-" * 120)

    for o in orders:
        dir_display = o["direccion"][:33]
        if o["depto"]:
            dir_display = f"{o['direccion'][:25]} {o['depto']}"
        cliente = o["cliente"][:23] if o["cliente"] else ""
        estado = o["estado"].upper()
        print(f"#{o['pedido_nro']:<5} {o['fecha']:<12} {cliente:<25} {dir_display:<35} {int(o['cantidad']):>4} ${o['total']:>7,} {o['marca']:<8} {estado}")

    print(f"\nTotal: {len(activos)} activos, {len(anulados)} anulados")

    session["orders"] = activos

    if activos:
        resp = input("\nDesea subir los pedidos activos a un plan? (s/n): ").strip().lower()
        if resp == "s":
            subir_pedidos()


# === OPCION 2: Crear plan del dia ===

def crear_plan():
    print("\n--- Crear plan del dia (driv.in) ---\n")

    # Fecha
    today = datetime.now().strftime("%Y-%m-%d")
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"  1. Hoy ({today})")
    print(f"  2. Manana ({tomorrow})")
    print(f"  3. Otra fecha")
    choice = input("\nFecha del plan [1]: ").strip() or "1"

    if choice == "1":
        date = today
    elif choice == "2":
        date = tomorrow
    else:
        date = input("Ingrese fecha (YYYY-MM-DD): ").strip()

    # Nombre
    date_display = datetime.strptime(date, "%Y-%m-%d").strftime("%d/%m/%Y")
    default_name = f"{date_display}API"
    name = input(f"Nombre del plan [{default_name}]: ").strip() or default_name

    print(f"\nCreando plan '{name}' para {date}...")

    try:
        result = drivin_client.create_scenario(
            description=name,
            date=date,
            schema_code=None,
            clients=[],
        )
        # create_scenario envia schema_code=None, pero la API usa schema_name
        # Re-intentar con schema_name
    except Exception:
        pass

    try:
        from drivin_client import _request, _get_headers
        body = {
            "description": name,
            "date": date,
            "schema_name": "Optimización",
            "clients": [],
        }
        result = _request("POST", "scenarios", json_body=body)
        response = result.get("response", result)
        token = response.get("scenario_token", "")
        vehicles = response.get("vehicles_count", 0)

        session["scenario_token"] = token
        session["scenario_name"] = name

        print(f"\nPlan creado exitosamente!")
        print(f"  Token: {token}")
        print(f"  Vehiculos: {vehicles}")

    except Exception as e:
        print(f"Error al crear plan: {e}")


# === OPCION 3: Subir pedidos a un plan ===

def subir_pedidos():
    print("\n--- Subir pedidos a un plan (driv.in) ---\n")

    # Verificar que hay pedidos cargados
    if not session["orders"]:
        print("No hay pedidos cargados. Use la opcion 1 primero.")
        resp = input("Desea consultar pedidos ahora? (s/n): ").strip().lower()
        if resp == "s":
            consultar_pedidos()
        return

    # Verificar plan activo
    if not session["scenario_token"]:
        print("No hay plan activo. Use la opcion 2 primero o ingrese un token.")
        token = input("Token del escenario (o Enter para crear uno nuevo): ").strip()
        if token:
            session["scenario_token"] = token
            session["scenario_name"] = "Plan manual"
        else:
            crear_plan()
            if not session["scenario_token"]:
                return

    # Cargar cache de direcciones
    print("Cargando direcciones de driv.in...")
    if session["addresses_cache"] is None:
        session["addresses_cache"] = address_matcher.load_cache()
        if not session["addresses_cache"]:
            print("Cache vacio. Descargando direcciones...")
            count = address_matcher.refresh_cache()
            session["addresses_cache"] = address_matcher.load_cache()
            print(f"  {count} direcciones cargadas.")

    print(f"\nMatcheando {len(session['orders'])} pedidos...\n")

    # Matchear cada pedido
    clients_to_upload = []
    skipped = []

    for order in session["orders"]:
        code = address_matcher.match_order_interactive(
            direccion=order["direccion"],
            depto=order["depto"],
            comuna=order["comuna"],
            addresses=session["addresses_cache"],
        )

        if not code:
            skipped.append(order)
            continue

        # Determinar descripcion
        marca = order.get("marca", "Kowen")
        if order["cantidad"] == 0:
            description = f"{marca} - Retiro"
        else:
            description = marca

        date_suffix = order["fecha"].replace("-", "")[4:]  # MMDD
        order_code = f"{code}-{date_suffix}"

        clients_to_upload.append({
            "code": code,
            "orders": [{
                "code": order_code,
                "description": description,
                "units_1": int(order["cantidad"]),
            }]
        })

    if not clients_to_upload:
        print("\nNo hay pedidos para subir.")
        return

    # Confirmar
    print(f"\n{'=' * 40}")
    print(f"Resumen: {len(clients_to_upload)} pedidos a subir, {len(skipped)} omitidos")
    print(f"Plan: {session['scenario_name']}")
    confirm = input("Confirmar subida? (s/n): ").strip().lower()

    if confirm != "s":
        print("Subida cancelada.")
        return

    # Subir
    try:
        result = drivin_client.create_orders(
            clients=clients_to_upload,
            scenario_token=session["scenario_token"],
        )
        response = result.get("response", result)
        added = response.get("added", [])
        skipped_api = response.get("skipped", [])
        print(f"\nSubida exitosa!")
        print(f"  Agregados: {len(added)}")
        if skipped_api:
            print(f"  Omitidos por API: {len(skipped_api)}")

        # Preguntar si asignar conductor
        resp = input("\nDesea asignar conductor a estos pedidos? (s/n): ").strip().lower()
        if resp == "s":
            asignar_conductor_pedidos(clients_to_upload)

    except Exception as e:
        print(f"Error al subir pedidos: {e}")


# === OPCION 4: Asignar conductor ===

def asignar_conductor():
    print("\n--- Asignar conductor a ruta ---\n")

    if not session["scenario_token"]:
        token = input("Token del escenario: ").strip()
        if not token:
            print("Token requerido.")
            return
        session["scenario_token"] = token

    # Listar conductores y vehiculos
    try:
        vehicles_data = drivin_client.get_vehicles()
        vehicles = vehicles_data.get("response", [])

        print("Vehiculos y conductores disponibles:\n")
        for i, v in enumerate(vehicles, 1):
            driver = v.get("driver", {})
            driver_name = f"{driver.get('first_name', '')} {driver.get('last_name', '')}".strip() if driver else "Sin conductor"
            print(f"  {i}. {v['code']} ({v.get('model', '')}) - {driver_name}")

        choice = input(f"\nElija vehiculo (1-{len(vehicles)}): ").strip()
        if not choice.isdigit() or not (1 <= int(choice) <= len(vehicles)):
            print("Opcion invalida.")
            return

        vehicle = vehicles[int(choice) - 1]
        vehicle_code = vehicle["code"]

        # Obtener pedidos no asignados
        try:
            unassigned = drivin_client.get_unassigned(session["scenario_token"])
            ua_orders = unassigned.get("response", [])
            if ua_orders:
                print(f"\nPedidos sin asignar: {len(ua_orders)}")
                confirm = input(f"Asignar todos al vehiculo {vehicle_code}? (s/n): ").strip().lower()
                if confirm == "s":
                    # Construir clients para la ruta
                    clients = []
                    for order in ua_orders:
                        clients.append({
                            "code": order.get("address_code", order.get("code", "")),
                            "orders": [{
                                "code": order.get("order_code", order.get("code", "")),
                                "description": order.get("description", "Kowen"),
                                "units_1": order.get("units_1", 0),
                            }]
                        })
                    result = drivin_client.create_route(vehicle_code, clients, session["scenario_token"])
                    print("Ruta creada exitosamente!")
            else:
                print("No hay pedidos sin asignar.")
        except Exception as e:
            print(f"Error al obtener pedidos no asignados: {e}")

    except Exception as e:
        print(f"Error: {e}")


def asignar_conductor_pedidos(clients):
    """Asigna una lista de pedidos ya preparados a un conductor."""
    try:
        vehicles_data = drivin_client.get_vehicles()
        vehicles = vehicles_data.get("response", [])

        print("\nVehiculos disponibles:\n")
        for i, v in enumerate(vehicles, 1):
            driver = v.get("driver", {})
            driver_name = f"{driver.get('first_name', '')} {driver.get('last_name', '')}".strip() if driver else "Sin conductor"
            print(f"  {i}. {v['code']} ({v.get('model', '')}) - {driver_name}")

        choice = input(f"\nElija vehiculo (1-{len(vehicles)}): ").strip()
        if not choice.isdigit() or not (1 <= int(choice) <= len(vehicles)):
            print("Opcion invalida.")
            return

        vehicle = vehicles[int(choice) - 1]

        # Preguntar si todos o seleccionar
        all_orders = input("Asignar todos los pedidos a este vehiculo? (s/n): ").strip().lower()

        if all_orders == "s":
            selected = clients
        else:
            print("\nSeleccione pedidos (numeros separados por coma):")
            for i, c in enumerate(clients, 1):
                print(f"  {i}. {c['code']}")
            sel = input("Pedidos: ").strip()
            indices = [int(x.strip()) - 1 for x in sel.split(",") if x.strip().isdigit()]
            selected = [clients[i] for i in indices if 0 <= i < len(clients)]

        if not selected:
            print("No se seleccionaron pedidos.")
            return

        result = drivin_client.create_route(
            vehicle["code"], selected, session["scenario_token"]
        )
        print(f"\nRuta creada! {len(selected)} pedidos asignados a {vehicle['code']}.")

    except Exception as e:
        print(f"Error al asignar conductor: {e}")


# === OPCION 5: Ver estado de rutas ===

def ver_estado():
    print("\n--- Estado de rutas ---\n")

    today = datetime.now().strftime("%Y-%m-%d")
    date = input(f"Fecha [{today}]: ").strip() or today

    try:
        result = drivin_client.get_routes(date=date)
        routes = result.get("response", [])

        if not routes:
            print("No hay rutas para esta fecha.")
            return

        print(f"\n{'Vehiculo':<12} {'Conductor':<25} {'Pedidos':>8} {'Estado':<15}")
        print("-" * 65)

        for route in routes:
            vehicle = route.get("vehicle_code", "")
            driver = route.get("driver_name", route.get("driver", {}).get("name", "N/A"))
            total_orders = route.get("total_orders", 0)

            if route.get("is_finished"):
                estado = "Finalizada"
            elif route.get("is_started"):
                estado = "En curso"
            elif route.get("is_approved"):
                estado = "Aprobada"
            else:
                estado = "Pendiente"

            print(f"{vehicle:<12} {str(driver):<25} {total_orders:>8} {estado:<15}")

    except Exception as e:
        print(f"Error: {e}")


# === OPCION 6: Actualizar cache ===

def actualizar_cache():
    print("\n--- Actualizar cache de direcciones ---\n")
    print("Descargando todas las direcciones de driv.in...")

    try:
        count = address_matcher.refresh_cache()
        session["addresses_cache"] = address_matcher.load_cache()
        print(f"\nCache actualizado: {count} direcciones guardadas.")
    except Exception as e:
        print(f"Error: {e}")


# === MENU PRINCIPAL ===

def main():
    while True:
        clear_screen()
        print_header()

        print("  1. Consultar pedidos web (Bsale)")
        print("  2. Crear plan del dia (driv.in)")
        print("  3. Subir pedidos a un plan")
        print("  4. Asignar conductor a ruta")
        print("  5. Ver estado de rutas")
        print("  6. Actualizar cache de direcciones")
        print("  0. Salir")
        print()

        opcion = input("  Opcion: ").strip()

        if opcion == "1":
            consultar_pedidos()
            pause()
        elif opcion == "2":
            crear_plan()
            pause()
        elif opcion == "3":
            subir_pedidos()
            pause()
        elif opcion == "4":
            asignar_conductor()
            pause()
        elif opcion == "5":
            ver_estado()
            pause()
        elif opcion == "6":
            actualizar_cache()
            pause()
        elif opcion == "0":
            print("\nHasta luego!")
            sys.exit(0)
        else:
            print("\nOpcion invalida.")
            pause()


if __name__ == "__main__":
    main()
