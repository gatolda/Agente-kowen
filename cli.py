"""
CLI interactivo para Agente Kowen.
Gestiona pedidos, planes y rutas conectado a Google Sheets.
"""

import os
import sys
from datetime import datetime, timedelta

from dotenv import load_dotenv
load_dotenv()

import sheets_client
import operations
import log_client
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


# === OPCION 1: Ver pedidos de hoy ===

def ver_pedidos_hoy():
    print("\n--- Pedidos de hoy ---\n")

    fecha = datetime.now().strftime("%d/%m/%Y")
    otra = input(f"Fecha [{fecha}]: ").strip()
    if otra:
        fecha = otra

    pedidos = sheets_client.get_pedidos(fecha)

    if not pedidos:
        print(f"No hay pedidos para {fecha}.")
        return

    # Conteos
    pendientes = [p for p in pedidos if p.get("Estado Pedido") == "PENDIENTE"]
    en_camino = [p for p in pedidos if p.get("Estado Pedido") == "EN CAMINO"]
    entregados = [p for p in pedidos if p.get("Estado Pedido") == "ENTREGADO"]
    no_entregados = [p for p in pedidos if p.get("Estado Pedido") == "NO ENTREGADO"]

    print(f"  Total: {len(pedidos)} | Pendientes: {len(pendientes)} | "
          f"En camino: {len(en_camino)} | Entregados: {len(entregados)} | "
          f"No entregados: {len(no_entregados)}\n")

    print(f"{'#':<4} {'Direccion':<35} {'Depto':<10} {'Cant':>4} {'Marca':<8} {'Estado':<14} {'Codigo':<12} {'Repartidor'}")
    print("-" * 120)

    for p in pedidos:
        nro = p.get("#", "")
        dir_short = p.get("Direccion", "")[:33]
        depto = p.get("Depto", "")[:8]
        cant = p.get("Cant", "")
        marca = p.get("Marca", "")[:6]
        estado = p.get("Estado Pedido", "")[:12]
        codigo = p.get("Codigo Drivin", "")[:10]
        rep = p.get("Repartidor", "")[:15]
        print(f"{nro:<4} {dir_short:<35} {depto:<10} {cant:>4} {marca:<8} {estado:<14} {codigo:<12} {rep}")


# === OPCION 2: Consultar pedidos web ===

def consultar_pedidos():
    print("\n--- Consultar pedidos web (Bsale) ---\n")

    # Obtener ultimo Bsale del sistema
    all_pedidos = sheets_client.get_pedidos()
    bsale_nums = [int(p.get("Pedido Bsale", "0") or "0") for p in all_pedidos]
    default = max(bsale_nums) if bsale_nums else 3483

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

    activos = [o for o in orders if o["estado"] == "activo"]
    anulados = [o for o in orders if o["estado"] == "anulado"]

    # Verificar duplicados contra el sistema
    checked = operations.check_bsale_orders(activos)
    nuevos = [o for o in checked if not o["existe"]]
    existentes = [o for o in checked if o["existe"]]

    print(f"{'Nro':<6} {'Fecha':<12} {'Cliente':<25} {'Direccion':<35} {'Cant':>4} {'Marca':<8} {'Estado'}")
    print("-" * 100)

    for o in checked:
        dir_display = o["direccion"][:33]
        if o["depto"]:
            dir_display = f"{o['direccion'][:25]} {o['depto']}"
        cliente = o.get("cliente", "")[:23]
        flag = "EXISTE" if o["existe"] else "NUEVO"
        print(f"#{o['pedido_nro']:<5} {o['fecha']:<12} {cliente:<25} {dir_display:<35} {int(o['cantidad']):>4} {o['marca']:<8} {flag}")

    print(f"\nTotal: {len(nuevos)} nuevos, {len(existentes)} ya existen, {len(anulados)} anulados")

    session["orders"] = activos

    if nuevos:
        resp = input(f"\nImportar {len(nuevos)} pedidos nuevos a la planilla? (s/n): ").strip().lower()
        if resp == "s":
            count = operations.sync_from_bsale(activos)
            print(f"\n{count} pedidos importados a la planilla.")

            resp2 = input("Subir tambien a un plan de driv.in? (s/n): ").strip().lower()
            if resp2 == "s":
                subir_pedidos()


# === OPCION 3: Crear plan del dia ===

def crear_plan():
    print("\n--- Crear plan del dia (driv.in) ---\n")

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

    date_display = datetime.strptime(date, "%Y-%m-%d").strftime("%d/%m/%Y")
    default_name = f"{date_display}API"
    name = input(f"Nombre del plan [{default_name}]: ").strip() or default_name

    print(f"\nCreando plan '{name}' para {date}...")

    try:
        result = drivin_client.create_scenario(
            description=name,
            date=date,
            clients=[],
        )
        response = result.get("response", result)
        token = response.get("scenario_token", "")

        session["scenario_token"] = token
        session["scenario_name"] = name

        print(f"\nPlan creado exitosamente!")
        print(f"  Token: {token}")

    except Exception as e:
        print(f"Error al crear plan: {e}")


# === OPCION 4: Subir pedidos a un plan ===

def subir_pedidos():
    print("\n--- Subir pedidos a un plan (driv.in) ---\n")

    if not session["scenario_token"]:
        print("No hay plan activo. Use la opcion 3 primero o ingrese un token.")
        token = input("Token del escenario (o Enter para crear uno nuevo): ").strip()
        if token:
            session["scenario_token"] = token
            session["scenario_name"] = "Plan manual"
        else:
            crear_plan()
            if not session["scenario_token"]:
                return

    # Cargar pedidos de hoy sin codigo asignado en drivin
    fecha = datetime.now().strftime("%d/%m/%Y")
    pedidos = sheets_client.get_pedidos(fecha)
    pendientes = [p for p in pedidos
                  if p.get("Estado Pedido") == "PENDIENTE"
                  and not p.get("Plan Drivin", "").strip()]

    if not pendientes:
        print("No hay pedidos pendientes sin plan para hoy.")
        return

    # Cargar cache de direcciones
    print("Cargando direcciones de driv.in...")
    if session["addresses_cache"] is None:
        session["addresses_cache"] = address_matcher.load_cache()
        if not session["addresses_cache"]:
            print("Cache vacio. Descargando direcciones...")
            address_matcher.refresh_cache()
            session["addresses_cache"] = address_matcher.load_cache()

    print(f"\nMatcheando {len(pendientes)} pedidos...\n")

    clients_to_upload = []
    skipped = []
    updates = []

    date_suffix = datetime.now().strftime("%m%d")

    for p in pendientes:
        nro = p.get("#", "")
        direccion = p.get("Direccion", "")
        depto = p.get("Depto", "")
        comuna = p.get("Comuna", "")
        codigo = p.get("Codigo Drivin", "").strip()

        if not codigo:
            code, confidence = address_matcher.auto_match(
                direccion, depto, comuna, session["addresses_cache"]
            )
            if confidence in ("auto", "memory") and code:
                codigo = code
                updates.append((int(nro), {"codigo_drivin": code}))
            elif confidence == "ambiguous":
                # Matching interactivo
                code = address_matcher.match_order_interactive(
                    direccion, depto, comuna, session["addresses_cache"]
                )
                if code:
                    codigo = code
                    updates.append((int(nro), {"codigo_drivin": code}))
                    address_matcher.save_memory_entry(direccion, code)
                    log_client.log_match_manual(direccion, code)

        if not codigo:
            skipped.append(p)
            print(f"  SKIP: {direccion} {depto} (sin codigo)")
            continue

        marca = p.get("Marca", "KOWEN")
        cant = int(p.get("Cant", 0) or 0)
        desc = f"{marca} - Retiro" if cant == 0 else marca
        order_code = f"{codigo}-{date_suffix}"

        clients_to_upload.append({
            "code": codigo,
            "orders": [{"code": order_code, "description": desc, "units_1": cant}]
        })
        print(f"  OK: {direccion} -> {codigo}")

    # Guardar codigos asignados
    if updates:
        sheets_client.update_pedidos_batch(updates)

    if not clients_to_upload:
        print("\nNo hay pedidos para subir.")
        return

    print(f"\n{'=' * 40}")
    print(f"Resumen: {len(clients_to_upload)} pedidos a subir, {len(skipped)} omitidos")
    print(f"Plan: {session['scenario_name']}")
    confirm = input("Confirmar subida? (s/n): ").strip().lower()

    if confirm != "s":
        print("Subida cancelada.")
        return

    try:
        result = drivin_client.create_orders(
            clients=clients_to_upload,
            scenario_token=session["scenario_token"],
        )
        response = result.get("response", result)
        added = response.get("added", [])
        print(f"\nSubida exitosa! {len(added)} pedidos agregados.")

        # Marcar plan en la planilla
        plan_updates = []
        for p in pendientes:
            nro = p.get("#", "")
            codigo = p.get("Codigo Drivin", "").strip()
            if nro and nro.isdigit() and codigo:
                plan_updates.append((int(nro), {"plan_drivin": session["scenario_name"]}))
        if plan_updates:
            sheets_client.update_pedidos_batch(plan_updates)

    except Exception as e:
        print(f"Error al subir pedidos: {e}")


# === OPCION 5: Asignar conductor ===

def asignar_conductor():
    print("\n--- Asignar conductor a ruta ---\n")

    if not session["scenario_token"]:
        token = input("Token del escenario: ").strip()
        if not token:
            print("Token requerido.")
            return
        session["scenario_token"] = token

    try:
        vehicles_data = drivin_client.get_vehicles()
        vehicles = vehicles_data.get("response", [])

        print("Vehiculos disponibles:\n")
        for i, v in enumerate(vehicles, 1):
            driver = v.get("driver", {})
            driver_name = f"{driver.get('first_name', '')} {driver.get('last_name', '')}".strip() if driver else "Sin conductor"
            print(f"  {i}. {v['code']} ({v.get('model', '')}) - {driver_name}")

        choice = input(f"\nElija vehiculo (1-{len(vehicles)}): ").strip()
        if not choice.isdigit() or not (1 <= int(choice) <= len(vehicles)):
            print("Opcion invalida.")
            return

        vehicle = vehicles[int(choice) - 1]

        unassigned = drivin_client.get_unassigned(session["scenario_token"])
        ua_orders = unassigned.get("response", [])
        if ua_orders:
            print(f"\nPedidos sin asignar: {len(ua_orders)}")
            confirm = input(f"Asignar todos al vehiculo {vehicle['code']}? (s/n): ").strip().lower()
            if confirm == "s":
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
                drivin_client.create_route(vehicle["code"], clients, session["scenario_token"])
                print("Ruta creada exitosamente!")
        else:
            print("No hay pedidos sin asignar.")

    except Exception as e:
        print(f"Error: {e}")


# === OPCION 6: Ver estado de rutas ===

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


# === OPCION 7: Importar planilla reparto ===

def importar_planilla():
    print("\n--- Importar desde planilla reparto ---\n")

    fecha = datetime.now().strftime("%d/%m/%Y")
    otra = input(f"Fecha [{fecha}]: ").strip()
    if otra:
        fecha = otra

    print(f"Importando pedidos de planilla reparto ({fecha})...")

    try:
        count = operations.sync_from_planilla_reparto(fecha)
        print(f"\n{count} pedidos importados.")
    except Exception as e:
        print(f"Error: {e}")


# === OPCION 8: Ejecutar rutina diaria ===

def ejecutar_rutina():
    print("\n--- Ejecutar rutina diaria ---\n")

    fecha = datetime.now().strftime("%d/%m/%Y")
    print(f"Ejecutando rutina para {fecha}...")
    print("(Revisa ayer, importa nuevos, asigna codigos, sube a driv.in)\n")

    try:
        resultado = operations.rutina_diaria(fecha_hoy=fecha)

        print(f"Pedidos de ayer ({resultado['fecha_ayer']}):")
        print(f"  Entregados: {resultado['entregados_ayer']}")
        print(f"  No entregados: {resultado['no_entregados_ayer']}")
        print(f"  Movidos a hoy: {resultado['movidos_a_hoy']}")
        print(f"  Duplicados eliminados: {resultado['duplicados_eliminados']}")
        print(f"\nImportaciones de hoy:")
        print(f"  Planilla Kowen: {resultado['planilla_importados']}")
        print(f"  Planilla Cactus: {resultado.get('cactus_importados', 0)}")
        print(f"  Codigos asignados: {resultado['codigos_asignados']}")
        print(f"\nPlan driv.in:")
        print(f"  Plan: {resultado.get('drivin_plan', 'N/A')}")
        print(f"  Subidos: {resultado.get('drivin_subidos', 0)}")

        bsale_pend = resultado.get("bsale_pendientes", [])
        if bsale_pend:
            print(f"\n{len(bsale_pend)} pedidos Bsale sin planilla (pasar a planilla manualmente):")
            for p in bsale_pend[:10]:
                print(f"  #{p.get('pedido_nro', '')} {p.get('direccion', '')[:35]} "
                      f"({p.get('cantidad', 0)}x) - {p.get('cliente', '')[:25]}")
            if len(bsale_pend) > 10:
                print(f"  ...y {len(bsale_pend) - 10} mas")

        if resultado["errores"]:
            print(f"\nAdvertencias:")
            for err in resultado["errores"]:
                print(f"  - {err}")

    except Exception as e:
        print(f"Error: {e}")


# === OPCION 9: Resumen del dia ===

def resumen():
    print("\n--- Resumen del dia ---\n")

    fecha = datetime.now().strftime("%d/%m/%Y")
    otra = input(f"Fecha [{fecha}]: ").strip()
    if otra:
        fecha = otra

    r = operations.resumen_dia(fecha)

    print(f"  Fecha: {r['fecha']}")
    print(f"  Total pedidos: {r['total_pedidos']}")
    print(f"  Botellones: {r['total_botellones']}")
    print(f"  Entregados: {r['entregados']}")
    print(f"  Pendientes: {r['pendientes']}")
    print(f"  En camino: {r['en_camino']}")
    print(f"  No entregados: {r['no_entregados']}")
    print(f"  Pagados: {r['pagados']}")
    print(f"  Por cobrar: {r['por_cobrar']}")


# === OPCION V: Verificar contra driv.in ===

def verificar_drivin():
    print("\n--- Verificar pedidos contra driv.in ---\n")

    fecha = datetime.now().strftime("%d/%m/%Y")
    otra = input(f"Fecha [{fecha}]: ").strip()
    if otra:
        fecha = otra

    print("Consultando driv.in...")
    try:
        v = operations.verify_orders_drivin(fecha=fecha, auto_update=False)
        print(f"\nVerificados: {v['total_verificados']}")
        print(f"Entregados detectados: {v['entregados_detectados']}")
        print(f"No entregados detectados: {v['no_entregados_detectados']}")
        print(f"En camino detectados: {v['en_camino_detectados']}")

        if v["planes_sin_despachar"]:
            print(f"\nPlanes creados pero NO despachados:")
            for p in v["planes_sin_despachar"]:
                print(f"  - {p['plan']} (status: {p['status']})")

        if v["detalle"]:
            print(f"\nCambios detectados:")
            for d in v["detalle"]:
                print(f"  #{d['numero']} {d['direccion']}: {d['estado_anterior']} -> {d['estado_nuevo']}")

            aplicar = input("\nAplicar cambios? (s/n): ").strip().lower()
            if aplicar == "s":
                result = operations.verify_orders_drivin(fecha=fecha, auto_update=True)
                print(f"\n{result['actualizados']} pedidos actualizados!")
        elif v["total_verificados"] > 0:
            print("\nNo se detectaron cambios. Todo al dia.")

        if v["estancados"]:
            print(f"\nPedidos estancados (PENDIENTE por mas de 2 dias):")
            for e in v["estancados"]:
                print(f"  #{e['numero']} {e['direccion']} - {e['dias']} dias - "
                      f"Codigo: {e['codigo'] or 'sin codigo'} - Plan: {e['plan'] or 'sin plan'}")

    except Exception as e:
        print(f"Error: {e}")


# === OPCION 0: Actualizar cache ===

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

        print("  1. Ver pedidos de hoy")
        print("  2. Consultar pedidos web (Bsale)")
        print("  3. Crear plan del dia (driv.in)")
        print("  4. Subir pedidos a un plan")
        print("  5. Asignar conductor a ruta")
        print("  6. Ver estado de rutas")
        print("  7. Importar planilla reparto")
        print("  8. Ejecutar rutina diaria")
        print("  9. Resumen del dia")
        print("  V. Verificar pedidos contra driv.in")
        print("  0. Actualizar cache de direcciones")
        print("  Q. Salir")
        print()

        opcion = input("  Opcion: ").strip().upper()

        if opcion == "1":
            ver_pedidos_hoy()
            pause()
        elif opcion == "2":
            consultar_pedidos()
            pause()
        elif opcion == "3":
            crear_plan()
            pause()
        elif opcion == "4":
            subir_pedidos()
            pause()
        elif opcion == "5":
            asignar_conductor()
            pause()
        elif opcion == "6":
            ver_estado()
            pause()
        elif opcion == "7":
            importar_planilla()
            pause()
        elif opcion == "8":
            ejecutar_rutina()
            pause()
        elif opcion == "9":
            resumen()
            pause()
        elif opcion == "V":
            verificar_drivin()
            pause()
        elif opcion == "0":
            actualizar_cache()
            pause()
        elif opcion == "Q":
            print("\nHasta luego!")
            sys.exit(0)
        else:
            print("\nOpcion invalida.")
            pause()


if __name__ == "__main__":
    main()
