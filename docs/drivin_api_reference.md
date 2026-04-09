# Documentacion API Drivin (driv.in)

Fuente: Coleccion Postman oficial extraida desde apidocs-en.driv.in
Fecha de extraccion: 2026-03-30

---

## Informacion General

- **URL Base**: `https://external.driv.in/api/external/v2/`
- **URL QA/Testing**: `https://app2-qa.driv.in/`
- **Protocolo**: REST
- **Formato**: JSON (request y response)
- **Rate Limit**: 200 requests por minuto
- **Documentacion oficial**: https://apidocs.driv.in/ (ES), https://apidocs-en.driv.in/ (EN)
- **Soporte**: soporte@driv.in

---

## Autenticacion

Dos headers requeridos en cada request:

```
X-API-Key: {tu_api_key}
Content-Type: application/json
```

Cada organizacion tiene su propio `api_key`. Para obtenerlo, contactar soporte@driv.in para crear la organizacion en ambiente de pruebas.

---

## Conceptos Principales

- **Organization**: Tu cuenta en Drivin. Cada una tiene su propio api_key.
- **Crew**: Usuarios tipo "Driver" o "Assistant" que usan la app movil.
- **Warehouse (Deposito)**: Centro de distribucion, lugar fisico donde las rutas inician/terminan.
- **Schema**: Define restricciones y parametros para la planificacion de rutas.
- **Order (Pedido)**: Una visita a una direccion especifica (entrega, recogida, servicio).
- **Scenario (Plan)**: Usado para crear rutas basadas en fecha, esquema, vehiculos y pedidos.
- **Route (Ruta)**: Compuesta por uno o mas trips con visitas, vehiculo y tripulacion.
- **Trip (Viaje)**: Compuesto por uno o mas pedidos.

---

## Tipos de Planificacion

1. **Normal** (optimization_type=1): Minimiza flota y km para gestionar todos los pedidos.
2. **Con asignacion** (optimization_type=2): El cliente indica en que vehiculo va cada pedido, Drivin optimiza la secuencia.
3. **Balanceada** (optimization_type=3): Distribuye carga equitativamente entre vehiculos.
4. **Flota completa**: Considera todos los vehiculos sin necesariamente distribuir equitativamente.

---

## Endpoints

### SCENARIOS (Planes)

#### POST /scenarios - Crear Escenario
```
POST https://external.driv.in/api/external/v2/scenarios
```

Body:
```json
{
  "description": "Nombre del plan",
  "date": "2022-01-01",
  "fleet_name": null,
  "schema_name": "nombre esquema",
  "schema_code": "001",
  "clients": [
    {
      "code": null,
      "address": "Direccion 123",
      "reference": "Depto 208",
      "city": "Santiago",
      "country": "Chile",
      "lat": -33.4489,
      "lng": -70.6693,
      "name": "Nombre Direccion",
      "client_name": "Juan Perez",
      "client_code": null,
      "address_type": "Departamento",
      "contact_name": "Juan Perez",
      "contact_phone": "999999999",
      "contact_email": "email@contacto.com",
      "service_time": null,
      "time_windows": [
        { "start": "09:00", "end": "11:00" }
      ],
      "tags": [],
      "orders": [
        {
          "code": "00098765",
          "description": "Descripcion pedido",
          "units_1": 1,
          "units_2": 0,
          "units_3": 0,
          "priority": 1,
          "category": null
        }
      ]
    }
  ]
}
```

Response 200:
```json
{
  "success": true,
  "status": "OK",
  "response": {
    "scenario_token": "token_del_escenario",
    "addresses_count": 2,
    "orders_count": 2,
    "items_count": 4,
    "vehicles_count": 1,
    "added": ["00098765", "00089827"]
  }
}
```

#### POST /multipleleg - Crear Escenarios Multiples
```
POST https://external.driv.in/api/external/v2/multipleleg
```

#### GET /scenarios/{token}/status - Estado del Escenario
```
GET https://external.driv.in/api/external/v2/scenarios/{scenario_token}/status
```

Estados posibles:
- `Geocoding`: localizando direcciones
- `Incomplete`: faltan datos para optimizar
- `Ready`: listo para optimizar
- `Queueing`: esperando optimizacion
- `Optimizing`: optimizando
- `Optimized`: optimizado

Response 200:
```json
{
  "success": true,
  "status": "OK",
  "response": {
    "status": "Optimized"
  }
}
```

#### GET /scenarios?date={fecha} - Escenarios por Fecha
```
GET https://external.driv.in/api/external/v2/scenarios?date=2022-01-01
```

Response 200:
```json
{
  "status": "OK",
  "response": [
    {
      "token": "uuid-del-escenario",
      "deploy_date": "2022-01-01",
      "description": "Plan 2022-01-01",
      "status": "Optimized",
      "created_at": "2022-01-01T11:10:47-03:00"
    }
  ]
}
```

#### GET /unassigned?token={token} - Pedidos No Asignados
```
GET https://external.driv.in/api/external/v2/unassigned?token={scenario_token}
```

#### PUT /scenarios/{token}/optimize - Optimizar Escenario
```
PUT https://external.driv.in/api/external/v2/scenarios/{scenario_token}/optimize
```

#### PUT /scenarios/{token}/approve - Aprobar Escenario
```
PUT https://external.driv.in/api/external/v2/scenarios/{scenario_token}/approve
```

#### DELETE /scenarios/{token} - Eliminar Escenario
```
DELETE https://external.driv.in/api/external/v2/scenarios/{scenario_token}
```

---

### ORDERS (Pedidos)

#### POST /orders - Crear Pedidos
```
POST https://external.driv.in/api/external/v2/orders?schema_code={schema_code}
```

Se pueden crear en Order Manager (sin token) o en un Scenario especifico (con `?token={scenario_token}`).

Body:
```json
{
  "clients": [
    {
      "code": "201000345",
      "address": "Direccion 123",
      "reference": "Depto 208",
      "city": "Santiago",
      "country": "Chile",
      "lat": -33.4489,
      "lng": -70.6693,
      "name": "Nombre Cliente",
      "client_name": "Nombre Cliente",
      "address_type": "Departamento",
      "contact_name": "Nombre Contacto",
      "contact_phone": "999999999",
      "contact_email": "email@test.com",
      "service_time": 15,
      "time_windows": [
        { "start": "10:00", "end": "12:00" }
      ],
      "orders": [
        {
          "code": "100203955",
          "description": "Botellon 20L",
          "units_1": 1,
          "units_2": 0,
          "units_3": 0,
          "priority": 1
        }
      ]
    }
  ]
}
```

Response 200:
```json
{
  "success": true,
  "status": "OK",
  "response": {
    "added": ["100203955", "100203956"],
    "edited": [],
    "skipped": []
  }
}
```

#### GET /orders?token={token} - Obtener Pedidos
```
GET https://external.driv.in/api/external/v2/orders?token={scenario_token}
```

#### DELETE /orders/{order_code} - Eliminar Pedido
```
DELETE https://external.driv.in/api/external/v2/orders/{order_code}
```

Comportamiento:
- Si esta en Order Manager o en escenario (antes de iniciar ruta): se elimina completamente.
- Si la ruta ya comenzo y el pedido no fue gestionado: queda como "CANCELLED".
- Si ya fue gestionado: no se puede eliminar.

---

### ADDRESSES (Direcciones)

#### POST /addresses - Crear Direccion
```
POST https://external.driv.in/api/external/v2/addresses
```

Body:
```json
{
  "addresses": [
    {
      "code": "21839794-0",
      "address1": "NE Valley Road",
      "address2": "P106",
      "city": "Santiago",
      "state": "Region Metropolitana",
      "county": null,
      "country": "Chile",
      "lat": -33.4489,
      "lng": -70.6693,
      "postal_code": null,
      "name": "NOMBRE CLIENTE",
      "client_name": "EMPRESA",
      "client_code": null,
      "address_type": "Casa",
      "contact_name": "NOMBRE CONTACTO",
      "phone": null,
      "email": null,
      "update_all": true
    }
  ]
}
```

#### GET /addresses - Obtener Direcciones
```
GET https://external.driv.in/api/external/v2/addresses
```

---

### ROUTES (Rutas)

#### POST /routes?token={token} - Crear Ruta en Escenario
```
POST https://external.driv.in/api/external/v2/routes?token={scenario_token}
```

Body:
```json
{
  "vehicle_code": "VEHICULO01",
  "clients": [
    {
      "code": null,
      "address": "Direccion 123",
      "city": "Santiago",
      "country": "Chile",
      "lat": -33.4489,
      "lng": -70.6693,
      "orders": [
        {
          "code": "00098765",
          "description": "Descripcion",
          "units_1": 1
        }
      ]
    }
  ]
}
```

#### GET /results?token={token} - Rutas del Escenario
```
GET https://external.driv.in/api/external/v2/results?token={scenario_token}&unassigned=1&vehicle_code[]={vehicle_code}
```

Response incluye resumen de rutas con vehiculo, conductor, trips, ordenes, distancias, tiempos.

#### GET /routes - Obtener Rutas
```
GET https://external.driv.in/api/external/v2/routes?date=2022-01-01&approved=1&started=1&finished=1&vehicle_code={vehicle_code}&token={scenario_token}
```

Parametros minimos: `date` o `token` (al menos uno debe existir).

Response 200:
```json
{
  "status": "OK",
  "success": true,
  "response": [
    {
      "id": 2001220,
      "code": null,
      "vehicle_code": "VEHICULO01",
      "total_orders": 12,
      "total_addresses": 11,
      "is_approved": true,
      "is_started": false,
      "is_finished": false,
      "approved_at": "2022-01-01T20:49:30-03:00",
      "started_at": null,
      "finished_at": null,
      "token": "scenario_token"
    }
  ]
}
```

#### PUT /routes/{vehicle_code}?token={token} - Cambiar Vehiculo de Ruta
```
PUT https://external.driv.in/api/external/v2/routes/{vehicle_code}?token={scenario_token}
```

Body:
```json
{
  "code": "codigo_ruta",
  "platform": null,
  "vehicle_code": "NUEVO_VEHICULO",
  "driver_email": "conductor@email.com"
}
```

#### PUT /routes/{vehicle_code}/optimize?token={token} - Optimizar Ruta
```
PUT https://external.driv.in/api/external/v2/routes/{vehicle_code}/optimize?token={scenario_token}
```

#### PUT /routes/{vehicle_code}/approve?token={token} - Aprobar Ruta
```
PUT https://external.driv.in/api/external/v2/routes/{vehicle_code}/approve?token={scenario_token}
```

#### PUT /routes/{vehicle_code}/start?token={token} - Iniciar Ruta
```
PUT https://external.driv.in/api/external/v2/routes/{vehicle_code}/start?token={scenario_token}
```

Body:
```json
{
  "start_time": 1514764800000,
  "lat": -33.4489,
  "lng": -70.6693,
  "odometer_start": null
}
```

#### PUT /routes/{vehicle_code}/finish?token={token} - Finalizar Ruta
```
PUT https://external.driv.in/api/external/v2/routes/{vehicle_code}/finish?token={scenario_token}
```

Body:
```json
{
  "end_time": 1615733790,
  "lat": -33.4489,
  "lng": -70.6693,
  "odometer_end": null,
  "comment": null,
  "finish_code": null
}
```

#### DELETE /v3/routes/{trip_code}?token={token} - Eliminar Retorno
```
DELETE https://external.driv.in/api/external/v3/routes/{trip_code}?token={scenario_token}
```

Nota: Este endpoint usa v3.

---

### SCHEMAS (Esquemas)

#### POST /schemas - Crear Esquemas
```
POST https://external.driv.in/api/external/v2/schemas
```

Body:
```json
{
  "schemas": [
    {
      "code": "011",
      "name": "Esquema Test",
      "return_trip": true,
      "multiple_trips": false,
      "reload_time": 45,
      "service_time": 10,
      "exclusive": false,
      "max_speed": 100,
      "optimization_type": 1,
      "fleet": "Flota Norte",
      "start_time": "09:00",
      "end_time": "21:00",
      "active": true,
      "deposit": {
        "name": "Bodega Principal",
        "code": "BOD01",
        "lat": -33.4489,
        "lng": -70.6693,
        "address_1": "Direccion Bodega",
        "city": "Santiago",
        "state": null,
        "county": null,
        "country": "Chile"
      }
    }
  ]
}
```

optimization_type: 1=Normal, 2=Con asignacion, 3=Balanceada

#### GET /schemas - Obtener Esquemas
```
GET https://external.driv.in/api/external/v2/schemas
```

---

### USERS (Usuarios)

#### POST /users - Crear Usuarios
```
POST https://external.driv.in/api/external/v2/users
```

Body:
```json
{
  "users": [
    {
      "email": "conductor@test.com",
      "first_name": "Juan",
      "last_name": "Perez",
      "phone": "999999999",
      "role_name": "driver",
      "external_organization": null,
      "fleet": null,
      "dni": null,
      "active": true
    }
  ]
}
```

Roles disponibles: driver, supplier, observer, fleet_contractor, admin, etc.

#### GET /users - Obtener Usuarios
```
GET https://external.driv.in/api/external/v2/users?role_name=driver
```

---

### VEHICLES (Vehiculos)

#### POST /vehicles - Crear Vehiculos
```
POST https://external.driv.in/api/external/v2/vehicles
```

Body:
```json
{
  "vehicles": [
    {
      "code": "AABB01",
      "description": "Camioneta 3/4",
      "detail": null,
      "capacity_1": 1250,
      "capacity_2": 0,
      "capacity_3": 0,
      "tags": [],
      "fleets": ["Flota Norte"]
    }
  ]
}
```

#### GET /vehicles - Obtener Vehiculos
```
GET https://external.driv.in/api/external/v2/vehicles
```

#### GET /fleets - Obtener Grupos de Vehiculos
```
GET https://external.driv.in/api/external/v2/fleets
```

---

### CLIENTS (Clientes)

#### POST /clients - Crear Clientes
```
POST https://external.driv.in/api/external/v2/clients
```

Body:
```json
{
  "clients": [
    {
      "code": "18458101",
      "name": "Nombre Cliente",
      "client_type": "customer",
      "contact_name": "Nombre Contacto",
      "contact_phone": "13340285",
      "contact_email": "email@gmail.com"
    }
  ]
}
```

---

### PROOF OF DELIVERY (Prueba de Entrega)

#### POST /reasons - Crear Razones
```
POST https://external.driv.in/api/external/v2/reasons
```

Body:
```json
{
  "reasons": [
    {
      "code": "AAAA",
      "description": "Aprobado",
      "reason_type": "approved",
      "active": true
    },
    {
      "code": "BBBB",
      "description": "Rechazado",
      "reason_type": "rejected",
      "active": true
    }
  ]
}
```

#### POST /pods - Registrar Prueba de Entrega
```
POST https://external.driv.in/api/external/v2/pods
```

Body:
```json
{
  "orders": [
    {
      "order_code": "numero_pedido",
      "scenario_token": "token_escenario",
      "pod_lat": -33.4489,
      "pod_lng": -70.6693,
      "pod_timestamp": 1514764800000,
      "order_status": "approved",
      "reason_name": "nombre_razon",
      "reason_code": "codigo_razon",
      "comment": "comentario",
      "delivered_by": "usuario@mail.com"
    }
  ]
}
```

#### GET /pods - Obtener Pruebas de Entrega
```
GET https://external.driv.in/api/external/v2/pods?start_date=2022-06-10&end_date=2022-06-10
```

#### PUT /orders/resend - Reenviar Informacion de Entrega
```
PUT https://external.driv.in/api/external/v2/orders/resend
```

---

### GPS

#### POST /positions - Enviar Datos GPS
```
POST https://external.driv.in/api/external/v2/positions
```

Body:
```json
{
  "positions": [
    {
      "device_number": 1829377,
      "vehicle_code": "AABB09",
      "lat": -33.4489,
      "lng": -70.6693,
      "accuracy": null,
      "speed": null,
      "battery_level": null,
      "heading": null,
      "timestamp": 1519135237474,
      "acceleration": null
    }
  ],
  "events": [
    {
      "name": null,
      "status": null,
      "device_number": null,
      "timestamp": null
    }
  ]
}
```

---

### SAME DAY DELIVERY (Entrega Mismo Dia)

#### POST /orders (con autoassign) - Entrega Mismo Dia
```
POST https://external.driv.in/api/external/v2/orders?token={scenario_token}&schema_code={schema_code}&autoassign=1
```

Body:
```json
{
  "clients": [
    {
      "code": null,
      "name": "NOMBRE CLIENTE",
      "address": "Direccion 123",
      "city": "Santiago",
      "country": "Chile",
      "contact_name": "Nombre Contacto",
      "orders": [
        {
          "code": "PICKUP100",
          "units": 1,
          "delivery_date": "2022-06-25",
          "supplier_code": null,
          "supplier_name": null,
          "category": "PICKUP",
          "order_type": "SDD"
        }
      ]
    }
  ]
}
```

---

### WEBHOOKS

Drivin puede enviar webhooks a tu endpoint cuando:
1. **Webhook Routes**: Se envia cuando un escenario es aprobado o cuando una ruta comienza.
2. **Webhook Proof of Delivery**: Se envia cuando se registra una prueba de entrega.

Para configurar webhooks, contactar soporte@driv.in con tu endpoint y autenticacion.

Estructura del Webhook de Rutas:
```json
{
  "vehicle": "AZM770",
  "route_id": 3000001,
  "route_code": null,
  "description": "NOMBRE PLAN",
  "deploy_date": "2021-05-31",
  "supplier_code": "supplier_code",
  "scenario_token": "scenario_token",
  "approved_at": "2021-05-30T23:10:44-05:00",
  "started_at": null,
  "fleet_sequence": null,
  "driver": {
    "email": "email@driver.com",
    "full_name": "Nombre Conductor",
    "phone": "999999999"
  },
  "summary": { ... }
}
```

---

## Flujo Tipico de Integracion

1. **Crear Schema** (POST /schemas) - Define parametros de planificacion
2. **Crear Vehiculos** (POST /vehicles) - Registra tu flota
3. **Crear Usuarios/Conductores** (POST /users) - Registra conductores
4. **Crear Escenario con Pedidos** (POST /scenarios) - Crea plan con direcciones y pedidos
5. **Verificar Estado** (GET /scenarios/{token}/status) - Esperar "Ready"
6. **Optimizar** (PUT /scenarios/{token}/optimize) - Optimiza rutas
7. **Verificar Estado** - Esperar "Optimized"
8. **Aprobar** (PUT /scenarios/{token}/approve) - Aprueba rutas
9. **Iniciar Rutas** (PUT /routes/{vehicle}/start) - Conductores inician
10. **Registrar Entregas** (POST /pods) - Pruebas de entrega
11. **Finalizar Rutas** (PUT /routes/{vehicle}/finish) - Fin de ruta

---

## Respuestas de Error Comunes

```json
{
  "success": false,
  "response": {
    "description": "Invalid Schema name"
  }
}
```

```json
{
  "status": "Error",
  "response": {
    "description": "Invalid Token"
  }
}
```

Los errores de validacion incluyen un array `details` con los campos especificos que fallaron.
