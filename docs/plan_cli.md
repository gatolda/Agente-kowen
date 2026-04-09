# Plan: CLI Interactivo + Matching Automatico

## Archivos nuevos

### 1. `bsale_client.py`
Cliente API de Bsale para obtener pedidos web (document_type=32).
- `get_web_orders(since_number)` - Busca pedidos posteriores a un numero dado
- Extrae: numero, fecha, cliente, telefono, email, direccion, depto, comuna, cantidad, total
- Detecta marca (Kowen/Cactus) segun producto

### 2. `address_matcher.py`
Motor de matching direccion Bsale -> codigo driv.in.

**Algoritmo en 3 etapas:**
1. **Filtro por numero de calle** - Extrae numero de la direccion, filtra candidatos (2746 -> ~5)
2. **Similitud de nombre de calle** - Normaliza (lowercase, sin acentos, sin prefijos av/calle), calcula overlap de tokens. Umbral >= 0.6
3. **Match de departamento** - Si hay multiples candidatos en mismo edificio, compara depto

**Interaccion con usuario:**
- 1 match con score >= 0.6: auto-match
- 2-5 matches: mostrar lista numerada para elegir
- 0 matches: pedir codigo manual o crear nueva direccion

**Cache local:** `direcciones_drivin.csv` (ya existe). Se refresca bajo demanda.

### 3. `cli.py`
Menu interactivo en espanol, sin IA, sin tokens.

```
======================================
  KOWEN - Gestor de Entregas
======================================

1. Consultar pedidos web (Bsale)
2. Crear plan del dia (driv.in)
3. Subir pedidos a un plan
4. Asignar conductor a ruta
5. Ver estado de rutas
6. Actualizar cache de direcciones
0. Salir
```

## Modificaciones a archivos existentes

### `drivin_client.py`
- Agregar `get_all_addresses()` con paginacion
- Agregar `get_results(scenario_token)`

### `requirements.txt`
- Agregar `unidecode>=1.3.0`

## Orden de implementacion
1. `bsale_client.py`
2. `drivin_client.py` (modificaciones)
3. `address_matcher.py`
4. `cli.py`

## Reglas de negocio
- Codigo driv.in: el numero siempre coincide con la numeracion de la direccion
- Marcas: Kowen (principal) y Cactus. Campo description debe indicar la marca
- Retiros: description "Kowen - Retiro", units_1 = 0
- Cache CSV sobre SQLite (2746 filas, simple y editable en Excel)
