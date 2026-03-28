"""
Agente Kowen - Clasificador de correos con Claude.
Usa tool_use de Anthropic para interactuar con Gmail.
"""

import json
import anthropic
from gmail_client import (
    get_gmail_service,
    get_unread_emails,
    add_label,
    mark_as_read,
)
from sheets_client import save_to_sheets

MODEL = "claude-sonnet-4-6"

SYSTEM_PROMPT = """Eres el agente de Kowen, una planta purificadora de agua en Santiago, Chile.
Tu trabajo es procesar correos de Gmail: leerlos, clasificarlos y etiquetarlos.

CATEGORIAS (usa exactamente estos nombres en minusculas):
- pagos: comprobantes de transferencia, depositos, confirmaciones de pago
- pedidos: confirmaciones de orden de pedido del ecommerce (Bsale, Webpay), solicitudes de botellones
- cotizaciones: solicitudes de precio, presupuestos
- sii: documentos del Servicio de Impuestos Internos (facturas electronicas, boletas, DTEs)
- servicios: cuentas de servicios basicos, proveedores, notificaciones de plataformas
- spam: publicidad, newsletters, correos no deseados

INSTRUCCIONES:
1. Usa la herramienta 'get_unread_emails' para obtener los correos no leidos
2. Para CADA correo, analiza el contenido y clasifícalo
3. Usa 'save_to_sheets' para guardar los datos extraidos en Google Sheets
4. Usa 'add_label' para etiquetar cada correo con su categoria
5. Usa 'mark_as_read' para marcar cada correo como leido
6. Al final, da un resumen de cuantos correos procesaste y en que categorias

DATOS A EXTRAER segun categoria:
- pagos: nombre, banco, monto, fecha_transferencia, numero_operacion
- pedidos: cliente, telefono, direccion, productos, total, origen (Webpay Plus, MercadoPago, etc.)
- otros: solo los campos basicos (fecha, remitente, asunto, resumen)

IMPORTANTE sobre pedidos de Kowen:
- Los pedidos del ecommerce tienen asunto tipo "Confirmacion Orden de Pedido No XXXX"
- Contienen datos de contacto, direccion de despacho, forma de pago (Webpay Plus, MercadoPago)
- Detalles de items como "Recarga de Botellon de Agua Purificada 20L"
- Total de compra al final
"""

# Definicion de herramientas para Claude
TOOLS = [
    {
        "name": "get_unread_emails",
        "description": "Obtiene los correos no leidos de Gmail que no han sido procesados por Kowen. Retorna una lista con id, threadId, fecha, hora, remitente, asunto y cuerpo de cada correo.",
        "input_schema": {
            "type": "object",
            "properties": {
                "max_results": {
                    "type": "integer",
                    "description": "Numero maximo de correos a obtener. Por defecto 20.",
                    "default": 20,
                }
            },
            "required": [],
        },
    },
    {
        "name": "add_label",
        "description": "Agrega una etiqueta de categoria a un correo de Gmail. Las categorias validas son: pagos, pedidos, cotizaciones, sii, servicios, spam.",
        "input_schema": {
            "type": "object",
            "properties": {
                "message_id": {
                    "type": "string",
                    "description": "ID del mensaje de Gmail",
                },
                "category": {
                    "type": "string",
                    "enum": [
                        "pagos",
                        "pedidos",
                        "cotizaciones",
                        "sii",
                        "servicios",
                        "spam",
                    ],
                    "description": "Categoria para etiquetar el correo",
                },
            },
            "required": ["message_id", "category"],
        },
    },
    {
        "name": "mark_as_read",
        "description": "Marca un correo de Gmail como leido.",
        "input_schema": {
            "type": "object",
            "properties": {
                "message_id": {
                    "type": "string",
                    "description": "ID del mensaje de Gmail",
                },
            },
            "required": ["message_id"],
        },
    },
    {
        "name": "save_to_sheets",
        "description": "Guarda los datos extraidos de un correo en Google Sheets, en la hoja correspondiente a su categoria. Debes extraer los campos relevantes del correo antes de llamar esta herramienta.",
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "enum": [
                        "pagos",
                        "pedidos",
                        "cotizaciones",
                        "sii",
                        "servicios",
                        "spam",
                    ],
                    "description": "Categoria del correo",
                },
                "data": {
                    "type": "object",
                    "description": "Datos extraidos del correo. Campos comunes: fecha_correo, hora_correo, remitente, asunto, resumen, gmailMessageId, threadId. Para pagos: nombre, banco, monto, fecha_transferencia, numero_operacion. Para pedidos: cliente, telefono, direccion, productos, total, origen.",
                    "properties": {
                        "fecha_correo": {"type": "string"},
                        "hora_correo": {"type": "string"},
                        "remitente": {"type": "string"},
                        "asunto": {"type": "string"},
                        "resumen": {"type": "string"},
                        "gmailMessageId": {"type": "string"},
                        "threadId": {"type": "string"},
                        "nombre": {"type": "string"},
                        "banco": {"type": "string"},
                        "monto": {"type": "string"},
                        "fecha_transferencia": {"type": "string"},
                        "numero_operacion": {"type": "string"},
                        "texto_clave": {"type": "string"},
                        "cliente": {"type": "string"},
                        "telefono": {"type": "string"},
                        "direccion": {"type": "string"},
                        "productos": {"type": "string"},
                        "total": {"type": "string"},
                        "origen": {"type": "string"},
                    },
                },
            },
            "required": ["category", "data"],
        },
    },
]


def execute_tool(service, tool_name, tool_input):
    """Ejecuta una herramienta y retorna el resultado."""
    if tool_name == "get_unread_emails":
        max_results = tool_input.get("max_results", 20)
        emails = get_unread_emails(service, max_results=max_results)
        if not emails:
            return "No hay correos nuevos sin procesar."
        return json.dumps(emails, ensure_ascii=False, indent=2)

    elif tool_name == "add_label":
        return add_label(service, tool_input["message_id"], tool_input["category"])

    elif tool_name == "mark_as_read":
        return mark_as_read(service, tool_input["message_id"])

    elif tool_name == "save_to_sheets":
        return save_to_sheets(tool_input["category"], tool_input["data"])

    return f"Herramienta '{tool_name}' no reconocida."


def run_agent(max_emails=20):
    """Ejecuta el agente de clasificacion de correos."""
    print("Conectando a Gmail...")
    service = get_gmail_service()

    print("Iniciando agente Claude...")
    client = anthropic.Anthropic()

    messages = [
        {
            "role": "user",
            "content": f"Procesa los correos no leidos (maximo {max_emails}). Clasifica y etiqueta cada uno.",
        }
    ]

    # Agentic loop
    while True:
        response = client.messages.create(
            model=MODEL,
            max_tokens=16000,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=messages,
        )

        # Si Claude termino (no mas tool calls)
        if response.stop_reason == "end_turn":
            for block in response.content:
                if block.type == "text":
                    print("\n" + block.text)
            break

        # Procesar bloques de respuesta
        tool_use_blocks = [b for b in response.content if b.type == "tool_use"]

        # Mostrar texto si hay
        for block in response.content:
            if block.type == "text" and block.text.strip():
                print(block.text)

        # Agregar respuesta del asistente al historial
        messages.append({"role": "assistant", "content": response.content})

        # Ejecutar herramientas y recopilar resultados
        tool_results = []
        for tool_block in tool_use_blocks:
            print(f"  -> Ejecutando: {tool_block.name}({json.dumps(tool_block.input, ensure_ascii=False)[:100]})")

            try:
                result = execute_tool(service, tool_block.name, tool_block.input)
            except Exception as e:
                result = f"Error: {e}"

            tool_results.append(
                {
                    "type": "tool_result",
                    "tool_use_id": tool_block.id,
                    "content": str(result),
                }
            )

        # Enviar resultados de herramientas
        messages.append({"role": "user", "content": tool_results})

    print("\nAgente finalizado.")
