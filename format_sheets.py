"""Formatear la planilla Kowen - Gestor de Pedidos en Google Sheets."""
import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import json

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = os.getenv("GOOGLE_SHEETS_KOWEN_ID", "11cG1jArLtQrfmAqX-Qqsfx3Eqkns3Z80Ff9rCk2WwQU")

def get_creds():
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as f:
            f.write(creds.to_json())
    return creds

def main():
    creds = get_creds()
    service = build("sheets", "v4", credentials=creds)

    # Get sheet IDs
    meta = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    sheet_ids = {}
    for s in meta["sheets"]:
        title = s["properties"]["title"]
        sheet_ids[title] = s["properties"]["sheetId"]

    print(f"Sheets encontradas: {list(sheet_ids.keys())}")

    requests = []

    # --- HEADER FORMATTING (dark blue bg, white text, bold) for all sheets ---
    header_color = {"red": 0.1, "green": 0.2, "blue": 0.45}
    white = {"red": 1, "green": 1, "blue": 1}

    for title, sid in sheet_ids.items():
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sid, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": header_color,
                        "textFormat": {"foregroundColor": white, "bold": True, "fontSize": 10},
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE"
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)"
            }
        })
        # Freeze header row
        requests.append({
            "updateSheetProperties": {
                "properties": {"sheetId": sid, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount"
            }
        })

    # --- COLUMN WIDTHS for OPERACION DIARIA ---
    op_id = sheet_ids.get("OPERACION DIARIA")
    if op_id is not None:
        # 27 columns: #, Fecha, Direccion, Depto, Comuna, Codigo Drivin, Cant, Marca, Documento,
        # Repartidor, Vuelta, Zona, Estado Pedido, Canal, Observaciones, Com. Chofer,
        # Cliente, Telefono, Email, Efectivo, Transferencia, Forma Pago, Estado Pago,
        # Fecha Pago, Aliado, Plan Drivin, Pedido Bsale
        widths = [
            40,   # #
            90,   # Fecha
            200,  # Direccion
            70,   # Depto
            100,  # Comuna
            100,  # Codigo Drivin
            45,   # Cant
            70,   # Marca
            80,   # Documento
            120,  # Repartidor
            55,   # Vuelta
            65,   # Zona
            110,  # Estado Pedido
            65,   # Canal
            150,  # Observaciones
            150,  # Com. Chofer
            140,  # Cliente
            100,  # Telefono
            150,  # Email
            80,   # Efectivo
            90,   # Transferencia
            100,  # Forma Pago
            100,  # Estado Pago
            90,   # Fecha Pago
            90,   # Aliado
            110,  # Plan Drivin
            100,  # Pedido Bsale
        ]
        for i, w in enumerate(widths):
            requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": op_id,
                        "dimension": "COLUMNS",
                        "startIndex": i,
                        "endIndex": i + 1
                    },
                    "properties": {"pixelSize": w},
                    "fields": "pixelSize"
                }
            })

    # --- DROPDOWN VALIDATIONS for OPERACION DIARIA ---
    dropdowns = {
        7:  ["KOWEN", "CACTUS"],                                           # Marca
        8:  ["Boleta", "Factura", "Guia", "Ticket"],                       # Documento
        10: ["1a", "2a", "3a"],                                            # Vuelta
        11: ["Sur", "Oriente"],                                            # Zona
        12: ["PENDIENTE", "EN CAMINO", "ENTREGADO", "NO ENTREGADO"],       # Estado Pedido
        13: ["WEB", "WSP", "EMAIL", "MANUAL"],                             # Canal
        21: ["Efectivo", "Transferencia", "Webpay"],                       # Forma Pago
        22: ["PENDIENTE", "PAGADO", "POR CONFIRMAR"],                      # Estado Pago
    }

    if op_id is not None:
        for col_idx, values in dropdowns.items():
            requests.append({
                "setDataValidation": {
                    "range": {
                        "sheetId": op_id,
                        "startRowIndex": 1,
                        "endRowIndex": 1000,
                        "startColumnIndex": col_idx,
                        "endColumnIndex": col_idx + 1
                    },
                    "rule": {
                        "condition": {
                            "type": "ONE_OF_LIST",
                            "values": [{"userEnteredValue": v} for v in values]
                        },
                        "showCustomUi": True,
                        "strict": False
                    }
                }
            })

    # --- CONDITIONAL FORMATTING for Estado Pedido (col 12) ---
    estado_colors = {
        "PENDIENTE":     {"red": 1, "green": 0.95, "blue": 0.8},      # amarillo claro
        "EN CAMINO":     {"red": 0.8, "green": 0.9, "blue": 1},       # azul claro
        "ENTREGADO":     {"red": 0.8, "green": 1, "blue": 0.8},       # verde claro
        "NO ENTREGADO":  {"red": 1, "green": 0.8, "blue": 0.8},       # rojo claro
    }

    if op_id is not None:
        for estado, color in estado_colors.items():
            requests.append({
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{
                            "sheetId": op_id,
                            "startRowIndex": 1,
                            "endRowIndex": 1000,
                            "startColumnIndex": 12,
                            "endColumnIndex": 13
                        }],
                        "booleanRule": {
                            "condition": {
                                "type": "TEXT_EQ",
                                "values": [{"userEnteredValue": estado}]
                            },
                            "format": {"backgroundColor": color}
                        }
                    },
                    "index": 0
                }
            })

    # --- CONDITIONAL FORMATTING for Estado Pago (col 22) ---
    pago_colors = {
        "PENDIENTE":      {"red": 1, "green": 0.95, "blue": 0.8},
        "PAGADO":         {"red": 0.8, "green": 1, "blue": 0.8},
        "POR CONFIRMAR":  {"red": 1, "green": 0.9, "blue": 0.7},
    }

    if op_id is not None:
        for estado, color in pago_colors.items():
            requests.append({
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{
                            "sheetId": op_id,
                            "startRowIndex": 1,
                            "endRowIndex": 1000,
                            "startColumnIndex": 22,
                            "endColumnIndex": 23
                        }],
                        "booleanRule": {
                            "condition": {
                                "type": "TEXT_EQ",
                                "values": [{"userEnteredValue": estado}]
                            },
                            "format": {"backgroundColor": color}
                        }
                    },
                    "index": 0
                }
            })

    # --- COLUMN WIDTHS for CLIENTES ---
    cl_id = sheet_ids.get("CLIENTES")
    if cl_id is not None:
        cl_widths = [140, 100, 150, 200, 70, 100, 100, 70, 90, 90, 100, 80]
        for i, w in enumerate(cl_widths):
            requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": cl_id,
                        "dimension": "COLUMNS",
                        "startIndex": i,
                        "endIndex": i + 1
                    },
                    "properties": {"pixelSize": w},
                    "fields": "pixelSize"
                }
            })

    # --- COLUMN WIDTHS for PAGOS ---
    pg_id = sheet_ids.get("PAGOS")
    if pg_id is not None:
        pg_widths = [90, 90, 100, 150, 120, 140, 90]
        for i, w in enumerate(pg_widths):
            requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": pg_id,
                        "dimension": "COLUMNS",
                        "startIndex": i,
                        "endIndex": i + 1
                    },
                    "properties": {"pixelSize": w},
                    "fields": "pixelSize"
                }
            })

    # Execute all requests
    print(f"Enviando {len(requests)} cambios de formato...")
    result = service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"requests": requests}
    ).execute()

    print(f"Formato aplicado exitosamente. {result.get('replies', []).__len__()} operaciones completadas.")
    print(f"\nPlanilla: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit")

if __name__ == "__main__":
    main()
