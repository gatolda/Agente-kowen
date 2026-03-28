# Agente Kowen

## Negocio
Kowen es una planta purificadora de agua en Santiago, Chile. Servicio principal: delivery de botellones de agua purificada 20L a domicilio.

## Objetivo del proyecto
Construir un agente en Python que orqueste la operacion del negocio:
- Recopilar ventas web desde Bsale (API)
- Subir pedidos masivos a driv.in (desde plantilla Excel)
- Consultar estado de pedidos en driv.in
- Gestionar entregas y cobros

## Integraciones
- **Bsale** (ERP/POS): API REST `https://api.bsale.cl/v1/` — pendiente obtener token
- **driv.in**: gestion de rutas y delivery — pendiente documentar API
- **Google Sheets**: almacenamiento de datos (pedidos, pagos, etc.)
- **Google Drive**: documentos del negocio
- **Gmail**: procesamiento de emails (automatizado via n8n)

## Stack
- Python
- Despliegue: web, terminal CLI, REST API

## Automatizacion existente
Workflow n8n que corre cada hora:
1. Lee emails no leidos de Gmail
2. Clasifica con IA (pagos, pedidos, cotizaciones, sii, servicios, spam)
3. Guarda en Google Sheets por categoria
4. Para pedidos: busca codigo_drivein del cliente antes de registrar

## Idioma
Toda comunicacion en espanol.
