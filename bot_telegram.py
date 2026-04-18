"""
Bot de Telegram para Kowen.
Permite consultar pedidos, estado de rutas, ejecutar rutina y recibir notificaciones.

Configuracion:
1. Crear bot con @BotFather en Telegram
2. Agregar TELEGRAM_BOT_TOKEN al .env
3. Ejecutar: python bot_telegram.py
"""

import os
import logging
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

import sheets_client
import operations
import log_client
import bsale_client
import drivin_client
import address_matcher

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# --- Comandos ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Menu principal."""
    keyboard = [
        [InlineKeyboardButton("Pedidos de hoy", callback_data="hoy")],
        [InlineKeyboardButton("Pedidos nuevos (Bsale)", callback_data="pedidos")],
        [InlineKeyboardButton("Estado de rutas", callback_data="rutas")],
        [InlineKeyboardButton("Resumen del dia", callback_data="resumen")],
        [InlineKeyboardButton("Ejecutar rutina", callback_data="rutina")],
        [InlineKeyboardButton("Verificar driv.in", callback_data="verificar")],
        [InlineKeyboardButton("Procesar correos / pagos", callback_data="correos")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "*KOWEN - Gestor de Entregas*\n"
        f"Fecha: {datetime.now().strftime('%d/%m/%Y')}\n\n"
        "Selecciona una opcion:",
        reply_markup=reply_markup,
        parse_mode="Markdown",
    )


async def hoy_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra pedidos de hoy desde la planilla."""
    fecha = datetime.now().strftime("%d/%m/%Y")
    if context.args:
        fecha = context.args[0]

    msg = await update.message.reply_text(f"Cargando pedidos del {fecha}...")

    try:
        pedidos = sheets_client.get_pedidos(fecha)

        if not pedidos:
            await msg.edit_text(f"No hay pedidos para {fecha}.")
            return

        pendientes = sum(1 for p in pedidos if p.get("Estado Pedido") == "PENDIENTE")
        en_camino = sum(1 for p in pedidos if p.get("Estado Pedido") == "EN CAMINO")
        entregados = sum(1 for p in pedidos if p.get("Estado Pedido") == "ENTREGADO")
        total_bot = sum(int(p.get("Cant", 0) or 0) for p in pedidos)

        text = (
            f"*Pedidos {fecha}*\n"
            f"Total: {len(pedidos)} | {total_bot} botellones\n"
            f"Pendientes: {pendientes} | En camino: {en_camino} | Entregados: {entregados}\n\n"
        )

        for p in pedidos[:20]:  # Max 20 para no exceder limite de Telegram
            nro = p.get("#", "")
            dir_short = p.get("Direccion", "")[:25]
            cant = p.get("Cant", "")
            estado = p.get("Estado Pedido", "")
            codigo = p.get("Codigo Drivin", "")

            icon = ""
            if estado == "ENTREGADO":
                icon = "[OK]"
            elif estado == "EN CAMINO":
                icon = "[>>]"
            elif estado == "NO ENTREGADO":
                icon = "[X]"

            code_flag = "" if codigo else " *SIN CODIGO*"
            text += f"#{nro} {dir_short} | {cant} bot {icon}{code_flag}\n"

        if len(pedidos) > 20:
            text += f"\n... y {len(pedidos) - 20} mas"

        await msg.edit_text(text, parse_mode="Markdown")

    except Exception as e:
        await msg.edit_text(f"Error: {e}")


async def pedidos_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Consulta pedidos nuevos en Bsale."""
    msg = await update.message.reply_text("Buscando pedidos nuevos en Bsale...")

    try:
        # Obtener ultimo numero Bsale del sistema
        all_pedidos = sheets_client.get_pedidos()
        bsale_nums = [int(p.get("Pedido Bsale", "0") or "0") for p in all_pedidos]
        since = max(bsale_nums) if bsale_nums else 3483

        if context.args:
            try:
                since = int(context.args[0])
            except ValueError:
                pass

        orders = bsale_client.get_web_orders(since)
        activos = [o for o in orders if o["estado"] == "activo"]

        if not activos:
            await msg.edit_text("No hay pedidos nuevos en Bsale.")
            return

        # Verificar duplicados
        checked = operations.check_bsale_orders(activos)
        nuevos = [o for o in checked if not o["existe"]]

        text = f"*{len(activos)} pedidos en Bsale* ({len(nuevos)} nuevos)\n\n"
        for o in checked[:15]:
            dir_short = o["direccion"][:25]
            flag = "[EXISTE]" if o["existe"] else "[NUEVO]"
            text += (
                f"*#{o['pedido_nro']}* {flag}\n"
                f"  {dir_short} | {int(o['cantidad'])} bot | {o['marca']}\n\n"
            )

        if nuevos:
            text += f"\nUsa /importar para agregar los {len(nuevos)} nuevos a la planilla."

        await msg.edit_text(text, parse_mode="Markdown")

    except Exception as e:
        await msg.edit_text(f"Error: {e}")


async def importar_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Importa pedidos nuevos de Bsale a la planilla."""
    msg = await update.message.reply_text("Importando pedidos de Bsale...")

    try:
        all_pedidos = sheets_client.get_pedidos()
        bsale_nums = [int(p.get("Pedido Bsale", "0") or "0") for p in all_pedidos]
        since = max(bsale_nums) if bsale_nums else 3483

        orders = bsale_client.get_web_orders(since)
        activos = [o for o in orders if o["estado"] == "activo"]

        if not activos:
            await msg.edit_text("No hay pedidos nuevos para importar.")
            return

        count = operations.sync_from_bsale(activos)
        await msg.edit_text(f"*{count} pedidos importados* a la planilla.")

    except Exception as e:
        await msg.edit_text(f"Error: {e}")


async def rutas_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Consulta estado de rutas del dia."""
    date = datetime.now().strftime("%Y-%m-%d")
    if context.args:
        date = context.args[0]

    msg = await update.message.reply_text(f"Consultando rutas del {date}...")

    try:
        result = drivin_client.get_routes(date=date)
        routes = result.get("response", [])

        if not routes:
            await msg.edit_text("No hay rutas para hoy.")
            return

        text = f"*Rutas del {date}:*\n\n"
        for route in routes:
            vehicle = route.get("vehicle_code", "")
            driver = route.get("driver_name", route.get("driver", {}).get("name", "N/A"))
            total = route.get("total_orders", 0)

            if route.get("is_finished"):
                estado = "[OK] Finalizada"
            elif route.get("is_started"):
                estado = "[>>] En curso"
            elif route.get("is_approved"):
                estado = "[+] Aprobada"
            else:
                estado = "[...] Pendiente"

            text += f"*{vehicle}* - {driver}\n  {total} pedidos | {estado}\n\n"

        await msg.edit_text(text, parse_mode="Markdown")

    except Exception as e:
        await msg.edit_text(f"Error: {e}")


async def resumen_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Resumen del dia."""
    fecha = datetime.now().strftime("%d/%m/%Y")
    if context.args:
        fecha = context.args[0]

    msg = await update.message.reply_text(f"Generando resumen del {fecha}...")

    try:
        r = operations.resumen_dia(fecha)

        text = (
            f"*Resumen {r['fecha']}*\n\n"
            f"Pedidos: {r['total_pedidos']}\n"
            f"Botellones: {r['total_botellones']}\n\n"
            f"Entregados: {r['entregados']}\n"
            f"Pendientes: {r['pendientes']}\n"
            f"En camino: {r['en_camino']}\n"
            f"No entregados: {r['no_entregados']}\n\n"
            f"Pagados: {r['pagados']}\n"
            f"Por cobrar: {r['por_cobrar']}"
        )

        await msg.edit_text(text, parse_mode="Markdown")

    except Exception as e:
        await msg.edit_text(f"Error: {e}")


async def rutina_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ejecuta la rutina diaria completa."""
    msg = await update.message.reply_text(
        "Ejecutando rutina diaria...\n"
        "(Esto puede tardar unos segundos)"
    )

    try:
        fecha = datetime.now().strftime("%d/%m/%Y")
        resultado = operations.rutina_diaria(fecha_hoy=fecha)

        errores_text = ""
        if resultado["errores"]:
            errores_text = "\n*Advertencias:*\n" + "\n".join(f"- {e}" for e in resultado["errores"])

        bsale_pend = resultado.get("bsale_pendientes", [])
        bsale_alerta = ""
        if bsale_pend:
            bsale_alerta = f"\n\n*Bsale sin planilla: {len(bsale_pend)}*\n"
            bsale_alerta += "\n".join(
                f"  #{p.get('pedido_nro', '')} {p.get('direccion', '')[:30]} ({p.get('cantidad', 0)})"
                for p in bsale_pend[:8]
            )
            if len(bsale_pend) > 8:
                bsale_alerta += f"\n  ...y {len(bsale_pend) - 8} mas"

        text = (
            f"*Rutina completada*\n\n"
            f"*Ayer ({resultado['fecha_ayer']}):*\n"
            f"  Entregados: {resultado['entregados_ayer']}\n"
            f"  Movidos a hoy: {resultado['movidos_a_hoy']}\n"
            f"  Duplicados eliminados: {resultado['duplicados_eliminados']}\n\n"
            f"*Hoy ({resultado['fecha_hoy']}):*\n"
            f"  Planilla: +{resultado['planilla_importados']}\n"
            f"  Cactus: +{resultado.get('cactus_importados', 0)}\n"
            f"  Codigos: {resultado['codigos_asignados']}\n"
            f"  Driv.in: {resultado.get('drivin_subidos', 0)} subidos\n"
            f"  Plan: {resultado.get('drivin_plan', 'N/A')}"
            f"{bsale_alerta}"
            f"{errores_text}"
        )

        await msg.edit_text(text, parse_mode="Markdown")

    except Exception as e:
        await msg.edit_text(f"Error en rutina: {e}")


async def verificar_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Verifica estado de pedidos contra driv.in."""
    msg = await update.message.reply_text("Verificando pedidos contra driv.in...")

    try:
        fecha = datetime.now().strftime("%d/%m/%Y")
        v = operations.verify_orders_drivin(fecha=fecha, auto_update=True)

        text = f"*Verificacion driv.in*\n\n"
        text += f"Verificados: {v['total_verificados']}\n"
        text += f"Actualizados: {v['actualizados']}\n"

        if v["entregados_detectados"]:
            text += f"Entregados: +{v['entregados_detectados']}\n"
        if v["no_entregados_detectados"]:
            text += f"No entregados: {v['no_entregados_detectados']}\n"

        if v["planes_sin_despachar"]:
            text += f"\n*Planes sin despachar:*\n"
            for p in v["planes_sin_despachar"]:
                text += f"  - {p['plan']} ({p['status']})\n"

        if v["estancados"]:
            text += f"\n*{len(v['estancados'])} pedidos estancados:*\n"
            for e in v["estancados"][:10]:
                text += f"  #{e['numero']} {e['direccion']} ({e['dias']}d)\n"

        if v["detalle"]:
            text += f"\n*Cambios aplicados:*\n"
            for d in v["detalle"][:10]:
                text += f"  #{d['numero']}: {d['estado_anterior']} -> {d['estado_nuevo']}\n"
        elif v["total_verificados"] > 0 and not v["actualizados"]:
            text += "\nTodo al dia, sin cambios."

        await msg.edit_text(text, parse_mode="Markdown")

    except Exception as e:
        await msg.edit_text(f"Error: {e}")


async def correos_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Procesa emails no leidos y concilia pagos."""
    import payments
    msg = await update.message.reply_text("Leyendo correos no leidos...")

    try:
        r = payments.procesar_emails_no_leidos(max_emails=30, marcar_leidos=False)

        if r["total"] == 0:
            await msg.edit_text("No hay correos no leidos.")
            return

        cats = r.get("por_categoria", {})
        conciliados = r.get("pagos_conciliados", [])
        sugeridos = r.get("pagos_sugeridos", [])
        sin_match = r.get("pagos_sin_match", [])
        alertas = r.get("alertas", [])

        text = f"*Correos procesados: {r['total']}*\n"
        if cats:
            text += "\n" + ", ".join(f"{k}:{v}" for k, v in cats.items()) + "\n"

        if conciliados:
            text += f"\n*Pagos conciliados auto ({len(conciliados)}):*\n"
            for p in conciliados[:8]:
                text += (f"  #{p['pedido']} {p['cliente'][:20]} "
                         f"${p['monto']} (score {p['score']})\n")

        if sugeridos:
            text += f"\n*Pagos para revisar ({len(sugeridos)}):*\n"
            for p in sugeridos[:5]:
                pago = p["pago"]
                text += (f"  {pago.get('remitente_nombre', '')[:20]} "
                         f"${pago.get('monto', '')} -> "
                         f"{len(p['candidatos'])} candidatos\n")

        if sin_match:
            text += f"\n*Pagos sin match ({len(sin_match)}):*\n"
            for p in sin_match[:5]:
                pago = p["pago"]
                text += (f"  {pago.get('remitente_nombre', '')[:20]} "
                         f"${pago.get('monto', '')}\n")

        if alertas:
            text += f"\n*Alertas ({len(alertas)}):*\n"
            for a in alertas[:5]:
                text += f"  [{a['categoria']}] {a['subject'][:35]}\n"

        await msg.edit_text(text, parse_mode="Markdown")

    except Exception as e:
        await msg.edit_text(f"Error: {e}")


async def planes_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra planes del dia."""
    date = datetime.now().strftime("%Y-%m-%d")

    msg = await update.message.reply_text(f"Consultando planes del {date}...")

    try:
        result = drivin_client.get_scenarios_by_date(date)
        scenarios = result.get("response", [])

        if not scenarios:
            await msg.edit_text("No hay planes para hoy.")
            return

        text = f"*Planes del {date}:*\n\n"
        for s in scenarios:
            text += (
                f"*{s.get('description', 'Sin nombre')}*\n"
                f"  Token: `{s.get('token', '')}`\n"
                f"  Pedidos: {s.get('orders_count', 0)}\n\n"
            )

        await msg.edit_text(text, parse_mode="Markdown")

    except Exception as e:
        await msg.edit_text(f"Error: {e}")


# --- Callback para botones ---

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja los botones del menu."""
    query = update.callback_query
    await query.answer()

    if query.data == "hoy":
        fecha = datetime.now().strftime("%d/%m/%Y")
        msg = await query.edit_message_text(f"Cargando pedidos del {fecha}...")
        try:
            pedidos = sheets_client.get_pedidos(fecha)
            if not pedidos:
                await msg.edit_text(f"No hay pedidos para {fecha}.")
                return

            pendientes = sum(1 for p in pedidos if p.get("Estado Pedido") == "PENDIENTE")
            entregados = sum(1 for p in pedidos if p.get("Estado Pedido") == "ENTREGADO")
            total_bot = sum(int(p.get("Cant", 0) or 0) for p in pedidos)

            text = (
                f"*Pedidos {fecha}*\n"
                f"Total: {len(pedidos)} | {total_bot} bot.\n"
                f"Pendientes: {pendientes} | Entregados: {entregados}\n\n"
            )
            for p in pedidos[:15]:
                nro = p.get("#", "")
                dir_short = p.get("Direccion", "")[:25]
                cant = p.get("Cant", "")
                estado = p.get("Estado Pedido", "")[:3]
                text += f"#{nro} {dir_short} | {cant} | {estado}\n"

            if len(pedidos) > 15:
                text += f"\n... y {len(pedidos) - 15} mas"

            await msg.edit_text(text, parse_mode="Markdown")
        except Exception as e:
            await msg.edit_text(f"Error: {e}")

    elif query.data == "pedidos":
        msg = await query.edit_message_text("Buscando pedidos en Bsale...")
        try:
            all_p = sheets_client.get_pedidos()
            bsale_nums = [int(p.get("Pedido Bsale", "0") or "0") for p in all_p]
            since = max(bsale_nums) if bsale_nums else 3483

            orders = bsale_client.get_web_orders(since)
            activos = [o for o in orders if o["estado"] == "activo"]

            if not activos:
                await msg.edit_text("No hay pedidos nuevos.")
                return

            checked = operations.check_bsale_orders(activos)
            nuevos = [o for o in checked if not o["existe"]]

            text = f"*{len(activos)} pedidos* ({len(nuevos)} nuevos)\n\n"
            for o in checked[:10]:
                flag = "[EXISTE]" if o["existe"] else "[NUEVO]"
                text += f"#{o['pedido_nro']} {o['direccion'][:25]} {flag}\n"

            if nuevos:
                text += f"\nUsa /importar para agregar los nuevos."

            await msg.edit_text(text, parse_mode="Markdown")
        except Exception as e:
            await msg.edit_text(f"Error: {e}")

    elif query.data == "rutas":
        date = datetime.now().strftime("%Y-%m-%d")
        msg = await query.edit_message_text("Consultando rutas...")
        try:
            result = drivin_client.get_routes(date=date)
            routes = result.get("response", [])

            if not routes:
                await msg.edit_text("No hay rutas para hoy.")
                return

            text = "*Rutas de hoy:*\n\n"
            for route in routes:
                vehicle = route.get("vehicle_code", "")
                total = route.get("total_orders", 0)
                if route.get("is_finished"):
                    estado = "[OK]"
                elif route.get("is_started"):
                    estado = "[>>]"
                else:
                    estado = "[...]"
                text += f"{estado} *{vehicle}* - {total} pedidos\n"

            await msg.edit_text(text, parse_mode="Markdown")
        except Exception as e:
            await msg.edit_text(f"Error: {e}")

    elif query.data == "resumen":
        fecha = datetime.now().strftime("%d/%m/%Y")
        msg = await query.edit_message_text("Generando resumen...")
        try:
            r = operations.resumen_dia(fecha)
            text = (
                f"*Resumen {r['fecha']}*\n\n"
                f"Pedidos: {r['total_pedidos']} ({r['total_botellones']} bot.)\n"
                f"Entregados: {r['entregados']} | Pendientes: {r['pendientes']}\n"
                f"Pagados: {r['pagados']} | Por cobrar: {r['por_cobrar']}"
            )
            await msg.edit_text(text, parse_mode="Markdown")
        except Exception as e:
            await msg.edit_text(f"Error: {e}")

    elif query.data == "rutina":
        msg = await query.edit_message_text("Ejecutando rutina diaria...")
        try:
            fecha = datetime.now().strftime("%d/%m/%Y")
            resultado = operations.rutina_diaria(fecha_hoy=fecha)
            bsale_pend = resultado.get("bsale_pendientes", [])
            text = (
                f"*Rutina completada*\n\n"
                f"Ayer: {resultado['entregados_ayer']} entregados, "
                f"{resultado['movidos_a_hoy']} movidos\n"
                f"Hoy: +{resultado['planilla_importados']} planilla, "
                f"+{resultado.get('cactus_importados', 0)} Cactus\n"
                f"Codigos: {resultado['codigos_asignados']} | "
                f"Driv.in: {resultado.get('drivin_subidos', 0)}"
            )
            if bsale_pend:
                text += f"\nBsale sin planilla: {len(bsale_pend)}"
            if resultado["errores"]:
                text += "\n\n*Errores:*\n" + "\n".join(f"- {e}" for e in resultado["errores"])
            await msg.edit_text(text, parse_mode="Markdown")
        except Exception as e:
            await msg.edit_text(f"Error: {e}")

    elif query.data == "correos":
        import payments
        msg = await query.edit_message_text("Leyendo correos no leidos...")
        try:
            r = payments.procesar_emails_no_leidos(max_emails=30, marcar_leidos=False)
            if r["total"] == 0:
                await msg.edit_text("No hay correos no leidos.")
                return
            cats = r.get("por_categoria", {})
            conciliados = r.get("pagos_conciliados", [])
            sugeridos = r.get("pagos_sugeridos", [])
            sin_match = r.get("pagos_sin_match", [])
            alertas = r.get("alertas", [])
            text = f"*Correos: {r['total']}*\n"
            if cats:
                text += ", ".join(f"{k}:{v}" for k, v in cats.items()) + "\n"
            if conciliados:
                text += f"\nConciliados auto: {len(conciliados)}\n"
                for p in conciliados[:5]:
                    text += f"  #{p['pedido']} {p['cliente'][:20]} ${p['monto']}\n"
            if sugeridos:
                text += f"\nPara revisar: {len(sugeridos)}\n"
            if sin_match:
                text += f"\nSin match: {len(sin_match)}\n"
            if alertas:
                text += f"\nAlertas: {len(alertas)}\n"
                for a in alertas[:3]:
                    text += f"  [{a['categoria']}] {a['subject'][:30]}\n"
            await msg.edit_text(text, parse_mode="Markdown")
        except Exception as e:
            await msg.edit_text(f"Error: {e}")

    elif query.data == "verificar":
        msg = await query.edit_message_text("Verificando contra driv.in...")
        try:
            fecha = datetime.now().strftime("%d/%m/%Y")
            v = operations.verify_orders_drivin(fecha=fecha, auto_update=True)
            text = f"*Verificacion driv.in*\n\nVerificados: {v['total_verificados']}\nActualizados: {v['actualizados']}"
            if v["estancados"]:
                text += f"\n\n*{len(v['estancados'])} estancados:*"
                for e in v["estancados"][:5]:
                    text += f"\n  #{e['numero']} {e['direccion']} ({e['dias']}d)"
            if v["planes_sin_despachar"]:
                text += f"\n\n*Planes sin despachar:*"
                for p in v["planes_sin_despachar"]:
                    text += f"\n  - {p['plan']}"
            if not v["actualizados"] and v["total_verificados"] > 0:
                text += "\n\nTodo al dia."
            await msg.edit_text(text, parse_mode="Markdown")
        except Exception as e:
            await msg.edit_text(f"Error: {e}")


# --- Main ---

def main():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        print("Error: TELEGRAM_BOT_TOKEN no configurado en .env")
        print("1. Crea un bot con @BotFather en Telegram")
        print("2. Agrega TELEGRAM_BOT_TOKEN=tu_token al archivo .env")
        return

    app = Application.builder().token(token).build()

    # Comandos
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("hoy", hoy_cmd))
    app.add_handler(CommandHandler("pedidos", pedidos_cmd))
    app.add_handler(CommandHandler("importar", importar_cmd))
    app.add_handler(CommandHandler("rutas", rutas_cmd))
    app.add_handler(CommandHandler("resumen", resumen_cmd))
    app.add_handler(CommandHandler("rutina", rutina_cmd))
    app.add_handler(CommandHandler("planes", planes_cmd))
    app.add_handler(CommandHandler("verificar", verificar_cmd))
    app.add_handler(CommandHandler("correos", correos_cmd))

    # Botones
    app.add_handler(CallbackQueryHandler(button_callback))

    print("Bot de Telegram iniciado. Ctrl+C para detener.")
    print("Comandos: /start /hoy /pedidos /importar /rutas /resumen /rutina /planes /verificar /correos")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
