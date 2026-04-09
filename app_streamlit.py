"""
Dashboard web de Kowen con Streamlit.
Conectado a Google Sheets (Pedidos 2026).
Ejecutar: streamlit run app_streamlit.py
"""

import os
import streamlit as st
from datetime import datetime, timedelta

from dotenv import load_dotenv
load_dotenv()

import sheets_client
import bsale_client
import drivin_client
import address_matcher


# --- Configuracion ---

st.set_page_config(
    page_title="Kowen - Gestor de Pedidos",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- CSS moderno ---
st.markdown("""
<style>
    /* General */
    .block-container { padding: 1rem 2rem 2rem 2rem; }
    [data-testid="stSidebar"] { background: linear-gradient(180deg, #0f1b2d 0%, #1a2d4a 100%); }
    [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3,
    [data-testid="stSidebar"] p, [data-testid="stSidebar"] span, [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] .stMarkdown, [data-testid="stSidebar"] summary,
    [data-testid="stSidebar"] .sidebar-logo, [data-testid="stSidebar"] .sidebar-date,
    [data-testid="stSidebar"] small, [data-testid="stSidebar"] caption { color: #e0e6ed !important; }
    [data-testid="stSidebar"] .stSelectbox label,
    [data-testid="stSidebar"] .stTextInput label,
    [data-testid="stSidebar"] .stNumberInput label { font-size: 12px !important; opacity: 0.85; }
    [data-testid="stSidebar"] input, [data-testid="stSidebar"] select, [data-testid="stSidebar"] textarea {
        background: #1e3350 !important;
        border: 1px solid rgba(255,255,255,0.2) !important;
        color: white !important;
    }
    [data-testid="stSidebar"] input::placeholder { color: rgba(255,255,255,0.4) !important; }
    [data-testid="stSidebar"] [data-baseweb="select"] > div { background: #1e3350 !important; color: white !important; }
    [data-testid="stSidebar"] [data-baseweb="tab"] { color: #e0e6ed !important; }
    [data-testid="stSidebar"] .stAlert p, [data-testid="stSidebar"] .stAlert span { color: inherit !important; }
    [data-testid="stSidebar"] .stMarkdown hr { border-color: rgba(255,255,255,0.1); }

    /* Metricas */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #f8fafc 0%, #eef2f7 100%);
        border: none;
        border-radius: 12px;
        padding: 14px 18px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    }
    div[data-testid="stMetric"] label { font-size: 11px !important; text-transform: uppercase; letter-spacing: 0.5px; color: #7b8794 !important; }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] { font-size: 28px !important; font-weight: 700; color: #1a2d4a !important; }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] { gap: 4px; border-bottom: 2px solid #eef2f7; }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px 8px 0 0;
        padding: 8px 20px;
        font-weight: 500;
        font-size: 13px;
    }
    .stTabs [aria-selected="true"] { background: #1a2d4a !important; color: white !important; }

    /* Dataframe */
    [data-testid="stDataFrame"] { border-radius: 10px; overflow: hidden; box-shadow: 0 1px 4px rgba(0,0,0,0.06); }

    /* Botones */
    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #1a5276 0%, #2980b9 100%) !important;
        border: none !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
        letter-spacing: 0.3px;
    }
    .stButton > button[kind="secondary"] {
        border-radius: 8px !important;
        font-weight: 500 !important;
    }

    /* Cards */
    .card {
        background: white;
        border-radius: 12px;
        padding: 20px;
        box-shadow: 0 1px 4px rgba(0,0,0,0.06);
        border: 1px solid #eef2f7;
        margin-bottom: 12px;
    }
    .card-header {
        font-size: 14px;
        font-weight: 600;
        color: #1a2d4a;
        margin-bottom: 12px;
        padding-bottom: 8px;
        border-bottom: 2px solid #eef2f7;
    }

    /* Eliminar boton rojo */
    .delete-btn > button {
        background: #e74c3c !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
    }
    .delete-btn > button:hover { background: #c0392b !important; }

    /* Chat */
    .chat-container {
        background: #f8fafc;
        border-radius: 12px;
        border: 1px solid #eef2f7;
        padding: 0;
        overflow: hidden;
    }
    .chat-header {
        background: linear-gradient(135deg, #1a2d4a 0%, #2c3e50 100%);
        color: white;
        padding: 12px 16px;
        font-weight: 600;
        font-size: 14px;
    }

    /* Section titles */
    .section-title {
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 1px;
        color: #7b8794;
        font-weight: 600;
        margin: 20px 0 10px 0;
    }

    /* Sidebar logo */
    .sidebar-logo {
        font-size: 26px;
        font-weight: 800;
        letter-spacing: 3px;
        color: white !important;
        text-align: center;
        padding: 20px 0 4px 0;
    }
    .sidebar-date {
        text-align: center;
        font-size: 12px;
        opacity: 0.7;
        margin-bottom: 10px;
    }
</style>
""", unsafe_allow_html=True)


# --- Estado de sesion ---

if "addresses_cache" not in st.session_state:
    st.session_state.addresses_cache = address_matcher.load_cache()
if "scenario_token" not in st.session_state:
    st.session_state.scenario_token = None
if "scenario_name" not in st.session_state:
    st.session_state.scenario_name = None
if "chat_messages" not in st.session_state:
    st.session_state.chat_messages = []


# ============================================================
# SIDEBAR
# ============================================================

with st.sidebar:
    st.markdown('<div class="sidebar-logo">KOWEN</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="sidebar-date">{datetime.now().strftime("%A %d / %B / %Y")}</div>', unsafe_allow_html=True)

    # --- Agregar pedido manual ---
    st.markdown("---")
    with st.expander("➕  Agregar pedido", expanded=False):
        addr_cache = st.session_state.addresses_cache

        new_dir_search = st.text_input("Buscar direccion", placeholder="Escriba para buscar...", key="addr_search")

        selected_addr_code = ""
        auto_dir = new_dir_search
        auto_comuna = ""

        if new_dir_search and len(new_dir_search) >= 3 and addr_cache:
            from unidecode import unidecode
            search_norm = unidecode(new_dir_search).lower()

            def _get(a, k):
                return a.get(k, "") if isinstance(a, dict) else getattr(a, k, "")

            matches = []
            for a in addr_cache:
                text = unidecode(f"{_get(a, 'name')} {_get(a, 'address1')}").lower()
                if search_norm in text:
                    matches.append(a)
                if len(matches) >= 10:
                    break

            if matches:
                labels = [f"{_get(a, 'address1')} - {_get(a, 'name')} [{_get(a, 'code')}]" for a in matches]
                sel = st.selectbox("Coincidencias", labels, key="addr_match_sel")
                idx = labels.index(sel)
                selected_addr_code = _get(matches[idx], "code")
                auto_dir = _get(matches[idx], "address1")
                auto_comuna = _get(matches[idx], "city")
            else:
                st.caption("Sin coincidencias — direccion nueva")

        with st.form("form_nuevo_pedido", clear_on_submit=True):
            new_fecha = st.date_input("Fecha del pedido", value=datetime.now().date(), key="new_fecha")
            new_dir = st.text_input("Direccion", value=auto_dir)
            c1, c2 = st.columns(2)
            with c1:
                new_depto = st.text_input("Depto", placeholder="1511")
            with c2:
                new_comuna = st.text_input("Comuna", value=auto_comuna)
            c3, c4 = st.columns(2)
            with c3:
                new_cant = st.number_input("Cant", min_value=0, value=3, step=1)
            with c4:
                new_marca = st.selectbox("Marca", ["KOWEN", "CACTUS"])
            c5, c6 = st.columns(2)
            with c5:
                new_canal = st.selectbox("Canal", ["MANUAL", "WSP", "EMAIL", "WEB"])
            with c6:
                new_doc = st.selectbox("Doc", ["Boleta", "Factura", "Guia", "Ticket"])
            new_cliente = st.text_input("Cliente", placeholder="Nombre")
            new_telefono = st.text_input("Telefono", placeholder="912345678")
            new_email = st.text_input("Email", placeholder="correo@ejemplo.com")
            new_obs = st.text_input("Obs", placeholder="Ej: retirar bidones")

            if st.form_submit_button("Agregar pedido", type="primary", use_container_width=True):
                if new_dir:
                    num = sheets_client.add_pedido({
                        "fecha": new_fecha.strftime("%d/%m/%Y"),
                        "direccion": new_dir, "depto": new_depto, "comuna": new_comuna,
                        "codigo_drivin": selected_addr_code, "cant": new_cant,
                        "marca": new_marca, "documento": new_doc, "canal": new_canal,
                        "cliente": new_cliente, "telefono": new_telefono, "email": new_email,
                        "observaciones": new_obs,
                        "estado_pedido": "PENDIENTE", "estado_pago": "PENDIENTE",
                    })
                    st.success(f"Pedido #{num} agregado!")
                    if new_cliente:
                        existing = sheets_client.find_cliente(new_cliente)
                        if not existing:
                            sheets_client.add_cliente({
                                "nombre": new_cliente, "telefono": new_telefono,
                                "email": new_email, "direccion": new_dir,
                                "depto": new_depto, "comuna": new_comuna,
                                "codigo_drivin": selected_addr_code, "marca": new_marca,
                            })
                            st.info(f"Cliente '{new_cliente}' registrado.")
                    st.rerun()

    # --- Importar Bsale ---
    with st.expander("🌐  Importar desde Bsale", expanded=False):
        bsale_fecha = st.date_input("Fecha destino", value=datetime.now().date(), key="bsale_fecha")

        if st.button("🔄 Sincronizar pedidos web", key="btn_bsale_check", use_container_width=True):
            with st.spinner("Consultando Bsale..."):
                try:
                    # Buscar ultimo nro Bsale en el sistema
                    all_p = sheets_client.get_pedidos()
                    last_bsale = 0
                    for p in all_p:
                        b = p.get("Pedido Bsale", "")
                        if b and b.isdigit():
                            last_bsale = max(last_bsale, int(b))
                    # Si no hay, buscar desde un rango razonable
                    since = max(last_bsale - 5, 1) if last_bsale > 0 else 3480

                    orders = bsale_client.get_web_orders(since)
                    activos = [o for o in orders if o["estado"] == "activo"]
                    if activos:
                        checked = sheets_client.check_bsale_orders(activos)
                        st.session_state["bsale_checked"] = checked

                        # Auto-matchear direcciones
                        if not st.session_state.addresses_cache:
                            address_matcher.refresh_cache()
                            st.session_state.addresses_cache = address_matcher.load_cache()

                        nuevos = [o for o in checked if not o["existe"]]
                        matched = []
                        for o in nuevos:
                            res, conf = address_matcher.auto_match(
                                direccion=o.get("direccion", ""),
                                depto=o.get("depto", ""),
                                comuna=o.get("comuna", ""),
                                addresses=st.session_state.addresses_cache,
                            )
                            matched.append({"order": o, "result": res, "confidence": conf})
                        st.session_state["bsale_matched"] = matched
                    else:
                        st.warning("Sin pedidos nuevos.")
                        st.session_state.pop("bsale_checked", None)
                        st.session_state.pop("bsale_matched", None)
                except Exception as e:
                    st.error(f"Error: {e}")

        if st.session_state.get("bsale_checked"):
            checked = st.session_state["bsale_checked"]
            nuevos = [o for o in checked if not o["existe"]]
            existentes = [o for o in checked if o["existe"]]

            if existentes:
                st.warning(f"**{len(existentes)} ya existen** (se omiten):")
                for o in existentes:
                    st.caption(f"~~#{o['pedido_nro']} | {o['direccion']}~~ — {o['motivo']}")

            matched = st.session_state.get("bsale_matched", [])
            if matched:
                st.success(f"**{len(matched)} nuevos:**")
                bsale_codes = {}
                for i, m in enumerate(matched):
                    o = m["order"]
                    conf = m["confidence"]
                    res = m["result"]
                    code = ""

                    if conf == "auto":
                        code = res
                        st.caption(f"✅ #{o['pedido_nro']} | {o['direccion']} | {o.get('cantidad',0)} bot → `{code}`")
                    elif conf == "ambiguous" and res:
                        opts = [f"{c.code} - {c.name}" for c in res]
                        sel = st.selectbox(f"#{o['pedido_nro']} {o['direccion']}", opts, key=f"bs_sel_{i}")
                        code = res[opts.index(sel)].code
                    else:
                        code = st.text_input(f"#{o['pedido_nro']} {o['direccion']} (sin match)", key=f"bs_man_{i}", placeholder="Codigo drivin...")

                    bsale_codes[i] = code

                # Boton importar + subir
                has_plan = st.session_state.scenario_token is not None
                btn_label = f"Importar {len(matched)} + subir a driv.in" if has_plan else f"Importar {len(matched)} pedidos"
                if not has_plan:
                    st.caption("Conecta un plan driv.in para subirlos automaticamente.")

                if st.button(btn_label, key="btn_bsale_go", type="primary", use_container_width=True):
                    fecha_dest = bsale_fecha.strftime("%d/%m/%Y")
                    fecha_suffix = bsale_fecha.strftime("%m%d")

                    # 1. Guardar en planilla
                    pedidos_nuevos = []
                    for i, m in enumerate(matched):
                        o = m["order"]
                        code = bsale_codes.get(i, "")
                        pedidos_nuevos.append({
                            "fecha": fecha_dest,
                            "direccion": o.get("direccion", ""),
                            "depto": o.get("depto", ""),
                            "comuna": o.get("comuna", ""),
                            "codigo_drivin": code,
                            "cant": o.get("cantidad", 0),
                            "marca": o.get("marca", "KOWEN").upper(),
                            "cliente": o.get("cliente", ""),
                            "telefono": o.get("telefono", ""),
                            "email": o.get("email", ""),
                            "canal": "WEB",
                            "estado_pedido": "PENDIENTE",
                            "estado_pago": "PENDIENTE",
                            "pedido_bsale": str(o["pedido_nro"]),
                            "plan_drivin": st.session_state.scenario_name or "",
                        })
                    nums = sheets_client.add_pedidos(pedidos_nuevos)
                    st.success(f"{len(nums)} pedidos guardados en planilla.")

                    # 2. Subir a driv.in si hay plan
                    if has_plan:
                        clients = []
                        for i, m in enumerate(matched):
                            code = bsale_codes.get(i, "")
                            if not code:
                                continue
                            o = m["order"]
                            marca = o.get("marca", "Kowen")
                            cant = int(o.get("cantidad", 0))
                            desc = f"{marca} - Retiro" if cant == 0 else marca
                            order_code = f"{code}-{fecha_suffix}"
                            clients.append({
                                "code": code,
                                "orders": [{"code": order_code, "description": desc, "units_1": cant}]
                            })
                        if clients:
                            try:
                                r = drivin_client.create_orders(
                                    clients=clients,
                                    scenario_token=st.session_state.scenario_token,
                                )
                                added = r.get("response", r).get("added", [])
                                st.success(f"{len(added)} pedidos subidos a driv.in!")
                            except Exception as e:
                                st.error(f"Error driv.in: {e}")

                    st.session_state.pop("bsale_checked", None)
                    st.session_state.pop("bsale_matched", None)
                    st.rerun()
            elif not existentes:
                st.info("Sin pedidos nuevos.")

    # --- Limpiar planilla ---
    with st.expander("⚠️  Limpiar pedidos", expanded=False):
        st.caption("Elimina todos los pedidos de una fecha.")
        clean_date = st.date_input("Fecha a limpiar", value=datetime.now().date(), key="clean_date")
        clean_fecha = clean_date.strftime("%d/%m/%Y")
        if st.button(f"Borrar pedidos del {clean_fecha}", key="btn_clean", use_container_width=True):
            st.session_state["confirm_clean"] = clean_fecha
        if st.session_state.get("confirm_clean"):
            cf = st.session_state["confirm_clean"]
            st.error(f"¿Borrar TODOS los pedidos del **{cf}**?")
            cc1, cc2 = st.columns(2)
            with cc1:
                if st.button("Si, borrar", key="btn_clean_yes", use_container_width=True):
                    ps = sheets_client.get_pedidos(cf)
                    nums = [int(p["#"]) for p in ps if p.get("#", "").isdigit()]
                    for n in sorted(nums, reverse=True):
                        sheets_client.delete_pedido(n)
                    st.session_state.pop("confirm_clean", None)
                    st.success(f"{len(nums)} pedidos eliminados.")
                    st.rerun()
            with cc2:
                if st.button("Cancelar", key="btn_clean_no", use_container_width=True):
                    st.session_state.pop("confirm_clean", None)
                    st.rerun()

    # --- Limpiar entregados ---
    with st.expander("✅  Limpiar entregados", expanded=False):
        st.caption("Consulta driv.in y elimina pedidos ya entregados de una fecha.")
        clean_ent_date = st.date_input("Fecha a revisar", value=(datetime.now() + timedelta(days=1)).date(), key="clean_ent_date")
        clean_ent_fecha = clean_ent_date.strftime("%d/%m/%Y")

        if st.button(f"Revisar entregados del {clean_ent_fecha}", key="btn_clean_ent", use_container_width=True):
            with st.spinner("Consultando driv.in..."):
                try:
                    # Obtener estados reales de driv.in (buscar en todas las fechas recientes)
                    pods = drivin_client.get_pods(
                        (clean_ent_date - timedelta(days=3)).strftime("%Y-%m-%d"),
                        clean_ent_date.strftime("%Y-%m-%d"),
                    )
                    pod_response = pods.get("response", [])

                    # Mapear code -> status
                    drivin_status = {}
                    for pod in pod_response:
                        code = pod.get("address_code", "")
                        orders = pod.get("orders", [])
                        if orders:
                            drivin_status[code] = orders[0].get("status", "unknown")

                    # Revisar pedidos de la fecha
                    pedidos_fecha = sheets_client.get_pedidos(clean_ent_fecha)
                    entregados = []
                    en_camino = []
                    for p in pedidos_fecha:
                        cod = p.get("Codigo Drivin", "")
                        real_status = drivin_status.get(cod, "")
                        if real_status == "approved":
                            entregados.append(p)
                        elif p.get("Estado Pedido") in ("EN CAMINO",) and real_status in ("pending", ""):
                            en_camino.append(p)

                    st.session_state["clean_ent_result"] = {
                        "entregados": entregados,
                        "en_camino": en_camino,
                        "fecha": clean_ent_fecha,
                    }
                except Exception as e:
                    st.error(f"Error consultando driv.in: {e}")

        if st.session_state.get("clean_ent_result"):
            result = st.session_state["clean_ent_result"]
            ent = result["entregados"]
            enc = result["en_camino"]
            fecha_r = result["fecha"]

            if ent:
                st.warning(f"**{len(ent)} pedidos ya entregados** en driv.in:")
                for p in ent:
                    st.caption(f"  #{p.get('#')} — {p.get('Direccion','')} ({p.get('Canal','')})")
            else:
                st.success("No hay pedidos entregados para eliminar.")

            if enc:
                st.info(f"**{len(enc)} pedidos** se pondrán en PENDIENTE (estaban EN CAMINO):")
                for p in enc:
                    st.caption(f"  #{p.get('#')} — {p.get('Direccion','')}")

            if ent or enc:
                if st.button("Aplicar limpieza", key="btn_apply_clean_ent", type="primary", use_container_width=True):
                    # Eliminar entregados (de mayor a menor)
                    nums_del = sorted([int(p["#"]) for p in ent if p.get("#", "").isdigit()], reverse=True)
                    for n in nums_del:
                        sheets_client.delete_pedido(n)

                    # Poner EN CAMINO -> PENDIENTE
                    if enc:
                        updates = [(int(p["#"]), {"estado_pedido": "PENDIENTE"}) for p in enc if p.get("#", "").isdigit()]
                        if updates:
                            sheets_client.update_pedidos_batch(updates)

                    st.session_state.pop("clean_ent_result", None)
                    st.success(f"Eliminados {len(nums_del)} entregados, {len(enc)} puestos en PENDIENTE.")
                    st.rerun()

            if st.button("Cancelar", key="btn_cancel_clean_ent", use_container_width=True):
                st.session_state.pop("clean_ent_result", None)
                st.rerun()

    # --- Importar desde driv.in ---
    with st.expander("🚛  Importar desde driv.in", expanded=False):
        imp_date = st.date_input("Fecha", value=datetime.now().date(), key="imp_drivin_date")
        imp_fecha = imp_date.strftime("%d/%m/%Y")
        # Buscar planes de esa fecha
        imp_planes = []
        try:
            r = drivin_client.get_scenarios_by_date(imp_date.strftime("%Y-%m-%d"))
            for s in r.get("response", []):
                imp_planes.append({"label": s.get("description", ""), "token": s.get("token", s.get("scenario_token", ""))})
        except Exception:
            pass
        if imp_planes:
            imp_labels = [p["label"] for p in imp_planes]
            imp_sel = st.selectbox("Plan", imp_labels, key="imp_drivin_plan")
            imp_token = imp_planes[imp_labels.index(imp_sel)]["token"]
            if st.button("Importar pedidos de ruta", key="btn_imp_drivin", use_container_width=True):
                with st.spinner("Importando desde driv.in..."):
                    try:
                        count = sheets_client.import_from_drivin(imp_token, imp_fecha)
                        if count > 0:
                            st.success(f"{count} pedidos importados!")
                            st.rerun()
                        else:
                            st.info("Sin pedidos nuevos.")
                    except Exception as e:
                        st.error(f"Error: {e}")
        else:
            st.caption("Sin planes para esa fecha.")

    # --- Importar desde Planilla Reparto ---
    with st.expander("📊  Importar desde Planilla Reparto", expanded=False):
        rep_date = st.date_input("Fecha a importar", value=datetime.now().date(), key="rep_date")
        rep_fecha_str = rep_date.strftime("%d/%m/%Y")
        if st.button("Importar PRIMER TURNO", key="btn_reparto", use_container_width=True):
            with st.spinner(f"Leyendo planilla reparto ({rep_fecha_str})..."):
                try:
                    count = sheets_client.sync_from_planilla_reparto(rep_fecha_str)
                    if count > 0:
                        st.success(f"{count} pedidos importados!")
                        st.rerun()
                    else:
                        st.info("Sin pedidos nuevos para esa fecha.")
                except Exception as e:
                    st.error(f"Error: {e}")

    # --- Plan driv.in ---
    with st.expander("🚛  Plan driv.in", expanded=True):
        if st.session_state.scenario_token:
            st.success(f"**{st.session_state.scenario_name}**")
            st.caption(f"`{st.session_state.scenario_token}`")
            if st.button("Desconectar", use_container_width=True, key="btn_desconectar"):
                st.session_state.scenario_token = None
                st.session_state.scenario_name = None
                st.rerun()
        else:
            plan_tab1, plan_tab2, plan_tab3 = st.tabs(["Recientes", "Nuevo", "Token"])

            with plan_tab1:
                planes = []
                for delta in range(0, 3):
                    fecha_b = (datetime.now() + timedelta(days=delta)).strftime("%Y-%m-%d")
                    try:
                        r = drivin_client.get_scenarios_by_date(fecha_b)
                        for s in r.get("response", []):
                            planes.append({
                                "label": f"{s.get('description', '')} ({fecha_b})",
                                "token": s.get("token", s.get("scenario_token", "")),
                                "name": s.get("description", ""),
                            })
                    except Exception:
                        pass
                if planes:
                    labels = [p["label"] for p in planes]
                    sel_idx = st.selectbox("Plan", range(len(labels)), format_func=lambda i: labels[i])
                    if st.button("Conectar", key="btn_con_rec", use_container_width=True):
                        st.session_state.scenario_token = planes[sel_idx]["token"]
                        st.session_state.scenario_name = planes[sel_idx]["name"]
                        st.rerun()
                else:
                    st.caption("Sin planes recientes")

            with plan_tab2:
                plan_date = st.date_input("Fecha", value=datetime.now().date(), key="plan_date")
                plan_name = st.text_input("Nombre", value=f"{plan_date.strftime('%d/%m/%Y')}API")
                if st.button("Crear", key="btn_crear", use_container_width=True):
                    with st.spinner("Creando..."):
                        try:
                            r = drivin_client._request("POST", "scenarios", json_body={
                                "description": plan_name,
                                "date": plan_date.strftime("%Y-%m-%d"),
                                "schema_name": "Optimización", "clients": [],
                            })
                            resp = r.get("response", r)
                            st.session_state.scenario_token = resp.get("scenario_token", "")
                            st.session_state.scenario_name = plan_name
                            st.rerun()
                        except Exception as e:
                            st.error(f"{e}")

            with plan_tab3:
                ext_t = st.text_input("Token")
                ext_n = st.text_input("Nombre", value="Plan")
                if st.button("Conectar", key="btn_con_tok", use_container_width=True):
                    if ext_t:
                        st.session_state.scenario_token = ext_t
                        st.session_state.scenario_name = ext_n
                        st.rerun()

    # --- Herramientas ---
    st.markdown("---")
    if st.button("🔄 Actualizar cache", use_container_width=True):
        with st.spinner("Descargando..."):
            count = address_matcher.refresh_cache()
            st.session_state.addresses_cache = address_matcher.load_cache()
            st.success(f"{count} direcciones.")

    st.markdown("---")
    st.caption("Kowen v2.1 — Gestor de Pedidos")


# ============================================================
# MAIN AREA
# ============================================================

# Header con resumen
col_title, col_date = st.columns([3, 1])
with col_title:
    st.markdown("## 📋 Operacion Diaria")
with col_date:
    filter_date = st.date_input("Fecha", value=datetime.now().date(), key="filter_date", label_visibility="collapsed")

fecha_str = filter_date.strftime("%d/%m/%Y")

# Cargar datos
all_pedidos_dia = sheets_client.get_pedidos(fecha_str)
resumen = {
    "total": len(all_pedidos_dia),
    "botellones": sum(int(p.get("Cant", 0) or 0) for p in all_pedidos_dia),
    "entregados": sum(1 for p in all_pedidos_dia if p.get("Estado Pedido") == "ENTREGADO"),
    "pendientes": sum(1 for p in all_pedidos_dia if p.get("Estado Pedido") == "PENDIENTE"),
    "en_camino": sum(1 for p in all_pedidos_dia if p.get("Estado Pedido") == "EN CAMINO"),
    "no_entregados": sum(1 for p in all_pedidos_dia if p.get("Estado Pedido") == "NO ENTREGADO"),
    "pagados": sum(1 for p in all_pedidos_dia if p.get("Estado Pago") == "PAGADO"),
}

# Metricas
m1, m2, m3, m4, m5, m6 = st.columns(6)
m1.metric("Pedidos", resumen["total"])
m2.metric("Botellones", resumen["botellones"])
m3.metric("Pendientes", resumen["pendientes"])
m4.metric("En Camino", resumen["en_camino"])
m5.metric("Entregados", resumen["entregados"])
m6.metric("Pagados", resumen["pagados"])

# Filtro estado
col_filt, col_space = st.columns([2, 4])
with col_filt:
    filter_estado = st.selectbox("Filtrar por estado", ["Todos", "PENDIENTE", "EN CAMINO", "ENTREGADO", "NO ENTREGADO"], label_visibility="collapsed")

pedidos = all_pedidos_dia
if filter_estado != "Todos":
    pedidos = [p for p in pedidos if p.get("Estado Pedido", "") == filter_estado]


# --- CONTENIDO PRINCIPAL: dos columnas (tabla + chat) ---

main_col, chat_col = st.columns([3, 1])

with main_col:

    if not pedidos:
        st.info(f"No hay pedidos para {fecha_str}. Usa el sidebar para agregar o importar.")
    else:
        tab_vista, tab_editar, tab_drivin = st.tabs(["📋 Vista general", "✏️ Editar / Eliminar", "🚛 Subir a driv.in"])

        # === VISTA ===
        with tab_vista:
            display = []
            for p in pedidos:
                display.append({
                    "#": p.get("#", ""),
                    "Direccion": p.get("Direccion", ""),
                    "Depto": p.get("Depto", ""),
                    "Cant": p.get("Cant", ""),
                    "Marca": p.get("Marca", ""),
                    "Repartidor": p.get("Repartidor", ""),
                    "Estado": p.get("Estado Pedido", ""),
                    "Canal": p.get("Canal", ""),
                    "Cliente": p.get("Cliente", ""),
                    "F. Pago": p.get("Forma Pago", ""),
                    "E. Pago": p.get("Estado Pago", ""),
                    "Vuelta": p.get("Vuelta", ""),
                    "Zona": p.get("Zona", ""),
                    "Cod. Drivin": p.get("Codigo Drivin", ""),
                    "Obs": p.get("Observaciones", ""),
                })
            st.dataframe(display, use_container_width=True, hide_index=True, height=450)

            # --- Mover pedidos rapido ---
            st.markdown("")
            move_labels = [f"#{p.get('#','')} — {p.get('Direccion','')} ({p.get('Cant','')} bot)" for p in pedidos]
            mv_col1, mv_col2, mv_col3 = st.columns([3, 1, 1])
            with mv_col1:
                selected_to_move = st.multiselect("Seleccionar pedidos para mover", range(len(move_labels)),
                                                   format_func=lambda i: move_labels[i], key="mv_select")
            with mv_col2:
                move_target = st.date_input("Mover a", value=(datetime.now() + timedelta(days=1)).date(), key="mv_target")
            with mv_col3:
                st.markdown("")
                st.markdown("")
                if st.button(f"📅 Mover ({len(selected_to_move)})", key="btn_mv_batch", use_container_width=True, disabled=not selected_to_move):
                    target_str = move_target.strftime("%d/%m/%Y")
                    updates = []
                    for idx in selected_to_move:
                        num = int(pedidos[idx].get("#", 0))
                        if num:
                            updates.append((num, {"fecha": target_str}))
                    if updates:
                        sheets_client.update_pedidos_batch(updates)
                        st.success(f"{len(updates)} pedidos movidos a {target_str}")
                        st.rerun()

        # === EDITAR / ELIMINAR ===
        with tab_editar:
            pedido_labels = [f"#{p.get('#', '')} — {p.get('Direccion', '')} ({p.get('Cliente', '')})" for p in pedidos]

            if pedido_labels:
                selected_idx = st.selectbox("Seleccionar pedido", range(len(pedido_labels)),
                                             format_func=lambda i: pedido_labels[i], key="sel_edit")
                sel = pedidos[selected_idx]
                sel_num = int(sel.get("#", 0))

                # Info del pedido seleccionado
                st.markdown(f"""
                <div class="card">
                    <div class="card-header">Pedido #{sel_num} — {sel.get('Direccion', '')} {sel.get('Depto', '')}</div>
                    <b>Cliente:</b> {sel.get('Cliente', '-')} &nbsp;|&nbsp;
                    <b>Cant:</b> {sel.get('Cant', '-')} &nbsp;|&nbsp;
                    <b>Marca:</b> {sel.get('Marca', '-')} &nbsp;|&nbsp;
                    <b>Canal:</b> {sel.get('Canal', '-')} &nbsp;|&nbsp;
                    <b>Cod:</b> {sel.get('Codigo Drivin', '-')}
                </div>
                """, unsafe_allow_html=True)

                with st.form(f"form_edit_{sel_num}"):
                    e1, e2, e3 = st.columns(3)
                    with e1:
                        estado_opts = ["PENDIENTE", "EN CAMINO", "ENTREGADO", "NO ENTREGADO"]
                        ed_estado = st.selectbox("Estado Pedido", estado_opts,
                            index=estado_opts.index(sel.get("Estado Pedido", "PENDIENTE")) if sel.get("Estado Pedido") in estado_opts else 0)
                    with e2:
                        rep_opts = ["", "Angel Salas", "Leo Carreño", "Sebastian Ramirez", "Nicolas Muñoz", "Daniel Araya", "Yhoel Del Campo"]
                        ed_rep = st.selectbox("Repartidor", rep_opts,
                            index=rep_opts.index(sel.get("Repartidor", "")) if sel.get("Repartidor", "") in rep_opts else 0)
                    with e3:
                        ed_cant = st.number_input("Cantidad", value=int(sel.get("Cant", 0) or 0), min_value=0)

                    e4, e5, e6 = st.columns(3)
                    with e4:
                        vuelta_opts = ["", "1a", "2a", "3a"]
                        ed_vuelta = st.selectbox("Vuelta", vuelta_opts,
                            index=vuelta_opts.index(sel.get("Vuelta", "")) if sel.get("Vuelta", "") in vuelta_opts else 0)
                    with e5:
                        zona_opts = ["", "Sur", "Oriente"]
                        ed_zona = st.selectbox("Zona", zona_opts,
                            index=zona_opts.index(sel.get("Zona", "")) if sel.get("Zona", "") in zona_opts else 0)
                    with e6:
                        doc_opts = ["", "Boleta", "Factura", "Guia", "Ticket"]
                        ed_doc = st.selectbox("Documento", doc_opts,
                            index=doc_opts.index(sel.get("Documento", "")) if sel.get("Documento", "") in doc_opts else 0)

                    e7, e8 = st.columns(2)
                    with e7:
                        fp_opts = ["", "Efectivo", "Transferencia", "Webpay"]
                        ed_fp = st.selectbox("Forma Pago", fp_opts,
                            index=fp_opts.index(sel.get("Forma Pago", "")) if sel.get("Forma Pago", "") in fp_opts else 0)
                    with e8:
                        ep_opts = ["PENDIENTE", "PAGADO", "POR CONFIRMAR"]
                        ed_ep = st.selectbox("Estado Pago", ep_opts,
                            index=ep_opts.index(sel.get("Estado Pago", "PENDIENTE")) if sel.get("Estado Pago", "PENDIENTE") in ep_opts else 0)

                    ed_obs = st.text_input("Observaciones", value=sel.get("Observaciones", ""))
                    ed_aliado = st.text_input("Aliado", value=sel.get("Aliado", ""))

                    if st.form_submit_button("💾 Guardar cambios", type="primary", use_container_width=True):
                        sheets_client.update_pedido(sel_num, {
                            "estado_pedido": ed_estado, "repartidor": ed_rep, "cant": ed_cant,
                            "vuelta": ed_vuelta, "zona": ed_zona, "documento": ed_doc,
                            "forma_pago": ed_fp, "estado_pago": ed_ep,
                            "observaciones": ed_obs, "aliado": ed_aliado,
                        })
                        st.success(f"Pedido #{sel_num} actualizado!")
                        st.rerun()

                # --- MOVER A OTRA FECHA ---
                st.markdown("")
                mv1, mv2 = st.columns([2, 1])
                with mv1:
                    move_date = st.date_input("Mover a fecha", value=(datetime.now() + timedelta(days=1)).date(), key="move_date")
                with mv2:
                    st.markdown("")
                    st.markdown("")
                    if st.button("📅 Mover", key="btn_move", use_container_width=True):
                        new_fecha_str = move_date.strftime("%d/%m/%Y")
                        sheets_client.update_pedido(sel_num, {"fecha": new_fecha_str})
                        st.success(f"Pedido #{sel_num} movido a {new_fecha_str}")
                        st.rerun()

                # --- BOTON ELIMINAR ---
                st.markdown("---")
                with st.container():
                    if st.session_state.get("confirm_delete") == sel_num:
                        st.error(f"¿Eliminar pedido **#{sel_num}** — {sel.get('Direccion', '')}?")
                        dc1, dc2 = st.columns(2)
                        with dc1:
                            if st.button("✅ Si, eliminar", key="btn_confirm_del", use_container_width=True):
                                sheets_client.delete_pedido(sel_num)
                                st.session_state.pop("confirm_delete", None)
                                st.success(f"Pedido #{sel_num} eliminado.")
                                st.rerun()
                        with dc2:
                            if st.button("Cancelar", key="btn_cancel_del", use_container_width=True):
                                st.session_state.pop("confirm_delete", None)
                                st.rerun()
                    else:
                        if st.button("🗑️ Eliminar este pedido", key="btn_delete", type="secondary", use_container_width=True):
                            st.session_state["confirm_delete"] = sel_num
                            st.rerun()

        # === SUBIR A DRIV.IN ===
        with tab_drivin:
            if not st.session_state.scenario_token:
                st.warning("Conecta un plan en el sidebar primero.")
            else:
                st.info(f"Plan: **{st.session_state.scenario_name}**")
                # Filtrar pedidos pendientes que NO tengan plan drivin asignado (evitar duplicados)
                pendientes = [p for p in pedidos if p.get("Estado Pedido") == "PENDIENTE" and not p.get("Plan Drivin", "").strip()]

                if not pendientes:
                    st.success("No hay pedidos pendientes.")
                else:
                    st.write(f"**{len(pendientes)}** pedidos pendientes")

                    if st.button("Matchear direcciones", key="btn_match"):
                        if not st.session_state.addresses_cache:
                            with st.spinner("Cargando cache..."):
                                address_matcher.refresh_cache()
                                st.session_state.addresses_cache = address_matcher.load_cache()

                        matched = []
                        for p in pendientes:
                            result, confidence = address_matcher.auto_match(
                                direccion=p.get("Direccion", ""),
                                depto=p.get("Depto", ""),
                                comuna=p.get("Comuna", ""),
                                addresses=st.session_state.addresses_cache,
                            )
                            matched.append({"pedido": p, "result": result, "confidence": confidence})
                        st.session_state["matched_drivin"] = matched

                    if st.session_state.get("matched_drivin"):
                        codes = {}
                        for i, m in enumerate(st.session_state["matched_drivin"]):
                            p = m["pedido"]
                            conf = m["confidence"]
                            result = m["result"]

                            c1, c2, c3 = st.columns([3, 2, 1])
                            with c1:
                                lbl = f"#{p.get('#', '')} | {p.get('Direccion', '')} {p.get('Depto', '')} | {p.get('Cliente', '')}"
                                if conf == "auto":
                                    st.success(lbl)
                                elif conf == "ambiguous":
                                    st.warning(lbl)
                                else:
                                    st.error(lbl)
                            with c2:
                                if conf == "auto":
                                    st.code(result)
                                    codes[i] = result
                                elif conf == "ambiguous" and result:
                                    opts = [f"{c.code} - {c.name}" for c in result]
                                    s = st.selectbox("Cod", opts, key=f"sel_d_{i}", label_visibility="collapsed")
                                    codes[i] = result[opts.index(s)].code
                                else:
                                    manual = st.text_input("Cod", key=f"man_d_{i}", label_visibility="collapsed", placeholder="Codigo...")
                                    codes[i] = manual
                            with c3:
                                st.write(f"**{p.get('Cant', '')}** bot.")

                        st.markdown("---")
                        if st.button("🚀 Subir al plan", key="btn_subir", type="primary", use_container_width=True):
                            clients = []
                            updates_list = []
                            for i, m in enumerate(st.session_state["matched_drivin"]):
                                code = codes.get(i, "")
                                if not code:
                                    continue
                                p = m["pedido"]
                                marca = p.get("Marca", "KOWEN")
                                cant = int(p.get("Cant", 0) or 0)
                                desc = f"{marca} - Retiro" if cant == 0 else marca
                                fecha_raw = p.get("Fecha", "")
                                if "/" in fecha_raw:
                                    parts = fecha_raw.split("/")
                                    suffix = f"{parts[1]}{parts[0]}"
                                else:
                                    suffix = datetime.now().strftime("%m%d")
                                clients.append({
                                    "code": code,
                                    "orders": [{"code": f"{code}-{suffix}", "description": desc, "units_1": cant}]
                                })
                                pn = int(p.get("#", 0))
                                if pn:
                                    updates_list.append((pn, {"codigo_drivin": code, "plan_drivin": st.session_state.scenario_name or ""}))

                            if clients:
                                with st.spinner("Subiendo..."):
                                    try:
                                        r = drivin_client.create_orders(clients=clients, scenario_token=st.session_state.scenario_token)
                                        added = r.get("response", r).get("added", [])
                                        if updates_list:
                                            sheets_client.update_pedidos_batch(updates_list)
                                        st.success(f"{len(added)} pedidos subidos!")
                                        st.session_state.pop("matched_drivin", None)
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Error: {e}")


# === CHAT CON IA (columna derecha) ===

# Herramientas que la IA puede ejecutar
_CHAT_TOOLS = [
    {
        "name": "mover_pedidos",
        "description": "Mueve pedidos a otra fecha. Puede filtrar por canal (WEB, WSP, EMAIL, MANUAL), por estado, o por numeros especificos.",
        "input_schema": {
            "type": "object",
            "properties": {
                "fecha_destino": {"type": "string", "description": "Fecha destino en formato DD/MM/YYYY"},
                "numeros": {"type": "array", "items": {"type": "integer"}, "description": "Lista de numeros (#) de pedidos a mover. Si no se especifica, usa filtros."},
                "canal": {"type": "string", "description": "Filtrar por canal: WEB, WSP, EMAIL, MANUAL"},
                "estado": {"type": "string", "description": "Filtrar por estado: PENDIENTE, EN CAMINO, ENTREGADO, NO ENTREGADO"},
            },
            "required": ["fecha_destino"],
        },
    },
    {
        "name": "cambiar_estado",
        "description": "Cambia el estado de uno o mas pedidos (Estado Pedido o Estado Pago).",
        "input_schema": {
            "type": "object",
            "properties": {
                "numeros": {"type": "array", "items": {"type": "integer"}, "description": "Numeros de pedidos a actualizar"},
                "estado_pedido": {"type": "string", "description": "Nuevo estado: PENDIENTE, EN CAMINO, ENTREGADO, NO ENTREGADO"},
                "estado_pago": {"type": "string", "description": "Nuevo estado pago: PENDIENTE, PAGADO, POR CONFIRMAR"},
            },
            "required": ["numeros"],
        },
    },
    {
        "name": "buscar_pedidos",
        "description": "Busca pedidos por criterios (cliente, direccion, canal, estado). Devuelve los que coincidan.",
        "input_schema": {
            "type": "object",
            "properties": {
                "texto": {"type": "string", "description": "Texto a buscar en direccion, cliente, observaciones"},
                "canal": {"type": "string", "description": "Filtrar por canal"},
                "estado": {"type": "string", "description": "Filtrar por estado pedido"},
                "fecha": {"type": "string", "description": "Fecha DD/MM/YYYY (por defecto la fecha actual del filtro)"},
            },
        },
    },
    {
        "name": "resumen_dia",
        "description": "Muestra resumen de un dia: total pedidos, botellones, estados, pagos.",
        "input_schema": {
            "type": "object",
            "properties": {
                "fecha": {"type": "string", "description": "Fecha DD/MM/YYYY (por defecto hoy)"},
            },
        },
    },
]


def _execute_tool(tool_name, tool_input, current_pedidos, current_fecha):
    """Ejecuta una herramienta del chat y devuelve el resultado como string."""
    import json

    if tool_name == "mover_pedidos":
        fecha_dest = tool_input["fecha_destino"]
        numeros = tool_input.get("numeros")
        canal_filter = tool_input.get("canal", "").upper()
        estado_filter = tool_input.get("estado", "").upper()

        # Si no hay numeros especificos, filtrar
        if not numeros:
            targets = []
            for p in current_pedidos:
                if canal_filter and p.get("Canal", "").upper() != canal_filter:
                    continue
                if estado_filter and p.get("Estado Pedido", "").upper() != estado_filter:
                    continue
                num = p.get("#", "")
                if num and str(num).isdigit():
                    targets.append(int(num))
            numeros = targets

        if not numeros:
            return "No se encontraron pedidos que coincidan con los filtros."

        updates = [(n, {"fecha": fecha_dest}) for n in numeros]
        sheets_client.update_pedidos_batch(updates)
        return f"Se movieron {len(numeros)} pedidos a {fecha_dest}: #{', #'.join(str(n) for n in numeros)}"

    elif tool_name == "cambiar_estado":
        numeros = tool_input["numeros"]
        upd = {}
        if tool_input.get("estado_pedido"):
            upd["estado_pedido"] = tool_input["estado_pedido"]
        if tool_input.get("estado_pago"):
            upd["estado_pago"] = tool_input["estado_pago"]
        if not upd:
            return "No se especifico que estado cambiar."
        updates = [(n, upd) for n in numeros]
        sheets_client.update_pedidos_batch(updates)
        return f"Se actualizaron {len(numeros)} pedidos: {upd}"

    elif tool_name == "buscar_pedidos":
        fecha = tool_input.get("fecha", current_fecha)
        pedidos = sheets_client.get_pedidos(fecha)
        texto = (tool_input.get("texto") or "").lower()
        canal = (tool_input.get("canal") or "").upper()
        estado = (tool_input.get("estado") or "").upper()

        results = []
        for p in pedidos:
            if texto:
                searchable = f"{p.get('Direccion','')} {p.get('Cliente','')} {p.get('Observaciones','')}".lower()
                if texto not in searchable:
                    continue
            if canal and p.get("Canal", "").upper() != canal:
                continue
            if estado and p.get("Estado Pedido", "").upper() != estado:
                continue
            results.append(p)

        if not results:
            return "No se encontraron pedidos."
        lines = []
        for p in results[:20]:
            lines.append(f"#{p.get('#','')}: {p.get('Direccion','')} - {p.get('Cant','')} bot - {p.get('Canal','')} - {p.get('Estado Pedido','')} - {p.get('Cliente','')}")
        return f"Encontrados {len(results)} pedidos:\n" + "\n".join(lines)

    elif tool_name == "resumen_dia":
        fecha = tool_input.get("fecha", current_fecha)
        r = sheets_client.resumen_dia(fecha)
        return json.dumps(r, ensure_ascii=False, indent=2)

    return "Herramienta no reconocida."


with chat_col:
    st.markdown("""
    <div class="chat-container">
        <div class="chat-header">💬 Asistente Kowen</div>
    </div>
    """, unsafe_allow_html=True)

    # Mostrar mensajes (solo user y assistant con texto)
    chat_container = st.container(height=380)
    with chat_container:
        for msg in st.session_state.chat_messages:
            if msg["role"] in ("user", "assistant") and msg.get("content"):
                with st.chat_message(msg["role"]):
                    st.markdown(msg["content"])

    # Input del chat
    user_input = st.chat_input("Pregunta algo...", key="chat_input")

    if user_input:
        st.session_state.chat_messages.append({"role": "user", "content": user_input})

        # Contexto para la IA
        pedidos_ctx = ""
        if all_pedidos_dia:
            lines = []
            for p in all_pedidos_dia[:50]:
                lines.append(
                    f"#{p.get('#','')}: Dir:{p.get('Direccion','')} {p.get('Depto','')}, "
                    f"Cant:{p.get('Cant','')}, Marca:{p.get('Marca','')}, "
                    f"Estado:{p.get('Estado Pedido','')}, Canal:{p.get('Canal','')}, "
                    f"Repartidor:{p.get('Repartidor','')}, Cliente:{p.get('Cliente','')}, "
                    f"Pago:{p.get('Estado Pago','')}, Bsale:{p.get('Pedido Bsale','')}"
                )
            pedidos_ctx = "\n".join(lines)

        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%d/%m/%Y")
        system_msg = (
            "Eres el asistente de Kowen, una planta purificadora de agua en Santiago, Chile. "
            "Ayudas con la operacion diaria: pedidos, entregas, pagos, rutas. "
            "Responde en espanol, breve y directo. "
            "TIENES HERRAMIENTAS para ejecutar acciones reales. Usalas cuando el usuario pida mover pedidos, cambiar estados, buscar, etc. "
            "Cuando el usuario dice 'mañana' se refiere a " + tomorrow + ". "
            "Cuando dice 'los pedidos web' o 'pedidos de bsale' filtra por canal=WEB. "
            f"\n\nFecha filtro actual: {fecha_str}"
            f"\nResumen: {resumen['total']} pedidos, {resumen['botellones']} botellones, "
            f"{resumen['entregados']} entregados, {resumen['pendientes']} pendientes, "
            f"{resumen['en_camino']} en camino, {resumen.get('no_entregados',0)} no entregados, "
            f"{resumen['pagados']} pagados."
            f"\n\nPedidos del dia:\n{pedidos_ctx}" if pedidos_ctx else ""
        )

        try:
            from anthropic import Anthropic
            client = Anthropic()

            # Construir mensajes para la API (filtrar solo user/assistant con content)
            api_messages = []
            for m in st.session_state.chat_messages:
                if m["role"] == "user":
                    api_messages.append({"role": "user", "content": m["content"]})
                elif m["role"] == "assistant" and m.get("content"):
                    api_messages.append({"role": "assistant", "content": m["content"]})

            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=1024,
                system=system_msg,
                messages=api_messages,
                tools=_CHAT_TOOLS,
            )

            # Procesar respuesta — puede incluir tool_use
            assistant_text = ""
            tool_results = []

            for block in response.content:
                if block.type == "text":
                    assistant_text += block.text
                elif block.type == "tool_use":
                    result = _execute_tool(block.name, block.input, all_pedidos_dia, fecha_str)
                    tool_results.append(f"**{block.name}**: {result}")

            # Si hubo tool use, hacer un segundo llamado para que la IA resuma
            if tool_results:
                tool_summary = "\n".join(tool_results)
                api_messages.append({"role": "assistant", "content": f"[Ejecutando acciones...]"})
                api_messages.append({"role": "user", "content": f"Resultados de las acciones ejecutadas:\n{tool_summary}\n\nResume brevemente lo que se hizo."})

                response2 = client.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=500,
                    system=system_msg,
                    messages=api_messages,
                )
                assistant_text = response2.content[0].text

            if not assistant_text:
                assistant_text = "Accion completada."

        except Exception as e:
            assistant_text = f"Error: {e}"

        st.session_state.chat_messages.append({"role": "assistant", "content": assistant_text})
        st.rerun()


# ============================================================
# TABS INFERIORES: Clientes, Pagos, Sync
# ============================================================

st.markdown("---")

tab_cl, tab_pg, tab_sync = st.tabs(["👥 Clientes", "💰 Pagos", "🔄 Sync driv.in"])

with tab_cl:
    clientes = sheets_client.get_clientes()
    if clientes:
        st.dataframe(clientes, use_container_width=True, hide_index=True, height=300)
    else:
        st.info("No hay clientes registrados.")

    with st.expander("➕ Agregar nuevo cliente"):
        with st.form("form_nuevo_cliente"):
            nc1, nc2 = st.columns(2)
            with nc1:
                nc_nombre = st.text_input("Nombre", key="nc_nombre")
            with nc2:
                nc_tel = st.text_input("Telefono", key="nc_tel")
            nc3, nc4 = st.columns(2)
            with nc3:
                nc_email = st.text_input("Email", key="nc_email")
            with nc4:
                nc_marca = st.selectbox("Marca", ["KOWEN", "CACTUS"], key="nc_marca")
            nc5, nc6 = st.columns(2)
            with nc5:
                nc_dir = st.text_input("Direccion", key="nc_dir")
            with nc6:
                nc_com = st.text_input("Comuna", key="nc_com")
            nc7, nc8 = st.columns(2)
            with nc7:
                nc_dep = st.text_input("Depto", key="nc_dep")
            with nc8:
                nc_cod = st.text_input("Codigo Drivin", key="nc_cod")
            nc_prec = st.text_input("Precio especial", placeholder="Vacio = precio normal", key="nc_prec")
            if st.form_submit_button("Agregar cliente", use_container_width=True):
                if nc_nombre:
                    sheets_client.add_cliente({
                        "nombre": nc_nombre, "telefono": nc_tel, "email": nc_email,
                        "direccion": nc_dir, "depto": nc_dep, "comuna": nc_com,
                        "codigo_drivin": nc_cod, "marca": nc_marca, "precio_especial": nc_prec,
                    })
                    st.success(f"Cliente '{nc_nombre}' agregado!")
                    st.rerun()

with tab_pg:
    pagos = sheets_client.get_pagos()
    if pagos:
        st.dataframe(pagos, use_container_width=True, hide_index=True, height=300)
    else:
        st.info("No hay pagos registrados.")

    with st.expander("➕ Registrar pago"):
        with st.form("form_pago"):
            pc1, pc2 = st.columns(2)
            with pc1:
                pg_monto = st.number_input("Monto", min_value=0, value=0, step=1000)
            with pc2:
                pg_medio = st.selectbox("Medio", ["Efectivo", "Transferencia", "Webpay"])
            pc3, pc4 = st.columns(2)
            with pc3:
                pg_cl = st.text_input("Cliente", key="pg_cl")
            with pc4:
                pg_ref = st.text_input("Referencia", placeholder="Nro operacion")
            pg_ped = st.text_input("Pedido vinculado (#)", key="pg_ped")
            if st.form_submit_button("Registrar pago", use_container_width=True):
                if pg_monto > 0:
                    sheets_client.add_pago({
                        "monto": pg_monto, "medio": pg_medio, "cliente": pg_cl,
                        "referencia": pg_ref, "pedido_vinculado": pg_ped, "estado": "PAGADO",
                    })
                    st.success("Pago registrado!")
                    st.rerun()

with tab_sync:
    sync_t1, sync_t2, sync_t3 = st.tabs(["📡 Desde driv.in", "📊 Hacia Planilla Reparto", "🚗 Asignar conductor"])

    with sync_t1:
        st.markdown("##### Actualizar estado, repartidor y comentarios desde driv.in")
        st.caption("Usa los PODs (proof of delivery) para traer el estado real de cada pedido.")
        sync_date = st.date_input("Fecha", value=datetime.now().date(), key="sync_date")
        sync_fecha = sync_date.strftime("%d/%m/%Y")

        if st.button("Sincronizar desde driv.in", key="btn_sync", type="primary", use_container_width=True):
            with st.spinner("Sincronizando..."):
                try:
                    count = sheets_client.sync_from_drivin(
                        fecha=sync_fecha,
                        plan_name=st.session_state.scenario_name or "",
                    )
                    if count > 0:
                        st.success(f"{count} pedidos actualizados!")
                        st.rerun()
                    else:
                        st.info("Sin cambios nuevos.")
                except Exception as e:
                    st.error(f"Error: {e}")

    with sync_t2:
        st.markdown("##### Sincronizar hacia Planilla Reparto (PRIMER TURNO)")
        st.caption("Escribe los pedidos del sistema en la planilla reparto de Google Sheets.")
        sync2_date = st.date_input("Fecha", value=datetime.now().date(), key="sync2_date")
        sync2_fecha = sync2_date.strftime("%d/%m/%Y")

        if st.button("Sincronizar hacia planilla reparto", key="btn_sync_reparto", type="primary", use_container_width=True):
            with st.spinner("Sincronizando..."):
                try:
                    count = sheets_client.sync_to_planilla_reparto(sync2_fecha)
                    if count > 0:
                        st.success(f"{count} pedidos sincronizados a planilla reparto!")
                    else:
                        st.info("No hay pedidos para sincronizar.")
                except Exception as e:
                    st.error(f"Error: {e}")

    with sync_t3:
        st.markdown("##### Asignar conductor")
        try:
            vdata = drivin_client.get_vehicles()
            vehicles = vdata.get("response", [])
            v_opts = {}
            for v in vehicles:
                d = v.get("driver", {})
                dname = f"{d.get('first_name', '')} {d.get('last_name', '')}".strip() if d else "Sin conductor"
                v_opts[f"{v['code']} — {dname}"] = v
            sel_v = st.selectbox("Vehiculo", options=list(v_opts.keys()))
            if st.button("Asignar todos", key="btn_asignar"):
                veh = v_opts[sel_v]
                with st.spinner("Asignando..."):
                    try:
                        od = drivin_client.get_orders(st.session_state.scenario_token)
                        ol = od.get("response", [])
                        if ol:
                            cls = [{"code": o.get("address_code", o.get("code", "")),
                                    "orders": [{"code": o.get("order_code", o.get("code", "")),
                                                "description": o.get("description", "Kowen"),
                                                "units_1": o.get("units_1", 0)}]} for o in ol]
                            drivin_client.create_route(veh["code"], cls, st.session_state.scenario_token)
                            st.success(f"Ruta con {len(cls)} pedidos!")
                        else:
                            st.info("Sin pedidos en plan.")
                    except Exception as e:
                        st.error(f"Error: {e}")
        except Exception as e:
            st.error(f"Error: {e}")
