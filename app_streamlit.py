"""
Dashboard web de Kowen con Streamlit.
Conectado a Google Sheets (Pedidos 2026).
Ejecutar: streamlit run app_streamlit.py
"""

import os
import shutil
import streamlit as st
from datetime import datetime, timedelta

from dotenv import load_dotenv
load_dotenv()

# Bridge Streamlit Cloud secrets -> os.environ
# (en Cloud las variables estan en st.secrets, no en env vars; el resto del
# codigo —config.py, sheets_client, drivin_client— lee de os.getenv, asi que
# hacemos el puente ANTES de importar esos modulos)
_secrets_loaded = []
_secrets_error = None
try:
    for _k, _v in st.secrets.items():
        if _k not in os.environ:
            os.environ[_k] = str(_v)
        _secrets_loaded.append(_k)
except Exception as _e:
    _secrets_error = str(_e)

# Mostrar estado temprano si faltan secrets criticos (debug de deploy)
_required = ["GOOGLE_SA_JSON", "DRIVIN_API_KEY", "GOOGLE_SHEETS_KOWEN_ID"]
_missing = [k for k in _required if not os.environ.get(k)]
if _missing:
    st.error(
        f"**Faltan secrets criticos:** {', '.join(_missing)}\n\n"
        f"Secrets cargados: {_secrets_loaded or '(ninguno)'}\n\n"
        f"Error al leer st.secrets: {_secrets_error or 'ninguno'}\n\n"
        "Configurar en Streamlit Cloud: Settings -> Secrets."
    )
    st.stop()

# Limpiar cache corrupto de Streamlit (previene error "null bytes")
_cache_dir = os.path.join(os.path.dirname(__file__), ".streamlit", "cache")
if os.path.exists(_cache_dir):
    try:
        shutil.rmtree(_cache_dir)
    except Exception as e:
        print(f"[warn] No se pudo limpiar cache Streamlit: {e}")

import sheets_client
import operations
import log_client
import bsale_client
import drivin_client
import address_matcher
import observability

observability.init_sentry(component="streamlit")


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

    st.caption("La rutina diaria corre automatica 7am lun-vie. Usa 🔧 Mantenimiento si necesitas forzarla.")

    # --- Agregar pedido manual ---
    st.markdown("---")
    with st.expander("➕  Agregar pedido manual", expanded=True):
        import frontend_helpers as fh
        from streamlit_searchbox import st_searchbox

        addr_cache = st.session_state.addresses_cache

        # Cargar cache de clientes una vez por sesion (invalidar con boton si hace falta)
        if "clientes_cache" not in st.session_state:
            try:
                st.session_state.clientes_cache = sheets_client.get_clientes()
            except Exception:
                st.session_state.clientes_cache = []

        # Inicializar datos del form
        if "nuevo_pedido_data" not in st.session_state:
            st.session_state.nuevo_pedido_data = {
                "cliente": "", "telefono": "", "email": "",
                "direccion": "", "depto": "", "comuna": "",
                "codigo_drivin": "", "marca": "KOWEN",
            }

        def _search_fn(query):
            results = fh.search_unified(
                query,
                st.session_state.clientes_cache,
                addr_cache,
                limit=10,
            )
            return [(r["label"], r["key"]) for r in results]

        selected_key = st_searchbox(
            _search_fn,
            placeholder="Buscar cliente, tel, email o direccion…",
            key="searchbox_cliente",
            clear_on_submit=False,
        )

        # Si el usuario selecciono algo, auto-rellenar
        if selected_key and selected_key != st.session_state.get("last_selected_key"):
            st.session_state.last_selected_key = selected_key
            if selected_key.startswith("cli::"):
                nombre = selected_key[5:]
                for c in st.session_state.clientes_cache:
                    if c.get("Nombre") == nombre:
                        st.session_state.nuevo_pedido_data = fh.cliente_to_form_data(c)
                        break
            elif selected_key.startswith("dir::"):
                for a in (addr_cache or []):
                    code = a.get("code", "") if isinstance(a, dict) else getattr(a, "code", "")
                    addr = a.get("address1", "") if isinstance(a, dict) else getattr(a, "address1", "")
                    if f"dir::{code or addr}" == selected_key:
                        city = a.get("city", "") if isinstance(a, dict) else getattr(a, "city", "")
                        name = a.get("name", "") if isinstance(a, dict) else getattr(a, "name", "")
                        st.session_state.nuevo_pedido_data = fh.direccion_to_form_data({
                            "codigo_drivin": code,
                            "direccion": addr,
                            "comuna": city,
                            "nombre_ref": name,
                        })
                        break
            st.rerun()

        data = st.session_state.nuevo_pedido_data

        # Deteccion de duplicado: pedidos hoy para mismo cliente/dir/codigo
        try:
            _pedidos_hoy = sheets_client.get_pedidos(datetime.now().strftime("%d/%m/%Y"))
        except Exception:
            _pedidos_hoy = []
        dups = fh.pedidos_mismo_cliente_hoy(
            _pedidos_hoy,
            cliente=data.get("cliente"),
            direccion=data.get("direccion"),
            codigo_drivin=data.get("codigo_drivin"),
        )
        if dups and (data.get("cliente") or data.get("direccion")):
            st.warning(
                f"⚠️ Ya hay {len(dups)} pedido(s) hoy para este cliente/direccion: "
                + ", ".join(f"#{p.get('#', '?')}" for p in dups[:5])
            )

        with st.form("form_nuevo_pedido", clear_on_submit=True):
            new_fecha = st.date_input("Fecha del pedido", value=datetime.now().date(), key="new_fecha")
            new_dir = st.text_input("Direccion", value=data["direccion"])
            c1, c2 = st.columns(2)
            with c1:
                new_depto = st.text_input("Depto", value=data["depto"], placeholder="1511")
            with c2:
                new_comuna = st.text_input("Comuna", value=data["comuna"])
            c3, c4 = st.columns(2)
            with c3:
                new_cant = st.number_input("Cant", min_value=0, value=3, step=1)
            with c4:
                marca_default = data.get("marca", "KOWEN") or "KOWEN"
                marca_idx = ["KOWEN", "CACTUS"].index(marca_default) if marca_default in ["KOWEN", "CACTUS"] else 0
                new_marca = st.selectbox("Marca", ["KOWEN", "CACTUS"], index=marca_idx)
            c5, c6 = st.columns(2)
            with c5:
                new_canal = st.selectbox("Canal", ["MANUAL", "WSP", "EMAIL", "WEB"])
            with c6:
                new_doc = st.selectbox("Doc", ["Boleta", "Factura", "Guia", "Ticket"])
            new_cliente = st.text_input("Cliente", value=data["cliente"], placeholder="Nombre")
            new_telefono = st.text_input("Telefono", value=data["telefono"], placeholder="912345678")
            new_email = st.text_input("Email", value=data["email"], placeholder="correo@ejemplo.com")
            new_codigo = st.text_input("Codigo drivin", value=data["codigo_drivin"], placeholder="auto si se detecta")
            new_obs = st.text_input("Obs", placeholder="Ej: retirar bidones")

            col_sub, col_reset = st.columns([3, 1])
            submitted = col_sub.form_submit_button("Agregar pedido", type="primary", use_container_width=True)
            reset = col_reset.form_submit_button("Limpiar", use_container_width=True)

            if reset:
                st.session_state.nuevo_pedido_data = {
                    "cliente": "", "telefono": "", "email": "",
                    "direccion": "", "depto": "", "comuna": "",
                    "codigo_drivin": "", "marca": "KOWEN",
                }
                st.session_state.last_selected_key = None
                st.rerun()

            if submitted and new_dir:
                num = sheets_client.add_pedido({
                    "fecha": new_fecha.strftime("%d/%m/%Y"),
                    "direccion": new_dir, "depto": new_depto, "comuna": new_comuna,
                    "codigo_drivin": new_codigo, "cant": new_cant,
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
                            "codigo_drivin": new_codigo, "marca": new_marca,
                        })
                        st.info(f"Cliente '{new_cliente}' registrado.")
                        # Invalidar cache de clientes para que aparezca en el proximo search
                        st.session_state.pop("clientes_cache", None)
                st.session_state.nuevo_pedido_data = {
                    "cliente": "", "telefono": "", "email": "",
                    "direccion": "", "depto": "", "comuna": "",
                    "codigo_drivin": "", "marca": "KOWEN",
                }
                st.session_state.last_selected_key = None
                st.rerun()

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
                    except Exception as e:
                        print(f"[warn] get_scenarios_by_date {fecha_b} falló: {e}")
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

    # --- Mantenimiento (todo lo que el cron hace solo) ---
    st.markdown("---")
    with st.expander("🔧  Mantenimiento", expanded=False):
        st.caption("Acciones que el cron corre solo. Usa si necesitas forzar.")
        m_t1, m_t2, m_t3, m_t4, m_t5, m_t6 = st.tabs(
            ["Rutina", "Bsale", "P. Reparto", "P. Cactus", "Limpiar", "Cache"]
        )

        # --- Rutina ---
        with m_t1:
            if st.button("🚀 Forzar rutina diaria", key="btn_rutina", use_container_width=True, type="primary"):
                with st.spinner("Ejecutando rutina diaria..."):
                    try:
                        resultado = operations.rutina_diaria()
                        lines = []
                        if resultado["entregados_ayer"] or resultado["movidos_a_hoy"]:
                            lines.append(f"**Ayer ({resultado['fecha_ayer']}):**")
                            lines.append(f"  Entregados: {resultado['entregados_ayer']}")
                            lines.append(f"  Movidos a hoy: {resultado['movidos_a_hoy']}")
                        if resultado["planilla_importados"]:
                            lines.append(f"Planilla: +{resultado['planilla_importados']} pedidos")
                        if resultado.get("cactus_importados"):
                            lines.append(f"Cactus: +{resultado['cactus_importados']} pedidos")
                        bsale_pend = resultado.get("bsale_pendientes", [])
                        if bsale_pend:
                            lines.append(f"⚠️ {len(bsale_pend)} pedidos Bsale sin planilla (pasar manualmente)")
                        if resultado["codigos_asignados"]:
                            lines.append(f"Códigos asignados: {resultado['codigos_asignados']}")
                        if resultado.get("drivin_subidos"):
                            lines.append(f"driv.in: {resultado['drivin_subidos']} subidos al plan **{resultado.get('drivin_plan', '')}**")
                            token = resultado.get("drivin_token", "")
                            if token:
                                st.session_state.scenario_token = token
                                st.session_state.scenario_name = resultado.get("drivin_plan", "")
                        if resultado["errores"]:
                            for err in resultado["errores"]:
                                lines.append(f"⚠️ {err}")
                        if lines:
                            st.success("\n\n".join(lines))
                        else:
                            st.info("Sin cambios. Todo al día.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")

        # --- Bsale ---
        with m_t2:
            bsale_fecha = st.date_input("Fecha destino", value=datetime.now().date(), key="bsale_fecha")

            if st.button("🔄 Sincronizar pedidos web", key="btn_bsale_check", use_container_width=True):
                with st.spinner("Consultando Bsale..."):
                    try:
                        all_p = sheets_client.get_pedidos()
                        last_bsale = 0
                        for p in all_p:
                            b = p.get("Pedido Bsale", "")
                            if b and b.isdigit():
                                last_bsale = max(last_bsale, int(b))
                        since = max(last_bsale - 5, 1) if last_bsale > 0 else 3480

                        orders = bsale_client.get_web_orders(since)
                        activos = [o for o in orders if o["estado"] == "activo"]
                        if activos:
                            checked = operations.check_bsale_orders(activos)
                            st.session_state["bsale_checked"] = checked

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

                    has_plan = st.session_state.scenario_token is not None
                    btn_label = f"Importar {len(matched)} + subir a driv.in" if has_plan else f"Importar {len(matched)} pedidos"
                    if not has_plan:
                        st.caption("Conecta un plan driv.in para subirlos automaticamente.")

                    if st.button(btn_label, key="btn_bsale_go", type="primary", use_container_width=True):
                        fecha_dest = bsale_fecha.strftime("%d/%m/%Y")
                        fecha_suffix = bsale_fecha.strftime("%m%d")

                        pedidos_nuevos = []
                        for i, m in enumerate(matched):
                            o = m["order"]
                            code = bsale_codes.get(i, "")
                            conf = m["confidence"]
                            if conf in ("ambiguous", "none") and code:
                                address_matcher.save_memory_entry(o.get("direccion", ""), code)
                                log_client.log_match_manual(o.get("direccion", ""), code)
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

        # --- Planilla Reparto ---
        with m_t3:
            rep_date = st.date_input("Fecha a importar", value=datetime.now().date(), key="rep_date")
            rep_fecha_str = rep_date.strftime("%d/%m/%Y")
            if st.button("Importar PRIMER TURNO", key="btn_reparto", use_container_width=True):
                with st.spinner(f"Leyendo planilla reparto ({rep_fecha_str})..."):
                    try:
                        count = operations.sync_from_planilla_reparto(rep_fecha_str)
                        if count > 0:
                            st.success(f"{count} pedidos importados!")
                            st.rerun()
                        else:
                            st.info("Sin pedidos nuevos para esa fecha.")
                    except Exception as e:
                        st.error(f"Error: {e}")

        # --- Planilla Cactus ---
        with m_t4:
            cac_date = st.date_input("Fecha Cactus", value=datetime.now().date(), key="cac_date")
            cac_fecha_str = cac_date.strftime("%d/%m/%Y")
            if st.button("Importar Cactus", key="btn_cactus", use_container_width=True):
                with st.spinner(f"Leyendo planilla Cactus ({cac_fecha_str})..."):
                    try:
                        count = operations.sync_from_planilla_cactus(cac_fecha_str)
                        if count > 0:
                            st.success(f"{count} pedidos Cactus importados!")
                            st.rerun()
                        else:
                            st.info("Sin pedidos Cactus nuevos para esa fecha.")
                    except Exception as e:
                        st.error(f"Error: {e}")

        # --- Limpiar pedidos ---
        with m_t5:
            st.caption("Elimina TODOS los pedidos de una fecha.")
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

        # --- Cache direcciones ---
        with m_t6:
            st.caption("Descarga direcciones de driv.in para autocompletar.")
            if st.button("🔄 Actualizar cache", key="btn_cache", use_container_width=True):
                with st.spinner("Descargando..."):
                    count = address_matcher.refresh_cache()
                    st.session_state.addresses_cache = address_matcher.load_cache()
                    st.success(f"{count} direcciones.")

    # --- Alertas de errores recurrentes ---
    try:
        errores_rec = log_client.get_errores_recurrentes(dias=7)
        if errores_rec:
            st.markdown("---")
            st.markdown('<p class="section-title">Alertas</p>', unsafe_allow_html=True)
            for err in errores_rec:
                st.warning(f"**{err['accion']}** — {err['conteo']}x en 7 dias. Ej: {err['ejemplo'][:60]}")
    except Exception as e:
        print(f"[warn] No se pudieron obtener errores recurrentes: {e}")

    st.markdown("---")
    st.caption("Kowen v2.2 — Gestor de Pedidos")


# ============================================================
# TABS PRINCIPALES
# ============================================================

tab_op, tab_lotes, tab_cl, tab_cr, tab_sync, tab_log = st.tabs([
    "🚨 Operación", "📋 Carga rápida", "👥 Clientes", "📧 Correos", "🔄 Sync driv.in", "🗂 Log",
])

with tab_op:
    import reports as _reports
    import pandas as pd

    # =========== Controles superiores (fijos) ===========
    ctl1, ctl2, ctl3 = st.columns([1.2, 1, 2])
    with ctl1:
        op_fecha = st.date_input("Fecha", value=datetime.now().date(), key="op_fecha")
    with ctl2:
        op_marca = st.selectbox("Marca", ["Todas", "Kowen", "Cactus"], key="op_marca")
    with ctl3:
        st.write("")
        if st.button("🔄 Refrescar datos", key="btn_op_refresh", use_container_width=True):
            st.rerun()
    op_fecha_str = op_fecha.strftime("%d/%m/%Y")

    # =========== Cargar datos ===========
    try:
        ruta = _reports.get_ruta_del_dia(op_fecha_str)
    except Exception as e:
        st.error(f"Error leyendo ruta del dia: {e}")
        ruta = {"pedidos": [], "stats": {}, "scenario": {"existe": False}, "solo_en_drivin": []}

    try:
        diag = operations.diagnostico_salud(dias_estancado=2)
    except Exception as e:
        st.warning(f"No se pudo leer diagnostico de salud: {e}")
        diag = None

    pedidos_ruta = ruta["pedidos"]
    if op_marca != "Todas":
        pedidos_ruta = [p for p in pedidos_ruta if (p.get("marca", "") or "").lower() == op_marca.lower()]
    stats = ruta["stats"]
    scenario = ruta["scenario"]

    # =========== Alert bar (issues accionables) ===========
    issues = []
    if diag:
        if diag["huerfanos"]:
            issues.append(f"💸 {len(diag['huerfanos'])} huérfanos de pago")
        if diag["estancados"]:
            issues.append(f"🕰 {len(diag['estancados'])} estancados ≥2d")
        if diag["pagos_sin_pedido"]:
            issues.append(f"❓ {len(diag['pagos_sin_pedido'])} pagos sin pedido")
        if diag["pendientes_sin_codigo"]:
            issues.append(f"🔖 {len(diag['pendientes_sin_codigo'])} sin código drivin")
    solo_drivin_count = len(ruta.get("solo_en_drivin", []))
    if solo_drivin_count:
        issues.append(f"⚠ {solo_drivin_count} pedidos en driv.in sin planilla")

    if issues:
        st.warning("  ·  ".join(issues) + "  →  ver en sub-tab *Cobros* o *Adquisición*")

    # =========== KPI strip (botellones) ===========
    total_ped = stats.get("total", 0)
    bot_total = stats.get("botellones_total", 0)
    bot_entr = stats.get("botellones_entregados", 0)
    bot_cob = stats.get("botellones_cobrados", 0)
    bot_pc = stats.get("botellones_por_cobrar", 0)
    por_cobrar_monto = stats.get("por_cobrar", 0)

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Pedidos del día", total_ped,
              delta=f"{bot_total} botellones", delta_color="off")
    k2.metric("Botellones ENTREGADOS",
              f"{bot_entr} / {bot_total}",
              delta=f"{stats.get('pct_bot_entregados', 0)}%")
    k3.metric("Botellones COBRADOS",
              f"{bot_cob} / {bot_total}",
              delta=f"{stats.get('pct_bot_cobrados', 0)}%")
    k4.metric("Por cobrar",
              f"{bot_pc} botellones",
              delta=f"${por_cobrar_monto:,.0f}".replace(",", "."),
              delta_color="inverse")

    # =========== Sub-tabs ===========
    sub_hoy, sub_plan, sub_cobros, sub_adq, sub_hist = st.tabs([
        "📋 Hoy", "🚚 Plan driv.in", "💰 Cobros", "📥 Adquisición", "📈 Histórico",
    ])

    # ------------ SUB-TAB HOY (listado principal) ------------
    with sub_hoy:
        if not pedidos_ruta:
            st.info("No hay pedidos para esta fecha.")
        else:
            def _row_style(r):
                """Devuelve style CSS completo para <tr>: fondo + borde izq si accionable.
                Colores solidos pastel que se ven bien tanto en light como dark theme."""
                est = r["estado"]; pago = r["estado_pago"]
                if est == "ENTREGADO" and pago == "PAGADO":
                    # Verde pastel — completo (objetivo cumplido)
                    return "background:#d1fae5; border-left:3px solid #16a34a;"
                if est == "ENTREGADO":
                    # Amarillo pastel — entregado sin pagar (accionable: cobrar)
                    return "background:#fef3c7; border-left:3px solid #eab308;"
                if est == "NO ENTREGADO":
                    # Rojo pastel — no entregado (accionable: reintentar)
                    return "background:#fee2e2; border-left:3px solid #ef4444;"
                if est == "EN CAMINO":
                    # Azul pastel — en proceso
                    return "background:#dbeafe; border-left:3px solid #3b82f6;"
                # Pendiente — gris muy claro para distinguir del fondo
                return "background:#f9fafb; border-left:3px solid #e5e7eb;"

            def _completo_icon(r):
                """Icono ✓ grande al final de las filas completas."""
                if r["estado"] == "ENTREGADO" and r["estado_pago"] == "PAGADO":
                    return '<span style="color:#16a34a; font-weight:700; font-size:16px;">✓</span>'
                return ""

            def _badge(text, color):
                return (f'<span style="background:{color}; color:#fff; padding:2px 8px; '
                        f'border-radius:10px; font-size:11px; font-weight:600;">{text}</span>')

            def _estado_cell(est):
                cmap = {
                    "ENTREGADO": "#16a34a", "EN CAMINO": "#3b82f6",
                    "PENDIENTE": "#64748b", "NO ENTREGADO": "#ef4444",
                }
                return _badge(est or "—", cmap.get(est, "#64748b"))

            def _pago_cell(pago, est):
                if pago == "PAGADO":
                    return _badge("PAGADO ✓", "#15803d")
                if est == "ENTREGADO":
                    return _badge("POR COBRAR", "#f59e0b")
                return '<span style="color:#888; font-size:11px;">—</span>'

            rows_html = []
            for r in pedidos_ruta:
                dir_display = r["direccion"] + (f", {r['depto']}" if r.get("depto") else "")
                monto_str = f"${r['monto']:,.0f}".replace(",", ".")
                # Colores de texto neutros (legibles en light y dark theme)
                rows_html.append(
                    f'<tr style="{_row_style(r)}">'
                    f'<td style="padding:7px 10px; font-family:monospace; color:#6b7280;">#{r["numero"]}</td>'
                    f'<td style="padding:7px 10px; color:#111827;"><b>{r["cliente"] or "—"}</b></td>'
                    f'<td style="padding:7px 10px; color:#374151;">{dir_display}</td>'
                    f'<td style="padding:7px 10px; color:#374151;">{r["comuna"] or "—"}</td>'
                    f'<td style="padding:7px 10px; text-align:center; color:#111827; font-weight:600;">{r["cantidad"] or "—"}</td>'
                    f'<td style="padding:7px 10px; color:#374151;">{r["repartidor"] or "—"}</td>'
                    f'<td style="padding:7px 10px;">{_estado_cell(r["estado"])}</td>'
                    f'<td style="padding:7px 10px;">{_pago_cell(r["estado_pago"], r["estado"])}</td>'
                    f'<td style="padding:7px 10px; font-family:monospace; text-align:right; color:#111827; font-weight:600;">{monto_str}</td>'
                    f'<td style="padding:7px 10px; text-align:center; width:28px;">{_completo_icon(r)}</td>'
                    f'</tr>'
                )

            # HTML en UNA sola linea (sin indentacion) para que Streamlit lo renderice
            head_cols = (
                '<th style="padding:10px; font-size:11px; text-transform:uppercase;">#</th>'
                '<th style="padding:10px; font-size:11px; text-transform:uppercase;">Cliente</th>'
                '<th style="padding:10px; font-size:11px; text-transform:uppercase;">Dirección</th>'
                '<th style="padding:10px; font-size:11px; text-transform:uppercase;">Comuna</th>'
                '<th style="padding:10px; font-size:11px; text-transform:uppercase; text-align:center;">Cant</th>'
                '<th style="padding:10px; font-size:11px; text-transform:uppercase;">Repartidor</th>'
                '<th style="padding:10px; font-size:11px; text-transform:uppercase;">Estado</th>'
                '<th style="padding:10px; font-size:11px; text-transform:uppercase;">Pago</th>'
                '<th style="padding:10px; font-size:11px; text-transform:uppercase; text-align:right;">Monto</th>'
                '<th style="padding:10px; font-size:11px; text-transform:uppercase; text-align:center;">✓</th>'
            )
            table_html = (
                '<div style="max-height:640px; overflow-y:auto; border:1px solid #e5e7eb; border-radius:6px;">'
                '<table style="width:100%; border-collapse:collapse; font-size:13px;">'
                '<thead style="position:sticky; top:0; background:#f3f4f6;">'
                f'<tr style="text-align:left; color:#374151; border-bottom:1px solid #d1d5db;">{head_cols}</tr>'
                '</thead>'
                f'<tbody>{"".join(rows_html)}</tbody>'
                '</table>'
                '</div>'
            )
            st.markdown(table_html, unsafe_allow_html=True)

            # --- Eliminar pedido manualmente ---
            st.markdown("")
            with st.expander("🗑 Eliminar pedido individual", expanded=False):
                numeros = [str(p.get("numero", "")) for p in pedidos_ruta if p.get("numero")]
                if not numeros:
                    st.info("Sin pedidos para eliminar.")
                else:
                    ec1, ec2, ec3 = st.columns([2, 2, 1])
                    with ec1:
                        nro_sel = st.selectbox(
                            "Elegí el pedido a eliminar",
                            options=numeros,
                            format_func=lambda n: next(
                                (f"#{n} — {p.get('cliente','')[:25] or '—'} · "
                                 f"{p.get('direccion','')[:30]}"
                                 for p in pedidos_ruta if str(p.get("numero","")) == n),
                                f"#{n}"
                            ),
                            key="op_del_select",
                        )
                    with ec2:
                        confirm = st.checkbox(
                            "Confirmo que quiero eliminar este pedido",
                            key="op_del_confirm",
                        )
                    with ec3:
                        st.write("")
                        if st.button("🗑 Eliminar", key="op_del_btn",
                                     disabled=not confirm, type="primary",
                                     use_container_width=True):
                            try:
                                sheets_client.delete_pedido(int(nro_sel))
                                st.success(f"Pedido #{nro_sel} eliminado")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error: {e}")

    # ------------ SUB-TAB PLAN DRIV.IN ------------
    with sub_plan:
        if scenario.get("existe"):
            sc1, sc2, sc3 = st.columns(3)
            sc1.metric("Plan", scenario.get("description", "—"))
            sc2.metric("Estado", scenario.get("status", "?"))
            sc3.metric("Pedidos en el plan", sum(1 for p in pedidos_ruta if p.get("en_drivin")))
            st.caption(f"Token: `{(scenario.get('token') or '')[:20]}…`")
        else:
            st.info(f"Sin plan driv.in asociado a {op_fecha_str}.")

        # --- Diagnostico por fuente (canal + marca) ---
        st.markdown("---")
        st.markdown("##### 🔍 Diagnóstico de fuentes (breakdown del día)")
        st.caption("Te muestra de dónde viene cada pedido — útil para identificar desfases con las planillas Kowen/Cactus.")

        def _cant_of(p):
            try:
                return int(str(p.get("cantidad", 0) or 0).strip() or 0)
            except (ValueError, TypeError):
                return 0

        # Breakdown por canal (pedidos + botellones)
        by_canal = {}
        for p in pedidos_ruta:
            c = (p.get("canal") or "—").strip() or "(sin canal)"
            by_canal.setdefault(c, {"pedidos": 0, "botellones": 0})
            by_canal[c]["pedidos"] += 1
            by_canal[c]["botellones"] += _cant_of(p)

        # Breakdown por marca
        by_marca = {}
        for p in pedidos_ruta:
            m = (p.get("marca") or "—").strip().upper() or "—"
            by_marca.setdefault(m, {"pedidos": 0, "botellones": 0})
            by_marca[m]["pedidos"] += 1
            by_marca[m]["botellones"] += _cant_of(p)

        cd1, cd2 = st.columns(2)
        with cd1:
            st.markdown("**Por canal**")
            df_c = pd.DataFrame([
                {"Canal": k, "Pedidos": v["pedidos"], "Botellones": v["botellones"]}
                for k, v in sorted(by_canal.items(), key=lambda x: -x[1]["botellones"])
            ])
            st.dataframe(df_c, use_container_width=True, hide_index=True)

        with cd2:
            st.markdown("**Por marca**")
            df_m = pd.DataFrame([
                {"Marca": k, "Pedidos": v["pedidos"], "Botellones": v["botellones"]}
                for k, v in sorted(by_marca.items(), key=lambda x: -x[1]["botellones"])
            ])
            st.dataframe(df_m, use_container_width=True, hide_index=True)

        total_bot = sum(v["botellones"] for v in by_canal.values())
        total_drivin_bot = sum(_cant_of(p) for p in pedidos_ruta if p.get("en_drivin"))
        fuera_bot = total_bot - total_drivin_bot
        st.caption(
            f"**Total OPERACION DIARIA:** {len(pedidos_ruta)} pedidos · {total_bot} botellones  ·  "
            f"**En drivin:** {total_drivin_bot} botellones  ·  "
            f"**Fuera de drivin:** {fuera_bot}"
        )

        # --- Comparar con Planillas Kowen y Cactus ---
        st.markdown("---")
        st.markdown("##### 📒 Comparar con Planillas Kowen y Cactus")
        st.caption("Revisa cuántos pedidos de OPERACION DIARIA matchean con cada planilla fuente.")

        if st.button("🔍 Comparar con planillas", key="btn_diag_planillas", use_container_width=True):
            with st.spinner("Leyendo planillas Kowen, Cactus y OPERACION DIARIA..."):
                try:
                    diag_p = operations.diagnostico_vs_planillas(fecha=op_fecha_str)
                    st.session_state["_diag_planillas"] = diag_p
                except Exception as e:
                    st.error(f"Error: {e}")

        dp = st.session_state.get("_diag_planillas", None)
        if dp:
            dc1, dc2, dc3 = st.columns(3)
            dc1.metric("OPERACION DIARIA",
                       f"{dp['total_operacion']} ped",
                       delta=f"{dp['bot_operacion']} botellones", delta_color="off")
            dc2.metric("Planilla Kowen",
                       f"{dp['total_kowen']} ped",
                       delta=f"{dp['bot_kowen']} botellones", delta_color="off")
            dc3.metric("Planilla Cactus",
                       f"{dp['total_cactus']} ped",
                       delta=f"{dp['bot_cactus']} botellones", delta_color="off")

            mk = len(dp['matcheados_kowen'])
            mc = len(dp['matcheados_cactus'])
            smh = len(dp['sin_match_historico'])
            smp = len(dp['sin_match_pendientes'])
            fk = len(dp['faltantes_kowen'])
            fc = len(dp['faltantes_cactus'])

            st.info(
                f"**Análisis del match:**\n\n"
                f"- ✅ En OPERACION y matchean con **Planilla Kowen**: {mk}\n"
                f"- ✅ En OPERACION y matchean con **Planilla Cactus**: {mc}\n"
                f"- 📦 En OPERACION sin match pero **histórico** (ENTREGADO/PAGADO/NO ENTREGADO): {smh}\n"
                f"- 🗑 En OPERACION **PENDIENTE sin match** en ninguna planilla (candidatos a borrar): **{smp}**\n"
                f"- ➕ En Planilla Kowen **faltantes** en OPERACION: {fk}\n"
                f"- ➕ En Planilla Cactus **faltantes** en OPERACION: {fc}"
            )

            if dp['sin_match_pendientes']:
                with st.expander(f"🗑 Ver los {smp} pendientes sin match en planilla", expanded=False):
                    st.caption("Cada uno tiene un botón 🗑 para eliminarlo individualmente.")
                    # Header
                    hc = st.columns([0.6, 1.8, 2.2, 1.2, 0.5, 0.9, 1, 0.7])
                    for col, titulo in zip(hc, ["#", "Cliente", "Dirección", "Comuna", "Cant", "Marca", "Canal", ""]):
                        col.caption(f"**{titulo}**")
                    for p in dp['sin_match_pendientes']:
                        nro_str = str(p.get("#","")).strip()
                        cols = st.columns([0.6, 1.8, 2.2, 1.2, 0.5, 0.9, 1, 0.7])
                        cols[0].markdown(f"`#{nro_str}`")
                        cols[1].write(p.get("Cliente","") or "—")
                        cols[2].write(p.get("Direccion",""))
                        cols[3].write(p.get("Comuna",""))
                        cols[4].write(p.get("Cant",""))
                        cols[5].write(p.get("Marca",""))
                        cols[6].write(p.get("Canal",""))
                        with cols[7]:
                            if st.button("🗑", key=f"del_smp_{nro_str}", help=f"Eliminar pedido #{nro_str}"):
                                try:
                                    sheets_client.delete_pedido(int(nro_str))
                                    st.success(f"#{nro_str} eliminado")
                                    # Actualizar el diagnostico en session
                                    st.session_state["_diag_planillas"]["sin_match_pendientes"] = [
                                        x for x in dp['sin_match_pendientes']
                                        if str(x.get("#","")).strip() != nro_str
                                    ]
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")

            if dp['faltantes_kowen'] or dp['faltantes_cactus']:
                with st.expander(f"➕ Ver {fk + fc} pedidos de planilla sin reflejo en OPERACION", expanded=False):
                    combinados = [{**p, "_fuente": "Kowen"} for p in dp['faltantes_kowen']] + \
                                 [{**p, "_fuente": "Cactus"} for p in dp['faltantes_cactus']]
                    df_fc = pd.DataFrame([
                        {"Fuente": c.get("_fuente"), "Cliente": c.get("cliente","") or "—",
                         "Dirección": c.get("direccion",""), "Depto": c.get("depto","") or "—",
                         "Comuna": c.get("comuna","") or "—", "Cant": c.get("cant",""),
                         "Marca": c.get("marca",""), "Estado": c.get("estado_pedido","")}
                        for c in combinados
                    ])
                    st.dataframe(df_fc, use_container_width=True, hide_index=True)

            if smp > 0:
                st.markdown("")
                st.warning(
                    f"⚠ Hay **{smp} pedidos PENDIENTE** en OPERACION DIARIA que no existen en ninguna planilla fuente. "
                    "Son candidatos a borrar (probablemente residuos o importaciones viejas)."
                )

        # Detalle de los pedidos fuera de drivin
        fuera = [p for p in pedidos_ruta if not p.get("en_drivin")]
        if fuera:
            sin_cod = [p for p in fuera if not (p.get("codigo_drivin") or "").strip()]
            con_cod = [p for p in fuera if (p.get("codigo_drivin") or "").strip()]
            with st.expander(
                f"📋 Ver los {len(fuera)} pedidos fuera de drivin "
                f"({len(sin_cod)} sin código · {len(con_cod)} con código no subido)",
                expanded=False,
            ):
                if sin_cod:
                    st.markdown(f"**🔖 Sin código drivin ({len(sin_cod)}):**")
                    df_sc = pd.DataFrame([
                        {"#": p["numero"], "Cliente": p["cliente"] or "—",
                         "Dirección": p["direccion"], "Comuna": p["comuna"],
                         "Cant": p["cantidad"], "Marca": p["marca"],
                         "Canal": p["canal"], "Estado": p["estado"]}
                        for p in sin_cod
                    ])
                    st.dataframe(df_sc, use_container_width=True, hide_index=True)
                if con_cod:
                    st.markdown(f"**⚠ Con código drivin pero no en el plan ({len(con_cod)}):**")
                    df_cc = pd.DataFrame([
                        {"#": p["numero"], "Cliente": p["cliente"] or "—",
                         "Dirección": p["direccion"], "Comuna": p["comuna"],
                         "Cant": p["cantidad"], "Marca": p["marca"],
                         "Código": p["codigo_drivin"],
                         "Canal": p["canal"], "Estado": p["estado"]}
                        for p in con_cod
                    ])
                    st.dataframe(df_cc, use_container_width=True, hide_index=True)
                    st.caption(
                        "Estos tienen código pero no aparecen en el scenario drivin de hoy. "
                        "Puede ser que se hayan subido a otro plan o que falte hacer el sync."
                    )

        # Ejecutar rutina completa AHORA
        st.markdown("---")
        st.markdown("##### 🚀 Acciones rápidas")
        st.caption(
            "Normalmente la rutina corre automática cada hora (9-19h lun-vie). "
            "Usá estos botones solo si querés forzar una ejecución ahora."
        )
        rc1, rc2, rc3 = st.columns(3)
        with rc1:
            if st.button("🚀 Ejecutar rutina AHORA",
                         key="btn_rutina_now", type="primary", use_container_width=True,
                         help="Corre el flujo completo: importa planillas + asigna códigos + sube a drivin + optimize/approve + verifica PODs"):
                with st.spinner("Ejecutando rutina completa (puede tardar 30-60s)..."):
                    try:
                        r = operations.rutina_diaria(fecha_hoy=op_fecha_str)
                        msg_parts = [
                            f"✓ Planilla +{r.get('planilla_importados',0)}",
                            f"Cactus +{r.get('cactus_importados',0)}",
                            f"Códigos {r.get('codigos_asignados',0)}",
                            f"drivin +{r.get('drivin_subidos',0)}",
                        ]
                        avance = r.get("drivin_avance", {}) or {}
                        if avance.get("accion") and avance.get("accion") != "skip":
                            msg_parts.append(f"Scenario → {avance['accion']}")
                        st.success(" · ".join(msg_parts))
                        if r.get("errores"):
                            for err in r["errores"]:
                                st.warning(err)
                    except Exception as e:
                        st.error(f"Error: {e}")
        with rc2:
            if st.button("🔄 Solo verificar PODs",
                         key="btn_verify_now", use_container_width=True,
                         help="Consulta PODs de drivin y actualiza ENTREGADO/NO ENTREGADO. No sube pedidos nuevos."):
                with st.spinner("Consultando PODs de drivin..."):
                    try:
                        v = operations.verify_orders_drivin(fecha=op_fecha_str, auto_update=True)
                        st.session_state["_drivin_v"] = v
                        st.success(
                            f"✓ Verificados: {v.get('total_verificados',0)}  ·  "
                            f"Actualizados: **{v.get('actualizados',0)}**  ·  "
                            f"Entregados detectados: **{v.get('entregados_detectados',0)}**"
                        )
                    except Exception as e:
                        st.error(f"Error: {e}")
        with rc3:
            if st.button("🔍 Planes sin despachar",
                         key="btn_plan_chk", use_container_width=True):
                with st.spinner("Consultando driv.in..."):
                    try:
                        v = operations.verify_orders_drivin(fecha=op_fecha_str, auto_update=False)
                        st.session_state["_drivin_v"] = v
                    except Exception as e:
                        st.error(f"Error: {e}")

        v = st.session_state.get("_drivin_v", None)
        if v is not None:
            planes = v.get("planes_sin_despachar", [])
            if planes:
                st.warning(f"{len(planes)} plan(es) sin despachar")
                st.dataframe(pd.DataFrame(planes), use_container_width=True, hide_index=True)
            else:
                st.success("Todos los planes iniciados.")

        # Pedidos sin codigo driv.in — lista interactiva con sugerencias
        if diag and diag["pendientes_sin_codigo"]:
            st.markdown("---")
            st.markdown(f"##### 🔖 Pendientes sin código drivin ({len(diag['pendientes_sin_codigo'])})")
            st.caption(
                "Cada pedido tiene su sugerencia del matcher. Los que tienen match auto/memory se pueden "
                "asignar de golpe con el botón grande. Los ambiguos o sin match se resuelven fila por fila."
            )

            # Precomputar sugerencias (cache en session para no recalcular al cada rerun)
            sin_cod_key = f"_sin_cod_sug_{op_fecha_str}"
            if (sin_cod_key not in st.session_state
                    or len(st.session_state[sin_cod_key]) != len(diag["pendientes_sin_codigo"])):
                import address_matcher as _am
                addrs_cache = _am.load_cache()
                enriched_sc = []
                for p in diag["pendientes_sin_codigo"]:
                    res, conf = _am.auto_match(
                        direccion=p.get("direccion", ""),
                        depto=p.get("depto", ""),
                        comuna=p.get("comuna", ""),
                        addresses=addrs_cache,
                    )
                    candidatos = res if conf == "ambiguous" else []
                    codigo = res if conf in ("auto", "memory") else ""
                    enriched_sc.append({**p, "_conf": conf, "_codigo": codigo, "_cand": candidatos})
                st.session_state[sin_cod_key] = enriched_sc

            sin_cod_list = st.session_state[sin_cod_key]
            auto_asignables = [p for p in sin_cod_list if p["_conf"] in ("auto", "memory")]

            if auto_asignables:
                bc1, bc2 = st.columns([2, 1])
                with bc1:
                    st.info(
                        f"✨ **{len(auto_asignables)} pedidos** tienen match automático de alta confianza. "
                        "Podés asignarlos todos de una vez."
                    )
                with bc2:
                    if st.button(f"⚡ Asignar {len(auto_asignables)} códigos auto",
                                 key="btn_assign_auto",
                                 type="primary",
                                 use_container_width=True):
                        with st.spinner("Asignando códigos..."):
                            try:
                                updates = []
                                for p in auto_asignables:
                                    nro = int(str(p.get("numero", "")).strip())
                                    updates.append((nro, {"codigo_drivin": p["_codigo"]}))
                                if updates:
                                    sheets_client.update_pedidos_batch(updates)
                                st.success(f"✓ {len(updates)} códigos asignados. Ahora ejecutá 'Rutina AHORA' para subirlos al plan drivin.")
                                st.session_state.pop(sin_cod_key, None)
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error: {e}")

            # Fila por fila — para los que necesitan revisión manual
            st.markdown("")
            with st.expander(f"Ver lista detallada ({len(sin_cod_list)})", expanded=False):
                for i, p in enumerate(sin_cod_list):
                    conf = p.get("_conf", "none")
                    codigo_sug = p.get("_codigo", "") or ""
                    candidatos = p.get("_cand", []) or []
                    badge = {"auto": "✅ auto", "memory": "🧠 memoria",
                             "ambiguous": "⚠️ elegir", "none": "❓ sin match"}.get(conf, "❓")

                    with st.container(border=True):
                        top = st.columns([3, 1])
                        with top[0]:
                            st.markdown(f"**#{p.get('numero', '?')}** · {p.get('cliente', '') or '—'}")
                            dir_str = p.get("direccion", "")
                            if p.get("depto"):
                                dir_str += f", {p['depto']}"
                            if p.get("comuna"):
                                dir_str += f"  ·  {p['comuna']}"
                            st.caption(dir_str)
                        with top[1]:
                            st.caption(badge)

                        inp_col = st.columns([2, 1])
                        with inp_col[0]:
                            if conf == "ambiguous" and candidatos:
                                opts = [f"{c.code} — {c.name} ({c.city})" for c in candidatos]
                                opts = ["(escribir manualmente)"] + opts
                                sel = st.selectbox("Código", opts,
                                                    key=f"sc_sel_{i}",
                                                    label_visibility="collapsed")
                                if sel == opts[0]:
                                    codigo_in = st.text_input("Manual",
                                                              key=f"sc_man_{i}",
                                                              label_visibility="collapsed",
                                                              placeholder="Escribir código drivin")
                                else:
                                    codigo_in = candidatos[opts.index(sel) - 1].code
                            else:
                                codigo_in = st.text_input("Código drivin",
                                                          value=codigo_sug,
                                                          key=f"sc_code_{i}",
                                                          label_visibility="collapsed",
                                                          placeholder="Escribir código drivin")
                        with inp_col[1]:
                            if st.button("✓ Asignar", key=f"sc_btn_{i}",
                                         use_container_width=True,
                                         disabled=not codigo_in):
                                try:
                                    nro = int(str(p.get("numero", "")).strip())
                                    sheets_client.update_pedido(nro, {"codigo_drivin": codigo_in.strip()})
                                    # Si el match era ambiguo o manual, guardar aprendizaje
                                    if conf in ("ambiguous", "none"):
                                        try:
                                            import address_matcher as _am
                                            _am.save_memory_entry(p.get("direccion", ""), codigo_in.strip())
                                        except Exception:
                                            pass
                                    st.success(f"#{nro} ← {codigo_in.strip()}")
                                    st.session_state.pop(sin_cod_key, None)
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")

        # --- Sincronizacion con drivin (Opcion C selectiva) ---
        st.markdown("---")
        st.markdown("##### 🔄 Sincronizar planilla con plan driv.in")
        st.caption(
            "Borra pedidos PENDIENTE sin reflejo en drivin (residuos) y crea los que "
            "estan en drivin sin planilla. **No toca** drivin, ni pedidos ENTREGADO/PAGADO/"
            "NO ENTREGADO."
        )

        modo_estricto = st.checkbox(
            "🔥 Modo estricto — borrar TODO lo que no esté en drivin por código (también ENTREGADO/PAGADO)",
            value=False,
            key="sync_modo_estricto",
            help="Útil cuando el verify marcó falsamente pedidos viejos como ENTREGADO por match por dirección. "
                 "En modo estricto solo sobreviven los pedidos con código drivin que aparece en el scenario del día.",
        )

        if st.button("🔍 Simular sincronización (dry-run)", key="btn_sync_dryrun", use_container_width=True):
            with st.spinner("Consultando drivin y comparando con planilla..."):
                try:
                    plan = operations.sync_operacion_con_drivin(
                        fecha=op_fecha_str, dry_run=True,
                        modo_estricto=modo_estricto,
                    )
                    plan["_modo_estricto"] = modo_estricto
                    st.session_state["_sync_plan"] = plan
                except Exception as e:
                    st.error(f"Error: {e}")

        plan = st.session_state.get("_sync_plan", None)
        if plan and not plan.get("ejecutado"):
            st.info(
                f"**Plan del dry-run para {plan['fecha']}:**\n\n"
                f"- Scenario drivin: `{plan['scenario'] or '(ninguno)'}`\n"
                f"- Pedidos en planilla hoy: **{plan['total_planilla_antes']}**\n"
                f"- Orders en drivin: **{plan['total_drivin']}**\n"
                f"- A preservar (histórico + pendientes OK): **{len(plan['a_preservar'])}**\n"
                f"- 🗑 A borrar (residuos pendientes): **{len(plan['a_borrar'])}**\n"
                f"- ➕ A crear (drivin sin planilla): **{len(plan['a_crear'])}**\n\n"
                f"Total después: **{len(plan['a_preservar']) + len(plan['a_crear'])}** pedidos"
            )

            if plan["a_borrar"]:
                with st.expander(f"🗑 Ver los {len(plan['a_borrar'])} que se borrarían — desglose por origen", expanded=False):
                    # Breakdown por canal + fecha original (para entender de dónde vienen)
                    from collections import Counter
                    by_canal = Counter((p.get("Canal", "") or "(sin canal)") for p in plan["a_borrar"])
                    fechas_originales = Counter((p.get("Fecha", "") or "(sin fecha)") for p in plan["a_borrar"])
                    sin_codigo = sum(1 for p in plan["a_borrar"] if not (p.get("Codigo Drivin", "") or "").strip())
                    con_bsale = sum(1 for p in plan["a_borrar"] if (p.get("Pedido Bsale", "") or "").strip())

                    st.markdown("**Desglose:**")
                    partes = [f"`{k}`: **{v}**" for k, v in by_canal.most_common()]
                    st.caption("Por canal → " + " · ".join(partes))
                    st.caption(f"Con pedido Bsale asociado: **{con_bsale}** · Sin código drivin: **{sin_codigo}**")
                    if len(fechas_originales) > 1:
                        partes_f = [f"`{k}`: {v}" for k, v in sorted(fechas_originales.items())]
                        st.caption("Fechas originales → " + " · ".join(partes_f))

                    st.markdown("")
                    df_b = pd.DataFrame([
                        {"#": p.get("#", ""),
                         "Fecha orig.": p.get("Fecha", ""),
                         "Canal": p.get("Canal", "") or "(sin canal)",
                         "Cliente": p.get("Cliente", "") or "—",
                         "Dirección": p.get("Direccion", ""),
                         "Comuna": p.get("Comuna", ""),
                         "Cant": p.get("Cant", ""),
                         "Marca": p.get("Marca", ""),
                         "Código": p.get("Codigo Drivin", "") or "—",
                         "Bsale": p.get("Pedido Bsale", "") or "—",
                         "Estado": p.get("Estado Pedido", "")}
                        for p in plan["a_borrar"]
                    ])
                    st.dataframe(df_b, use_container_width=True, hide_index=True)

            if plan["a_crear"]:
                with st.expander(f"➕ Ver los {len(plan['a_crear'])} que se crearían", expanded=False):
                    df_c = pd.DataFrame(plan["a_crear"])
                    st.dataframe(df_c, use_container_width=True, hide_index=True)

            # Multiselect para marcar pedidos que se mueven a mañana en vez de borrarse
            reprogramar = []
            if plan["a_borrar"]:
                st.markdown("")
                st.markdown("**📅 Reprogramar a mañana en vez de borrar** (ej: desasignados, rechazados)")
                opts = [str(p.get("#", "")).strip() for p in plan["a_borrar"]
                        if str(p.get("#", "")).strip()]
                def _fmt_op(n):
                    for p in plan["a_borrar"]:
                        if str(p.get("#", "")).strip() == n:
                            return (f"#{n} — {(p.get('Cliente') or '—')[:25]} · "
                                    f"{p.get('Direccion','')[:30]} · {p.get('Comuna','')[:15]}")
                    return f"#{n}"
                reprogramar = st.multiselect(
                    "Seleccioná los que deben ir al próximo día hábil (el resto se borra):",
                    options=opts,
                    format_func=_fmt_op,
                    key="sync_reprogramar",
                )
                if reprogramar:
                    st.caption(
                        f"✓ {len(reprogramar)} pedido(s) pasarán a mañana con estado PENDIENTE. "
                        f"Los otros {len(opts) - len(reprogramar)} se borrarán."
                    )

            st.warning(
                "⚠️ Si aplicás, **no hay deshacer** — los pedidos borrados quedarán eliminados. "
                "Los reprogramados cambian de fecha y quedan PENDIENTES mañana."
            )
            if st.button("✓ APLICAR sincronización",
                         key="btn_sync_apply", type="primary", use_container_width=True):
                with st.spinner("Aplicando cambios..."):
                    try:
                        r = operations.sync_operacion_con_drivin(
                            fecha=op_fecha_str,
                            dry_run=False,
                            reprogramar_a_manana=reprogramar,
                            modo_estricto=plan.get("_modo_estricto", False),
                        )
                        msg_parts = [f"✓ Borrados: **{r['borrados_ok']}**"]
                        if r.get("reprogramados_ok"):
                            msg_parts.append(
                                f"Reprogramados a {r.get('fecha_reprogramacion','')}: "
                                f"**{r['reprogramados_ok']}**"
                            )
                        if r.get("creados_ok"):
                            msg_parts.append(f"Creados: **{r['creados_ok']}**")
                        st.success(" · ".join(msg_parts))
                        st.session_state.pop("_sync_plan", None)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error al aplicar: {e}")

    # ------------ SUB-TAB COBROS ------------
    with sub_cobros:
        # Por cobrar hoy (entregados sin pago)
        por_cobrar_pedidos = [r for r in pedidos_ruta
                              if r["estado"] == "ENTREGADO" and r["estado_pago"] != "PAGADO"]
        pagados_hoy_pedidos = [r for r in pedidos_ruta if r["estado_pago"] == "PAGADO"]

        ck1, ck2 = st.columns(2)
        ck1.metric("Por cobrar hoy",
                   f"{len(por_cobrar_pedidos)} pedidos",
                   delta=f"${sum(r['monto'] for r in por_cobrar_pedidos):,.0f}".replace(",", "."),
                   delta_color="inverse")
        ck2.metric("Cobrados hoy",
                   f"{len(pagados_hoy_pedidos)} pedidos",
                   delta=f"${sum(r['monto'] for r in pagados_hoy_pedidos):,.0f}".replace(",", "."))

        st.markdown("---")

        # Por cobrar — acciones rápidas
        if por_cobrar_pedidos:
            st.markdown(f"##### 💰 Por cobrar ({len(por_cobrar_pedidos)})")
            def _wa_link(telefono, cliente, pedido, monto):
                import urllib.parse
                tel = "".join(c for c in str(telefono or "") if c.isdigit())
                if not tel:
                    return None
                if not tel.startswith("56"):
                    tel = "56" + tel.lstrip("0")
                msg = (f"Hola {cliente}, te escribimos de Kowen por el pedido #{pedido}. "
                       f"Quedo pendiente el cobro de ${monto:,.0f}. Gracias!").replace(",", ".")
                return f"https://wa.me/{tel}?text={urllib.parse.quote(msg)}"

            for p in por_cobrar_pedidos:
                col = st.columns([0.5, 1.8, 2, 0.9, 0.9, 1.4])
                col[0].markdown(f"**#{p['numero']}**")
                col[1].write(p["cliente"] or "—")
                dir_p = p["direccion"] + (f", {p['depto']}" if p.get("depto") else "")
                col[2].write(dir_p)
                col[3].write(p["telefono"] or "—")
                col[4].markdown(f"**${p['monto']:,.0f}**".replace(",", "."))
                with col[5]:
                    pop_cols = st.columns([2, 0.7])
                    with pop_cols[0]:
                        try:
                            popover = st.popover("💵 Cobrar", use_container_width=True)
                        except Exception:
                            popover = st.expander("💵 Cobrar")
                        with popover:
                            default_medio = "Transferencia"
                            if "efectivo" in (p.get("forma_pago") or "").lower():
                                default_medio = "Efectivo"
                            elif "webpay" in (p.get("forma_pago") or "").lower():
                                default_medio = "Webpay"
                            cob_monto = st.number_input(
                                "Monto real",
                                min_value=0,
                                value=int(p["monto"] or 0),
                                step=100,
                                key=f"cm_{p['numero']}",
                                help="Monto real cobrado (no el estimado)",
                            )
                            cob_medio = st.selectbox(
                                "Medio de pago",
                                ["Transferencia", "Efectivo", "Webpay"],
                                index=["Transferencia", "Efectivo", "Webpay"].index(default_medio),
                                key=f"cmd_{p['numero']}",
                            )
                            cob_ref = st.text_input(
                                "Referencia",
                                placeholder="wsp, nro op, etc.",
                                key=f"cr_{p['numero']}",
                            )
                            cob_obs = st.text_input(
                                "Observación adicional (opcional)",
                                placeholder="Ej: CRÉDITO 1 bot a favor",
                                key=f"co_{p['numero']}",
                                help="Se agrega a las observaciones del pedido. Útil para dejar notas de crédito, retiros, etc.",
                            )
                            if st.button("✓ Confirmar cobro",
                                         key=f"cc_{p['numero']}",
                                         type="primary",
                                         use_container_width=True,
                                         disabled=cob_monto <= 0):
                                try:
                                    fecha_cobro = datetime.now().strftime("%d/%m/%Y")
                                    campo = "efectivo" if cob_medio == "Efectivo" else "transferencia"
                                    updates = {
                                        "estado_pago": "PAGADO",
                                        "fecha_pago": fecha_cobro,
                                        "forma_pago": cob_medio,
                                        campo: cob_monto,
                                    }
                                    # Concatenar observacion nueva a la existente
                                    if cob_obs.strip():
                                        obs_prev = (p.get("observaciones") or "").strip()
                                        updates["observaciones"] = (
                                            f"{obs_prev} | {cob_obs.strip()}" if obs_prev
                                            else cob_obs.strip()
                                        )
                                    sheets_client.update_pedido(p["numero"], updates)
                                    sheets_client.add_pago({
                                        "fecha": fecha_cobro,
                                        "monto": cob_monto,
                                        "medio": cob_medio,
                                        "referencia": cob_ref,
                                        "cliente": p.get("cliente", ""),
                                        "pedido_vinculado": str(p["numero"]),
                                        "estado": "PAGADO",
                                    })
                                    st.success(f"#{p['numero']} cobrado · ${cob_monto:,.0f}".replace(",", "."))
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")
                    with pop_cols[1]:
                        wa = _wa_link(p["telefono"], p["cliente"], p["numero"], p["monto"])
                        if wa:
                            st.markdown(
                                f'<a href="{wa}" target="_blank"><button style="width:100%;padding:7px;font-size:13px;border:1px solid #ccc;background:white;border-radius:4px;cursor:pointer;">💬</button></a>',
                                unsafe_allow_html=True)
        else:
            st.success("Nadie con deuda pendiente hoy.")

        # Huérfanos + pagos sin pedido
        if diag:
            if diag["huerfanos"]:
                st.markdown("---")
                st.markdown(f"##### 💸 Huérfanos PAGADO ({len(diag['huerfanos'])})")
                st.caption("Pedidos marcados PAGADO sin fila en PAGOS — verificar si el pago es legítimo.")
                df_h = pd.DataFrame(diag["huerfanos"])
                st.dataframe(df_h[["numero", "fecha", "cliente", "forma_pago", "monto"]],
                             use_container_width=True, hide_index=True)

            if diag["pagos_sin_pedido"]:
                st.markdown("---")
                st.markdown(f"##### ❓ PAGOS con pedido# inexistente ({len(diag['pagos_sin_pedido'])})")
                df_p = pd.DataFrame(diag["pagos_sin_pedido"])
                st.dataframe(df_p[["pedido_num", "fecha", "monto", "cliente"]],
                             use_container_width=True, hide_index=True)

        # Historial de pagos + formulario (del tab Pagos antiguo)
        with st.expander("📜 Historial de pagos y registrar nuevo", expanded=False):
            pagos = sheets_client.get_pagos()
            if pagos:
                st.dataframe(pagos, use_container_width=True, hide_index=True, height=280)
            else:
                st.info("No hay pagos registrados.")
            with st.form("form_pago_op"):
                pc1, pc2 = st.columns(2)
                with pc1:
                    pg_monto = st.number_input("Monto", min_value=0, value=0, step=1000)
                with pc2:
                    pg_medio = st.selectbox("Medio", ["Efectivo", "Transferencia", "Webpay"])
                pc3, pc4 = st.columns(2)
                with pc3:
                    pg_cl = st.text_input("Cliente", key="pg_cl_op")
                with pc4:
                    pg_ref = st.text_input("Referencia", placeholder="Nro operacion")
                pg_ped = st.text_input("Pedido vinculado (#)", key="pg_ped_op")
                if st.form_submit_button("Registrar pago", use_container_width=True):
                    if pg_monto > 0:
                        sheets_client.add_pago({
                            "monto": pg_monto, "medio": pg_medio, "cliente": pg_cl,
                            "referencia": pg_ref, "pedido_vinculado": pg_ped, "estado": "PAGADO",
                        })
                        st.success("Pago registrado!")
                        st.rerun()

    # ------------ SUB-TAB ADQUISICIÓN ------------
    with sub_adq:
        # Contador por canal del día
        canales = {}
        for p in pedidos_ruta:
            c = (p.get("canal") or "—").strip() or "—"
            canales[c] = canales.get(c, 0) + 1
        if canales:
            partes = [f"**{k}**: {v}" for k, v in sorted(canales.items(), key=lambda x: -x[1])]
            st.caption("Canales hoy: " + " · ".join(partes))
            st.markdown("")

        # Bsale pendientes — botón + listado con 1-click import
        st.markdown("##### 🛒 Pedidos Bsale sin importar")
        if st.button("🔄 Consultar Bsale ahora", key="btn_bsale_pend_op", use_container_width=True):
            with st.spinner("Consultando Bsale..."):
                try:
                    r = operations.check_bsale_pendientes()
                    pendientes = r.get("pendientes", [])
                    enriched = []
                    for p in pendientes:
                        sug = operations.sugerir_codigo_bsale(p)
                        enriched.append({**p, "_sug": sug})
                    st.session_state["_bsale_pend"] = enriched
                except Exception as e:
                    st.error(f"Error: {e}")

        pendientes = st.session_state.get("_bsale_pend", None)
        if pendientes is not None:
            if not pendientes:
                st.success("Bsale al día — sin pedidos pendientes.")
            else:
                st.caption("Verifica el código drivin sugerido y clickea Importar.")
                for i, p in enumerate(pendientes):
                    sug = p.get("_sug", {}) or {}
                    conf = sug.get("confianza", "none")
                    sug_code = sug.get("codigo", "") or ""
                    candidatos = sug.get("candidatos", []) or []
                    badge_icon = {"auto": "✅", "memory": "🧠", "ambiguous": "⚠️", "none": "❓"}.get(conf, "❓")
                    badge_text = {
                        "auto": "match automático alta confianza",
                        "memory": "aprendido de corrección previa",
                        "ambiguous": "varios candidatos — elegir",
                        "none": "sin match — ingresar manualmente",
                    }.get(conf, "sin match")

                    with st.container(border=True):
                        top = st.columns([3, 1])
                        with top[0]:
                            st.markdown(
                                f"**Bsale #{p.get('pedido_nro','?')}** · {p.get('cliente','')[:30]}"
                                f"  —  {p.get('cantidad',0)} bot {p.get('marca','KOWEN')}"
                            )
                            dir_str = p.get("direccion","")
                            if p.get("depto"):
                                dir_str += f", dpto {p['depto']}"
                            if p.get("comuna"):
                                dir_str += f"  ·  {p['comuna']}"
                            st.caption(dir_str)
                        with top[1]:
                            st.caption(f"{badge_icon} {badge_text}")
                        bot_col = st.columns([2, 1])
                        with bot_col[0]:
                            if conf == "ambiguous" and candidatos:
                                opts = [f"{c.code} — {c.name} ({c.city})" for c in candidatos]
                                opts = ["(escribir código manualmente)"] + opts
                                sel = st.selectbox("Código drivin", opts, key=f"bsop_sel_{i}",
                                                   label_visibility="collapsed")
                                if sel == opts[0]:
                                    codigo_in = st.text_input("Código manual", key=f"bsop_man_{i}",
                                                              label_visibility="collapsed",
                                                              placeholder="Escribir código drivin")
                                else:
                                    codigo_in = candidatos[opts.index(sel) - 1].code
                            else:
                                codigo_in = st.text_input("Código drivin", value=sug_code, key=f"bsop_code_{i}",
                                                          label_visibility="collapsed",
                                                          placeholder="Escribir código drivin")
                        with bot_col[1]:
                            if st.button("Importar", key=f"bsop_import_{i}", use_container_width=True,
                                         type="primary", disabled=not codigo_in):
                                try:
                                    r = operations.importar_bsale_a_operacion(
                                        pedido_bsale=p, codigo_drivin=codigo_in.strip(),
                                    )
                                    msg = f"#{r['numero']} importado"
                                    if r["subido_drivin"]:
                                        msg += " + subido a driv.in"
                                    else:
                                        msg += f" (drivin: {r['motivo_no_subido']})"
                                    st.success(msg)
                                    st.session_state["_bsale_pend"] = [
                                        x for j, x in enumerate(pendientes) if j != i
                                    ]
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")

        # Pedidos en drivin sin planilla
        solo_drivin = ruta.get("solo_en_drivin", [])
        if solo_drivin:
            st.markdown("---")
            st.markdown(f"##### ⚠️ {len(solo_drivin)} pedido(s) en driv.in sin planilla")
            st.caption("Cargados directo en drivin. Revisá si corresponde importarlos.")
            st.dataframe(pd.DataFrame(solo_drivin), use_container_width=True, hide_index=True)

    # ------------ SUB-TAB HISTÓRICO ------------
    with sub_hist:
        st.info("Próximamente: ventas últimos 7 días, clientes top, comunas top, tasa de entrega.")


with tab_lotes:
    import pandas as pd
    import frontend_helpers as fh
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode

    st.markdown("##### Carga rapida de pedidos")
    st.caption("Pega desde Excel o escribe directo en la grilla. La IA detecta clientes existentes y auto-rellena codigo drivin al validar.")

    if "clientes_cache" not in st.session_state:
        try:
            st.session_state.clientes_cache = sheets_client.get_clientes()
        except Exception:
            st.session_state.clientes_cache = []

    # Esquema de columnas
    col_defs = ["Cliente", "Direccion", "Depto", "Comuna", "Cant", "Marca", "Telefono", "Canal", "Doc", "Obs", "Codigo"]
    blank_rows = 8

    if "batch_df" not in st.session_state:
        st.session_state.batch_df = pd.DataFrame(
            [{c: "" for c in col_defs} for _ in range(blank_rows)]
        )
        for i in range(blank_rows):
            st.session_state.batch_df.at[i, "Cant"] = "3"
            st.session_state.batch_df.at[i, "Marca"] = "KOWEN"
            st.session_state.batch_df.at[i, "Canal"] = "MANUAL"
            st.session_state.batch_df.at[i, "Doc"] = "Boleta"

    col_a, col_b, col_c = st.columns([1, 1, 1])
    with col_a:
        lotes_fecha = st.date_input("Fecha", value=datetime.now().date(), key="lotes_fecha")
    with col_b:
        if st.button("Agregar fila", use_container_width=True, key="btn_lotes_addrow"):
            nueva = pd.DataFrame([{c: "" for c in col_defs}])
            nueva.at[0, "Cant"] = "3"; nueva.at[0, "Marca"] = "KOWEN"
            nueva.at[0, "Canal"] = "MANUAL"; nueva.at[0, "Doc"] = "Boleta"
            st.session_state.batch_df = pd.concat([st.session_state.batch_df, nueva], ignore_index=True)
            st.rerun()
    with col_c:
        if st.button("Vaciar", use_container_width=True, key="btn_lotes_clear"):
            del st.session_state["batch_df"]
            st.rerun()

    gob = GridOptionsBuilder.from_dataframe(st.session_state.batch_df)
    gob.configure_default_column(editable=True, resizable=True, filter=False, sortable=False)
    gob.configure_column("Marca", cellEditor="agSelectCellEditor", cellEditorParams={"values": ["KOWEN", "CACTUS"]})
    gob.configure_column("Canal", cellEditor="agSelectCellEditor", cellEditorParams={"values": ["MANUAL", "WSP", "EMAIL", "WEB"]})
    gob.configure_column("Doc", cellEditor="agSelectCellEditor", cellEditorParams={"values": ["Boleta", "Factura", "Guia", "Ticket"]})
    gob.configure_grid_options(stopEditingWhenCellsLoseFocus=True, enableRangeSelection=True)
    grid_opts = gob.build()

    grid_out = AgGrid(
        st.session_state.batch_df,
        gridOptions=grid_opts,
        update_mode=GridUpdateMode.VALUE_CHANGED,
        data_return_mode=DataReturnMode.AS_INPUT,
        height=320,
        theme="alpine-dark",
        fit_columns_on_grid_load=True,
        key="grid_lotes",
    )
    edited_df = pd.DataFrame(grid_out["data"]) if grid_out.get("data") is not None else st.session_state.batch_df

    # --- Validar + preview ---
    col_v, col_g = st.columns([1, 1])
    with col_v:
        validar = st.button("Validar", use_container_width=True, key="btn_lotes_validar")
    with col_g:
        guardar = st.button("Guardar pedidos", type="primary", use_container_width=True, key="btn_lotes_save")

    if validar or guardar:
        st.session_state.batch_df = edited_df
        addr_cache = st.session_state.addresses_cache
        filas_validas = []
        errores = []
        for i, row in edited_df.iterrows():
            dir_r = str(row.get("Direccion", "")).strip()
            cli_r = str(row.get("Cliente", "")).strip()
            if not dir_r and not cli_r:
                continue  # fila vacia, skip
            if not dir_r:
                errores.append(f"Fila {i+1}: falta direccion")
                continue
            # Match contra CLIENTES para auto-rellenar codigo/comuna si faltan
            codigo = str(row.get("Codigo", "")).strip()
            comuna = str(row.get("Comuna", "")).strip()
            tel = str(row.get("Telefono", "")).strip()
            if cli_r and (not codigo or not comuna or not tel):
                for c in st.session_state.clientes_cache:
                    if fh._norm(c.get("Nombre", "")) == fh._norm(cli_r):
                        if not codigo: codigo = c.get("Codigo Drivin", "")
                        if not comuna: comuna = c.get("Comuna", "")
                        if not tel: tel = c.get("Telefono", "")
                        break
            try:
                cant = int(str(row.get("Cant", "0") or "0"))
            except ValueError:
                cant = 0
            filas_validas.append({
                "fecha": lotes_fecha.strftime("%d/%m/%Y"),
                "direccion": dir_r, "depto": str(row.get("Depto", "")).strip(),
                "comuna": comuna, "codigo_drivin": codigo, "cant": cant,
                "marca": str(row.get("Marca", "KOWEN")) or "KOWEN",
                "documento": str(row.get("Doc", "Boleta")) or "Boleta",
                "canal": str(row.get("Canal", "MANUAL")) or "MANUAL",
                "cliente": cli_r, "telefono": tel,
                "email": "", "observaciones": str(row.get("Obs", "")).strip(),
                "estado_pedido": "PENDIENTE", "estado_pago": "PENDIENTE",
            })

        st.markdown(f"**{len(filas_validas)} fila(s) validas** · {len(errores)} con error")
        for e in errores:
            st.error(e)
        if filas_validas:
            st.dataframe(
                pd.DataFrame([{
                    "Cliente": p["cliente"], "Direccion": p["direccion"],
                    "Comuna": p["comuna"], "Cant": p["cant"],
                    "Marca": p["marca"], "Codigo": p["codigo_drivin"] or "—",
                } for p in filas_validas]),
                use_container_width=True, hide_index=True,
            )

        if guardar and filas_validas and not errores:
            creados = 0
            clientes_nuevos = 0
            with st.spinner(f"Guardando {len(filas_validas)} pedidos..."):
                for p in filas_validas:
                    try:
                        sheets_client.add_pedido(p)
                        creados += 1
                        if p["cliente"]:
                            existing = sheets_client.find_cliente(p["cliente"])
                            if not existing:
                                sheets_client.add_cliente({
                                    "nombre": p["cliente"], "telefono": p["telefono"],
                                    "email": "", "direccion": p["direccion"],
                                    "depto": p["depto"], "comuna": p["comuna"],
                                    "codigo_drivin": p["codigo_drivin"], "marca": p["marca"],
                                })
                                clientes_nuevos += 1
                    except Exception as ex:
                        st.error(f"Error guardando {p['cliente']}: {ex}")
            st.success(f"{creados} pedido(s) creados · {clientes_nuevos} cliente(s) nuevos")
            st.session_state.pop("batch_df", None)
            st.session_state.pop("clientes_cache", None)
            st.rerun()

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

with tab_cr:
    st.markdown("##### Pagos por confirmar")
    st.caption("Lee correos no leidos, clasifica pagos y propone match con pedidos. Revisá y confirmá cada uno.")

    c_btn, c_max = st.columns([3, 1])
    with c_btn:
        if st.button("🔄 Leer correos nuevos", type="primary", use_container_width=True, key="btn_correos"):
            with st.spinner("Leyendo Gmail, clasificando..."):
                try:
                    import payments
                    cr_max_val = st.session_state.get("cr_max", 30)
                    r = payments.procesar_emails_no_leidos(max_emails=cr_max_val)
                    st.session_state["_correos_result"] = r
                except Exception as e:
                    st.error(f"Error: {e}")
    with c_max:
        st.number_input("Max emails", min_value=1, max_value=100, value=30, key="cr_max", label_visibility="collapsed")

    res = st.session_state.get("_correos_result")
    if res:
        por_confirmar = res.get("pagos_por_confirmar", [])
        alertas = res.get("alertas", [])

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Leidos", res.get("total", 0))
        c2.metric("Por confirmar", len(por_confirmar))
        c3.metric("Duplicados", res.get("duplicados", 0))
        c4.metric("Alertas", len(alertas))

        cats = res.get("por_categoria", {})
        if cats:
            st.caption("Por categoria: " + ", ".join(f"{k}={v}" for k, v in cats.items()))

        st.markdown("---")

        if not por_confirmar:
            st.info("No hay pagos por confirmar.")
        else:
            import payments
            import gmail_client
            st.markdown(f"### {len(por_confirmar)} pago(s) por confirmar")
            st.caption("Ordenados por score descendente. Los mas confiables arriba.")

            for i, p in enumerate(por_confirmar):
                pago = p["pago"]
                candidatos = p["candidatos"]
                top = candidatos[0] if candidatos else None
                score_top = p["score_top"]
                rut = pago.get("remitente_rut", "")

                # Header con score
                if score_top >= 80:
                    badge = f"🟢 {score_top}%"
                elif score_top >= 60:
                    badge = f"🟡 {score_top}%"
                elif score_top >= 40:
                    badge = f"🟠 {score_top}%"
                else:
                    badge = "⚪ sin match"

                header = (
                    f"{badge} — **{pago.get('remitente_nombre', '(sin nombre)')}** "
                    f"${pago.get('monto', '0')} · {pago.get('fecha', '')}"
                )
                if rut:
                    header += f" · RUT {rut}"

                with st.container(border=True):
                    st.markdown(header)

                    col_info, col_match = st.columns([1, 2])
                    with col_info:
                        st.caption(f"**Banco:** {pago.get('banco', '-')}")
                        st.caption(f"**Medio:** {pago.get('medio', '-')}")
                        st.caption(f"**Ref:** {pago.get('referencia', '-')}")
                        if pago.get("glosa"):
                            st.caption(f"**Glosa:** {pago.get('glosa', '')[:80]}")
                        st.caption(f"**Asunto:** {p['email_subject'][:60]}")

                    with col_match:
                        if top:
                            rut_tag = ""
                            if top.get("rut_match") == 100:
                                rut_tag = " ✅ RUT conocido"
                            elif top.get("rut_match") == 0:
                                rut_tag = " ⚠️ RUT apunta a otro cliente"
                            st.markdown(
                                f"**Match sugerido:** #{top['numero']} · {top['cliente']} · "
                                f"{top['fecha']} · ${top['monto']}{rut_tag}"
                            )
                            if len(candidatos) > 1:
                                st.caption(f"{len(candidatos) - 1} candidato(s) alternativo(s)")
                        else:
                            st.markdown("**Sin candidatos** — registrar como SIN_MATCH o rechazar")

                    # Selector si hay alternativos
                    pedido_elegido = top
                    if len(candidatos) > 1:
                        cand_labels = [
                            f"#{c['numero']} · {c['cliente'][:30]} · {c['fecha']} · ${c['monto']} ({c['score']}%)"
                            for c in candidatos
                        ]
                        sel_idx = st.selectbox(
                            "Cambiar match",
                            range(len(cand_labels)),
                            format_func=lambda j: cand_labels[j],
                            key=f"conf_sel_{i}",
                        )
                        pedido_elegido = candidatos[sel_idx]

                    b1, b2, b3 = st.columns(3)
                    with b1:
                        if pedido_elegido and st.button(
                            f"✅ Confirmar #{pedido_elegido['numero']}",
                            key=f"conf_ok_{i}",
                            type="primary",
                            use_container_width=True,
                        ):
                            try:
                                pedidos_all = sheets_client.get_pedidos()
                                pedido_full = next(
                                    (pp for pp in pedidos_all if pp.get("#") == pedido_elegido["numero"]),
                                    None,
                                )
                                if pedido_full:
                                    payments.confirmar_pago(p["email_id"], pago, pedido_full)
                                    st.success(
                                        f"Pago vinculado a #{pedido_elegido['numero']} "
                                        f"({pedido_full.get('Cliente', '')})"
                                    )
                                    # Quitar de la cola
                                    st.session_state["_correos_result"]["pagos_por_confirmar"] = [
                                        x for x in por_confirmar if x["email_id"] != p["email_id"]
                                    ]
                                    st.rerun()
                                else:
                                    st.error(f"No encontre el pedido #{pedido_elegido['numero']}")
                            except Exception as e:
                                st.error(f"Error: {e}")
                    with b2:
                        if st.button("❌ Rechazar", key=f"conf_no_{i}", use_container_width=True):
                            try:
                                payments.rechazar_pago(p["email_id"], pago, razon="rechazado en dashboard")
                                st.info("Registrado sin match y archivado.")
                                st.session_state["_correos_result"]["pagos_por_confirmar"] = [
                                    x for x in por_confirmar if x["email_id"] != p["email_id"]
                                ]
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error: {e}")
                    with b3:
                        if st.button("⏭️ Saltar", key=f"conf_skip_{i}", use_container_width=True):
                            st.session_state["_correos_result"]["pagos_por_confirmar"] = [
                                x for x in por_confirmar if x["email_id"] != p["email_id"]
                            ]
                            st.rerun()

        if alertas:
            st.markdown("---")
            st.info(f"📬 {len(alertas)} correo(s) no-pago para revisar:")
            for a in alertas:
                st.write(f"- [{a['categoria']}] **{a['subject']}** — {a['from']}")

        if res.get("errores"):
            st.markdown("---")
            st.error("Errores:")
            for e in res["errores"]:
                st.write(f"- {e}")


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
                    count = operations.sync_from_drivin(
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
                    count = operations.sync_to_planilla_reparto(sync2_fecha)
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


with tab_log:
    st.markdown("##### Log de eventos del sistema")
    st.caption("Rutinas, matches, correcciones, errores — escrito por el scheduler y el dashboard.")

    import log_client as _log_client
    from sheets_client import _read_sheet, TAB_LOG

    col_f1, col_f2, col_f3 = st.columns([1, 1, 2])
    with col_f1:
        tipo_filtro = st.selectbox(
            "Tipo",
            ["Todos", "RUTINA", "IMPORT", "MATCH", "ERROR", "CORRECCION", "DRIVIN"],
            key="log_tipo",
        )
    with col_f2:
        limite_dias = st.number_input("Ultimos N dias", min_value=1, max_value=30, value=7, key="log_dias")
    with col_f3:
        st.write("")  # spacer
        if st.button("Refrescar", key="btn_log_refresh"):
            st.rerun()

    # --- Errores recurrentes ---
    errores_rec = _log_client.get_errores_recurrentes(dias=int(limite_dias))
    if errores_rec:
        st.markdown("##### Errores recurrentes (>= 2 veces)")
        for err in errores_rec:
            with st.expander(
                f"⚠ {err['accion']}: {err['conteo']} veces · ultimo {err['ultimo_error']}",
            ):
                st.code(err["ejemplo"][:500] or "(sin detalle)", language=None)
    else:
        st.info(f"Sin errores recurrentes en los ultimos {limite_dias} dias.")

    st.markdown("---")

    # --- Lista de eventos recientes ---
    try:
        rows = _read_sheet(TAB_LOG)
    except Exception as e:
        st.error(f"No se pudo leer la hoja LOG: {e}")
        rows = []

    if len(rows) >= 2:
        import pandas as pd

        headers = rows[0]
        data_rows = rows[1:]
        df = pd.DataFrame(data_rows, columns=headers[:len(data_rows[0])] if data_rows else headers)

        # Filtrar por tipo
        if tipo_filtro != "Todos" and "Tipo" in df.columns:
            df = df[df["Tipo"] == tipo_filtro]

        # Filtrar por fecha (ultimos N dias)
        if "Fecha/Hora" in df.columns:
            try:
                df["_dt"] = pd.to_datetime(df["Fecha/Hora"], format="%d/%m/%Y %H:%M:%S", errors="coerce")
                corte = datetime.now() - timedelta(days=int(limite_dias))
                df = df[df["_dt"].notna() & (df["_dt"] >= corte)]
                df = df.sort_values("_dt", ascending=False).drop(columns=["_dt"])
            except Exception:
                pass

        st.markdown(f"##### Eventos ({len(df)})")
        if len(df) == 0:
            st.info("Sin eventos con esos filtros.")
        else:
            st.dataframe(df.head(200), use_container_width=True, hide_index=True)
    else:
        st.info("El log esta vacio.")
