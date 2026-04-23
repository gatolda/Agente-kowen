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
        tab_vista, tab_editar, tab_drivin, tab_verify = st.tabs(["📋 Vista general", "✏️ Editar / Eliminar", "🚛 Subir a driv.in", "🔍 Verificar driv.in"])

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

        # === CODIGOS Y DRIV.IN ===
        with tab_drivin:
            # Separar pedidos por estado de codigo
            con_codigo = [p for p in pedidos if p.get("Codigo Drivin", "").strip()]
            sin_codigo = [p for p in pedidos if not p.get("Codigo Drivin", "").strip()]
            pendientes_subir = [p for p in con_codigo
                                if p.get("Estado Pedido") == "PENDIENTE"
                                and not p.get("Plan Drivin", "").strip()]

            # --- Resumen rapido ---
            rc1, rc2, rc3 = st.columns(3)
            with rc1:
                st.metric("Con codigo", len(con_codigo))
            with rc2:
                st.metric("Sin codigo", len(sin_codigo), delta=f"-{len(sin_codigo)}" if sin_codigo else None, delta_color="inverse")
            with rc3:
                st.metric("Listos para subir", len(pendientes_subir))

            # === PASO 1: Asignar codigos ===
            if sin_codigo:
                st.markdown("### Paso 1: Asignar codigos")
                st.caption("Estos pedidos no tienen codigo driv.in. Selecciona el correcto para cada uno.")

                # Auto-matchear al cargar (sin boton)
                if "matched_drivin" not in st.session_state or st.button("Volver a buscar", key="btn_rematch"):
                    if not st.session_state.get("addresses_cache"):
                        with st.spinner("Cargando direcciones..."):
                            address_matcher.refresh_cache()
                            st.session_state.addresses_cache = address_matcher.load_cache()

                    matched = []
                    for p in sin_codigo:
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
                    autos = [m for m in st.session_state["matched_drivin"] if m["confidence"] in ("auto", "memory")]
                    ambiguos = [m for m in st.session_state["matched_drivin"] if m["confidence"] == "ambiguous"]
                    sin_match = [m for m in st.session_state["matched_drivin"] if m["confidence"] == "none"]

                    # --- Automaticos (resueltos) ---
                    if autos:
                        with st.expander(f"Resueltos automaticamente ({len(autos)})", expanded=False):
                            for i, m in enumerate(st.session_state["matched_drivin"]):
                                if m["confidence"] not in ("auto", "memory"):
                                    continue
                                p = m["pedido"]
                                c1, c2, c3 = st.columns([4, 2, 1])
                                with c1:
                                    st.success(f"#{p.get('#', '')} {p.get('Direccion', '')} {p.get('Depto', '')}")
                                with c2:
                                    st.code(m["result"])
                                    codes[i] = m["result"]
                                with c3:
                                    st.write(f"**{p.get('Cant', '')}** bot.")

                    # --- Ambiguos (necesitan eleccion) ---
                    if ambiguos:
                        st.warning(f"**{len(ambiguos)} pedidos necesitan tu eleccion:**")
                        for i, m in enumerate(st.session_state["matched_drivin"]):
                            if m["confidence"] != "ambiguous":
                                continue
                            p = m["pedido"]
                            result = m["result"]

                            st.markdown(f"**#{p.get('#', '')}** — {p.get('Direccion', '')} {p.get('Depto', '')} — {p.get('Cant', '')} bot.")
                            if result:
                                opts = ["-- Seleccionar --"] + [f"{c.code} — {c.name} ({c.address1})" for c in result]
                                s = st.selectbox(
                                    f"Codigo para #{p.get('#', '')}",
                                    opts,
                                    key=f"sel_d_{i}",
                                    label_visibility="collapsed",
                                )
                                if s != "-- Seleccionar --":
                                    idx = opts.index(s) - 1
                                    codes[i] = result[idx].code
                            else:
                                manual = st.text_input(
                                    f"Codigo para #{p.get('#', '')}",
                                    key=f"man_d_{i}",
                                    label_visibility="collapsed",
                                    placeholder="Escribir codigo drivin...",
                                )
                                if manual:
                                    codes[i] = manual

                    # --- Sin match ---
                    if sin_match:
                        st.error(f"**{len(sin_match)} sin coincidencia** — escribe el codigo manualmente:")
                        for i, m in enumerate(st.session_state["matched_drivin"]):
                            if m["confidence"] != "none":
                                continue
                            p = m["pedido"]
                            c1, c2 = st.columns([3, 2])
                            with c1:
                                st.write(f"#{p.get('#', '')} — {p.get('Direccion', '')} {p.get('Depto', '')} — {p.get('Cant', '')} bot.")
                            with c2:
                                manual = st.text_input(
                                    f"Codigo para #{p.get('#', '')}",
                                    key=f"man_d_{i}",
                                    label_visibility="collapsed",
                                    placeholder="Codigo drivin...",
                                )
                                if manual:
                                    codes[i] = manual

                    # --- Boton asignar codigos ---
                    total_con_code = sum(1 for v in codes.values() if v)
                    total_sin = len(st.session_state["matched_drivin"])
                    st.markdown("---")

                    if total_con_code > 0:
                        if st.button(f"Guardar {total_con_code} codigos", key="btn_guardar_codes", type="primary", use_container_width=True):
                            updates_list = []
                            for i, m in enumerate(st.session_state["matched_drivin"]):
                                code = codes.get(i, "")
                                if not code:
                                    continue
                                p = m["pedido"]
                                conf = m["confidence"]
                                pn = p.get("#", "")
                                if pn and pn.isdigit():
                                    updates_list.append((int(pn), {"codigo_drivin": code}))
                                # Guardar en memoria si fue seleccion manual
                                if conf in ("ambiguous", "none") and code:
                                    address_matcher.save_memory_entry(p.get("Direccion", ""), code)
                                    log_client.log_match_manual(p.get("Direccion", ""), code)

                            if updates_list:
                                sheets_client.update_pedidos_batch(updates_list)
                                st.success(f"{len(updates_list)} codigos guardados!")
                                st.session_state.pop("matched_drivin", None)
                                st.rerun()
                    else:
                        st.info("Selecciona codigos arriba para poder guardarlos.")

            # === PASO 2: Subir a driv.in ===
            # Refrescar lista de pendientes para subir (puede haber cambiado tras asignar codigos)
            if sin_codigo:
                st.markdown("---")

            st.markdown("### Paso 2: Subir a driv.in")

            if not st.session_state.scenario_token:
                st.caption("Conecta un plan en el sidebar o crea uno con el boton de arriba.")
            else:
                st.info(f"Plan activo: **{st.session_state.scenario_name}**")

            # Pedidos con codigo, pendientes y sin plan
            listos = [p for p in pedidos
                      if p.get("Codigo Drivin", "").strip()
                      and p.get("Estado Pedido") == "PENDIENTE"
                      and not p.get("Plan Drivin", "").strip()]

            if not listos:
                if not sin_codigo:
                    st.success("Todos los pedidos ya tienen plan asignado.")
                else:
                    st.caption("Asigna los codigos de arriba primero.")
            else:
                st.write(f"**{len(listos)} pedidos** listos para subir:")
                for p in listos:
                    st.write(f"  #{p.get('#','')} — {p.get('Direccion','')} {p.get('Depto','')} — {p.get('Cant','')} bot. — `{p.get('Codigo Drivin','')}`")

                if st.session_state.scenario_token:
                    if st.button(f"Subir {len(listos)} pedidos al plan", key="btn_subir", type="primary", use_container_width=True):
                        clients = []
                        updates_list = []
                        for p in listos:
                            code = p.get("Codigo Drivin", "")
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
                            pn = p.get("#", "")
                            if pn and pn.isdigit():
                                updates_list.append((int(pn), {"plan_drivin": st.session_state.scenario_name or ""}))

                        with st.spinner("Subiendo a driv.in..."):
                            try:
                                r = drivin_client.create_orders(clients=clients, scenario_token=st.session_state.scenario_token)
                                added = r.get("response", r).get("added", [])
                                if updates_list:
                                    sheets_client.update_pedidos_batch(updates_list)
                                st.success(f"{len(added)} pedidos subidos al plan!")
                                st.session_state.pop("matched_drivin", None)
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error: {e}")

        # === TAB VERIFICAR DRIV.IN ===
        with tab_verify:
            st.markdown("### Verificar estado real contra driv.in")
            st.caption("Compara los pedidos PENDIENTE con la informacion real de driv.in (PODs, planes, rutas).")

            if st.button("Verificar ahora", key="btn_verify", type="primary"):
                with st.spinner("Consultando driv.in..."):
                    try:
                        verificacion = operations.verify_orders_drivin(
                            fecha=fecha_str, days_back=7, auto_update=False
                        )
                        st.session_state["verificacion_result"] = verificacion
                    except Exception as e:
                        st.error(f"Error: {e}")

            if st.session_state.get("verificacion_result"):
                v = st.session_state["verificacion_result"]

                # Metricas
                vc1, vc2, vc3, vc4 = st.columns(4)
                with vc1:
                    st.metric("Verificados", v["total_verificados"])
                with vc2:
                    st.metric("Entregados", v["entregados_detectados"],
                              delta=f"+{v['entregados_detectados']}" if v["entregados_detectados"] else None)
                with vc3:
                    st.metric("No entregados", v["no_entregados_detectados"])
                with vc4:
                    st.metric("Estancados", len(v["estancados"]),
                              delta=f"{len(v['estancados'])}" if v["estancados"] else None,
                              delta_color="inverse")

                # Planes sin despachar
                if v["planes_sin_despachar"]:
                    st.warning(f"**{len(v['planes_sin_despachar'])} planes creados pero NO despachados:**")
                    for plan in v["planes_sin_despachar"]:
                        st.write(f"  - **{plan['plan']}** — Estado: {plan['status']}")

                # Cambios detectados
                if v["detalle"]:
                    st.success(f"**{len(v['detalle'])} cambios de estado detectados:**")
                    for d in v["detalle"]:
                        st.write(f"  #{d['numero']} {d['direccion']}: "
                                 f"{d['estado_anterior']} → **{d['estado_nuevo']}** (fuente: {d['fuente']})")

                    if st.button("Aplicar cambios", key="btn_apply_verify", type="primary"):
                        with st.spinner("Actualizando..."):
                            try:
                                result = operations.verify_orders_drivin(
                                    fecha=fecha_str, days_back=7, auto_update=True
                                )
                                st.success(f"{result['actualizados']} pedidos actualizados!")
                                st.session_state.pop("verificacion_result", None)
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error: {e}")
                elif v["total_verificados"] > 0:
                    st.info("No se detectaron cambios de estado. Todo al dia.")

                # Pedidos estancados
                if v["estancados"]:
                    st.markdown("---")
                    st.error(f"**{len(v['estancados'])} pedidos estancados** (PENDIENTE por mas de 2 dias sin actividad en driv.in):")
                    for e in v["estancados"]:
                        st.write(f"  #{e['numero']} — {e['direccion']} — "
                                 f"Desde {e['fecha']} ({e['dias']} dias) — "
                                 f"Codigo: {e['codigo'] or 'sin codigo'} — Plan: {e['plan'] or 'sin plan'}")

                    st.caption("Estos pedidos probablemente necesitan reprogramarse o verificarse manualmente.")


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
        r = operations.resumen_dia(fecha)
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

tab_salud, tab_lotes, tab_rep, tab_cl, tab_pg, tab_cr, tab_sync, tab_log = st.tabs([
    "🚨 Salud", "📋 Carga rapida", "📊 Reporte", "👥 Clientes", "💰 Pagos", "📧 Correos", "🔄 Sync driv.in", "🗂 Log",
])

with tab_salud:
    st.markdown("##### Salud del sistema")
    st.caption("Todo lo que requiere accion humana, en un solo lugar.")

    col_ref, col_dias = st.columns([1, 1])
    with col_ref:
        if st.button("Refrescar", key="btn_salud_refresh", use_container_width=True):
            st.rerun()
    with col_dias:
        dias_est = st.number_input("Dias para marcar estancado", min_value=1, max_value=15, value=2, key="salud_dias_est")

    try:
        diag = operations.diagnostico_salud(dias_estancado=int(dias_est))
    except Exception as e:
        st.error(f"Error cargando diagnostico: {e}")
        diag = None

    if diag:
        # --- KPIs ---
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Total pedidos", diag["total_pedidos"])
        k2.metric("Estancados", len(diag["estancados"]),
                  delta=None if not diag["estancados"] else "accion", delta_color="inverse")
        k3.metric("Huerfanos PAGADO", len(diag["huerfanos"]),
                  delta=None if not diag["huerfanos"] else "accion", delta_color="inverse")
        k4.metric("PAGOS sin pedido", len(diag["pagos_sin_pedido"]),
                  delta=None if not diag["pagos_sin_pedido"] else "accion", delta_color="inverse")
        k5.metric("Sin codigo", len(diag["pendientes_sin_codigo"]),
                  delta=None if not diag["pendientes_sin_codigo"] else "accion", delta_color="inverse")

        # --- Totales por estado ---
        if diag["totales_por_estado"]:
            partes = [f"**{k}**: {v}" for k, v in sorted(diag["totales_por_estado"].items(), key=lambda x: -x[1])]
            st.caption(" · ".join(partes))

        st.markdown("---")

        # --- Estancados ---
        if diag["estancados"]:
            with st.expander(f"🕰 Estancados ({len(diag['estancados'])}) — pedidos PENDIENTE con ≥{dias_est} dias", expanded=True):
                import pandas as pd
                df_e = pd.DataFrame(diag["estancados"])
                st.dataframe(df_e[["numero", "fecha", "dias", "direccion", "cliente", "codigo"]],
                             use_container_width=True, hide_index=True)

        # --- Huerfanos ---
        if diag["huerfanos"]:
            with st.expander(f"💸 Huerfanos ({len(diag['huerfanos'])}) — marcados PAGADO sin fila en PAGOS", expanded=False):
                import pandas as pd
                df_h = pd.DataFrame(diag["huerfanos"])
                st.dataframe(df_h[["numero", "fecha", "cliente", "forma_pago", "monto"]],
                             use_container_width=True, hide_index=True)
                st.caption("_Pueden ser ingresos manuales legitimos (efectivo anotado a mano) o errores — revisar._")

        # --- Pagos sin pedido ---
        if diag["pagos_sin_pedido"]:
            with st.expander(f"❓ PAGOS sin pedido ({len(diag['pagos_sin_pedido'])}) — pedido# inexistente en OPERACION DIARIA", expanded=False):
                import pandas as pd
                df_p = pd.DataFrame(diag["pagos_sin_pedido"])
                st.dataframe(df_p[["pedido_num", "fecha", "monto", "cliente"]],
                             use_container_width=True, hide_index=True)
                st.caption("_Pedido# mal escrito al crear la fila en PAGOS._")

        # --- Pendientes sin codigo ---
        if diag["pendientes_sin_codigo"]:
            with st.expander(f"🔖 Pendientes sin codigo driv.in ({len(diag['pendientes_sin_codigo'])})", expanded=False):
                import pandas as pd
                df_s = pd.DataFrame(diag["pendientes_sin_codigo"])
                st.dataframe(df_s[["numero", "fecha", "direccion", "comuna"]],
                             use_container_width=True, hide_index=True)
                st.caption("_Asignar codigo desde la pestaña Reporte o ejecutar la rutina._")

        if not any([diag["estancados"], diag["huerfanos"], diag["pagos_sin_pedido"], diag["pendientes_sin_codigo"]]):
            st.success("Todo al dia — sin acciones pendientes.")

        st.markdown("---")

        # --- APIs externas (bajo demanda, costosas) ---
        st.markdown("##### Revisiones con APIs externas")
        st.caption("Bsale, driv.in y Gmail — se consultan bajo demanda porque son lentas.")

        c1, c2 = st.columns(2)

        with c1:
            if st.button("Bsale: pedidos sin planilla", key="btn_bsale_pend", use_container_width=True):
                with st.spinner("Consultando Bsale..."):
                    try:
                        r = operations.check_bsale_pendientes()
                        pendientes = r.get("pendientes", [])
                        # Pre-computar sugerencias una sola vez
                        enriched = []
                        for p in pendientes:
                            sug = operations.sugerir_codigo_bsale(p)
                            enriched.append({**p, "_sug": sug})
                        st.session_state["_bsale_pend"] = enriched
                    except Exception as e:
                        st.error(f"Error: {e}")

        with c2:
            if st.button("driv.in: planes sin despachar", key="btn_drivin_pend", use_container_width=True):
                with st.spinner("Consultando driv.in..."):
                    try:
                        hoy_str = datetime.now().strftime("%d/%m/%Y")
                        v = operations.verify_orders_drivin(fecha=hoy_str, auto_update=False)
                        st.session_state["_drivin_v"] = v
                    except Exception as e:
                        st.error(f"Error: {e}")
            v = st.session_state.get("_drivin_v", None)
            if v is not None:
                planes = v.get("planes_sin_despachar", [])
                if planes:
                    st.warning(f"{len(planes)} plan(es) sin despachar")
                    import pandas as pd
                    st.dataframe(pd.DataFrame(planes), use_container_width=True, hide_index=True)
                else:
                    st.success("Todos los planes iniciados.")

        # --- Detalle Bsale pendientes con sugerencia + boton importar ---
        pendientes = st.session_state.get("_bsale_pend", None)
        if pendientes is not None:
            if not pendientes:
                st.success("Bsale al dia — sin pedidos pendientes.")
            else:
                st.markdown(f"##### {len(pendientes)} pedido(s) Bsale sin importar")
                st.caption("Verifica el codigo drivin sugerido y clickea Importar. El pedido queda en OPERACION DIARIA y se sube al plan drivin del dia si existe.")

                for i, p in enumerate(pendientes):
                    sug = p.get("_sug", {}) or {}
                    conf = sug.get("confianza", "none")
                    sug_code = sug.get("codigo", "") or ""
                    candidatos = sug.get("candidatos", []) or []

                    badge_icon = {"auto": "✅", "memory": "🧠", "ambiguous": "⚠️", "none": "❓"}.get(conf, "❓")
                    badge_text = {
                        "auto": "match automatico alta confianza",
                        "memory": "aprendido de correccion previa",
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

                        # Campo codigo: selectbox si ambiguo, text_input en otro caso
                        bot_col = st.columns([2, 1])
                        with bot_col[0]:
                            if conf == "ambiguous" and candidatos:
                                opts = [f"{c.code} — {c.name} ({c.city})" for c in candidatos]
                                opts = ["(escribir codigo manualmente)"] + opts
                                sel = st.selectbox(
                                    "Codigo drivin",
                                    opts,
                                    key=f"bsale_sel_{i}",
                                    label_visibility="collapsed",
                                )
                                if sel == opts[0]:
                                    codigo_in = st.text_input(
                                        "Codigo manual",
                                        key=f"bsale_man_{i}",
                                        label_visibility="collapsed",
                                        placeholder="Escribir codigo drivin",
                                    )
                                else:
                                    codigo_in = candidatos[opts.index(sel) - 1].code
                            else:
                                codigo_in = st.text_input(
                                    "Codigo drivin",
                                    value=sug_code,
                                    key=f"bsale_code_{i}",
                                    label_visibility="collapsed",
                                    placeholder="Escribir codigo drivin",
                                )

                        with bot_col[1]:
                            if st.button(
                                "Importar",
                                key=f"bsale_import_{i}",
                                use_container_width=True,
                                type="primary",
                                disabled=not codigo_in,
                            ):
                                try:
                                    r = operations.importar_bsale_a_operacion(
                                        pedido_bsale=p,
                                        codigo_drivin=codigo_in.strip(),
                                    )
                                    msg = f"#{r['numero']} importado"
                                    if r["subido_drivin"]:
                                        msg += " + subido a driv.in"
                                    else:
                                        msg += f" (drivin: {r['motivo_no_subido']})"
                                    st.success(msg)
                                    # Quitar el importado de la lista
                                    st.session_state["_bsale_pend"] = [
                                        x for j, x in enumerate(pendientes) if j != i
                                    ]
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")

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

with tab_rep:
    import reports as _reports

    st.markdown("""
    <style>
    .rep-hero-header { background: linear-gradient(180deg, rgba(127,29,29,0.18) 0%, transparent 100%);
        border: 1px solid #7f1d1d; border-left: 4px solid #ef4444;
        border-radius: 10px; padding: 14px 18px; margin-bottom: 12px; }
    .rep-hero-header h3 { color: #fca5a5; margin: 0; font-size: 15px; font-weight: 700; }
    .rep-hero-header .sub { color: #fca5a5; font-size: 13px; opacity: 0.9; }
    .rep-section-title { font-size: 11px; text-transform: uppercase; letter-spacing: 1px;
        color: #888; padding: 14px 0 8px; margin-top: 6px; border-top: 1px dashed #ddd; }
    .rep-section-title.critical { color: #ef4444; border-top-color: #fca5a5; }
    .rep-atraso-3 { color: #ef4444; font-weight: 700; background: rgba(239,68,68,0.12);
        padding: 2px 8px; border-radius: 3px; font-size: 11px; }
    .rep-atraso-2 { color: #ef4444; font-weight: 700; font-size: 11px; }
    .rep-atraso-1 { color: #f59e0b; font-weight: 700; font-size: 11px; }
    .rep-atraso-0 { color: #888; font-size: 11px; }
    </style>
    """, unsafe_allow_html=True)

    # --- Controles ---
    col_fecha, col_marca, col_space = st.columns([1, 1, 3])
    with col_fecha:
        rep_fecha = st.date_input("Fecha", value=datetime.now().date(), key="rep_fecha")
    with col_marca:
        rep_marca = st.selectbox("Marca", ["Todas", "Kowen", "Cactus"], key="rep_marca")

    rep_fecha_str = rep_fecha.strftime("%d/%m/%Y")

    # --- Cargar ruta del dia ---
    try:
        ruta = _reports.get_ruta_del_dia(rep_fecha_str)
    except Exception as e:
        st.error(f"Error leyendo ruta del dia: {e}")
        ruta = {"pedidos": [], "stats": {}, "scenario": {"existe": False}, "solo_en_drivin": []}

    pedidos_ruta = ruta["pedidos"]
    if rep_marca != "Todas":
        pedidos_ruta = [p for p in pedidos_ruta if (p.get("marca", "") or "").lower() == rep_marca.lower()]
    stats = ruta["stats"]

    # --- KPIs: foco en botellones (pedidos + entregados + cobrados) ---
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

    # --- Tabla listado con colores por estado ---
    st.markdown("")  # spacing
    if not pedidos_ruta:
        st.info("No hay pedidos para esta fecha.")
    else:
        def _row_bg(r):
            est = r["estado"]; pago = r["estado_pago"]
            if est == "ENTREGADO" and pago == "PAGADO":
                return "background:rgba(22,163,74,0.15);"
            if est == "ENTREGADO":
                return "background:rgba(134,239,172,0.10);"
            if est == "EN CAMINO":
                return "background:rgba(59,130,246,0.10);"
            if est == "NO ENTREGADO":
                return "background:rgba(239,68,68,0.12);"
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
            rows_html.append(f"""
            <tr style="{_row_bg(r)}">
                <td style="padding:7px 10px; font-family:monospace; color:#cbd5e1;">#{r['numero']}</td>
                <td style="padding:7px 10px;"><b>{r['cliente'] or '—'}</b></td>
                <td style="padding:7px 10px; color:#cbd5e1;">{dir_display}</td>
                <td style="padding:7px 10px; text-align:center;">{r['cantidad'] or '—'}</td>
                <td style="padding:7px 10px;">{_estado_cell(r['estado'])}</td>
                <td style="padding:7px 10px;">{_pago_cell(r['estado_pago'], r['estado'])}</td>
                <td style="padding:7px 10px; font-family:monospace; text-align:right;">{monto_str}</td>
            </tr>
            """)

        st.markdown(f"""
        <div style="max-height:580px; overflow-y:auto; border:1px solid #27272a; border-radius:6px;">
        <table style="width:100%; border-collapse:collapse; font-size:13px;">
            <thead style="position:sticky; top:0; background:#0f0f10;">
                <tr style="text-align:left; color:#9ca3af; border-bottom:1px solid #27272a;">
                    <th style="padding:10px; font-size:11px; letter-spacing:0.06em; text-transform:uppercase;">#</th>
                    <th style="padding:10px; font-size:11px; letter-spacing:0.06em; text-transform:uppercase;">Cliente</th>
                    <th style="padding:10px; font-size:11px; letter-spacing:0.06em; text-transform:uppercase;">Dirección</th>
                    <th style="padding:10px; font-size:11px; letter-spacing:0.06em; text-transform:uppercase; text-align:center;">Cant</th>
                    <th style="padding:10px; font-size:11px; letter-spacing:0.06em; text-transform:uppercase;">Estado</th>
                    <th style="padding:10px; font-size:11px; letter-spacing:0.06em; text-transform:uppercase;">Pago</th>
                    <th style="padding:10px; font-size:11px; letter-spacing:0.06em; text-transform:uppercase; text-align:right;">Monto</th>
                </tr>
            </thead>
            <tbody>{''.join(rows_html)}</tbody>
        </table>
        </div>
        """, unsafe_allow_html=True)

    # Alerta de pedidos en drivin sin planilla (expander colapsado)
    solo_drivin = ruta.get("solo_en_drivin", [])
    if solo_drivin:
        with st.expander(f"⚠️ {len(solo_drivin)} pedido(s) en driv.in sin reflejo en planilla", expanded=False):
            import pandas as pd
            st.dataframe(pd.DataFrame(solo_drivin), use_container_width=True, hide_index=True)


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
