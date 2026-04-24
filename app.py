import streamlit as st
import sqlite3
import pandas as pd
import os

st.set_page_config(
    page_title="Query Hub · Loja Integrada",
    page_icon="🗄️",
    layout="wide",
)

DB_PATH = "data/queries.db"

# ── CSS LI Branding ───────────────────────────────────────────────────────────
st.markdown("""
<style>
    :root {
        --li-turquesa: #00C4A0;
        --li-roxo: #6B2FA0;
        --li-turquesa-light: #E6FAF6;
        --li-roxo-light: #F0E8F8;
        --li-turquesa-dark: #00957A;
        --li-roxo-dark: #4E1F78;
    }
    .block-container { padding-top: 2rem !important; max-width: 1100px; }
    .hero-banner {
        background: linear-gradient(135deg, #00C4A0 0%, #6B2FA0 100%);
        padding: 28px 32px;
        border-radius: 16px;
        margin-bottom: 1.5rem;
        color: white;
    }
    .hero-title { font-size: 22px; font-weight: 600; margin-bottom: 4px; }
    .hero-sub { font-size: 14px; opacity: .85; }
    .badge {
        display: inline-block;
        padding: 3px 10px;
        border-radius: 99px;
        font-size: 11px;
        font-weight: 500;
        margin-right: 6px;
    }
    .badge-dados        { background:#E6FAF6; color:#00957A; }
    .badge-automacao    { background:#F0E8F8; color:#4E1F78; }
    .badge-engenharia   { background:#E8F4FF; color:#1a5fa5; }
    .badge-logistica    { background:#FFF8E6; color:#92500E; }
    .badge-atendimento  { background:#FFF0F6; color:#9D174D; }
    .badge-financeiro   { background:#F0FFF4; color:#1a6b3c; }
    .badge-parceria     { background:#FFF0F0; color:#991B1B; }
    .badge-crm          { background:#FFF5ED; color:#92400E; }
    .meta { font-size: 12px; color: #888; margin-top: 6px; }
    .stButton > button { border-radius: 8px !important; }
    .stButton > button[kind="primary"] {
        background-color: #00C4A0 !important;
        border-color: #00C4A0 !important;
        color: white !important;
    }
    .stButton > button[kind="primary"]:hover {
        background-color: #00957A !important;
        border-color: #00957A !important;
    }
    .stTabs [aria-selected="true"] {
        color: #6B2FA0 !important;
        border-bottom-color: #6B2FA0 !important;
    }
    .info-box {
        background: #E6FAF6;
        border-left: 3px solid #00C4A0;
        padding: 10px 14px;
        border-radius: 0 8px 8px 0;
        font-size: 13px;
        color: #00957A;
        margin-bottom: 1rem;
    }
    .edit-box {
        background: #F0E8F8;
        border-left: 3px solid #6B2FA0;
        padding: 10px 14px;
        border-radius: 0 8px 8px 0;
        font-size: 13px;
        color: #4E1F78;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)


# ── Banco de dados ────────────────────────────────────────────────────────────
def get_db():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS queries (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            nome        TEXT NOT NULL,
            descricao   TEXT,
            sql_texto   TEXT NOT NULL,
            area        TEXT DEFAULT 'dados',
            tabelas     TEXT,
            autor       TEXT,
            status      TEXT DEFAULT 'pendente',
            usos        INTEGER DEFAULT 0,
            criado_em   TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS uso_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            query_id    INTEGER,
            copiado_em  TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit()
    conn.close()

def get_queries(status="aprovada", area=None, busca=None):
    conn = get_db()
    sql = "SELECT * FROM queries WHERE status = ?"
    params = [status]
    if area and area != "todos":
        sql += " AND area = ?"
        params.append(area)
    if busca:
        sql += " AND (nome LIKE ? OR descricao LIKE ? OR tabelas LIKE ? OR sql_texto LIKE ?)"
        like = f"%{busca}%"
        params += [like, like, like, like]
    sql += " ORDER BY usos DESC, criado_em DESC"
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return rows

def registrar_uso(query_id):
    conn = get_db()
    conn.execute("UPDATE queries SET usos = usos + 1 WHERE id = ?", (query_id,))
    conn.execute("INSERT INTO uso_log (query_id) VALUES (?)", (query_id,))
    conn.commit()
    conn.close()

def submeter_query(nome, descricao, sql_texto, area, tabelas, autor):
    conn = get_db()
    conn.execute(
        "INSERT INTO queries (nome, descricao, sql_texto, area, tabelas, autor, status) VALUES (?,?,?,?,?,?,?)",
        (nome, descricao, sql_texto, area, tabelas, autor, "pendente")
    )
    conn.commit()
    conn.close()

def atualizar_query(query_id, nome, descricao, sql_texto, area, tabelas):
    conn = get_db()
    conn.execute(
        "UPDATE queries SET nome=?, descricao=?, sql_texto=?, area=?, tabelas=? WHERE id=?",
        (nome, descricao, sql_texto, area, tabelas, query_id)
    )
    conn.commit()
    conn.close()

def aprovar_query(query_id):
    conn = get_db()
    conn.execute("UPDATE queries SET status='aprovada' WHERE id=?", (query_id,))
    conn.commit()
    conn.close()

def rejeitar_query(query_id):
    conn = get_db()
    conn.execute("UPDATE queries SET status='rejeitada' WHERE id=?", (query_id,))
    conn.commit()
    conn.close()

def get_stats():
    conn = get_db()
    total     = conn.execute("SELECT COUNT(*) FROM queries WHERE status='aprovada'").fetchone()[0]
    areas     = conn.execute("SELECT COUNT(DISTINCT area) FROM queries WHERE status='aprovada'").fetchone()[0]
    usos      = conn.execute("SELECT COALESCE(SUM(usos),0) FROM queries WHERE status='aprovada'").fetchone()[0]
    pendentes = conn.execute("SELECT COUNT(*) FROM queries WHERE status='pendente'").fetchone()[0]
    conn.close()
    return total, areas, usos, pendentes


# ── Execução via API Metabase ─────────────────────────────────────────────────
METABASE_URL = "https://metabase.network.awsli.com.br"
METABASE_DB_ID = 11

def metabase_disponivel():
    try:
        s = st.secrets["metabase"]
        return "api_key" in s
    except:
        return False

def executar_no_metabase(sql_texto):
    try:
        import urllib.request, json
        s = st.secrets["metabase"]
        payload = json.dumps({
            "database": METABASE_DB_ID,
            "native":   {"query": sql_texto},
            "type":     "native",
        }).encode("utf-8")
        req = urllib.request.Request(
            f"{METABASE_URL}/api/dataset",
            data=payload,
            headers={"Content-Type": "application/json", "x-api-key": s["api_key"]},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        if "error" in data:
            return None, data["error"]
        cols = [c["display_name"] for c in data["data"]["cols"]]
        rows = data["data"]["rows"][:500]
        return pd.DataFrame(rows, columns=cols), None
    except Exception as e:
        return None, str(e)


# ── Init ──────────────────────────────────────────────────────────────────────
init_db()

# ── Login geral ───────────────────────────────────────────────────────────────
def check_login():
    if "logado" not in st.session_state:
        st.session_state["logado"] = False
    return st.session_state["logado"]

def tela_login():
    col1, col2, col3 = st.columns([1, 1.5, 1])
    with col2:
        st.markdown("""
        <div style="background:linear-gradient(135deg,#00C4A0,#6B2FA0);padding:28px;border-radius:16px;text-align:center;color:white;margin:2rem 0 1.5rem">
            <div style="font-size:32px;margin-bottom:8px">🗄️</div>
            <div style="font-size:20px;font-weight:600;margin-bottom:4px">Query Hub</div>
            <div style="font-size:13px;opacity:.85">Loja Integrada · Time de Automação</div>
        </div>
        """, unsafe_allow_html=True)
        senha = st.text_input("Senha de acesso", type="password", placeholder="Digite a senha")
        if st.button("Entrar", type="primary", use_container_width=True):
            try:
                senha_correta = st.secrets["acesso"]["senha"]
            except:
                senha_correta = ""
            if senha == senha_correta:
                st.session_state["logado"] = True
                st.rerun()
            else:
                st.error("Senha incorreta. Tente novamente.")
        st.caption("Acesso restrito aos colaboradores da Loja Integrada.")
    st.stop()

if not check_login():
    tela_login()

# ── Constantes ────────────────────────────────────────────────────────────────
AREAS = ["todos", "dados", "automacao", "engenharia", "logistica", "atendimento", "financeiro", "parceria", "crm"]
AREAS_FORM = ["dados", "automacao", "engenharia", "logistica", "atendimento", "financeiro", "parceria", "crm"]
BADGE_CLASS = {
    "dados": "badge-dados", "automacao": "badge-automacao",
    "engenharia": "badge-engenharia", "logistica": "badge-logistica",
    "atendimento": "badge-atendimento", "financeiro": "badge-financeiro",
    "parceria": "badge-parceria", "crm": "badge-crm",
}

# ── Hero Banner ───────────────────────────────────────────────────────────────
total, areas, usos, pendentes = get_stats()

st.markdown("""
<div class="hero-banner">
    <div class="hero-title">🗄️ Query Hub</div>
    <div class="hero-sub">Repositório central de queries SQL · Loja Integrada · Time de Automação</div>
</div>
""", unsafe_allow_html=True)

# ── Stats ─────────────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
c1.metric("Queries aprovadas", total)
c2.metric("Times cobertos", areas)
c3.metric("Usos registrados", usos)
c4.metric("Aguardando aprovação", pendentes)

st.divider()

# ── Session state para edição ─────────────────────────────────────────────────
if "editando_id" not in st.session_state:
    st.session_state["editando_id"] = None

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_catalogo, tab_aprovacao, tab_submeter = st.tabs([
    "📋 Catálogo",
    f"✅ Aprovação ({pendentes})",
    "➕ Sugerir query",
])


# ════════════════════════════════════════════════════════════════════════════
# TAB CATÁLOGO
# ════════════════════════════════════════════════════════════════════════════
with tab_catalogo:
    col_busca, col_area = st.columns([3, 1])
    with col_busca:
        busca = st.text_input("", placeholder="🔍  Buscar por nome, tabela ou área... ex: churn, gmv, pedidos", label_visibility="collapsed")
    with col_area:
        area_sel = st.selectbox("", AREAS, label_visibility="collapsed",
            format_func=lambda x: "Todos os times" if x == "todos" else x.capitalize())

    queries = get_queries(area=area_sel if area_sel != "todos" else None, busca=busca or None)

    if not queries:
        st.info("Nenhuma query encontrada. Tente outro termo ou área.")
    else:
        for q in queries:
            badge_cls = BADGE_CLASS.get(q["area"], "badge-dados")
            with st.expander(f"**{q['nome']}**  ·  {q['descricao'] or ''}"):

                # ── Modo edição ──
                if st.session_state["editando_id"] == q["id"]:
                    st.markdown('<div class="edit-box">✏️ Modo edição — altere os campos e salve</div>', unsafe_allow_html=True)

                    with st.form(f"edit_form_{q['id']}"):
                        e_nome     = st.text_input("Nome", value=q["nome"])
                        e_desc     = st.text_area("Descrição", value=q["descricao"] or "", height=80)
                        col_ea, col_eb = st.columns(2)
                        with col_ea:
                            e_area = st.selectbox("Área", AREAS_FORM,
                                index=AREAS_FORM.index(q["area"]) if q["area"] in AREAS_FORM else 0,
                                format_func=lambda x: x.capitalize())
                        with col_eb:
                            e_tabelas = st.text_input("Tabelas", value=q["tabelas"] or "")
                        e_sql = st.text_area("SQL", value=q["sql_texto"], height=200)

                        col_s, col_c = st.columns([1, 1])
                        with col_s:
                            salvar = st.form_submit_button("💾 Salvar alterações", type="primary", use_container_width=True)
                        with col_c:
                            cancelar = st.form_submit_button("Cancelar", use_container_width=True)

                        if salvar:
                            if not e_nome or not e_sql:
                                st.error("Nome e SQL são obrigatórios.")
                            else:
                                atualizar_query(q["id"], e_nome, e_desc, e_sql, e_area, e_tabelas)
                                st.session_state["editando_id"] = None
                                st.toast("Query atualizada! ✓", icon="✅")
                                st.rerun()
                        if cancelar:
                            st.session_state["editando_id"] = None
                            st.rerun()

                # ── Modo visualização ──
                else:
                    col_info, col_acoes = st.columns([4, 1])
                    with col_info:
                        st.markdown(
                            f'<span class="badge {badge_cls}">{q["area"].capitalize()}</span>'
                            f'<span class="meta">tabelas: {q["tabelas"] or "—"}  ·  autor: {q["autor"] or "—"}  ·  usado {q["usos"]}x</span>',
                            unsafe_allow_html=True
                        )
                    with col_acoes:
                        col_cp, col_ed = st.columns(2)
                        with col_cp:
                            if st.button("📋", key=f"copy_{q['id']}", help="Copiar SQL"):
                                registrar_uso(q["id"])
                                st.toast("SQL copiado! ✓", icon="✅")
                        with col_ed:
                            if st.button("✏️", key=f"edit_{q['id']}", help="Editar query"):
                                st.session_state["editando_id"] = q["id"]
                                st.rerun()

                    st.code(q["sql_texto"], language="sql")

                    if metabase_disponivel():
                        if st.button("▶ Executar no Metabase", key=f"run_{q['id']}", type="primary"):
                            with st.spinner("Executando query no StarRocks..."):
                                df, erro = executar_no_metabase(q["sql_texto"])
                            if erro:
                                st.error(f"Erro: {erro}")
                            else:
                                st.success(f"{len(df)} linhas retornadas")
                                st.dataframe(df, use_container_width=True)
                                csv = df.to_csv(index=False).encode("utf-8")
                                st.download_button(
                                    "⬇ Baixar CSV",
                                    data=csv,
                                    file_name=f"{q['nome'].replace(' ', '_')}.csv",
                                    mime="text/csv",
                                    key=f"dl_{q['id']}"
                                )


# ════════════════════════════════════════════════════════════════════════════
# TAB APROVAÇÃO — protegida por senha
# ════════════════════════════════════════════════════════════════════════════
with tab_aprovacao:
    if "aprovacao_autenticada" not in st.session_state:
        st.session_state["aprovacao_autenticada"] = False

    if not st.session_state["aprovacao_autenticada"]:
        col1, col2, col3 = st.columns([1, 1.5, 1])
        with col2:
            st.markdown("""
            <div style="background:#F0E8F8;border-radius:12px;padding:20px;text-align:center;margin:1rem 0">
                <div style="font-size:24px;margin-bottom:8px">🔐</div>
                <div style="font-size:15px;font-weight:600;color:#4E1F78;margin-bottom:4px">Área restrita</div>
                <div style="font-size:12px;color:#6B2FA0">Exclusivo para o time de automação</div>
            </div>
            """, unsafe_allow_html=True)
            senha_ap = st.text_input("Senha do time de automação", type="password", placeholder="Digite a senha")
            if st.button("Entrar na aprovação", type="primary", use_container_width=True):
                try:
                    senha_correta = st.secrets["aprovacao"]["senha"]
                except:
                    senha_correta = ""
                if senha_ap == senha_correta:
                    st.session_state["aprovacao_autenticada"] = True
                    st.rerun()
                else:
                    st.error("Senha incorreta.")
    else:
        col_titulo, col_sair = st.columns([5, 1])
        with col_titulo:
            st.markdown("##### Queries aguardando aprovação")
        with col_sair:
            if st.button("Sair"):
                st.session_state["aprovacao_autenticada"] = False
                st.rerun()

        pendentes_list = get_queries(status="pendente")

        if not pendentes_list:
            st.success("Nenhuma query aguardando aprovação. 🎉")
        else:
            for q in pendentes_list:
                with st.expander(f"**{q['nome']}**  ·  submetida por {q['autor'] or 'anônimo'}"):
                    st.markdown(f"**Descrição:** {q['descricao'] or '—'}")
                    st.markdown(f"**Área:** `{q['area']}`  ·  **Tabelas:** `{q['tabelas'] or '—'}`")
                    st.code(q["sql_texto"], language="sql")

                    if metabase_disponivel():
                        if st.button("🧪 Testar antes de aprovar", key=f"test_{q['id']}"):
                            with st.spinner("Testando..."):
                                df, erro = executar_no_metabase(q["sql_texto"])
                            if erro:
                                st.error(f"Query com erro: {erro}")
                            else:
                                st.success(f"Query OK — {len(df)} linhas")
                                st.dataframe(df.head(10), use_container_width=True)

                    col_ap, col_rej, _ = st.columns([1, 1, 4])
                    with col_ap:
                        if st.button("✅ Aprovar", key=f"ap_{q['id']}", type="primary"):
                            aprovar_query(q["id"])
                            st.rerun()
                    with col_rej:
                        if st.button("❌ Rejeitar", key=f"rej_{q['id']}"):
                            rejeitar_query(q["id"])
                            st.rerun()


# ════════════════════════════════════════════════════════════════════════════
# TAB SUBMETER
# ════════════════════════════════════════════════════════════════════════════
with tab_submeter:
    st.markdown("""
    <div class="info-box">
        Como funciona: você sugere → time de automação revisa → query entra no catálogo para toda a empresa
    </div>
    """, unsafe_allow_html=True)

    if "form_enviado" not in st.session_state:
        st.session_state["form_enviado"] = False

    if st.session_state["form_enviado"]:
        st.success("Query enviada com sucesso! O time de automação vai revisar em breve. 🚀")
        if st.button("Enviar outra query"):
            st.session_state["form_enviado"] = False
            st.rerun()
    else:
        with st.form("form_query", clear_on_submit=True):
            nome      = st.text_input("Nome da query *", placeholder="ex: Churn semanal por plano")
            descricao = st.text_area("Descrição", placeholder="O que essa query retorna? Quando usar?", height=80)
            col_a, col_b = st.columns(2)
            with col_a:
                area = st.selectbox("Time / Área *", AREAS_FORM, format_func=lambda x: x.capitalize())
            with col_b:
                tabelas = st.text_input("Tabelas usadas", placeholder="ex: orders, merchants")
            autor     = st.text_input("Seu nome", placeholder="ex: Amanda Lino")
            sql_texto = st.text_area("SQL *", placeholder="SELECT ...", height=200)

            submitted = st.form_submit_button("Enviar para aprovação", type="primary", use_container_width=True)
            if submitted:
                if not nome or not sql_texto:
                    st.error("Nome e SQL são obrigatórios.")
                else:
                    submeter_query(nome, descricao, sql_texto, area, tabelas, autor)
                    st.session_state["form_enviado"] = True
                    st.rerun()
