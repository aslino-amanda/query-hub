import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
import os

st.set_page_config(
    page_title="Query Hub · Loja Integrada",
    page_icon="🗄️",
    layout="wide",
)

DB_PATH = "data/queries.db"

# ── CSS ──────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .block-container { padding-top: 2rem; max-width: 960px; }
    .badge {
        display: inline-block;
        padding: 2px 10px;
        border-radius: 99px;
        font-size: 11px;
        font-weight: 500;
        margin-right: 6px;
    }
    .badge-dados        { background:#DBEAFE; color:#1E40AF; }
    .badge-automacao    { background:#EDE9FE; color:#4C1D95; }
    .badge-engenharia   { background:#D1FAE5; color:#065F46; }
    .badge-logistica    { background:#FEF3C7; color:#92400E; }
    .badge-atendimento  { background:#FCE7F3; color:#9D174D; }
    .badge-financeiro   { background:#ECFDF5; color:#065F46; }
    .badge-parceria     { background:#FEE2E2; color:#991B1B; }
    .badge-crm          { background:#FFF7ED; color:#92400E; }
    .meta { font-size: 12px; color: #999; margin-top: 6px; }
</style>
""", unsafe_allow_html=True)


# ── Banco de dados (catálogo) ─────────────────────────────────────────────────
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
            area        TEXT DEFAULT 'outros',
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
    usos_mes  = conn.execute("SELECT COALESCE(SUM(usos),0) FROM queries WHERE status='aprovada'").fetchone()[0]
    pendentes = conn.execute("SELECT COUNT(*) FROM queries WHERE status='pendente'").fetchone()[0]
    conn.close()
    return total, areas, usos_mes, pendentes


# ── Execução via API Metabase ─────────────────────────────────────────────────
METABASE_URL = "https://metabase.network.awsli.com.br"
METABASE_DB_ID = 11

def metabase_disponivel():
    try:
        s = st.secrets["metabase"]
        return "api_key" in s
    except:
        return False

def executar_no_starrocks(sql_texto):
    try:
        import urllib.request
        import json
        s = st.secrets["metabase"]
        api_key = s["api_key"]

        payload = json.dumps({
            "database": METABASE_DB_ID,
            "native":   {"query": sql_texto},
            "type":     "native",
        }).encode("utf-8")

        req = urllib.request.Request(
            f"{METABASE_URL}/api/dataset",
            data=payload,
            headers={
                "Content-Type":  "application/json",
                "x-api-key":     api_key,
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        if "error" in data:
            return None, data["error"]

        cols = [c["display_name"] for c in data["data"]["cols"]]
        rows = data["data"]["rows"][:500]
        df = pd.DataFrame(rows, columns=cols)
        return df, None
    except Exception as e:
        return None, str(e)


# ── Init ──────────────────────────────────────────────────────────────────────
init_db()

# ── Login ─────────────────────────────────────────────────────────────────────
def check_login():
    if "logado" not in st.session_state:
        st.session_state["logado"] = False
    return st.session_state["logado"]

def tela_login():
    st.markdown("## 🗄️ Query Hub")
    st.caption("Loja Integrada · Time de Automação")
    st.divider()
    st.markdown("##### Acesso restrito")
    st.caption("Digite a senha para acessar o repositório de queries.")
    senha = st.text_input("Senha", type="password", placeholder="Digite sua senha")
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
    st.stop()

if not check_login():
    tela_login()

AREAS = ["todos", "dados", "automacao", "engenharia", "logistica", "atendimento", "financeiro", "parceria", "crm"]
BADGE_CLASS = {
    "dados":        "badge-dados",
    "automacao":    "badge-automacao",
    "engenharia":   "badge-engenharia",
    "logistica":    "badge-logistica",
    "atendimento":  "badge-atendimento",
    "financeiro":   "badge-financeiro",
    "parceria":     "badge-parceria",
    "crm":          "badge-crm",
}

# ── Header ────────────────────────────────────────────────────────────────────
col_logo, col_btn = st.columns([5, 1])
with col_logo:
    st.markdown("## 🗄️ Query Hub")
    st.caption("Loja Integrada · Time de Automação")
with col_btn:
    st.write("")
    if st.button("＋ Sugerir query", use_container_width=True):
        st.session_state["show_form"] = True

# ── Stats ─────────────────────────────────────────────────────────────────────
total, areas, usos_mes, pendentes = get_stats()
c1, c2, c3, c4 = st.columns(4)
c1.metric("Queries aprovadas", total)
c2.metric("Áreas cobertas", areas)
c3.metric("Usos registrados", usos_mes)
c4.metric("Aguardando aprovação", pendentes)

# ── Aviso Metabase ────────────────────────────────────────────────────────────
if not metabase_disponivel():
    st.info("💡 Execução de queries desativada — configure a chave de API do Metabase em Settings → Secrets para ativar.")

st.divider()

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
        busca = st.text_input("", placeholder="🔍  Buscar por nome, tabela, área... ex: churn, gmv, pedidos", label_visibility="collapsed")
    with col_area:
        area_sel = st.selectbox("Área", AREAS, label_visibility="collapsed")

    queries = get_queries(area=area_sel if area_sel != "todos" else None, busca=busca or None)

    if not queries:
        st.info("Nenhuma query encontrada. Tente outro termo ou área.")
    else:
        for q in queries:
            badge_cls = BADGE_CLASS.get(q["area"], "badge-outros")
            with st.expander(f"**{q['nome']}**  ·  {q['descricao'] or ''}"):
                col_info, col_acoes = st.columns([4, 1])
                with col_info:
                    st.markdown(
                        f'<span class="badge {badge_cls}">{q["area"].capitalize()}</span>'
                        f'<span class="meta">tabelas: {q["tabelas"] or "—"}  ·  autor: {q["autor"] or "—"}  ·  usado {q["usos"]}x</span>',
                        unsafe_allow_html=True
                    )
                with col_acoes:
                    if st.button("📋 Copiar SQL", key=f"copy_{q['id']}"):
                        registrar_uso(q["id"])
                        st.toast("SQL copiado! ✓", icon="✅")

                st.code(q["sql_texto"], language="sql")

                # ── Botão Executar ──
                if metabase_disponivel():
                    if st.button("▶ Executar no Metabase", key=f"run_{q['id']}", type="primary"):
                        with st.spinner("Executando query..."):
                            df, erro = executar_no_starrocks(q["sql_texto"])
                        if erro:
                            st.error(f"Erro ao executar: {erro}")
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
        st.markdown("##### Acesso restrito ao time de automação")
        senha_input = st.text_input("Senha", type="password", placeholder="Digite a senha do time")
        if st.button("Entrar", type="primary"):
            try:
                senha_correta = st.secrets["aprovacao"]["senha"]
            except:
                senha_correta = ""
            if senha_input == senha_correta:
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
                        if st.button("🧪 Testar query", key=f"test_{q['id']}"):
                            with st.spinner("Testando..."):
                                df, erro = executar_no_starrocks(q["sql_texto"])
                            if erro:
                                st.error(f"Query com erro: {erro}")
                            else:
                                st.success(f"Query OK — {len(df)} linhas retornadas")
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
    st.markdown("##### Sugira uma query para o catálogo")
    st.caption("O time de automação vai revisar antes de publicar.")

    if "form_enviado" not in st.session_state:
        st.session_state["form_enviado"] = False

    if st.session_state["form_enviado"]:
        st.success("Query enviada! O time vai revisar em breve. 🚀")
        if st.button("Enviar outra query"):
            st.session_state["form_enviado"] = False
            st.rerun()
    else:
        with st.form("form_query", clear_on_submit=True):
            nome      = st.text_input("Nome da query *", placeholder="ex: Churn semanal por plano")
            descricao = st.text_area("Descrição", placeholder="O que essa query retorna? Quando usar?", height=80)
            col_a, col_b = st.columns(2)
            with col_a:
                area = st.selectbox("Área *", ["dados", "automacao", "engenharia", "logistica", "atendimento", "financeiro", "parceria", "crm"])
            with col_b:
                tabelas = st.text_input("Tabelas usadas", placeholder="ex: orders, merchants")
            autor     = st.text_input("Seu nome", placeholder="ex: João Silva")
            sql_texto = st.text_area("SQL *", placeholder="SELECT ...", height=200)

            submitted = st.form_submit_button("Enviar para aprovação", type="primary", use_container_width=True)
            if submitted:
                if not nome or not sql_texto:
                    st.error("Nome e SQL são obrigatórios.")
                else:
                    submeter_query(nome, descricao, sql_texto, area, tabelas, autor)
                    st.session_state["form_enviado"] = True
                    st.rerun()
