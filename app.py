# app.py
# -*- coding: utf-8 -*-
import os
import itertools
from pathlib import Path
from datetime import datetime
from contextlib import contextmanager
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import select

# ----- ENGINE -----
from engine.storage import db, init_db, Transaction, Statement, Category
from engine.importador import detect_and_load, to_standard_df, import_file_as_statement
from engine.budgets import (
    totais_consolidados,
    df_despesas_por_categoria,
    df_categoria_x_mes,
    df_gastos_por_origem,
    transactions_for_category_month,
    df_radar_completo_x_mes,
)
from engine.classificador import apply_category_bulk, classify_batch, apply_smart_rules_to_statement, apply_radar_rules_to_statement
from engine.utils import save_temp, fmt_brl

# ===== BOOTSTRAP =====
FIN_DATA_DIR = os.getenv("FIN_DATA_DIR", "./data")
FIN_BACKUP_DIR = os.getenv("FIN_BACKUP_DIR", "./backup")
os.makedirs(FIN_DATA_DIR, exist_ok=True)
os.makedirs(FIN_BACKUP_DIR, exist_ok=True)
os.makedirs(os.path.join(FIN_DATA_DIR, "tmp"), exist_ok=True)

init_db()
from engine.portfolio_storage import init_db as portfolio_init_db
portfolio_init_db()
st.set_page_config(page_title="Meu Financeiro", layout="wide")

# ===== CONSTS/UTIL =====
MESES = ["jan","fev","mar","abr","mai","jun","jul","ago","set","out","nov","dez"]
MESES_MAP = {"jan":1,"fev":2,"mar":3,"abr":4,"mai":5,"jun":6,"jul":7,"ago":8,"set":9,"out":10,"nov":11,"dez":12}
CONTAS_OPTS = ["AAdvantage CC", "XP CC", "Personnalite CC", "Personnalite conta", "XP conta", "Santander conta", "BTG conta"]

def load_version() -> str:
    candidates = [
        Path(__file__).resolve().parent / "VERSION",
        Path(__file__).resolve().parent.parent / "VERSION",
        Path.cwd() / "VERSION",
    ]
    for version_path in candidates:
        try:
            if version_path.exists():
                value = version_path.read_text(encoding="utf-8").strip()
                if value:
                    return value
        except Exception:
            continue
    return "DEV"

APP_VERSION = load_version()

def _fmt_short(value: float) -> str:
    try:
        val = float(value)
    except Exception:
        return str(value)
    sign = "-" if val < 0 else ""
    val = abs(val)
    if val >= 1_000_000:
        return f"{sign}{val/1_000_000:.1f}M"
    if val >= 1_000:
        return f"{sign}{val/1_000:.1f}k"
    return f"{sign}{val:.0f}"

def card_kpi(label: str, valor: float, col, meta: float | None = None, detalhe: str | None = None):
    detail_html = "<div class='kpi-detail kpi-detail--empty'>&nbsp;</div>"
    if detalhe is not None:
        detail_html = detalhe
    elif meta is not None:
        delta = valor - meta
        is_good = valor >= meta
        status = "Bom" if is_good else "Atenção"
        color = "#1b7f5d" if is_good else "#b3261e"
        detail_html = (
            f"<div class='kpi-detail' style='color:{color};'>"
            f"{status} · Meta {_fmt_short(meta)} · Δ {_fmt_short(delta)}"
            "</div>"
        )
    col.markdown(
        f"""
        <div class='kpi-card'>
            <div class='kpi-row'>
                <div class='kpi-label'>{label}</div>
                {detail_html}
            </div>
            <div class='kpi-value'>{fmt_brl(valor, with_symbol=True)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

_panel_counter = itertools.count()

@contextmanager
def panel(title: str | None = None):
    key = f"panel_{next(_panel_counter)}"
    container = st.container(border=True, key=key)
    with container:
        if title:
            st.markdown(f"<div class='md-panel-title'>{title}</div>", unsafe_allow_html=True)
        yield

def adjust_negative_axis(fig, series):
    if not series.empty and series.le(0).all():
        fig.update_yaxes(autorange="reversed")

def backup_arquivo_local(caminho_original: str, prefix: str = "upload"):
    try:
        base = os.path.basename(caminho_original)
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        dest = os.path.join(FIN_BACKUP_DIR, f"{prefix}_{ts}_{base}")
        with open(caminho_original, "rb") as src, open(dest, "wb") as dst:
            dst.write(src.read())
        return dest
    except Exception:
        return None

def list_statements(origin_filter:str="", period_filter:str=""):
    q = db.session.query(Statement)
    if origin_filter:
        q = q.filter(Statement.origin_label.ilike(f"%{origin_filter}%"))
    if period_filter:
        if period_filter.isdigit():
            q = q.filter(Statement.period_yyyymm == int(period_filter))
        else:
            q = q.filter(Statement.period_label.ilike(f"%{period_filter}%"))
    q = q.order_by(Statement.imported_at.desc()).limit(200)
    rows = []
    for s in q.all():
        rows.append({
            "Lote": s.id,
            "Quando": s.imported_at.strftime("%d/%m/%Y %H:%M"),
            "Origem": s.origin_label or "",
            "Período": s.period_label or s.period_yyyymm,
            "Conta": s.account_id or "",
            "Arquivo": s.source_name or "",
            "Linhas": s.rows or 0,
        })
    return pd.DataFrame(rows)

def compute_budget_data(origin_filter: str | None, year: int | None = None):
    tabela = df_categoria_x_mes(year=year, origin=origin_filter)
    cols_mes = [c for c in tabela.columns if c != "Categoria"]
    categorias = db.session.query(Category).order_by(Category.name.asc()).all()
    rows = []
    meta_map = {}
    month_counts = {}
    for cat in categorias:
        media = 0.0
        month_count = len(cols_mes)
        if not tabela.empty and cols_mes:
            match = tabela.loc[tabela["Categoria"] == cat.name]
            if not match.empty:
                serie = pd.to_numeric(match[cols_mes].iloc[0], errors="coerce").fillna(0.0)
                valid = [float(v) for v in serie.tolist() if abs(float(v)) > 1e-6]
                if valid:
                    month_count = len(valid)
                    media = float(sum(valid) / len(valid))
        month_counts[cat.name] = month_count
        rows.append({"Categoria": cat.name, "Media": media, "Meta": cat.budget_meta})
        meta_map[cat.name] = cat.budget_meta if cat.budget_meta is not None else media
    return rows, meta_map, tabela, cols_mes, categorias, month_counts

def format_period_label(yyyymm: int | None) -> str:
    if not yyyymm:
        return "-"
    try:
        yyyymm = int(yyyymm)
    except Exception:
        return str(yyyymm)
    ano = yyyymm // 100
    mes = yyyymm % 100
    if mes < 1 or mes > 12:
        return str(yyyymm)
    return f"{MESES[mes-1]}/{str(ano)[-2:]}"

def parse_period_label(label: str, fallback_year: int | None = None) -> int | None:
    if not label or "/" not in label:
        return None
    mes_lbl, ano_lbl = label.split("/", 1)
    mes = MESES_MAP.get(mes_lbl.strip().lower()[:3])
    if not mes:
        return None
    ano_lbl = ano_lbl.strip()
    try:
        ano = int(ano_lbl)
        if ano < 100:
            ano += 2000
    except Exception:
        ano = fallback_year or datetime.now().year
    return ano * 100 + mes

def available_years() -> list[int]:
    rows = db.session.execute(select(Transaction.period_yyyymm).distinct()).all()
    anos = sorted({int(row[0]) // 100 for row in rows if row[0]})
    return anos

def available_periods() -> list[int]:
    rows = db.session.execute(select(Transaction.period_yyyymm).distinct()).all()
    periodos = sorted({int(row[0]) for row in rows if row[0]})
    return periodos

def available_origins() -> list[str]:
    rows = db.session.execute(select(Transaction.origin_label).distinct()).all()
    origens = sorted({str(row[0]) for row in rows if row[0]}, key=lambda s: s.lower())
    return origens or CONTAS_OPTS

# ===== SIDEBAR / NAV =====
with st.sidebar:
    st.title("Meu Dinheiro")
    page = st.radio(
        "Navegação",
        ["Meu Dinheiro", "Radar", "Despesas", "Categorias", "Importar Extratos", "Portfólio"],
        index=0
    )
    st.caption(f"v{APP_VERSION} - DEV")
    st.markdown("---")
    st.subheader("Filtros")
    anos_disp = available_years()
    if not anos_disp:
        anos_disp = [datetime.now().year]
    default_ano_idx = len(anos_disp) - 1
    filtro_ano = st.selectbox("Ano", anos_disp, index=default_ano_idx)

    origens = ["Todas"] + available_origins()
    selected_origin = st.selectbox("Conta / Origem", origens, index=0)

    periodos = available_periods()
    if periodos:
        period_labels = [format_period_label(p) for p in periodos]
        default_period_idx = len(period_labels) - 1
        selected_period_label = st.selectbox("Período", period_labels, index=default_period_idx)
        selected_period = dict(zip(period_labels, periodos))[selected_period_label]
    elif st.session_state.md_view == VIEW_MENSAL:
        selected_period_label = "-"
        st.selectbox("Período", ["Sem períodos disponíveis"], index=0, disabled=True)
        selected_period = None

origin_filter = selected_origin if selected_origin and selected_origin != "Todas" else None

# ===== MEU DINHEIRO =====
if page == "Meu Dinheiro":
    st.header("Meu Dinheiro")
    st.caption("Visão consolidada das despesas e receitas importadas.")
    if origin_filter:
        st.caption(f"Conta filtrada: **{selected_origin}**")

    st.markdown(
        """
        <style>
            section.main > div.block-container {
                max-width: 1100px;
                margin: 0 auto;
                padding-top: 1rem;
            }
            div[data-testid="stMetricValue"] {
                font-size: 1.5rem;
            }
            .md-panel-title {
                font-size: 1rem;
                font-weight: 600;
                margin-bottom: 16px;
            }
            div[data-testid="stContainer"][data-st-key^="panel_"] > div {
                background: #fff6ec !important;
                border: 1px solid #ffd3b5 !important;
                border-radius: 16px !important;
                padding: 16px 20px !important;
                margin-bottom: 18px !important;
            }
            div[data-testid="stMetric"] {
                background: #fff6ec;
                padding: 12px 18px;
                border-radius: 16px;
                border: 1px solid #ffe0c2;
                box-shadow: 0 6px 16px rgba(15,23,42,0.08);
            }
            .stDataFrame [role="columnheader"],
            div[data-testid="stDataFrame"] [role="columnheader"] {
                background-color: #fff3e6 !important;
                color: #5c2d1a !important;
                border-bottom: 1px solid #ffd8b5 !important;
            }

            .stDataFrame [role="gridcell"],
            div[data-testid="stDataFrame"] [role="gridcell"] {
                border-bottom: 1px solid #ffe5cc !important;
            }
            .stDataFrame [data-testid="StyledDataFrame"],
            div[data-testid="stDataFrame"] [data-testid="StyledDataFrame"] {
                border: 1px solid #ffe0c2 !important;
                border-radius: 8px;
            }
            .view-toggle div[data-st-key="btn_anual"] button {
                border-radius: 999px;
                font-weight: 600;
                background: #fff6ec;
                border: 1px solid #ffe0c2;
                color: #5c2d1a;
            }
            .view-toggle div[data-st-key="btn_anual"] button:hover {
                background: #ffe8d2;
            }
            .view-toggle div[data-st-key="btn_mensal"] button {
                border-radius: 999px;
                font-weight: 600;
                background: #ffd6ad;
                border: 1px solid #ffba75;
                color: #4a1f0e;
            }
            .view-toggle div[data-st-key="btn_mensal"] button:hover {
                background: #ffc48e;
            }
            .view-toggle div[data-st-key="btn_orcamento"] button {
                border-radius: 999px;
                font-weight: 600;
                background: #ffe3c7;
                border: 1px solid #ffc493;
                color: #4a1f0e;
            }
            .view-toggle div[data-st-key="btn_orcamento"] button:hover {
                background: #ffd4a8;
            }
            .annual-metrics div[data-testid="column"]:nth-child(4) div[data-testid="stMetric"] {
                background: #ffd6ad;
                border: 1px solid #ffba75;
            }
            .annual-metrics div[data-testid="column"]:nth-child(4) div[data-testid="stMetricValue"],
            .annual-metrics div[data-testid="column"]:nth-child(4) div[data-testid="stMetricLabel"] {
                color: #4a1f0e;
            }
            .monthly-metrics div[data-testid="column"]:last-child div[data-testid="stMetric"] {
                background: #ffd6ad;
                border: 1px solid #ffba75;
            }
            .monthly-metrics div[data-testid="column"]:last-child div[data-testid="stMetricValue"],
            .monthly-metrics div[data-testid="column"]:last-child div[data-testid="stMetricLabel"] {
                color: #4a1f0e;
            }
            div[data-testid="stContainer"][data-st-key^="radar_card_"] > div {
                background: #fff6ec;
                border: 1px solid #ffd3b5;
                border-radius: 16px;
                padding: 18px 20px;
                margin-bottom: 24px;
            }
            .annual-metrics div[data-testid="column"]:nth-child(4) div[data-testid="stMetric"],
            .monthly-metrics div[data-testid="column"]:last-child div[data-testid="stMetric"] {
                background: #ffd6ad;
                border: 1px solid #ffba75;
            }
            .annual-metrics div[data-testid="column"]:nth-child(4) div[data-testid="stMetricValue"],
            .monthly-metrics div[data-testid="column"]:last-child div[data-testid="stMetricValue"] {
                color: #3e1807;
            }
            .annual-metrics div[data-testid="column"]:nth-child(4) div[data-testid="stMetricLabel"],
            .monthly-metrics div[data-testid="column"]:last-child div[data-testid="stMetricLabel"] {
                color: #6d2c14;
            }
            .view-toggle div[data-st-key="btn_anual"] button {
                border-radius: 999px;
                font-weight: 600;
                background: #fff6ec;
                border: 1px solid #ffe0c2;
                color: #5c2d1a;
            }
            .view-toggle div[data-st-key="btn_anual"] button:hover {
                background: #ffe8d2;
            }
            .view-toggle div[data-st-key="btn_mensal"] button {
                border-radius: 999px;
                font-weight: 600;
                background: #ffd6ad;
                border: 1px solid #ffba75;
                color: #4a1f0e;
            }
            .view-toggle div[data-st-key="btn_mensal"] button:hover {
                background: #ffc48e;
            }
            .alert-badge-green {
                display: inline-block;
                background: #e8fff4;
                color: #1b7f5d;
                font-weight: 600;
                padding: 4px 12px;
                border-radius: 999px;
                border: 1px solid #b4f0d3;
                margin-bottom: 0.5rem;
            }
            .alert-badge {
                display: inline-block;
                background: #ffe8e5;
                color: #7d1c0d;
                font-weight: 600;
                padding: 4px 12px;
                border-radius: 999px;
                border: 1px solid #f5b1a6;
                margin-bottom: 0.5rem;
            }
            .kpi-card {
                background: #fff6ec;
                border: 1px solid #ffd3b5;
                border-radius: 16px;
                padding: 12px 18px;
                margin-bottom: 12px;
                box-shadow: 0 6px 18px rgba(15,23,42,0.08);
            }
            .kpi-row {
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 8px;
            }
            .kpi-label {
                font-size: 0.9rem;
                color: #5c2d1a;
            }
            .kpi-value {
                font-size: 1.6rem;
                font-weight: 600;
                color: #2f1509;
            }
            .kpi-detail {
                font-size: 0.85rem;
                font-weight: 600;
            }
            .kpi-detail--empty {
                visibility: hidden;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("""
<script>
function fixTableHeaders() {
    document.querySelectorAll('div[data-testid="stDataFrame"] div[class*="gdg-s"]').forEach(el => {
        el.style.setProperty('--gdg-bg-header', '#fff3e6', 'important');
        el.style.setProperty('--gdg-bg-header-hovered', '#ffe8cc', 'important');
        el.style.setProperty('--gdg-text-header', '#5c2d1a', 'important');
    });
}
const observer = new MutationObserver(fixTableHeaders);
observer.observe(document.body, {childList: true, subtree: true});
fixTableHeaders();
</script>
""", unsafe_allow_html=True)

    VIEW_ANUAL = "annual"
    VIEW_MENSAL = "monthly"
    VIEW_ORCAMENTO = "budget"

    if "md_view" not in st.session_state:
        st.session_state.md_view = VIEW_ANUAL

    st.markdown('<div class="view-toggle">', unsafe_allow_html=True)
    toggle1, toggle2, toggle3, toggle_info = st.columns([1, 1, 1, 2])
    with toggle1:
        if st.button("Visão Anual", use_container_width=True, key="btn_anual"):
            st.session_state.md_view = VIEW_ANUAL
    with toggle2:
        if st.button("Visão Mensal", use_container_width=True, key="btn_mensal"):
            st.session_state.md_view = VIEW_MENSAL
    with toggle3:
        if st.button("Orçamento", use_container_width=True, key="btn_orcamento"):
            st.session_state.md_view = VIEW_ORCAMENTO
    with toggle_info:
        current_label = {
            VIEW_ANUAL: "Visão Anual",
            VIEW_MENSAL: "Visão Mensal",
            VIEW_ORCAMENTO: "Orçamento",
        }.get(st.session_state.md_view, "Visão Anual")
        st.caption(f"Visão atual: **{current_label}**")
    st.markdown('</div>', unsafe_allow_html=True)

    st.divider()

    if st.session_state.md_view == VIEW_ANUAL:
        st.subheader(f"Resumo de {filtro_ano}")
        tot_desp_ano, tot_rec_ano = totais_consolidados(year=filtro_ano, origin=origin_filter)
        periodo_desp, periodo_rec = (
            totais_consolidados(period_yyyymm=selected_period, origin=origin_filter)
            if selected_period else
            (0.0, 0.0)
        )

        _, budget_meta_map_year, _, cols_mes_ano, categorias_ano, _ = compute_budget_data(origin_filter, year=filtro_ano)
        meses_importados = len(cols_mes_ano) or 0
        meta_total_ano = sum((budget_meta_map_year.get(cat.name, 0.0) or 0.0) * 12 for cat in categorias_ano)
        meta_proporcional = (meta_total_ano / 12) * meses_importados if meta_total_ano else 0.0
        no_ritmo = abs(tot_desp_ano) <= abs(meta_proporcional)

        with st.container():
            st.markdown('<div class="annual-metrics">', unsafe_allow_html=True)
            kpi1, kpi2, kpi3, kpi4 = st.columns(4)
            if meta_total_ano:
                _status_ritmo = "No ritmo" if no_ritmo else "Acima do ritmo"
                _cor_ritmo = "#1b7f5d" if no_ritmo else "#b3261e"
                _detalhe_desp = (
                    f"<div class='kpi-detail' style='color:{_cor_ritmo};'>"
                    f"Meta anual {_fmt_short(meta_total_ano)} · Ritmo: {_status_ritmo}"
                    "</div>"
                )
                card_kpi("Despesas", tot_desp_ano, kpi1, detalhe=_detalhe_desp)
            else:
                card_kpi("Despesas", tot_desp_ano, kpi1)
            card_kpi("Receitas", tot_rec_ano, kpi2)
            card_kpi("Saldo", tot_rec_ano + tot_desp_ano, kpi3)
            label_periodo = selected_period_label if selected_period else "Período"
            card_kpi("Saldo (período)", periodo_rec + periodo_desp, kpi4)
            st.markdown('</div>', unsafe_allow_html=True)
        if meta_total_ano:
            _diferenca = abs(abs(tot_desp_ano) - abs(meta_proporcional))
            _meses_label = f"{meses_importados} {'mês' if meses_importados == 1 else 'meses'}"
            if not no_ritmo:
                st.markdown(
                    f"<div class='alert-badge'>Atenção: despesas {fmt_brl(_diferenca)} acima do ritmo esperado "
                    f"para o período (meta {_meses_label}: {fmt_brl(abs(meta_proporcional))})</div>",
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f"<div class='alert-badge-green'>No ritmo: despesas {fmt_brl(_diferenca)} abaixo da meta do período</div>",
                    unsafe_allow_html=True,
                )

        col_cat, col_origin = st.columns((2, 1), gap="large")
        _, annual_meta_map, _, _, _, _ = compute_budget_data(origin_filter, year=filtro_ano)
        with col_cat:
            gcat = df_despesas_por_categoria(
                year=filtro_ano,
                origin=origin_filter,
                excluir_positivas=True,
            )
            meta_totals = {}
            if not gcat.empty:
                categories_order = gcat["Categoria"].tolist()
                palette = px.colors.qualitative.Pastel + px.colors.qualitative.Set2 + px.colors.qualitative.Safe
                color_map = {
                    cat: palette[i % len(palette)]
                    for i, cat in enumerate(categories_order)
                }
                meta_totals = {}
                meta_line = []
                for cat in categories_order:
                    monthly_meta = annual_meta_map.get(cat, 0.0) or 0.0
                    meta_total = monthly_meta * 12
                    meta_totals[cat] = meta_total
                    meta_line.append(-abs(meta_total))
                fig = go.Figure()
                fig.add_bar(
                    x=categories_order,
                    y=gcat["Total"],
                    name="Despesa",
                    marker_color=[color_map[c] for c in categories_order],
                    hovertemplate="%{x}: %{y:.2f}<extra></extra>",
                )
                fig.add_trace(
                    go.Scatter(
                        x=categories_order,
                        y=meta_line,
                        mode="lines+markers",
                        name="Meta anual",
                        line=dict(color="#d95829", dash="dash"),
                        marker=dict(symbol="circle", size=8),
                        hovertemplate="Meta %{x}: %{y:.2f}<extra></extra>",
                    )
                )
                fig.update_layout(
                    xaxis_title="Categoria",
                    yaxis_title="Total (R$)",
                    bargap=0.2,
                    height=320,
                    margin=dict(l=10, r=10, t=30, b=0),
                    legend=dict(orientation="h", y=1.1, x=0),
                )
                adjust_negative_axis(fig, gcat["Total"])
                with panel("Gastos por Categoria"):
                    st.plotly_chart(fig, use_container_width=True, key="gcat_ano")
            else:
                st.info("Sem dados de categorias para os filtros selecionados.")
        if not gcat.empty:
            meta_col = gcat["Categoria"].map(lambda c: meta_totals.get(c, 0.0)).fillna(0.0)
            gcat_fmt = gcat.assign(
                Meta_fmt=meta_col.map(fmt_brl),
                Total_fmt=gcat["Total"].map(fmt_brl),
            )
            st.dataframe(
                gcat_fmt[["Categoria", "Meta_fmt", "Total_fmt"]],
                use_container_width=True,
                height=300,
                hide_index=True,
            )

        with col_origin:
            gorig = df_gastos_por_origem(year=filtro_ano, origin=origin_filter)
            if not gorig.empty:
                pie_data = gorig.assign(ValorAbs=gorig["Total"].abs())
                fig2 = px.pie(pie_data, names="Origem", values="ValorAbs", hole=0.45)
                fig2.update_layout(showlegend=True, height=320, legend=dict(orientation="h", y=-0.15), margin=dict(l=10, r=10, t=30, b=0))
                with panel("Gastos por Origem"):
                    st.plotly_chart(fig2, use_container_width=True, key="gorig_ano")
            else:
                st.info("Sem dados por origem para os filtros selecionados.")
        if not gorig.empty:
            st.dataframe(
                gorig.assign(Total_fmt=gorig["Total"].map(fmt_brl)),
                use_container_width=True,
                height=300,
                hide_index=True,
            )
            top5 = gcat.nsmallest(5, "Total")
            if not top5.empty:
                with panel("Top gastos do ano"):
                    for idx, row in top5.iterrows():
                        c1, c2 = st.columns([5, 1])
                        with c1:
                            st.markdown(f"**{row['Categoria']}** — {fmt_brl(row['Total'])}")
                        with c2:
                            if st.button("Detalhar", key=f"top-ano-{idx}"):
                                st.session_state["det-cat-ano"] = row["Categoria"]
                                st.rerun()
        st.divider()
        st.subheader("Despesas por Categoria e por Mês")

        tabela = df_categoria_x_mes(year=filtro_ano, origin=origin_filter)
        if tabela.empty:
            st.info("Sem dados para montar a tabela.")
        else:
            tabela_fmt = tabela.copy()
            cols_mes = [c for c in tabela.columns if c != "Categoria"]
            for col in cols_mes:
                tabela_fmt[col] = tabela_fmt[col].map(fmt_brl)
            col_cfg = {"Categoria": st.column_config.TextColumn("Categoria", width="medium")}
            for col in cols_mes:
                col_cfg[col] = st.column_config.TextColumn(col.upper(), width="small")
            st.dataframe(
                tabela_fmt,
                use_container_width=True,
                height=300,
                hide_index=True,
                column_config=col_cfg,
            )

            st.markdown("##### Detalhar Categoria por Mês")
            det_col1, det_col2 = st.columns(2)
            det_cat = det_col1.selectbox("Categoria", tabela["Categoria"].tolist(), key="det-cat-ano")
            det_mes = det_col2.selectbox("Mês", cols_mes, key="det-mes-ano")

            periodo_det = parse_period_label(det_mes, fallback_year=filtro_ano)
            if periodo_det:
                detail_df = transactions_for_category_month(det_cat, periodo_det, origin=origin_filter)
                if detail_df.empty:
                    st.info("Nenhum lançamento encontrado para a combinação selecionada.")
                else:
                    st.markdown(f"**Lançamentos de {det_cat} em {det_mes.upper()}**")
                    detail_show = detail_df.copy()
                    detail_show["Data"] = pd.to_datetime(detail_show["date"]).dt.strftime("%d/%m/%Y")
                    detail_show["Valor"] = detail_show["amount"].map(fmt_brl)
                    st.dataframe(
                        detail_show[["Data", "description", "Valor", "account_id"]],
                        use_container_width=True,
                        height=300,
                        hide_index=True,
                        column_config={
                            "Data": st.column_config.TextColumn("Data", width="small"),
                            "description": st.column_config.TextColumn("Descrição", width="large"),
                            "Valor": st.column_config.TextColumn("Valor", width="small"),
                            "account_id": st.column_config.TextColumn("Conta/Origem", width="small"),
                        },
                    )
            else:
                st.warning("Não foi possível interpretar o mês selecionado.")

        if not tabela.empty:
            st.markdown("#### Evolução por categoria (últimos 6 meses)")
            trend_cat = st.selectbox("Categoria", tabela["Categoria"].tolist(), key="trend-cat-ano")
            cols_mes_trend = [c for c in tabela.columns if c != "Categoria"]
            if cols_mes_trend:
                recent_cols = cols_mes_trend[-6:]
                serie = tabela.loc[tabela["Categoria"] == trend_cat, recent_cols].iloc[0]
                valores = pd.to_numeric(serie, errors="coerce").fillna(0.0)
                fig_trend = go.Figure()
                fig_trend.add_trace(
                    go.Scatter(
                        x=recent_cols,
                        y=valores,
                        mode="lines+markers",
                        line=dict(color="#d95829"),
                    )
                )
                fig_trend.update_layout(xaxis_title="", yaxis_title="Total (R$)", height=320, margin=dict(l=10, r=10, t=20, b=0))
                st.plotly_chart(fig_trend, use_container_width=True, key="trend_ano")

    elif st.session_state.md_view == VIEW_MENSAL:
        st.subheader("Resumo Mensal")
        if not selected_period:
            st.info("Nenhum período disponível para exibir.")
        else:
            st.caption(f"Período selecionado: **{selected_period_label}**")
            tot_desp_m, tot_rec_m = totais_consolidados(
                period_yyyymm=selected_period,
                origin=origin_filter,
            )

            with st.container():
                st.markdown('<div class="monthly-metrics">', unsafe_allow_html=True)
                cA, cB, cC = st.columns(3)
                _, budget_meta_map_all, _, _, categorias_meta, _ = compute_budget_data(origin_filter)
                meta_total_mensal = sum((budget_meta_map_all.get(cat.name, 0.0) or 0.0) for cat in categorias_meta)
                if meta_total_mensal:
                    _no_ritmo_m = abs(tot_desp_m) <= abs(meta_total_mensal)
                    _status_m = "No ritmo" if _no_ritmo_m else "Acima do ritmo"
                    _cor_m = "#1b7f5d" if _no_ritmo_m else "#b3261e"
                    _detalhe_m = (
                        f"<div class='kpi-detail' style='color:{_cor_m};'>"
                        f"Meta mensal: {_fmt_short(meta_total_mensal)} · Ritmo: {_status_m}"
                        "</div>"
                    )
                    card_kpi("Despesas (período)", tot_desp_m, cA, detalhe=_detalhe_m)
                else:
                    card_kpi("Despesas (período)", tot_desp_m, cA)
                card_kpi("Receitas (período)", tot_rec_m, cB)
                card_kpi("Saldo (período)", tot_rec_m + tot_desp_m, cC)
                st.markdown('</div>', unsafe_allow_html=True)
            if meta_total_mensal:
                _diferenca_m = abs(abs(tot_desp_m) - abs(meta_total_mensal))
                if not _no_ritmo_m:
                    st.markdown(
                        f"<div class='alert-badge'>Atenção: despesas {fmt_brl(_diferenca_m)} acima da meta mensal "
                        f"(meta: {fmt_brl(abs(meta_total_mensal))})</div>",
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(
                        f"<div class='alert-badge-green'>No ritmo: despesas {fmt_brl(_diferenca_m)} abaixo da meta mensal</div>",
                        unsafe_allow_html=True,
                    )

            col_cat_m, col_origin_m = st.columns((2, 1), gap="large")
            with col_cat_m:
                gcat_m = df_despesas_por_categoria(
                    period_yyyymm=selected_period,
                    origin=origin_filter,
                    excluir_positivas=True,
                )
                if not gcat_m.empty:
                    categories_order = gcat_m["Categoria"].tolist()
                    meta_line = [
                        -abs(budget_meta_map_all.get(cat, 0.0) or 0.0)
                        for cat in categories_order
                    ]
                    palette = px.colors.qualitative.Pastel + px.colors.qualitative.Set2 + px.colors.qualitative.Safe
                    color_map = {
                        cat: palette[i % len(palette)]
                        for i, cat in enumerate(categories_order)
                    }
                    figm = go.Figure()
                    figm.add_bar(
                        x=categories_order,
                        y=gcat_m["Total"],
                        name="Despesa",
                        marker_color=[color_map[c] for c in categories_order],
                        hovertemplate="%{x}: %{y:.2f}<extra></extra>",
                    )
                    figm.add_trace(
                        go.Scatter(
                            x=categories_order,
                            y=meta_line,
                            mode="lines+markers",
                            name="Meta",
                            line=dict(color="#d95829", dash="dash"),
                            marker=dict(symbol="circle", size=8),
                            hovertemplate="Meta %{x}: %{y:.2f}<extra></extra>",
                        )
                    )
                    figm.update_layout(
                        xaxis_title="Categoria",
                        yaxis_title="Total (R$)",
                        bargap=0.2,
                        height=320,
                        margin=dict(l=10, r=10, t=30, b=0),
                        legend=dict(orientation="h", y=1.1, x=0),
                    )
                    adjust_negative_axis(figm, gcat_m["Total"])
                    with panel("Gastos por Categoria (período)"):
                        st.plotly_chart(figm, use_container_width=True, key="gcat_mes")
                else:
                    st.info("Sem dados de categorias no período selecionado.")
            if not gcat_m.empty:
                meta_series = gcat_m["Categoria"].map(lambda c: budget_meta_map_all.get(c, 0.0)).fillna(0.0)
                st.dataframe(
                    gcat_m.assign(
                        Meta_fmt=meta_series.map(lambda v: fmt_brl(v)),
                        Total_fmt=gcat_m["Total"].map(fmt_brl),
                    )[["Categoria", "Meta_fmt", "Total_fmt"]],
                    use_container_width=True,
                    height=300,
                    hide_index=True,
                )
                top5_m = gcat_m.nsmallest(5, "Total")
                if not top5_m.empty:
                    with panel("Top gastos do período"):
                        for idx, row in top5_m.iterrows():
                            c1, c2 = st.columns([5, 1])
                            with c1:
                                st.markdown(f"**{row['Categoria']}** — {fmt_brl(row['Total'])}")
                            with c2:
                                if st.button("Detalhar período", key=f"top-mes-{idx}"):
                                    st.session_state["det-cat-ano"] = row["Categoria"]
                                    st.rerun()

            with col_origin_m:
                gorig_m = df_gastos_por_origem(period_yyyymm=selected_period, origin=origin_filter)
                if not gorig_m.empty:
                    pie_data_m = gorig_m.assign(ValorAbs=gorig_m["Total"].abs())
                    figm2 = px.pie(pie_data_m, names="Origem", values="ValorAbs", hole=0.45)
                    figm2.update_layout(showlegend=True, height=320, legend=dict(orientation="h", y=-0.15), margin=dict(l=10, r=10, t=30, b=0))
                    with panel("Gastos por Origem (período)"):
                        st.plotly_chart(figm2, use_container_width=True, key="gorig_mes")
                else:
                    st.info("Sem dados por origem no período selecionado.")
            if not gorig_m.empty:
                st.dataframe(
                    gorig_m.assign(Total_fmt=gorig_m["Total"].map(fmt_brl)),
                    use_container_width=True,
                    height=300,
                    hide_index=True,
                )

    elif st.session_state.md_view == VIEW_ORCAMENTO:
        st.subheader("Orçamento por Categoria")
        rows, meta_map, tabela_orc, cols_mes, categorias, _ = compute_budget_data(origin_filter)
        if not categorias:
            st.info("Nenhuma categoria cadastrada.")
        else:
            total_meta = sum((meta_map.get(cat.name, 0.0) or 0.0) for cat in categorias)
            col_table, col_card = st.columns((2, 1))
            with col_table:
                df_orc = pd.DataFrame(rows).sort_values("Categoria").reset_index(drop=True)
                if df_orc.empty:
                    st.info("Sem dados de categorias para exibir.")
                else:
                    st.caption("Use a coluna Meta para definir o alvo mensal de cada categoria. Deixe em branco para usar a média calculada.")
                    editor = st.data_editor(
                        df_orc,
                        key="orcamento_editor",
                        height=300,
                        hide_index=True,
                        column_config={
                            "Categoria": st.column_config.TextColumn("Categoria", disabled=True, width="medium"),
                            "Media": st.column_config.NumberColumn("Média Mensal (R$)", disabled=True, format="R$ %.2f"),
                            "Meta": st.column_config.NumberColumn("Meta (R$)", help="Deixe vazio para usar a média."),
                        },
                    )
                    if st.button("Salvar metas", key="btn_save_budget"):
                        try:
                            name_map = {c.name: c for c in categorias}
                            for _, row in editor.iterrows():
                                cat_obj = name_map.get(row["Categoria"])
                                if not cat_obj:
                                    continue
                                meta_val = row.get("Meta")
                                if pd.isna(meta_val):
                                    cat_obj.budget_meta = None
                                else:
                                    cat_obj.budget_meta = float(meta_val)
                            db.commit()
                            st.success("Metas atualizadas.")
                            st.rerun()
                        except Exception as exc:
                            st.error(f"Erro ao salvar metas: {exc}")
            with col_card:
                st.markdown("<div style='margin-top:8px'></div>", unsafe_allow_html=True)
                card_kpi("Meta Mensal", total_meta, st.container())

# ===== RADAR =====
elif page == "Radar":
    st.header("Radar")
    st.caption("Acompanhamento por keyword e categoria.")
    if origin_filter:
        st.caption(f"Conta filtrada: **{selected_origin}**")

    tabela_radar = df_radar_completo_x_mes(year=filtro_ano, origin=origin_filter)

    if tabela_radar.empty:
        st.info("Sem dados no Radar. Importe transações e configure keywords ou categorias Radar.")
    else:
        cols_mes = [c for c in tabela_radar.columns if c != "Item"]

        tabela_fmt = tabela_radar.copy()
        for col in cols_mes:
            tabela_fmt[col] = tabela_fmt[col].map(fmt_brl)
        col_cfg = {"Item": st.column_config.TextColumn("Item", width="medium")}
        for col in cols_mes:
            col_cfg[col] = st.column_config.TextColumn(col.upper(), width="small")

        with panel("Evolução do Radar"):
            st.dataframe(tabela_fmt, use_container_width=True, height=300, hide_index=True, column_config=col_cfg)

        st.markdown("#### Detalhar item")
        itens = tabela_radar["Item"].tolist()
        item_sel = st.selectbox("Item", itens, key="radar_item_sel")

        if item_sel:
            serie_row = tabela_radar.loc[tabela_radar["Item"] == item_sel]
            if not serie_row.empty:
                valores = pd.to_numeric(serie_row[cols_mes].iloc[0], errors="coerce").fillna(0.0)
                palette = px.colors.qualitative.Pastel + px.colors.qualitative.Set2 + px.colors.qualitative.Safe
                color = palette[itens.index(item_sel) % len(palette)]

                fig_r = go.Figure()
                fig_r.add_bar(
                    x=cols_mes,
                    y=valores.tolist(),
                    name=item_sel,
                    marker_color=color,
                    hovertemplate="%{x}: %{y:.2f}<extra></extra>",
                )

                if item_sel.startswith("Cat: "):
                    cat_name = item_sel[5:]
                    cat_obj = db.session.query(Category).filter(Category.name == cat_name).first()
                    if cat_obj and cat_obj.budget_meta:
                        meta_val = -abs(cat_obj.budget_meta)
                        fig_r.add_trace(go.Scatter(
                            x=cols_mes,
                            y=[meta_val] * len(cols_mes),
                            mode="lines+markers",
                            name="Meta",
                            line=dict(color="#d95829", dash="dash"),
                            marker=dict(symbol="circle", size=6),
                        ))

                fig_r.update_layout(
                    xaxis_title="",
                    yaxis_title="Total (R$)",
                    bargap=0.2,
                    height=320,
                    margin=dict(l=10, r=10, t=30, b=0),
                    legend=dict(orientation="h", y=1.1, x=0),
                )
                adjust_negative_axis(fig_r, valores)

                with panel(f"Evolução: {item_sel}"):
                    st.plotly_chart(fig_r, use_container_width=True, key="radar_detalhe_chart")

# ===== DESPESAS (EDIÇÃO POR ARQUIVO) =====
elif page == "Despesas":
    st.header("Despesas (edição por arquivo)")

    df_lotes = list_statements("", "")
    if df_lotes.empty:
        st.info("Nenhum extrato importado.")
    else:
        df_lotes["_label"] = df_lotes.apply(
            lambda r: f"Lote {r['Lote']} — {r['Origem']} — {r['Período']} — {r['Arquivo']}", axis=1
        )
        escolha = st.selectbox("Escolha o arquivo/lote para editar", df_lotes["_label"].tolist())
        lote_id = int(df_lotes.loc[df_lotes["_label"] == escolha, "Lote"].iloc[0])
        lote_origem = df_lotes.loc[df_lotes["_label"] == escolha, "Origem"].iloc[0]
        is_cc = str(lote_origem).strip().lower().endswith("cc")

        # carrega transações do lote
        q = db.session.query(Transaction).filter(Transaction.statement_id == lote_id).order_by(Transaction.id.desc())
        rows = q.all()
        lote_stmt = db.session.query(Statement).filter(Statement.id == lote_id).first()
        lote_period = lote_stmt.period_yyyymm if lote_stmt else None
        lote_account = lote_stmt.account_id if lote_stmt else None

        def _label_mes(yyyymm: int) -> str:
            M_MAP = {1:"jan",2:"fev",3:"mar",4:"abr",5:"mai",6:"jun",7:"jul",8:"ago",9:"set",10:"out",11:"nov",12:"dez"}
            if yyyymm is None: return "??/??"
            yyyymm = int(yyyymm); y = yyyymm // 100; m = yyyymm % 100
            return f"{M_MAP.get(m, '???')}/{str(y)[-2:]}"

        data = [{
            "id": r.id,
            "mês": _label_mes(r.period_yyyymm),
            "descrição": r.description,
            "valor": r.amount,
            "conta": r.account_id,
            "categoria": r.category,
            "radar": r.radar_label or "",
        } for r in rows]
        df_tx = pd.DataFrame(data)
        total_lote = float(df_tx["valor"].sum()) if not df_tx.empty else 0.0

        # categorias cadastradas
        all_cats = [c.name for c in db.session.query(Category).order_by(Category.name.asc()).all()]
        cats = [c for c in all_cats if c.strip().lower() != "receita"] if is_cc else all_cats

        if is_cc:
            st.info("Lote de **Cartão de Crédito**: valores positivos são **créditos** (nunca Receita). A categoria 'Receita' foi ocultada neste lote.")

        st.caption(f"Total do lote: **{fmt_brl(total_lote, with_symbol=True)}**. Edite a coluna **categoria** (dropdown) e clique em **Aplicar alterações**.")
        df_edit = st.data_editor(
                df_tx,
                num_rows="dynamic",
                use_container_width=True,
                height=300,
                hide_index=True,
                column_config={
                    "id": st.column_config.NumberColumn("id", disabled=True),
                    "mês": st.column_config.TextColumn("mês", disabled=True),
                    "descrição": st.column_config.TextColumn("descrição", disabled=True),
                    "valor": st.column_config.NumberColumn("valor", disabled=True),
                    "conta": st.column_config.TextColumn("conta", disabled=True),
                    "categoria": st.column_config.SelectboxColumn("categoria", options=cats, required=False),
                    "radar": st.column_config.TextColumn("radar"),
                }
            )

        col_e1, col_e2 = st.columns([1,2])
        with col_e1:
            if st.button("Aplicar alterações", type="primary"):
                try:
                    updated = apply_category_bulk(df_edit[["id","categoria"]])
                    from sqlalchemy import update as _sa_update
                    for _, r in df_edit.iterrows():
                        db.session.execute(
                            _sa_update(Transaction)
                            .where(Transaction.id == int(r["id"]))
                            .values(radar_label=(r["radar"].strip() if r["radar"] else None))
                        )
                    db.commit()
                    st.success(f"Categorias e radar atualizados em {updated} linhas.")
                except Exception as e:
                    st.error(f"Erro ao aplicar alterações: {e}")
        with col_e2:
            csv = df_tx.to_csv(index=False).encode("utf-8")
            st.download_button("Baixar CSV (snapshot)", data=csv, file_name=f"lote_{lote_id}.csv", mime="text/csv")

        with st.expander("Adicionar / editar despesa manual"):
            # adicionar
            manual_date = st.date_input("Data da despesa", datetime.today(), key=f"manual_date_{lote_id}")
            manual_desc = st.text_input("Descrição", key=f"manual_desc_{lote_id}")
            manual_val = st.number_input("Valor (use negativo para despesas)", value=0.0, step=0.01, format="%.2f", key=f"manual_val_{lote_id}")
            manual_account = st.text_input("Conta", value=lote_account or lote_origem or "", key=f"manual_account_{lote_id}")
            cat_options = [""] + cats if cats else [""]
            manual_cat = st.selectbox("Categoria (opcional)", options=cat_options, key=f"manual_cat_{lote_id}")
            if st.button("Salvar despesa manual", key=f"manual_btn_{lote_id}"):
                if not manual_desc.strip():
                    st.warning("Informe uma descrição.")
                elif manual_val == 0:
                    st.warning("O valor não pode ser zero.")
                else:
                    new_tx = Transaction(
                        date=manual_date,
                        description=manual_desc.strip(),
                        amount=float(manual_val),
                        account_id=manual_account.strip() or lote_origem,
                        category=manual_cat.strip() or None,
                        statement_id=lote_id,
                        origin_label=lote_origem,
                        period_yyyymm=lote_period
                    )
                    db.session.add(new_tx)
                    db.commit()
                    st.success("Despesa manual adicionada ao lote.")
                    st.rerun()

            st.divider()
            st.subheader("Editar valores existentes")
            num_edits = st.number_input("Número de linhas para ajustar", min_value=1, max_value=len(df_tx), value=1, key=f"manual_edit_count_{lote_id}")
            edit_ids = st.multiselect(
                "Selecione os IDs para alterar o valor (pode escolher múltiplos)",
                options=df_tx["id"].tolist(),
                max_selections=num_edits,
                key=f"manual_edit_ids_{lote_id}"
            )
            novo_valor = st.number_input("Novo valor (negativo = despesa, positivo = crédito)", value=0.0, step=0.01, format="%.2f", key=f"manual_edit_val_{lote_id}")
            if st.button("Aplicar novo valor", key=f"manual_edit_btn_{lote_id}"):
                if not edit_ids:
                    st.warning("Selecione ao menos um ID.")
                elif novo_valor == 0:
                    st.warning("Informe um valor diferente de zero.")
                else:
                    for tx_id in edit_ids:
                        db.session.execute(
                            Transaction.__table__.update()
                            .where(Transaction.id == int(tx_id))
                            .values(amount=float(novo_valor))
                        )
                    db.commit()
                    st.success(f"Atualizado o valor em {len(edit_ids)} lançamento(s).")
                    st.rerun()

# ===== IMPORTAR EXTRATOS =====
elif page == "Importar Extratos":
    st.header("Importar Extratos")

    with st.expander("Importar (Mês/Ano + Conta)", expanded=True):
        c1, c2, c3 = st.columns([1,1,2])
        with c1:
            mes = st.selectbox("Mês", MESES, index=9)
        with c2:
            ano = st.number_input("Ano", min_value=2000, max_value=2100, value=2025, step=1)
        with c3:
            account = st.selectbox("Account/Conta", options=CONTAS_OPTS, index=0)

        period_yyyymm = int(f"{ano}{MESES_MAP[mes]:02d}")
        period_label  = f"{mes[:3]}/{str(ano)[-2:]}"
        files = st.file_uploader(
            "Arquivos (.xlsx/.xls/.csv/.ofx/.pdf)",
            type=["xlsx","xls","csv","ofx","pdf"],
            accept_multiple_files=True
        )

        col_imp1, col_imp2 = st.columns([1,1])
        with col_imp1:
            importar = st.button("Importar", type="primary")
        with col_imp2:
            classif = st.button("Atualizar despesas (classificar)")

        if importar:
            if not files:
                st.error("Selecione ao menos um arquivo.")
            else:
                total_created, total_skipped, lotes = 0, 0, []
                total_rules_updates, total_rules_matches, total_rules = 0, 0, 0
                total_radar_updates, total_radar_matches, total_radar_rules = 0, 0, 0
                total_amount_imported = 0.0

                for f in files:
                    tmp = save_temp(f)
                    backup_arquivo_local(tmp, prefix=account or "upload")
                    df_raw = detect_and_load(tmp, original_name=f.name)
                    df_std = to_standard_df(
                        df_raw, account, f.name.split('.')[-1], path=tmp,
                        origin_hint=None, original_name=f.name, period_yyyymm=period_yyyymm
                    )
                    total_amount_imported += float(df_std["amount"].sum())

                    stmnt_id, created, skipped = import_file_as_statement(
                        tmp, account, df_std,
                        origin_label=account,   # “origem” = a própria conta
                        period_yyyymm=period_yyyymm,
                        period_label=period_label
                    )
                    # aplica regras inteligentes automaticamente
                    res = apply_smart_rules_to_statement(stmnt_id)
                    total_rules_updates += res.get("updated", 0)
                    total_rules_matches += res.get("matched", 0)
                    total_rules = max(total_rules, res.get("rules", 0))
                    # aplica radar inteligente automaticamente
                    res_radar = apply_radar_rules_to_statement(stmnt_id)
                    total_radar_updates += res_radar.get("updated", 0)
                    total_radar_matches += res_radar.get("matched", 0)
                    total_radar_rules = max(total_radar_rules, res_radar.get("rules", 0))

                    total_created += created
                    total_skipped += skipped
                    lotes.append(stmnt_id)

                st.success(
                    f"Importação concluída: {total_created} novas linhas (+{total_skipped} duplicadas). "
                    f"Lotes: {', '.join(map(str, lotes))} — {account} {period_label}. "
                    f"Regras inteligentes: {total_rules_updates} atualizações ({total_rules_matches} matches, {total_rules} regras ativas). "
                    f"Radar inteligente: {total_radar_updates} marcações ({total_radar_matches} matches, {total_radar_rules} keywords ativas). "
                    f"Total importado: {fmt_brl(total_amount_imported)}."
                )

        if classif:
            res = classify_batch()
            st.info(f"Classificação: classificados={res.get('classified',0)}, "
                    f"skipped={res.get('skipped',0)}, regras={res.get('rules_used',0)}.")

    st.markdown("---")
    st.subheader("Extratos importados (lotes)")
    f1, f2 = st.columns([2,2])
    with f1:
        origem_f = st.text_input("Filtrar por conta", value="")
    with f2:
        periodo_f = st.text_input("Filtrar por período (ex.: 202510 ou out/25)", value="")
    df_lotes = list_statements(origem_f, periodo_f)
    st.dataframe(df_lotes, use_container_width=True, height=300, hide_index=True)

    col_del1, col_del2, col_del3 = st.columns([1,1,2])
    with col_del1:
        lote_id = st.number_input("ID do lote", min_value=1, step=1, value=1)
    with col_del2:
        confirma_del = st.checkbox("Confirmo exclusão definitiva do lote")
    with col_del3:
        if st.button("Excluir lote", disabled=not confirma_del):
            n = db.session.query(Transaction).filter(Transaction.statement_id == int(lote_id)).delete()
            db.session.query(Statement).filter(Statement.id == int(lote_id)).delete()
            db.commit()
            st.success(f"Excluídos {n} lançãoamentos e o lote {int(lote_id)}.")
            df_lotes = list_statements(origem_f, periodo_f)
    st.dataframe(df_lotes, use_container_width=True, height=300, hide_index=True)

    format_cards = [
        {
            "title": "Formato AAdvantage CC",
            "bg": "#fff5eb",
            "border": "#ffd8b5",
            "body": (
                "Use CSV/XLS simples com as colunas <code>Data</code>, <code>Descrição</code>, <code>Valor</code>. "
                "Os valores devem estar negativos para despesas. Ex.: <em>15/07/2025; AMAZON PRIME; -37,90</em>."
            ),
        },
        {
            "title": "Formato XP CC",
            "bg": "#e8f6ff",
            "border": "#b6dfff",
            "body": (
                "CSV com colunas <code>Data</code>, <code>Estabelecimento</code>, <code>Valor</code>, <code>Parcela</code>. "
                "Valores já vêm com sinal correto. Ex.: <em>03/07/2025; COLEGIO OBJETIVO; -130,00; -</em>."
            ),
        },
        {
            "title": "Formato Personnalité CC",
            "bg": "#f5f0ff",
            "border": "#d9c7ff",
            "body": (
                "Envie o PDF original da fatura Personnalité. O sistema roda o conversor internamente "
                "e gera o arquivo <code>output.csv</code> com os campos corretos automaticamente."
            ),
        },
        {
            "title": "Formato Personnalité Conta",
            "bg": "#e8fff4",
            "border": "#b4f0d3",
            "body": (
                "Arquivo XLS/CSV do Itaú com <code>Data</code>, <code>Lançamento</code>, <code>Valor</code> "
                "(negativo para débitos). Ex.: <em>12/09/2025; Crédito de dividendos; 27,11</em>."
            ),
        },
        {
            "title": "Formato XP Conta",
            "bg": "#fff0f5",
            "border": "#ffc2d8",
            "body": (
                "CSV com <code>Data</code>, <code>Descrição</code>, <code>Valor</code>. "
                "Ex.: <em>08/09/2025; PIX Claro; -112,89</em>."
            ),
        },
        {
            "title": "Formato Santander Conta",
            "bg": "#fef7e0",
            "border": "#f4df88",
            "body": (
                "CSV (novo formato) com <code>Data</code>, <code>Descrição</code>, <code>Valor</code>. "
                "Ex.: <em>01/09/2025; Pagamento de Boleto; -968,48</em>."
            ),
        },
    ]
    for card in format_cards:
        st.markdown(
            f"""
            <div style="
                background-color:{card['bg']};
                border:1px solid {card['border']};
                border-radius:12px;
                padding:14px 18px;
                margin-top:12px;">
                <strong>{card['title']}</strong><br/>
                {card['body']}
            </div>
            """,
            unsafe_allow_html=True,
        )

# ===== CATEGORIAS =====
elif page == "Categorias":
    st.header("Categorias")

    # Toggle simples (sem cor)
    if "cat_view" not in st.session_state:
        st.session_state.cat_view = "Categorias"

    b1, b2, b3, info = st.columns([1,1,1,2])
    with b1:
        if st.button("Categorias", use_container_width=True):
            st.session_state.cat_view = "Categorias"
    with b2:
        if st.button("Categorias Inteligentes", use_container_width=True):
            st.session_state.cat_view = "Categorias Inteligentes"
    with b3:
        if st.button("Radar Inteligente", use_container_width=True):
            st.session_state.cat_view = "Radar Inteligente"
    with info:
        st.caption(f"Visão atual: **{st.session_state.cat_view}**")

    st.divider()

    # ---------- Visão: Categorias ----------
    if st.session_state.cat_view == "Categorias":
        cats = db.session.query(Category).order_by(Category.name.asc()).all()
        df_c = pd.DataFrame(
            [
                {
                    "id": c.id,
                    "categoria": c.name,
                    "Radar": "Radar" if getattr(c, "radar", 0) else "",
                }
                for c in cats
            ]
        )
        st.subheader("Lista")
        df_edit = st.data_editor(
            df_c,
            use_container_width=True,
            height=300,
            hide_index=True,
            column_config={
                "id": st.column_config.NumberColumn("ID", disabled=True),
                "categoria": st.column_config.TextColumn("Categoria", disabled=True),
                "Radar": st.column_config.SelectboxColumn("Radar", options=["", "Radar"]),
            },
            key="cat_radar_editor",
        )
        if st.button("Salvar Radar", key="save_radar"):
            try:
                for _, row in df_edit.iterrows():
                    cat_obj = db.session.get(Category, int(row["id"]))
                    if not cat_obj:
                        continue
                    cat_obj.radar = 1 if (row.get("Radar") == "Radar") else 0
                db.commit()
                st.success("Configuração de Radar atualizada.")
                st.rerun()
            except Exception as exc:
                st.error(f"Erro ao atualizar Radar: {exc}")

        st.subheader("Adicionar")
        nova = st.text_input("Nova categoria")
        if st.button("Adicionar categoria"):
            if nova and not db.session.query(Category).filter(Category.name.ilike(nova.strip())).first():
                db.session.add(Category(name=nova.strip()))
                db.commit()
                st.success("Categoria adicionada.")
            else:
                st.warning("Categoria vazia ou já existe.")

        st.subheader("Excluir")
        del_id = st.number_input("ID para excluir", min_value=0, step=1, value=0)
        if st.button("Excluir categoria"):
            if del_id > 0:
                db.session.query(Category).filter(Category.id == int(del_id)).delete()
                db.commit()
                st.success(f"Categoria {int(del_id)} excluída.")
            else:
                st.warning("Informe um ID válido.")

    # ---------- Visão: Categorias Inteligentes ----------
    elif st.session_state.cat_view == "Categorias Inteligentes":
        from engine.storage import SmartCategoryRule
        rules = db.session.query(SmartCategoryRule).order_by(SmartCategoryRule.id.desc()).all()
        df_r = pd.DataFrame([{"id": r.id, "keyword": r.keyword, "category": r.category, "active": r.active} for r in rules])

        st.subheader("Regras cadastradas")
        st.dataframe(df_r, use_container_width=True, height=300, hide_index=True)

        st.subheader("Adicionar regra")
        kcol, ccol = st.columns(2)
        with kcol:
            kw = st.text_input("Palavra-chave (ex.: Enel, Luz 2134)")
        with ccol:
            cat_opts = [c.name for c in db.session.query(Category).order_by(Category.name.asc()).all()]
            cat_sel = st.selectbox("Categoria", options=cat_opts) if cat_opts else st.text_input("Categoria (texto)")
        if st.button("Adicionar regra"):
            if kw and (cat_opts and cat_sel or not cat_opts):
                cat_final = cat_sel if cat_opts else cat_sel
                db.session.add(SmartCategoryRule(keyword=kw.strip(), category=cat_final.strip(), active=1))
                db.commit()
                st.success("Regra adicionada.")
            else:
                st.warning("Preencha palavra-chave e categoria.")

        st.subheader("Ativar/Desativar")
        rid = st.number_input("ID da regra", min_value=0, step=1, value=0)
        ativo = st.checkbox("Ativa", value=True)
        if st.button("Atualizar status"):
            if rid > 0:
                from sqlalchemy import update
                db.session.execute(
                    update(SmartCategoryRule)
                    .where(SmartCategoryRule.id == int(rid))
                    .values(active=(1 if ativo else 0))
                )
                db.commit()
                st.success("Status atualizado.")
            else:
                st.warning("Informe um ID válido.")

        st.caption("As regras são aplicadas automaticamente logo após cada importação.")

    # ---------- Visão: Radar Inteligente ----------
    elif st.session_state.cat_view == "Radar Inteligente":
        from engine.storage import RadarKeyword
        from sqlalchemy import update as sa_update

        radar_kws = db.session.query(RadarKeyword).order_by(RadarKeyword.id.desc()).all()
        df_rk = pd.DataFrame([{
            "id": r.id,
            "keyword": r.keyword,
            "label": r.label or "",
            "active": r.active,
        } for r in radar_kws])

        st.subheader("Keywords cadastradas")
        st.dataframe(df_rk, use_container_width=True, height=300, hide_index=True)

        st.subheader("Adicionar keyword")
        rk_col1, rk_col2 = st.columns(2)
        with rk_col1:
            rk_kw = st.text_input("Palavra-chave (ex.: SHELL, POSTO)")
        with rk_col2:
            rk_label = st.text_input("Label amigável (opcional, ex.: Gasolina)")
        if st.button("Adicionar keyword"):
            if rk_kw.strip():
                db.session.add(RadarKeyword(keyword=rk_kw.strip(), label=rk_label.strip() or None, active=1))
                db.commit()
                st.success("Keyword adicionada.")
                st.rerun()
            else:
                st.warning("Informe a palavra-chave.")

        st.subheader("Ativar/Desativar")
        rk_id = st.number_input("ID da keyword", min_value=0, step=1, value=0, key="rk_toggle_id")
        rk_ativo = st.checkbox("Ativa", value=True, key="rk_toggle_active")
        if st.button("Atualizar status", key="rk_toggle_btn"):
            if rk_id > 0:
                db.session.execute(
                    sa_update(RadarKeyword)
                    .where(RadarKeyword.id == int(rk_id))
                    .values(active=(1 if rk_ativo else 0))
                )
                db.commit()
                st.success("Status atualizado.")
                st.rerun()
            else:
                st.warning("Informe um ID válido.")

        st.subheader("Excluir")
        rk_del_id = st.number_input("ID para excluir", min_value=0, step=1, value=0, key="rk_del_id")
        if st.button("Excluir keyword", key="rk_del_btn"):
            if rk_del_id > 0:
                db.session.query(RadarKeyword).filter(RadarKeyword.id == int(rk_del_id)).delete()
                db.commit()
                st.success(f"Keyword {int(rk_del_id)} excluída.")
                st.rerun()
            else:
                st.warning("Informe um ID válido.")

elif page == "Portfólio":
    from modulos.portfolio_page import main as render
    render()


