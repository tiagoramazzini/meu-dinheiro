import json
import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, datetime, timedelta
import calendar
from typing import Optional
import sys
sys.path.insert(0, "engine")
import portfolio_storage as storage
import portfolio_data as data_layer

# st.set_page_config(
#     page_title="Portfólio de Investimentos",
#     page_icon="💰",
#     layout="wide",
# )


ASSET_TYPES = [
    "Ação BR",
    "FII",
    "ETF BR",
    "ETF Internacional",
    "Crypto",
    "Renda Fixa",
]

PALETTE = [
    "#00B4D8", "#0077B6", "#90E0EF", "#48CAE4", "#ADE8F4",
    "#023E8A", "#F77F00", "#FCBF49", "#EAE2B7", "#D62828",
]

storage.init_db()

# ─── Token check ───────────────────────────────────────────────────────────────

if not os.environ.get("BRAPI_TOKEN", ""):
    st.warning(
        "⚠️ **BRAPI_TOKEN não configurado.** "
        "Configure a variável de ambiente `BRAPI_TOKEN` com seu token de "
        "[brapi.dev](https://brapi.dev) para habilitar cotações de ativos BR, "
        "FIIs e ETFs brasileiros."
    )


# ─── Formatação ────────────────────────────────────────────────────────────────

def fmt_brl(value) -> str:
    if value is None:
        return "N/D"
    try:
        return f"R$ {float(value):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "N/D"


def fmt_pct(value) -> str:
    if value is None:
        return "N/D"
    try:
        return f"{float(value):+.2f}%"
    except Exception:
        return "N/D"


def fmt_date(d) -> str:
    if d is None:
        return "-"
    if isinstance(d, str):
        return d
    return d.strftime("%d/%m/%Y")


def fmt_qtd(v) -> str:
    if v is None:
        return "-"
    try:
        f = float(v)
        if f == int(f):
            return f"{int(f):,}"
        return f"{f:,.4f}"
    except Exception:
        return str(v)


def style_lp(val):
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return "color: gray"
        return "color: #2ecc71" if float(val) >= 0 else "color: #e74c3c"
    except Exception:
        return "color: gray"


def _kpi_card(label: str, value: str, color: str = "#ffffff") -> str:
    return (
        f'<div style="background:#1e1e2e;border-radius:10px;padding:16px;text-align:center;'
        f'border:1px solid #333;margin-bottom:12px">'
        f'<p style="color:#aaa;font-size:12px;margin:0">{label}</p>'
        f'<p style="color:{color};font-size:18px;font-weight:bold;margin:6px 0 0">{value}</p>'
        f"</div>"
    )


# ─── Auto Snapshot ─────────────────────────────────────────────────────────────

def maybe_save_snapshot(portfolio_rows: list[dict]):
    today = date.today()
    snapshots = storage.get_all_snapshots()
    if snapshots and snapshots[-1].snapshot_date == today:
        return
    total_value = sum(r["valor_atual"] for r in portfolio_rows if r["valor_atual"] is not None)
    total_invested = sum(r["valor_investido"] for r in portfolio_rows)
    if total_value > 0 or total_invested > 0:
        storage.save_snapshot(today, total_value, total_invested)


# ─── Dashboard ─────────────────────────────────────────────────────────────────

def render_dashboard(portfolio_rows: list[dict]):
    if not portfolio_rows:
        st.info("Adicione ativos na aba 'Gerenciar Ativos' para ver o dashboard.")
        return

    total_invested = sum(r["valor_investido"] for r in portfolio_rows)
    total_value = sum(r["valor_atual"] for r in portfolio_rows if r["valor_atual"] is not None)
    total_pl = total_value - total_invested
    total_pct = (total_value / total_invested - 1) * 100 if total_invested else 0.0
    month_divs = data_layer.get_current_month_dividends()

    # Próximos 30 dias de dividendos
    positions_json = json.dumps([
        {"ticker": r["ticker"], "name": r["name"], "asset_type": r["asset_type"], "quantity": r["quantity"]}
        for r in portfolio_rows
    ])
    upcoming_divs = data_layer.get_upcoming_dividends(positions_json)
    today = date.today()
    next30_total = sum(
        d["total_estimated"] for d in upcoming_divs
        if d["payment_date"] >= today and d["payment_date"] <= today + timedelta(days=30)
    )

    benchmark = data_layer.get_cdi_benchmark()
    cdi_total_pct = benchmark.get("cdi_accumulated_pct", 0.0)

    # ── Concentration alerts ──
    for r in portfolio_rows:
        if r["valor_atual"] is not None and total_value > 0:
            pct = r["valor_atual"] / total_value * 100
            if pct > 30:
                st.warning(
                    f"⚠️ **{r['ticker']}** representa **{pct:.1f}%** da carteira — considere diversificar."
                )

    type_totals: dict[str, float] = {}
    for r in portfolio_rows:
        v = r["valor_atual"] or r["valor_investido"]
        type_totals[r["asset_type"]] = type_totals.get(r["asset_type"], 0) + v

    for tipo, v in type_totals.items():
        if total_value > 0 and v / total_value * 100 > 60:
            st.warning(
                f"⚠️ **{tipo}** representa **{v / total_value * 100:.1f}%** da carteira — considere diversificar entre classes."
            )

    # ── Period selector ──
    snapshots = storage.get_all_snapshots()
    period_options = {
        "Este mês": 30,
        "3 meses": 90,
        "6 meses": 180,
        "12 meses": 365,
        "Desde o início": None,
    }
    selected_period = st.selectbox(
        "Período de análise",
        list(period_options.keys()),
        index=4,
        key="period_sel",
    )
    days_back = period_options[selected_period]

    if days_back is not None:
        cutoff = date.today() - timedelta(days=days_back)
        period_snaps = sorted(
            [s for s in snapshots if s.snapshot_date >= cutoff],
            key=lambda s: s.snapshot_date,
        )
    else:
        period_snaps = sorted(snapshots, key=lambda s: s.snapshot_date)

    if period_snaps:
        start_snap = period_snaps[0]
        period_return_pct = (
            (total_value - start_snap.total_value) / start_snap.total_value * 100
            if start_snap.total_value else 0.0
        )
        cdi_period_pct = data_layer.get_cdi_accumulated(
            start_snap.snapshot_date, date.today()
        ) * 100
    else:
        period_return_pct = total_pct
        cdi_period_pct = cdi_total_pct

    # ── KPI cards ──
    pl_color = "#2ecc71" if total_pl >= 0 else "#e74c3c"
    pct_color = "#2ecc71" if total_pct >= 0 else "#e74c3c"
    period_color = "#2ecc71" if period_return_pct >= 0 else "#e74c3c"
    vs_cdi = period_return_pct - cdi_period_pct
    vs_cdi_color = "#2ecc71" if vs_cdi >= 0 else "#e74c3c"

    realized_ytd = data_layer.get_realized_profit_ytd()
    ir_month = data_layer.get_ir_due_current_month()
    realized_color = "#2ecc71" if realized_ytd >= 0 else "#e74c3c"
    ir_color = "#D62828" if ir_month > 0 else "#aaa"

    kpis = [
        ("💼 Patrimônio Atual", fmt_brl(total_value), "#ffffff"),
        ("💵 Total Investido", fmt_brl(total_invested), "#ffffff"),
        ("📈 Lucro / Prejuízo", fmt_brl(total_pl), pl_color),
        ("📊 Rent. Total", fmt_pct(total_pct), pct_color),
        ("💸 Dividendos (mês)", fmt_brl(month_divs), "#FCBF49"),
        ("📅 Dividendos (30 dias)", fmt_brl(next30_total), "#FCBF49"),
        (f"📅 Retorno ({selected_period})", fmt_pct(period_return_pct), period_color),
        (f"🏦 CDI ({selected_period})", fmt_pct(cdi_period_pct), "#00B4D8"),
        ("⚖️ Carteira vs CDI", fmt_pct(vs_cdi), vs_cdi_color),
        (f"💰 Lucro Realizado ({date.today().year})", fmt_brl(realized_ytd), realized_color),
        ("🧾 IR Devido (mês atual)", fmt_brl(ir_month), ir_color),
    ]

    cols = st.columns(3)
    for i, (label, value, color) in enumerate(kpis):
        cols[i % 3].markdown(_kpi_card(label, value, color), unsafe_allow_html=True)


# ─── Tabela de Posições ────────────────────────────────────────────────────────

def render_positions_table(portfolio_rows: list[dict]):
    if not portfolio_rows:
        st.info("Nenhum ativo cadastrado ainda.")
        return

    rows = []
    for r in portfolio_rows:
        rows.append({
            "Ticker": r["ticker"],
            "Nome": r["name"],
            "Tipo": r["asset_type"],
            "Qtd": r["quantity"],
            "Preço Médio": r["avg_price"],
            "Cotação Atual": r["current_price"],
            "Valor Investido": r["valor_investido"],
            "Valor Atual": r["valor_atual"],
            "L/P R$": r["lucro_reais"],
            "L/P %": r["rentabilidade_pct"],
        })

    df = pd.DataFrame(rows)

    # Export CSV
    export_rows = []
    for r in portfolio_rows:
        export_rows.append({
            "Ticker": r["ticker"],
            "Nome": r["name"],
            "Tipo": r["asset_type"],
            "Quantidade": r["quantity"],
            "Preco_Medio": round(r["avg_price"], 4),
            "Cotacao_Atual": round(r["current_price"], 4) if r["current_price"] else "",
            "Valor_Investido": round(r["valor_investido"], 2),
            "Valor_Atual": round(r["valor_atual"], 2) if r["valor_atual"] else "",
            "LP_Reais": round(r["lucro_reais"], 2) if r["lucro_reais"] else "",
            "LP_Pct": round(r["rentabilidade_pct"], 4) if r["rentabilidade_pct"] else "",
        })
    total_inv = sum(r["valor_investido"] for r in portfolio_rows)
    total_atu = sum(r["valor_atual"] for r in portfolio_rows if r["valor_atual"])
    export_rows.append({
        "Ticker": "TOTAL", "Nome": "", "Tipo": "", "Quantidade": "",
        "Preco_Medio": "", "Cotacao_Atual": "",
        "Valor_Investido": round(total_inv, 2),
        "Valor_Atual": round(total_atu, 2),
        "LP_Reais": round(total_atu - total_inv, 2),
        "LP_Pct": round((total_atu / total_inv - 1) * 100, 4) if total_inv else "",
    })
    csv_bytes = pd.DataFrame(export_rows).to_csv(index=False, sep=",").encode("utf-8-sig")
    st.download_button(
        "⬇️ Exportar CSV",
        data=csv_bytes,
        file_name=f"posicoes_{date.today().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )

    format_map = {
        "Qtd": fmt_qtd,
        "Preço Médio": lambda v: fmt_brl(v) if v is not None else "N/D",
        "Cotação Atual": lambda v: fmt_brl(v) if v is not None else "Indisponível",
        "Valor Investido": fmt_brl,
        "Valor Atual": lambda v: fmt_brl(v) if v is not None else "N/D",
        "L/P R$": lambda v: fmt_brl(v) if v is not None else "N/D",
        "L/P %": lambda v: fmt_pct(v) if v is not None else "N/D",
    }

    styled = df.style.map(style_lp, subset=["L/P %", "L/P R$"])
    for col, fn in format_map.items():
        styled = styled.format(fn, subset=[col])

    st.dataframe(styled, width="stretch")

    # Totals row
    totals = {
        "Ticker": "TOTAL", "Nome": "", "Tipo": "",
        "Qtd": "-", "Preço Médio": "-", "Cotação Atual": "-",
        "Valor Investido": df["Valor Investido"].sum(),
        "Valor Atual": df["Valor Atual"].sum(),
        "L/P R$": df["L/P R$"].sum(),
        "L/P %": "-",
    }
    st.dataframe(
        pd.DataFrame([totals]).style.format({
            "Valor Investido": fmt_brl,
            "Valor Atual": fmt_brl,
            "L/P R$": fmt_brl,
        }),
        width="stretch",
        hide_index=True,
    )

    # Fundamentals expander
    st.divider()
    st.subheader("Indicadores Fundamentalistas")
    fund_tickers = [r["ticker"] for r in portfolio_rows if r["asset_type"] in {"Ação BR", "FII"}]
    if not fund_tickers:
        st.caption("Indicadores disponíveis apenas para Ações BR e FIIs.")
        return

    selected_fund = st.selectbox("Selecione o ativo", fund_tickers, key="fund_sel")
    fund_row = next((r for r in portfolio_rows if r["ticker"] == selected_fund), None)
    if not fund_row:
        return

    with st.expander(f"📊 Indicadores — {selected_fund}", expanded=True):
        with st.spinner("Buscando indicadores..."):
            fdata = data_layer.get_fundamentals(selected_fund, fund_row["asset_type"])

        if not fdata:
            st.warning(f"Indicadores não disponíveis para {selected_fund} no momento.")
            return

        stats = fdata.get("defaultKeyStatistics") or {}
        summary = fdata.get("summaryProfile") or {}

        def safe(d, key, pct=False, mult=1):
            v = d.get(key)
            if v is None:
                return "N/D"
            try:
                val = float(v) * mult
                return f"{val:.2f}%" if pct else f"{val:.2f}"
            except Exception:
                return "N/D"

        if fund_row["asset_type"] == "Ação BR":
            indicators = {
                "P/L": safe(stats, "trailingPE"),
                "P/VP": safe(stats, "priceToBook"),
                "Dividend Yield": safe(stats, "dividendYield", pct=True, mult=100),
                "ROE": safe(stats, "returnOnEquity", pct=True, mult=100),
                "Margem Líquida": safe(stats, "profitMargins", pct=True, mult=100),
                "Dívida/PL": safe(stats, "debtToEquity"),
                "EPS": safe(stats, "trailingEps"),
                "Setor": summary.get("sector", "N/D"),
            }
        else:
            indicators = {
                "P/VP": safe(stats, "priceToBook"),
                "Dividend Yield": safe(stats, "dividendYield", pct=True, mult=100),
                "EPS (Último rend.)": safe(stats, "trailingEps"),
                "Setor": summary.get("industry", "N/D"),
            }

        icols = st.columns(4)
        for i, (label, val) in enumerate(indicators.items()):
            icols[i % 4].metric(label, val)


# ─── Gráficos ──────────────────────────────────────────────────────────────────

def render_charts(portfolio_rows: list[dict]):
    if not portfolio_rows:
        st.info("Adicione ativos para ver os gráficos.")
        return

    dark = "plotly_dark"

    # 1. Donuts alocação
    type_totals: dict[str, float] = {}
    for r in portfolio_rows:
        v = r["valor_atual"] or r["valor_investido"]
        type_totals[r["asset_type"]] = type_totals.get(r["asset_type"], 0) + v

    fig_type = px.pie(
        values=list(type_totals.values()),
        names=list(type_totals.keys()),
        hole=0.45,
        title="Alocação por Tipo de Ativo",
        template=dark,
        color_discrete_sequence=PALETTE,
    )
    fig_type.update_traces(textposition="inside", textinfo="percent+label")

    asset_vals = [(r["ticker"], r["valor_atual"] or r["valor_investido"]) for r in portfolio_rows]
    asset_vals.sort(key=lambda x: x[1], reverse=True)
    top10 = asset_vals[:10]
    outros = sum(v for _, v in asset_vals[10:])
    if outros > 0:
        top10.append(("Outros", outros))
    labels_a, values_a = zip(*top10) if top10 else ([], [])

    fig_asset = px.pie(
        values=list(values_a),
        names=list(labels_a),
        hole=0.45,
        title="Alocação por Ativo (Top 10)",
        template=dark,
        color_discrete_sequence=PALETTE,
    )
    fig_asset.update_traces(textposition="inside", textinfo="percent+label")

    col1, col2 = st.columns(2)
    col1.plotly_chart(fig_type, width="stretch")
    col2.plotly_chart(fig_asset, width="stretch")

    # 2. Rentabilidade por ativo
    perf_rows = [
        (r["ticker"], r["rentabilidade_pct"])
        for r in portfolio_rows
        if r["rentabilidade_pct"] is not None
    ]
    perf_rows.sort(key=lambda x: x[1])
    if perf_rows:
        tickers_p, pcts_p = zip(*perf_rows)
        fig_perf = go.Figure(go.Bar(
            x=list(pcts_p),
            y=list(tickers_p),
            orientation="h",
            marker_color=["#2ecc71" if p >= 0 else "#D62828" for p in pcts_p],
            text=[fmt_pct(p) for p in pcts_p],
            textposition="outside",
        ))
        fig_perf.update_layout(
            title="Rentabilidade por Ativo (%)",
            template=dark,
            xaxis_title="Rentabilidade (%)",
            height=max(300, len(perf_rows) * 45),
        )
        st.plotly_chart(fig_perf, width="stretch")

    # 3. Evolução patrimonial vs CDI
    snapshots = storage.get_all_snapshots()
    if snapshots:
        snap_dates = [s.snapshot_date for s in snapshots]
        snap_values = [s.total_value for s in snapshots]
        snap_invested = [s.total_invested for s in snapshots]

        fig_evo = go.Figure()
        fig_evo.add_trace(go.Scatter(
            x=snap_dates, y=snap_values,
            mode="lines+markers", name="Patrimônio Atual",
            line=dict(color="#00B4D8", width=2),
        ))
        fig_evo.add_trace(go.Scatter(
            x=snap_dates, y=snap_invested,
            mode="lines", name="Total Investido",
            line=dict(color="#F77F00", width=2, dash="dash"),
        ))

        benchmark = data_layer.get_cdi_benchmark()
        if benchmark:
            oldest = benchmark["oldest_date"]
            base_inv = benchmark["total_invested"]
            cdi_bench = []
            for sd in snap_dates:
                if sd < oldest:
                    cdi_bench.append(base_inv)
                else:
                    acc = data_layer.get_cdi_accumulated(oldest, sd)
                    cdi_bench.append(base_inv * (1 + acc))
            fig_evo.add_trace(go.Scatter(
                x=snap_dates, y=cdi_bench,
                mode="lines", name="CDI 100%",
                line=dict(color="#90E0EF", width=2, dash="dot"),
            ))

        fig_evo.update_layout(
            title="Evolução Patrimonial vs Investido vs CDI",
            template=dark,
            xaxis_title="Data",
            yaxis_title="Valor (R$)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig_evo, width="stretch")

    # 4. Dividendos por mês
    monthly_divs = data_layer.get_monthly_dividends_last12()
    if monthly_divs:
        div_df = pd.DataFrame(monthly_divs)
        div_df["mes_ano"] = div_df.apply(lambda r: f"{r['month']:02d}/{r['year']}", axis=1)
        fig_div = px.bar(
            div_df, x="mes_ano", y="total", color="ticker",
            title="Dividendos por Mês (últimos 12 meses)",
            labels={"mes_ano": "Mês", "total": "Total (R$)", "ticker": "Ticker"},
            template=dark,
            color_discrete_sequence=PALETTE,
        )
        st.plotly_chart(fig_div, width="stretch")

    # 5. Histórico de preço por ativo
    st.divider()
    st.subheader("Histórico de Preço por Ativo")
    hist_tickers = [r["ticker"] for r in portfolio_rows if r["asset_type"] != "Renda Fixa"]
    if hist_tickers:
        selected_hist = st.selectbox("Ativo", hist_tickers, key="hist_sel")
        hist_row = next((r for r in portfolio_rows if r["ticker"] == selected_hist), None)
        if hist_row:
            with st.spinner(f"Buscando histórico de {selected_hist}..."):
                hist_df = data_layer.get_price_history(selected_hist, hist_row["asset_type"])

            if hist_df is not None and not hist_df.empty:
                fig_hist = go.Figure()
                fig_hist.add_trace(go.Scatter(
                    x=hist_df["date"],
                    y=hist_df["close"],
                    mode="lines+markers",
                    name="Fechamento",
                    line=dict(color=PALETTE[0], width=2),
                    fill="tozeroy",
                    fillcolor="rgba(0,180,216,0.1)",
                ))

                all_lots = storage.get_all_lots()
                ticker_lots = [l for l in all_lots if l.ticker == selected_hist]
                min_hist_date = hist_df["date"].min()

                for lot in ticker_lots:
                    if lot.purchase_date >= min_hist_date:
                        x_str = str(lot.purchase_date)
                        fig_hist.add_shape(
                            type="line",
                            x0=x_str, x1=x_str,
                            y0=0, y1=1,
                            yref="paper",
                            line=dict(color="#FCBF49", width=1, dash="dash"),
                        )
                        fig_hist.add_annotation(
                            x=x_str,
                            y=0.98,
                            yref="paper",
                            text=f"Compra {fmt_brl(lot.avg_price)} × {fmt_qtd(lot.quantity)}",
                            showarrow=False,
                            font=dict(color="#FCBF49", size=9),
                            textangle=-90,
                            xanchor="left",
                            bgcolor="rgba(0,0,0,0.4)",
                        )

                fig_hist.update_layout(
                    title=f"Histórico de Preço — {selected_hist} (12 meses)",
                    template=dark,
                    xaxis_title="Data",
                    yaxis_title="Preço",
                    hovermode="x unified",
                )
                st.plotly_chart(fig_hist, width="stretch")
            else:
                st.warning(f"Histórico não disponível para {selected_hist}.")

    # 6. Comparativo entre ativos
    st.divider()
    st.subheader("Comparar Ativos")
    comp_tickers = [r["ticker"] for r in portfolio_rows if r["asset_type"] != "Renda Fixa"]
    if len(comp_tickers) >= 2:
        selected_comp = st.multiselect(
            "Selecione os ativos para comparar",
            comp_tickers,
            default=comp_tickers[:2],
            key="comp_sel",
        )
        if len(selected_comp) >= 2:
            comp_rows = [r for r in portfolio_rows if r["ticker"] in selected_comp]
            tickers_json = json.dumps([
                {"ticker": r["ticker"], "asset_type": r["asset_type"]}
                for r in comp_rows
            ])
            with st.spinner("Carregando histórico para comparação..."):
                comp_hist = data_layer.get_comparison_history(tickers_json)

            if comp_hist:
                fig_comp = go.Figure()
                for i, ticker in enumerate(selected_comp):
                    if ticker not in comp_hist:
                        continue
                    tdata = comp_hist[ticker]
                    fig_comp.add_trace(go.Scatter(
                        x=[d["date"] for d in tdata],
                        y=[d["normalized"] for d in tdata],
                        mode="lines+markers",
                        name=ticker,
                        line=dict(color=PALETTE[i % len(PALETTE)], width=2),
                    ))
                fig_comp.add_hline(
                    y=100, line_dash="dot", line_color="#555",
                    annotation_text="Base 100",
                )
                fig_comp.update_layout(
                    title="Evolução Relativa — Base 100 (12 meses)",
                    template=dark,
                    xaxis_title="Data",
                    yaxis_title="Valor Normalizado",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                    hovermode="x unified",
                )
                st.plotly_chart(fig_comp, width="stretch")

            # Comparison table
            total_port_value = sum(
                r["valor_atual"] or r["valor_investido"] for r in portfolio_rows
            )
            comp_table = []
            for r in comp_rows:
                port_pct = (
                    (r["valor_atual"] or r["valor_investido"]) / total_port_value * 100
                    if total_port_value else 0.0
                )
                # Volatility from history
                vol = None
                if r["ticker"] in comp_hist:
                    closes = [d["close"] for d in comp_hist[r["ticker"]]]
                    if len(closes) >= 2:
                        series = pd.Series(closes)
                        pct_changes = series.pct_change().dropna()
                        vol = pct_changes.std() * 100

                # DY from fundamentals (Ação BR / FII only)
                dy = None
                if r["asset_type"] in {"Ação BR", "FII"}:
                    fdata = data_layer.get_fundamentals(r["ticker"], r["asset_type"])
                    if fdata:
                        raw = (fdata.get("defaultKeyStatistics") or {}).get("dividendYield")
                        if raw is not None:
                            try:
                                dy = float(raw) * 100
                            except Exception:
                                pass

                comp_table.append({
                    "Ticker": r["ticker"],
                    "Rentabilidade %": fmt_pct(r["rentabilidade_pct"]),
                    "Volatilidade (mensal)": f"{vol:.2f}%" if vol is not None else "N/D",
                    "Dividend Yield": f"{dy:.2f}%" if dy is not None else "N/D",
                    "Peso na Carteira %": f"{port_pct:.1f}%",
                })

            st.dataframe(
                pd.DataFrame(comp_table),
                width="stretch",
                hide_index=True,
            )
        else:
            st.info("Selecione pelo menos 2 ativos para comparar.")
    else:
        st.caption("Adicione ao menos 2 ativos para usar o comparativo.")

    # 7. Simulador de aportes
    st.divider()
    st.subheader("Simulador de Aportes")
    sim_col1, sim_col2 = st.columns([1, 2])

    with sim_col1:
        sim_aporte = st.number_input(
            "Aporte mensal (R$)",
            min_value=0.0,
            value=500.0,
            step=100.0,
            format="%.2f",
            key="sim_aporte",
        )
        sim_anos = st.slider(
            "Prazo (anos)",
            min_value=1,
            max_value=30,
            value=10,
            key="sim_anos",
        )
        sim_taxa_aa = st.number_input(
            "Rentabilidade esperada (% ao ano)",
            min_value=0.0,
            max_value=100.0,
            value=12.0,
            step=0.5,
            format="%.2f",
            key="sim_taxa",
        )
        sim_cdi = st.checkbox("Comparar com CDI", value=True, key="sim_cdi")

    with sim_col2:
        if sim_aporte > 0 and sim_anos > 0:
            n = sim_anos * 12
            r_month = (1 + sim_taxa_aa / 100) ** (1 / 12) - 1

            patrimonio = []
            aportado = []
            cdi_series = []

            cdi_monthly = data_layer.get_cdi_monthly_rate()

            pat = 0.0
            pat_cdi = 0.0
            for mes in range(1, n + 1):
                pat = pat * (1 + r_month) + sim_aporte
                pat_cdi = pat_cdi * (1 + cdi_monthly) + sim_aporte
                patrimonio.append(pat)
                aportado.append(sim_aporte * mes)
                cdi_series.append(pat_cdi)

            valor_final = patrimonio[-1]
            total_aportado = aportado[-1]
            juros = valor_final - total_aportado

            kpi_cols = st.columns(3)
            kpi_cols[0].metric("Valor Final", fmt_brl(valor_final))
            kpi_cols[1].metric("Total Aportado", fmt_brl(total_aportado))
            kpi_cols[2].metric("Juros Gerados", fmt_brl(juros))

            meses = list(range(1, n + 1))
            fig_sim = go.Figure()
            fig_sim.add_trace(go.Scatter(
                x=meses, y=patrimonio,
                name=f"Carteira ({sim_taxa_aa:.1f}% a.a.)",
                mode="lines",
                line=dict(color="#00B4D8", width=2),
                fill="tozeroy",
                fillcolor="rgba(0,180,216,0.07)",
            ))
            fig_sim.add_trace(go.Scatter(
                x=meses, y=aportado,
                name="Total Aportado",
                mode="lines",
                line=dict(color="#F77F00", width=2, dash="dash"),
            ))
            if sim_cdi:
                fig_sim.add_trace(go.Scatter(
                    x=meses, y=cdi_series,
                    name="CDI",
                    mode="lines",
                    line=dict(color="#90E0EF", width=2, dash="dot"),
                ))

            fig_sim.update_layout(
                title=f"Simulação — R$ {sim_aporte:,.0f}/mês por {sim_anos} anos",
                template=dark,
                xaxis_title="Mês",
                yaxis_title="Valor (R$)",
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                hovermode="x unified",
            )
            st.plotly_chart(fig_sim, width="stretch")


# ─── Agenda de Dividendos Futuros ──────────────────────────────────────────────

def render_agenda_tab(portfolio_rows: list[dict]):
    st.subheader("Agenda de Dividendos")
    st.caption("Dividendos esperados (últimos 30 dias e próximos 180 dias) para os ativos BR da carteira.")

    br_rows = [r for r in portfolio_rows if r["asset_type"] in {"Ação BR", "FII", "ETF BR"}]
    if not br_rows:
        st.info("A agenda exibe dividendos de Ações BR, FIIs e ETFs BR. Adicione esses ativos à carteira.")
        return

    positions_json = json.dumps([
        {"ticker": r["ticker"], "name": r["name"], "asset_type": r["asset_type"], "quantity": r["quantity"]}
        for r in br_rows
    ])

    with st.spinner("Buscando agenda de dividendos..."):
        upcoming = data_layer.get_upcoming_dividends(positions_json)

    if not upcoming:
        st.info("Nenhum dividendo encontrado para os próximos períodos. Verifique se os tickers estão corretos e se o BRAPI_TOKEN está configurado.")
        return

    today = date.today()
    next30 = sum(d["total_estimated"] for d in upcoming if d["payment_date"] >= today and d["payment_date"] <= today + timedelta(days=30))
    next_total = sum(d["total_estimated"] for d in upcoming if d["payment_date"] >= today)

    kpi_cols = st.columns(3)
    kpi_cols[0].markdown(_kpi_card("📅 Próximos 30 dias", fmt_brl(next30), "#FCBF49"), unsafe_allow_html=True)
    kpi_cols[1].markdown(_kpi_card("📆 Total esperado", fmt_brl(next_total), "#00B4D8"), unsafe_allow_html=True)
    kpi_cols[2].markdown(_kpi_card("📋 Eventos encontrados", str(len(upcoming)), "#ffffff"), unsafe_allow_html=True)

    st.divider()

    rows = []
    for d in upcoming:
        is_past = d["payment_date"] < today
        is_near = not is_past and d["payment_date"] <= today + timedelta(days=30)
        status = "✅ Pago" if is_past else ("🔜 Em breve" if is_near else "📅 Previsto")
        rows.append({
            "Ticker": d["ticker"],
            "Nome": d["name"],
            "Data Ex": fmt_date(d["ex_date"]) if d["ex_date"] else "-",
            "Data Pagamento": fmt_date(d["payment_date"]),
            "Valor/Cota": fmt_brl(d["amount_per_unit"]),
            "Total Estimado": fmt_brl(d["total_estimated"]),
            "Status": status,
        })

    df_agenda = pd.DataFrame(rows)
    st.dataframe(df_agenda, width="stretch", hide_index=True)


# ─── Resumo Mensal ─────────────────────────────────────────────────────────────

def render_resumo_tab():
    st.subheader("Resumo Mensal")

    today = date.today()
    # Build list of available months (from oldest snapshot or lot to today)
    all_snapshots = storage.get_all_snapshots()
    all_lots = storage.get_all_lots()

    if not all_snapshots and not all_lots:
        st.info("Adicione ativos e aguarde snapshots diários para ver o resumo mensal.")
        return

    earliest = today
    if all_snapshots:
        earliest = min(earliest, all_snapshots[0].snapshot_date)
    if all_lots:
        earliest = min(earliest, min(l.purchase_date for l in all_lots))

    # Generate months from earliest to today
    months = []
    cur = date(earliest.year, earliest.month, 1)
    while cur <= date(today.year, today.month, 1):
        months.append(cur)
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    months.reverse()

    month_labels = {m.strftime("%B/%Y").capitalize(): m for m in months}
    selected_label = st.selectbox("Mês de referência", list(month_labels.keys()), key="resumo_mes")
    sel_month = month_labels[selected_label]
    year, month = sel_month.year, sel_month.month

    start_snap, end_snap = data_layer.get_snapshots_for_month(year, month)
    month_divs = data_layer.get_dividends_for_month(year, month)
    month_lots = data_layer.get_lots_added_in_month(year, month)

    total_divs = sum(d.total_amount for d in month_divs)
    total_aportes = sum(l.quantity * l.avg_price for l in month_lots)

    # KPIs
    pat_inicio = start_snap.total_value if start_snap else None
    pat_fim = end_snap.total_value if end_snap else None
    variacao_pct = None
    if pat_inicio and pat_fim and pat_inicio > 0:
        variacao_pct = (pat_fim / pat_inicio - 1) * 100

    var_color = "#2ecc71" if (variacao_pct or 0) >= 0 else "#e74c3c"
    kpi_cols = st.columns(4)
    kpi_cols[0].markdown(_kpi_card("📌 Patrimônio Início", fmt_brl(pat_inicio)), unsafe_allow_html=True)
    kpi_cols[1].markdown(_kpi_card("📌 Patrimônio Fim", fmt_brl(pat_fim)), unsafe_allow_html=True)
    kpi_cols[2].markdown(_kpi_card("📈 Variação %", fmt_pct(variacao_pct), var_color), unsafe_allow_html=True)
    kpi_cols[3].markdown(_kpi_card("💸 Dividendos Recebidos", fmt_brl(total_divs), "#FCBF49"), unsafe_allow_html=True)

    kpi_cols2 = st.columns(4)
    kpi_cols2[0].markdown(_kpi_card("➕ Aportes Realizados", fmt_brl(total_aportes), "#00B4D8"), unsafe_allow_html=True)
    kpi_cols2[1].markdown(_kpi_card("🗂️ Lotes Adquiridos", str(len(month_lots)), "#ffffff"), unsafe_allow_html=True)

    st.divider()

    # Bar chart: last 12 months patrimony
    st.subheader("Patrimônio — Últimos 12 meses")
    monthly_end_values = []
    for m in months[:12]:
        _, es = data_layer.get_snapshots_for_month(m.year, m.month)
        monthly_end_values.append({
            "mes": m.strftime("%m/%Y"),
            "patrimonio": es.total_value if es else None,
            "investido": es.total_invested if es else None,
        })
    monthly_end_values.reverse()

    valid_months = [v for v in monthly_end_values if v["patrimonio"] is not None]
    if valid_months:
        fig_resumo = go.Figure()
        fig_resumo.add_trace(go.Bar(
            x=[v["mes"] for v in valid_months],
            y=[v["patrimonio"] for v in valid_months],
            name="Patrimônio",
            marker_color="#00B4D8",
        ))
        fig_resumo.add_trace(go.Bar(
            x=[v["mes"] for v in valid_months],
            y=[v["investido"] for v in valid_months],
            name="Total Investido",
            marker_color="#F77F00",
        ))
        fig_resumo.update_layout(
            template="plotly_dark",
            barmode="group",
            xaxis_title="Mês",
            yaxis_title="Valor (R$)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig_resumo, width="stretch")
    else:
        st.info("Sem dados de patrimônio para os últimos 12 meses ainda.")

    # Dividends table for selected month
    if month_divs:
        st.divider()
        st.subheader(f"Dividendos Recebidos — {selected_label}")
        div_rows = [{
            "Ticker": d.ticker,
            "Data Pagamento": fmt_date(d.payment_date),
            "Valor/Cota": fmt_brl(d.amount_per_unit),
            "Total": fmt_brl(d.total_amount),
        } for d in month_divs]
        st.dataframe(pd.DataFrame(div_rows), width="stretch", hide_index=True)

    # Lots table for selected month
    if month_lots:
        st.divider()
        st.subheader(f"Aportes Realizados — {selected_label}")
        lot_rows = [{
            "Ticker": l.ticker,
            "Nome": l.name,
            "Tipo": l.asset_type,
            "Qtd": fmt_qtd(l.quantity),
            "Preço": fmt_brl(l.avg_price),
            "Total": fmt_brl(l.quantity * l.avg_price),
            "Corretora": l.broker or "-",
        } for l in month_lots]
        st.dataframe(pd.DataFrame(lot_rows), width="stretch", hide_index=True)


# ─── Gerenciar Ativos ──────────────────────────────────────────────────────────

def render_manage_tab():
    lots = storage.get_all_lots()

    mode = st.radio(
        "Operação",
        ["➕ Adicionar Lote", "✏️ Editar Lote", "🗑️ Remover Lote", "📥 Importar CSV"],
        horizontal=True,
    )

    if mode == "➕ Adicionar Lote":
        _render_lot_form(lot=None)
    elif mode == "✏️ Editar Lote":
        if not lots:
            st.info("Nenhum lote cadastrado.")
            return
        lot_labels = {
            f"#{l.id} — {l.ticker} | {fmt_qtd(l.quantity)} × {fmt_brl(l.avg_price)} | {fmt_date(l.purchase_date)}": l
            for l in lots
        }
        selected_label = st.selectbox("Selecione o lote", list(lot_labels.keys()))
        _render_lot_form(lot=lot_labels[selected_label])
    elif mode == "🗑️ Remover Lote":
        if not lots:
            st.info("Nenhum lote cadastrado.")
            return
        lot_labels = {
            f"#{l.id} — {l.ticker} | {fmt_qtd(l.quantity)} × {fmt_brl(l.avg_price)} | {fmt_date(l.purchase_date)}": l
            for l in lots
        }
        selected_label = st.selectbox("Selecione o lote para remover", list(lot_labels.keys()))
        lot_rm = lot_labels[selected_label]
        st.warning(f"Confirma a remoção do lote #{lot_rm.id} — {lot_rm.ticker}?")
        if st.button("🗑️ Confirmar Remoção", type="primary"):
            storage.delete_lot(lot_rm.id)
            st.cache_data.clear()
            st.success(f"Lote #{lot_rm.id} removido!")
            st.rerun()
    elif mode == "📥 Importar CSV":
        _render_csv_import()

    if lots:
        st.divider()
        st.subheader(f"Todos os Lotes ({len(lots)})")
        lot_rows = [{
            "ID": l.id,
            "Ticker": l.ticker,
            "Nome": l.name,
            "Tipo": l.asset_type,
            "Qtd": fmt_qtd(l.quantity),
            "Preço Médio": fmt_brl(l.avg_price),
            "Total Investido": fmt_brl(l.quantity * l.avg_price),
            "Data Compra": fmt_date(l.purchase_date),
            "Corretora": l.broker or "-",
            "Notas": l.notes or "-",
        } for l in lots]
        st.dataframe(pd.DataFrame(lot_rows), width="stretch", hide_index=True)

    if lots:
        st.divider()
        st.subheader("Sincronizar Dividendos")
        unique_tickers = list(dict.fromkeys(l.ticker for l in lots))
        sync_ticker = st.selectbox("Ativo", unique_tickers, key="sync_sel")
        if st.button("🔄 Sincronizar Dividendos"):
            ticker_lots = [l for l in lots if l.ticker == sync_ticker]
            total_qty = sum(l.quantity for l in ticker_lots)
            asset_type = ticker_lots[0].asset_type if ticker_lots else "Ação BR"
            data_layer.sync_dividends(sync_ticker, asset_type, total_qty)
            st.cache_data.clear()
            st.success(f"Dividendos de {sync_ticker} sincronizados!")
            st.rerun()


def _render_lot_form(lot=None):
    editing = lot is not None
    with st.form("lot_form"):
        col1, col2 = st.columns(2)
        with col1:
            ticker_val = st.text_input("Ticker *", value=lot.ticker if editing else "")
            name_val = st.text_input("Nome *", value=lot.name if editing else "")
            asset_type_val = st.selectbox(
                "Tipo de Ativo *",
                ASSET_TYPES,
                index=ASSET_TYPES.index(lot.asset_type) if editing and lot.asset_type in ASSET_TYPES else 0,
            )
            broker_val = st.text_input("Corretora", value=lot.broker or "" if editing else "")
        with col2:
            quantity_val = st.number_input(
                "Quantidade *",
                min_value=0.0,
                value=float(lot.quantity) if editing else 0.0,
                format="%.6f",
            )
            avg_price_val = st.number_input(
                "Preço Médio de Compra (R$) *",
                min_value=0.0,
                value=float(lot.avg_price) if editing else 0.0,
                format="%.4f",
            )
            purchase_date_val = st.date_input(
                "Data de Compra *",
                value=lot.purchase_date if editing else date.today(),
            )
            notes_val = st.text_area("Notas", value=lot.notes or "" if editing else "")

        submitted = st.form_submit_button(
            "💾 Atualizar Lote" if editing else "💾 Adicionar Lote",
            type="primary",
        )
        if submitted:
            ticker_clean = ticker_val.strip().upper()
            if not ticker_clean or not name_val.strip():
                st.error("Ticker e Nome são obrigatórios.")
            elif quantity_val <= 0:
                st.error("Quantidade deve ser maior que zero.")
            elif avg_price_val <= 0:
                st.error("Preço médio deve ser maior que zero.")
            else:
                if editing:
                    storage.update_lot(
                        lot_id=lot.id,
                        ticker=ticker_clean,
                        name=name_val.strip(),
                        asset_type=asset_type_val,
                        quantity=quantity_val,
                        avg_price=avg_price_val,
                        purchase_date=purchase_date_val,
                        broker=broker_val.strip() or None,
                        notes=notes_val.strip() or None,
                    )
                    st.success(f"Lote #{lot.id} — {ticker_clean} atualizado!")
                else:
                    storage.add_lot(
                        ticker=ticker_clean,
                        name=name_val.strip(),
                        asset_type=asset_type_val,
                        quantity=quantity_val,
                        avg_price=avg_price_val,
                        purchase_date=purchase_date_val,
                        broker=broker_val.strip() or None,
                        notes=notes_val.strip() or None,
                    )
                    st.success(f"Lote {ticker_clean} adicionado!")
                st.cache_data.clear()
                st.rerun()


def _render_csv_import():
    st.subheader("Importar Lotes via CSV")
    st.caption(
        "Formato: `ticker,name,asset_type,quantity,avg_price,purchase_date,broker,notes`  |  "
        f"Tipos válidos: {', '.join(ASSET_TYPES)}  |  Data: DD/MM/YYYY ou YYYY-MM-DD"
    )

    template = "ticker,name,asset_type,quantity,avg_price,purchase_date,broker,notes\n"
    template += "PETR4,Petrobras PN,Ação BR,100,38.50,01/01/2024,Clear,\n"
    template += "HGLG11,CSHG Logística,FII,10,165.00,15/03/2024,XP,\n"
    st.download_button(
        "⬇️ Baixar template CSV",
        data=template.encode("utf-8-sig"),
        file_name="template_lotes.csv",
        mime="text/csv",
    )

    uploaded = st.file_uploader("Selecione o arquivo CSV", type="csv")
    if not uploaded:
        return

    try:
        df_csv = pd.read_csv(uploaded)
    except Exception as e:
        st.error(f"Erro ao ler o arquivo: {e}")
        return

    required_cols = {"ticker", "name", "asset_type", "quantity", "avg_price", "purchase_date"}
    missing = required_cols - set(df_csv.columns.str.lower())
    if missing:
        st.error(f"Colunas obrigatórias ausentes: {', '.join(missing)}")
        return

    df_csv.columns = [c.lower() for c in df_csv.columns]
    valid_rows, error_rows = [], []

    for idx, row in df_csv.iterrows():
        errors = []
        ticker = str(row.get("ticker", "")).strip().upper()
        name = str(row.get("name", "")).strip()
        asset_type = str(row.get("asset_type", "")).strip()

        if not ticker:
            errors.append("ticker vazio")
        if not name:
            errors.append("nome vazio")
        if asset_type not in ASSET_TYPES:
            errors.append(f"tipo inválido '{asset_type}'")

        try:
            quantity = float(row["quantity"])
            if quantity <= 0:
                errors.append("quantidade ≤ 0")
        except Exception:
            errors.append("quantidade inválida")
            quantity = 0

        try:
            avg_price = float(row["avg_price"])
            if avg_price <= 0:
                errors.append("preço ≤ 0")
        except Exception:
            errors.append("preço inválido")
            avg_price = 0

        date_str = str(row.get("purchase_date", "")).strip()
        purchase_date = None
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
            try:
                purchase_date = datetime.strptime(date_str, fmt).date()
                break
            except Exception:
                pass
        if purchase_date is None:
            errors.append(f"data inválida '{date_str}'")

        broker = str(row.get("broker", "")).strip() or None
        notes = str(row.get("notes", "")).strip() or None

        if errors:
            error_rows.append({"Linha": idx + 2, "Erros": "; ".join(errors)})
        else:
            valid_rows.append({
                "ticker": ticker, "name": name, "asset_type": asset_type,
                "quantity": quantity, "avg_price": avg_price,
                "purchase_date": purchase_date, "broker": broker, "notes": notes,
            })

    st.subheader(f"Preview — {len(valid_rows)} válidos, {len(error_rows)} com erro")

    if valid_rows:
        st.dataframe(pd.DataFrame([{
            "Ticker": r["ticker"], "Nome": r["name"], "Tipo": r["asset_type"],
            "Qtd": fmt_qtd(r["quantity"]), "Preço Médio": fmt_brl(r["avg_price"]),
            "Data Compra": fmt_date(r["purchase_date"]), "Corretora": r["broker"] or "-",
        } for r in valid_rows]), width="stretch", hide_index=True)

    if error_rows:
        st.error("Linhas com erro:")
        st.dataframe(pd.DataFrame(error_rows), width="stretch", hide_index=True)

    if valid_rows and st.button("✅ Confirmar Importação", type="primary"):
        ok, err = storage.bulk_add_lots(valid_rows)
        st.cache_data.clear()
        st.success(f"✅ {ok} lotes importados!" + (f" ⚠️ {err} erros." if err else ""))
        st.rerun()


# ─── Vendas & IR ───────────────────────────────────────────────────────────────

def render_vendas_tab(portfolio_rows: list[dict]):
    st.subheader("Vendas de Investimentos")

    sub_tabs = st.tabs(["📝 Registrar Venda", "📋 Histórico", "🧾 Apuração Mensal"])

    # ── 1. Registrar Venda ─────────────────────────────────────────────────────
    with sub_tabs[0]:
        lots = storage.get_all_lots()
        if not lots:
            st.info("Adicione lotes à carteira antes de registrar vendas.")
        else:
            unique_tickers = list(dict.fromkeys(l.ticker for l in lots))
            ticker_info = {}
            for l in lots:
                if l.ticker not in ticker_info:
                    ticker_info[l.ticker] = {"name": l.name, "asset_type": l.asset_type}

            col1, col2 = st.columns(2)
            with col1:
                sel_ticker = st.selectbox("Ticker *", unique_tickers, key="venda_ticker")
                info = ticker_info.get(sel_ticker, {})
                asset_type_venda = info.get("asset_type", "Ação BR")
                st.caption(f"Nome: {info.get('name', '-')}  ·  Tipo: {asset_type_venda}")

                avg_cost_preview, qty_available = storage.get_weighted_avg_cost(sel_ticker)
                st.caption(
                    f"Disponível: {fmt_qtd(qty_available)} cotas  ·  "
                    f"PM atual: {fmt_brl(avg_cost_preview)}"
                )

                qty_sell = st.number_input(
                    "Quantidade vendida *",
                    min_value=0.000001,
                    max_value=float(qty_available) if qty_available > 0 else 1.0,
                    value=min(1.0, float(qty_available)),
                    format="%.6f",
                    key="venda_qty",
                )
                sale_price_input = st.number_input(
                    "Preço de venda (R$) *",
                    min_value=0.0001,
                    value=float(avg_cost_preview) if avg_cost_preview > 0 else 1.0,
                    format="%.4f",
                    key="venda_preco",
                )

            with col2:
                sale_date_input = st.date_input(
                    "Data da venda *",
                    value=date.today(),
                    key="venda_data",
                )
                broker_venda = st.text_input("Corretora", key="venda_corretora")

                trade_type_venda = "swing"
                if asset_type_venda == "Ação BR":
                    trade_type_venda = st.radio(
                        "Tipo de operação",
                        ["swing", "day"],
                        horizontal=True,
                        key="venda_trade_type",
                    )

                notes_venda = st.text_area("Observações", key="venda_notes")

            st.divider()

            # Live preview
            if qty_sell > 0 and sale_price_input > 0:
                preview = data_layer.compute_sale_preview(
                    sel_ticker, qty_sell, sale_price_input,
                    sale_date_input, asset_type_venda, trade_type_venda,
                )

                if "error" in preview:
                    st.error(f"Erro: {preview['error']}")
                else:
                    gp = preview["gross_profit"]
                    gp_color = "#2ecc71" if gp >= 0 else "#e74c3c"
                    ir_color = "#D62828" if preview["ir_due"] > 0 else "#aaa"

                    st.subheader("Prévia da Operação")
                    p_cols = st.columns(5)
                    p_cols[0].markdown(
                        _kpi_card("📦 Preço Médio", fmt_brl(preview["avg_cost"]), "#aaa"),
                        unsafe_allow_html=True,
                    )
                    p_cols[1].markdown(
                        _kpi_card("💵 Receita Bruta", fmt_brl(preview["total_revenue"]), "#ffffff"),
                        unsafe_allow_html=True,
                    )
                    p_cols[2].markdown(
                        _kpi_card("📊 Custo Total", fmt_brl(preview["total_cost"]), "#aaa"),
                        unsafe_allow_html=True,
                    )
                    p_cols[3].markdown(
                        _kpi_card("💰 Lucro/Prejuízo", fmt_brl(gp), gp_color),
                        unsafe_allow_html=True,
                    )
                    p_cols[4].markdown(
                        _kpi_card(
                            f"🧾 IR ({preview['ir_rate']*100:.0f}%)",
                            fmt_brl(preview["ir_due"]),
                            ir_color,
                        ),
                        unsafe_allow_html=True,
                    )

                    if asset_type_venda == "Ação BR" and trade_type_venda == "swing":
                        yr, mo = sale_date_input.year, sale_date_input.month
                        existing_rev = storage.get_monthly_acao_br_revenue(yr, mo)
                        total_mo_rev = existing_rev + preview["total_revenue"]
                        if total_mo_rev <= 20_000.0:
                            st.info(
                                f"✅ Isento de IR: total de vendas de Ação BR no mês = "
                                f"{fmt_brl(total_mo_rev)} ≤ R$ 20.000,00"
                            )
                        else:
                            st.warning(
                                f"⚠️ Acima da isenção: total de vendas de Ação BR no mês = "
                                f"{fmt_brl(total_mo_rev)} > R$ 20.000,00 — alíquota 15%"
                            )
                    elif asset_type_venda in {"Renda Fixa", "ETF Internacional"}:
                        st.info("ℹ️ IR retido na fonte pela corretora / banco — não há apuração DARF.")

                    st.divider()
                    if st.button("✅ Confirmar Venda", type="primary", key="venda_confirm"):
                        try:
                            avg_cost = data_layer.compute_sale_preview(
                                sel_ticker, qty_sell, sale_price_input,
                                sale_date_input, asset_type_venda, trade_type_venda,
                            )
                            # Deduct lots FIFO and get actual avg cost
                            actual_avg = storage.deduct_lots_fifo(sel_ticker, qty_sell)
                            yr2, mo2 = sale_date_input.year, sale_date_input.month
                            exist_acao = storage.get_monthly_acao_br_revenue(yr2, mo2)
                            exist_crypto = storage.get_monthly_crypto_profit(yr2, mo2)
                            total_cost_s = qty_sell * actual_avg
                            total_rev_s = qty_sell * sale_price_input
                            gp_s = total_rev_s - total_cost_s
                            ir_rate_s, ir_due_s = data_layer.calculate_ir_for_sale(
                                asset_type_venda, trade_type_venda, sale_date_input,
                                sale_price_input, qty_sell, gp_s,
                                exist_acao, exist_crypto,
                            )
                            storage.create_sale(
                                ticker=sel_ticker,
                                name=info.get("name", sel_ticker),
                                asset_type=asset_type_venda,
                                quantity_sold=qty_sell,
                                sale_price=sale_price_input,
                                sale_date=sale_date_input,
                                avg_cost_at_sale=actual_avg,
                                ir_rate=ir_rate_s,
                                ir_due=ir_due_s,
                                broker=broker_venda.strip() or None,
                                notes=notes_venda.strip() or None,
                                trade_type=trade_type_venda if asset_type_venda == "Ação BR" else None,
                            )
                            st.cache_data.clear()
                            st.success(
                                f"✅ Venda de {fmt_qtd(qty_sell)} {sel_ticker} registrada! "
                                f"Lucro: {fmt_brl(gp_s)} · IR: {fmt_brl(ir_due_s)}"
                            )
                            st.rerun()
                        except ValueError as ve:
                            st.error(str(ve))
                        except Exception as ex:
                            st.error(f"Erro ao registrar venda: {ex}")

    # ── 2. Histórico de Vendas ─────────────────────────────────────────────────
    with sub_tabs[1]:
        all_sales = storage.get_all_sales()
        if not all_sales:
            st.info("Nenhuma venda registrada ainda.")
        else:
            total_revenue_all = sum(s.total_revenue for s in all_sales)
            total_profit_all = sum(s.gross_profit for s in all_sales)
            total_ir_all = sum(s.ir_due for s in all_sales)
            profit_color = "#2ecc71" if total_profit_all >= 0 else "#e74c3c"
            ir_hist_color = "#D62828" if total_ir_all > 0 else "#aaa"

            h_cols = st.columns(4)
            h_cols[0].markdown(
                _kpi_card("💵 Receita Total", fmt_brl(total_revenue_all), "#ffffff"),
                unsafe_allow_html=True,
            )
            h_cols[1].markdown(
                _kpi_card("💰 Lucro Bruto Total", fmt_brl(total_profit_all), profit_color),
                unsafe_allow_html=True,
            )
            h_cols[2].markdown(
                _kpi_card("🧾 IR Total Pago/Devido", fmt_brl(total_ir_all), ir_hist_color),
                unsafe_allow_html=True,
            )
            h_cols[3].markdown(
                _kpi_card("📋 Total de Vendas", str(len(all_sales)), "#ffffff"),
                unsafe_allow_html=True,
            )

            st.divider()

            rows_hist = []
            for s in all_sales:
                gp_sign = "+" if s.gross_profit >= 0 else ""
                rows_hist.append({
                    "Data": fmt_date(s.sale_date),
                    "Ticker": s.ticker,
                    "Nome": s.name,
                    "Tipo": s.asset_type,
                    "Qtd": fmt_qtd(s.quantity_sold),
                    "Preço Venda": fmt_brl(s.sale_price),
                    "PM Compra": fmt_brl(s.avg_cost_at_sale),
                    "Receita": fmt_brl(s.total_revenue),
                    "Custo": fmt_brl(s.total_cost),
                    "Lucro/Prejuízo": f"{gp_sign}{fmt_brl(s.gross_profit)}",
                    "Alíquota IR": f"{s.ir_rate*100:.0f}%",
                    "IR Devido": fmt_brl(s.ir_due),
                    "Operação": s.trade_type or "-",
                    "Corretora": s.broker or "-",
                })
            st.dataframe(pd.DataFrame(rows_hist), width="stretch", hide_index=True)

            # Profit by ticker bar chart
            by_ticker: dict[str, float] = {}
            for s in all_sales:
                by_ticker[s.ticker] = by_ticker.get(s.ticker, 0.0) + s.gross_profit
            if by_ticker:
                sorted_tickers = sorted(by_ticker.items(), key=lambda x: x[1], reverse=True)
                fig_hist = go.Figure(go.Bar(
                    x=[t for t, _ in sorted_tickers],
                    y=[p for _, p in sorted_tickers],
                    marker_color=[
                        "#2ecc71" if p >= 0 else "#e74c3c" for _, p in sorted_tickers
                    ],
                ))
                fig_hist.update_layout(
                    title="Lucro/Prejuízo por Ativo (Histórico)",
                    template="plotly_dark",
                    xaxis_title="Ticker",
                    yaxis_title="Lucro (R$)",
                )
                st.plotly_chart(fig_hist, width="stretch")

    # ── 3. Apuração Mensal ─────────────────────────────────────────────────────
    with sub_tabs[2]:
        all_sales_ap = storage.get_all_sales()
        if not all_sales_ap:
            st.info("Nenhuma venda registrada.")
        else:
            available_months_set = sorted(
                set((s.sale_date.year, s.sale_date.month) for s in all_sales_ap),
                reverse=True,
            )
            month_options = {
                f"{m:02d}/{y}": (y, m) for y, m in available_months_set
            }
            sel_ap_label = st.selectbox("Mês de apuração", list(month_options.keys()), key="ap_mes")
            sel_y, sel_m = month_options[sel_ap_label]

            month_sales = storage.get_sales_by_month(sel_y, sel_m)
            if not month_sales:
                st.info(f"Sem vendas em {sel_ap_label}.")
            else:
                total_ir_mo = sum(s.ir_due for s in month_sales)
                total_profit_mo = sum(s.gross_profit for s in month_sales)
                profit_mo_color = "#2ecc71" if total_profit_mo >= 0 else "#e74c3c"
                ir_mo_color = "#D62828" if total_ir_mo > 0 else "#aaa"

                ap_cols = st.columns(3)
                ap_cols[0].markdown(
                    _kpi_card("💰 Lucro Bruto", fmt_brl(total_profit_mo), profit_mo_color),
                    unsafe_allow_html=True,
                )
                ap_cols[1].markdown(
                    _kpi_card("🧾 IR a Pagar (DARF)", fmt_brl(total_ir_mo), ir_mo_color),
                    unsafe_allow_html=True,
                )
                ap_cols[2].markdown(
                    _kpi_card("📋 Vendas no mês", str(len(month_sales)), "#ffffff"),
                    unsafe_allow_html=True,
                )

                if total_ir_mo > 0:
                    darf_deadline = f"Último dia útil de {date(sel_y, sel_m % 12 + 1, 1).strftime('%B/%Y') if sel_m < 12 else f'janeiro/{sel_y+1}'}"
                    st.error(
                        f"🔴 DARF a recolher: **{fmt_brl(total_ir_mo)}** — vencimento até o "
                        f"{darf_deadline}."
                    )
                else:
                    st.success("✅ Nenhum IR a recolher neste mês.")

                st.divider()
                st.subheader(f"Detalhamento — {sel_ap_label}")

                ap_rows = []
                for s in month_sales:
                    gp_sign = "+" if s.gross_profit >= 0 else ""
                    ap_rows.append({
                        "Data": fmt_date(s.sale_date),
                        "Ticker": s.ticker,
                        "Tipo": s.asset_type,
                        "Qtd": fmt_qtd(s.quantity_sold),
                        "Receita": fmt_brl(s.total_revenue),
                        "Custo": fmt_brl(s.total_cost),
                        "Lucro/Prejuízo": f"{gp_sign}{fmt_brl(s.gross_profit)}",
                        "Alíquota": f"{s.ir_rate*100:.0f}%",
                        "IR Devido": fmt_brl(s.ir_due),
                        "Operação": s.trade_type or "-",
                    })

                df_ap = pd.DataFrame(ap_rows)
                st.dataframe(df_ap, width="stretch", hide_index=True)

                # Download for DARF reference
                csv_ap = df_ap.to_csv(index=False, sep=";", decimal=",").encode("utf-8-sig")
                st.download_button(
                    f"⬇️ Exportar apuração {sel_ap_label} (CSV)",
                    data=csv_ap,
                    file_name=f"apuracao_{sel_y}_{sel_m:02d}.csv",
                    mime="text/csv",
                    key="dl_ap",
                )


# ─── Dividendos ────────────────────────────────────────────────────────────────

def render_dividends_tab():
    dividends = storage.get_all_dividends()
    if not dividends:
        st.info("Nenhum dividendo registrado. Use 'Sincronizar Dividendos' na aba Gerenciar Ativos.")
        return

    rows = [{
        "Ticker": d.ticker,
        "Data Pagamento": fmt_date(d.payment_date),
        "Por Unidade": fmt_brl(d.amount_per_unit),
        "Total": fmt_brl(d.total_amount),
    } for d in dividends]

    st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
    st.metric("Total recebido (todos os períodos)", fmt_brl(sum(d.total_amount for d in dividends)))


# ─── Main ──────────────────────────────────────────────────────────────────────

# ─── Aba Rebalancear ───────────────────────────────────────────────────────────

_ASSET_TYPES_ORDER = [
    "Ação BR", "FII", "ETF BR", "ETF Internacional", "Crypto", "Renda Fixa"
]

PLOTLY_DARK = dict(
    plot_bgcolor="#0E1117",
    paper_bgcolor="#0E1117",
    font_color="#FAFAFA",
)


def render_rebalancear(portfolio_rows: list[dict]):
    sub1, sub2, sub3 = st.tabs([
        "🎯 Definir Alvos",
        "⚖️ Rebalancear",
        "🔍 Por Ativo",
    ])

    # ── Sub-aba 1: Definir Alvos ─────────────────────────────────────────────
    with sub1:
        st.subheader("🎯 Definir Alocação Alvo por Classe")
        st.caption(
            "Defina quanto (%) do portfólio você quer em cada classe. "
            "A soma deve ser exatamente 100%."
        )

        saved_targets = data_layer.get_targets()
        target_map = {t["asset_type"]: t["target_pct"] for t in saved_targets}

        # Compute current allocation
        total_val = sum(
            (r["valor_atual"] if r["valor_atual"] is not None else r["valor_investido"])
            for r in portfolio_rows
        )
        current_map: dict[str, float] = {}
        for r in portfolio_rows:
            val = r["valor_atual"] if r["valor_atual"] is not None else r["valor_investido"]
            current_map[r["asset_type"]] = current_map.get(r["asset_type"], 0.0) + (val or 0.0)

        inputs: dict[str, float] = {}
        col_left, col_right = st.columns([1, 1])

        with col_left:
            st.markdown("**Defina os alvos (%):**")
            for at in _ASSET_TYPES_ORDER:
                default_val = target_map.get(at, 0.0)
                inputs[at] = st.number_input(
                    at,
                    min_value=0.0,
                    max_value=100.0,
                    value=float(default_val),
                    step=1.0,
                    format="%.1f",
                    key=f"target_{at}",
                )

            total_pct = sum(inputs.values())
            diff = abs(total_pct - 100.0)

            if diff < 0.01:
                st.success(f"✅ Soma: {total_pct:.1f}% — Pronto para salvar!")
            else:
                st.error(f"❌ Soma atual: {total_pct:.1f}% — Falta ajustar {100 - total_pct:+.1f}%")
            st.progress(min(total_pct / 100.0, 1.0))

            if st.button("💾 Salvar Alvos", type="primary", disabled=(diff >= 0.1)):
                data_layer.save_targets_and_clear([
                    {"asset_type": at, "target_pct": pct}
                    for at, pct in inputs.items()
                ])
                st.success("Alvos salvos com sucesso!")
                st.rerun()

        with col_right:
            st.markdown("**Alvo definido vs Alocação atual:**")
            labels = _ASSET_TYPES_ORDER
            alvo_vals = [inputs.get(at, 0.0) for at in labels]
            atual_vals = [
                (current_map.get(at, 0.0) / total_val * 100) if total_val > 0 else 0.0
                for at in labels
            ]
            colors = PALETTE[:len(labels)]

            fig = go.Figure()
            fig.add_trace(go.Pie(
                labels=labels,
                values=alvo_vals,
                name="Alvo",
                hole=0.45,
                domain={"x": [0, 0.48]},
                marker_colors=colors,
                textinfo="label+percent",
                showlegend=False,
            ))
            fig.add_trace(go.Pie(
                labels=labels,
                values=atual_vals,
                name="Atual",
                hole=0.45,
                domain={"x": [0.52, 1.0]},
                marker_colors=colors,
                textinfo="label+percent",
                showlegend=False,
            ))
            fig.add_annotation(
                text="Alvo", x=0.21, y=0.5, showarrow=False,
                font=dict(size=13, color="#FAFAFA")
            )
            fig.add_annotation(
                text="Atual", x=0.79, y=0.5, showarrow=False,
                font=dict(size=13, color="#FAFAFA")
            )
            fig.update_layout(
                **PLOTLY_DARK,
                height=360,
                margin=dict(t=20, b=20, l=10, r=10),
            )
            st.plotly_chart(fig, use_container_width=True)

    # ── Sub-aba 2: Rebalancear ───────────────────────────────────────────────
    with sub2:
        targets = data_layer.get_targets()
        if not targets:
            st.info("⚠️ Defina seus alvos na aba **Definir Alvos** primeiro.")
            return

        st.subheader("⚖️ Plano de Rebalanceamento")

        col_ap, col_tol = st.columns([2, 1])
        with col_ap:
            aporte = st.number_input(
                "Tenho um aporte de R$ ___ para investir este mês (opcional)",
                min_value=0.0,
                value=0.0,
                step=100.0,
                format="%.2f",
                key="aporte_rebal",
            )
        with col_tol:
            tolerance = st.number_input(
                "Tolerância de desvio (%)",
                min_value=0.0,
                max_value=20.0,
                value=2.0,
                step=0.5,
                format="%.1f",
                key="tolerance_rebal",
            )

        plan = data_layer.compute_rebalancing(portfolio_rows, targets, aporte, tolerance)
        if not plan:
            st.warning("Portfólio vazio ou sem alvos configurados.")
            return

        # KPIs
        k1, k2, k3 = st.columns(3)
        k1.metric("Desvio máximo", f"{plan['desvio_max']:.1f}%")
        k2.metric("Classes fora do alvo", f"{plan['classes_fora_alvo']}")
        k3.metric("Valor a rebalancear", fmt_brl(plan["valor_total_rebalancear"]))

        st.markdown("---")
        st.markdown("**Tabela de Rebalanceamento**")

        has_sell = any(c["acao"] == "🔴 Vender" for c in plan["by_class"])
        if has_sell:
            st.warning(
                "⚠️ Rebalanceamento sugere venda em algumas classes. "
                "Venda pode gerar **evento tributável** — considere rebalancear via aporte."
            )

        table_rows = []
        for c in plan["by_class"]:
            desvio_str = f"{c['desvio']:+.1f}%"
            valor_acao = abs(c["diferenca"]) if c["acao"] != "✅ OK" else 0.0
            table_rows.append({
                "Classe": c["asset_type"],
                "% Atual": c["pct_atual"],
                "% Alvo": c["pct_alvo"],
                "Desvio": desvio_str,
                "Valor Atual": c["valor_atual"],
                "Valor Alvo": c["valor_alvo"],
                "Ação": c["acao"],
                "Valor (R$)": valor_acao,
            })
        df_rebal = pd.DataFrame(table_rows)

        def _color_acao(col):
            colors = []
            for v in col:
                if "Comprar" in v:
                    colors.append("color: #2ecc71")
                elif "Vender" in v:
                    colors.append("color: #e74c3c")
                else:
                    colors.append("color: #95a5a6")
            return colors

        def _color_desvio(col):
            out = []
            for v in col:
                try:
                    num = float(str(v).replace("%", "").replace("+", ""))
                    if abs(num) < tolerance:
                        out.append("color: #95a5a6")
                    elif num > 0:
                        out.append("color: #e74c3c")
                    else:
                        out.append("color: #2ecc71")
                except Exception:
                    out.append("")
            return out

        st.dataframe(
            df_rebal.style
            .format({
                "% Atual": "{:.1f}%",
                "% Alvo": "{:.1f}%",
                "Valor Atual": "R$ {:,.2f}",
                "Valor Alvo": "R$ {:,.2f}",
                "Valor (R$)": "R$ {:,.2f}",
            })
            .apply(_color_acao, subset=["Ação"])
            .apply(_color_desvio, subset=["Desvio"]),
            use_container_width=True,
            hide_index=True,
        )

        # Aporte distribution
        if aporte > 0 and plan.get("aporte_distribution"):
            st.markdown("---")
            st.markdown(f"**Distribuição do Aporte — {fmt_brl(aporte)}**")
            ap_rows = []
            for ap in plan["aporte_distribution"]:
                if ap["valor_aporte"] > 0:
                    ap_rows.append({
                        "Classe": ap["asset_type"],
                        "Alocar (R$)": ap["valor_aporte"],
                        "% do Aporte": ap["pct_do_aporte"],
                    })
            if ap_rows:
                df_ap = pd.DataFrame(ap_rows)
                st.dataframe(
                    df_ap.style.format({
                        "Alocar (R$)": "R$ {:,.2f}",
                        "% do Aporte": "{:.1f}%",
                    }),
                    use_container_width=True,
                    hide_index=True,
                )

    # ── Sub-aba 3: Por Ativo ─────────────────────────────────────────────────
    with sub3:
        targets = data_layer.get_targets()
        if not targets:
            st.info("⚠️ Defina seus alvos na aba **Definir Alvos** primeiro.")
            return

        tolerance3 = st.number_input(
            "Tolerância de desvio (%)",
            min_value=0.0,
            max_value=20.0,
            value=2.0,
            step=0.5,
            format="%.1f",
            key="tolerance_rebal3",
        )
        plan3 = data_layer.compute_rebalancing(portfolio_rows, targets, 0.0, tolerance3)
        if not plan3 or not plan3.get("by_ticker"):
            st.warning("Sem dados suficientes para detalhar por ativo.")
            return

        st.subheader("🔍 Sugestão por Ativo Individual")
        st.caption(
            "Para classes com compra sugerida, mostra quanto alocar em cada ativo "
            "para manter a proporção interna da classe."
        )

        for at in _ASSET_TYPES_ORDER:
            tickers = [r for r in plan3["by_ticker"] if r["asset_type"] == at]
            if not tickers:
                continue

            class_plan = next((c for c in plan3["by_class"] if c["asset_type"] == at), None)
            if not class_plan:
                continue

            label = f"{at} — {class_plan['acao']}  |  desvio {class_plan['desvio']:+.1f}%"
            with st.expander(label, expanded=(class_plan["acao"] != "✅ OK")):
                rows_t = []
                for r in tickers:
                    rows_t.append({
                        "Ticker": r["ticker"],
                        "Nome": r["name"],
                        "Valor Atual": r["valor_atual"],
                        "Peso na Classe": r["pct_na_classe"],
                        "Sugestão de Compra (R$)": r["sugestao_compra"],
                    })
                df_t = pd.DataFrame(rows_t)
                st.dataframe(
                    df_t.style.format({
                        "Valor Atual": "R$ {:,.2f}",
                        "Peso na Classe": "{:.1f}%",
                        "Sugestão de Compra (R$)": "R$ {:,.2f}",
                    }),
                    use_container_width=True,
                    hide_index=True,
                )

        # Export CSV
        st.markdown("---")
        export_rows = []
        for c in plan3["by_class"]:
            at = c["asset_type"]
            tickers = [r for r in plan3["by_ticker"] if r["asset_type"] == at]
            for r in tickers:
                export_rows.append({
                    "Classe": at,
                    "Ticker": r["ticker"],
                    "Nome": r["name"],
                    "% Alvo Classe": c["pct_alvo"],
                    "% Atual Classe": c["pct_atual"],
                    "Desvio Classe (%)": c["desvio"],
                    "Ação Classe": c["acao"],
                    "Valor Atual (R$)": r["valor_atual"],
                    "Peso na Classe (%)": r["pct_na_classe"],
                    "Sugestão Compra (R$)": r["sugestao_compra"],
                })
        if export_rows:
            csv_bytes = (
                pd.DataFrame(export_rows)
                .to_csv(index=False, sep=";", decimal=",")
                .encode("utf-8-sig")
            )
            st.download_button(
                label="⬇️ Exportar plano de rebalanceamento CSV",
                data=csv_bytes,
                file_name=f"rebalanceamento_{date.today().isoformat()}.csv",
                mime="text/csv",
                use_container_width=True,
            )


def main():
    st.markdown("[← Meu Dinheiro](/landing/)")
    st.title("💰 Portfólio de Investimentos")
    st.caption("Acompanhe seus investimentos em tempo real · brapi.dev · yfinance · BCB")

    portfolio_rows = data_layer.build_portfolio_data()
    maybe_save_snapshot(portfolio_rows)

    tabs = st.tabs([
        "📊 Dashboard",
        "📋 Posições",
        "📈 Gráficos",
        "📅 Agenda",
        "📆 Resumo",
        "⚙️ Gerenciar Ativos",
        "💸 Dividendos",
        "🔴 Vendas",
        "⚖️ Rebalancear",
    ])

    with tabs[0]:
        render_dashboard(portfolio_rows)

    with tabs[1]:
        render_positions_table(portfolio_rows)

    with tabs[2]:
        render_charts(portfolio_rows)

    with tabs[3]:
        render_agenda_tab(portfolio_rows)

    with tabs[4]:
        render_resumo_tab()

    with tabs[5]:
        render_manage_tab()

    with tabs[6]:
        render_dividends_tab()

    with tabs[7]:
        render_vendas_tab(portfolio_rows)

    with tabs[8]:
        render_rebalancear(portfolio_rows)

    st.divider()
    st.caption(
        f"Última atualização: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')} · "
        "Cotações: cache 5 min · CDI/Fundamentais/Agenda: cache 1h"
    )


if __name__ == "__main__":
    main()
