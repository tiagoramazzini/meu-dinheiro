# app.py
# -*- coding: utf-8 -*-
import os
from datetime import datetime
import pandas as pd
import streamlit as st
import plotly.express as px

# ----- ENGINE -----
from engine.storage import db, init_db, Transaction, Statement, Category
from engine.importador import detect_and_load, to_standard_df, import_file_as_statement
from engine.budgets import (
    totais_consolidados, df_despesas_por_categoria, df_categoria_x_mes, df_gastos_por_origem
)
from engine.classificador import apply_category_bulk, classify_batch, apply_smart_rules_to_statement
from engine.utils import save_temp, fmt_brl

# ===== BOOTSTRAP =====
FIN_DATA_DIR = os.getenv("FIN_DATA_DIR", "./data")
FIN_BACKUP_DIR = os.getenv("FIN_BACKUP_DIR", "./backup")
os.makedirs(FIN_DATA_DIR, exist_ok=True)
os.makedirs(FIN_BACKUP_DIR, exist_ok=True)
os.makedirs(os.path.join(FIN_DATA_DIR, "tmp"), exist_ok=True)

init_db()
st.set_page_config(page_title="Meu Financeiro", layout="wide")

# ===== CONSTS/UTIL =====
MESES = ["jan","fev","mar","abr","mai","jun","jul","ago","set","out","nov","dez"]
MESES_MAP = {"jan":1,"fev":2,"mar":3,"abr":4,"mai":5,"jun":6,"jul":7,"ago":8,"set":9,"out":10,"nov":11,"dez":12}
CONTAS_OPTS = ["AAdvantage CC", "XP CC", "Personnalite CC", "Personnalite conta", "XP conta", "Santander conta", "BTG conta"]

def card_kpi(label: str, valor: float, col):
    col.metric(label, fmt_brl(valor, with_symbol=True))

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

# ===== SIDEBAR / NAV =====
with st.sidebar:
    st.title("Meu Financeiro")
    page = st.radio(
        "Navegação",
        ["Meu Dinheiro", "Despesas", "Importar Extratos", "Categorias"],
        index=0
    )
    st.caption("v12.7 - DEV")

# ===== MEU DINHEIRO =====
if page == "Meu Dinheiro":
    st.header("Meu Dinheiro")

    # Toggle simples (sem cor/sinalização visual)
    if "md_view" not in st.session_state:
        st.session_state.md_view = "Ano"

    cta1, cta2, info = st.columns([1,1,2])
    with cta1:
        if st.button("Visão Anual", use_container_width=True):
            st.session_state.md_view = "Ano"
    with cta2:
        if st.button("Visão Mensal", use_container_width=True):
            st.session_state.md_view = "Mês"
    with info:
        st.caption(f"Visão atual: **{st.session_state.md_view}**")

    st.divider()

    # ---------------- Ano ----------------
    if st.session_state.md_view == "Ano":
        ano_dash = st.number_input("Ano", min_value=2000, max_value=2100, value=2025, step=1)

        tot_desp, tot_rec = totais_consolidados(year=ano_dash)

        cA, cB, cC = st.columns(3)
        card_kpi("Despesas", tot_desp, cA)
        card_kpi("Receitas", tot_rec, cB)
        card_kpi("Saldo", tot_rec + tot_desp, cC)

        st.markdown("#### Gastos por Categoria (apenas despesas)")
        gcat = df_despesas_por_categoria(year=ano_dash, excluir_positivas=True)
        if not gcat.empty:
            fig = px.bar(
                gcat,
                x="Categoria",
                y="Total",
                color="Categoria",           # cores por categoria
                hover_data={"Total":":.2f"},
            )
            fig.update_layout(xaxis_title="", yaxis_title="Total (R$)", bargap=0.2)
            st.plotly_chart(fig, use_container_width=True, key="gcat_ano")
            st.dataframe(gcat.assign(Total_fmt=gcat["Total"].map(fmt_brl)),
                         use_container_width=True, hide_index=True)
        else:
            st.info("Sem dados para o período.")

        st.markdown("#### Despesas - Categoria × Mês")
        gxmes = df_categoria_x_mes(year=ano_dash)
        if not gxmes.empty:
            st.dataframe(gxmes, use_container_width=True, hide_index=True)
        else:
            st.info("Sem dados para o período.")

    # ---------------- Mês ----------------
    else:
        c1, c2 = st.columns([1,1])
        with c1:
            mes = st.selectbox("Mês", MESES, index=9, key="mes_md")
        with c2:
            ano = st.number_input("Ano (mês)", min_value=2000, max_value=2100, value=2025, step=1, key="ano_md")

        period_yyyymm = int(f"{ano}{MESES_MAP[mes]:02d}")
        period_label = f"{mes}/{str(ano)[-2:]}"
        st.caption(f"Período selecionado: **{period_label}**")

        tot_desp_m, tot_rec_m = totais_consolidados(period_yyyymm=period_yyyymm)

        cA, cB, cC = st.columns(3)
        card_kpi("Despesas (mês)", tot_desp_m, cA)
        card_kpi("Receitas (mês)", tot_rec_m, cB)
        card_kpi("Saldo (mês)", tot_rec_m + tot_desp_m, cC)

        st.markdown("#### Gastos por Categoria (mês)")
        gcat_m = df_despesas_por_categoria(period_yyyymm=period_yyyymm, excluir_positivas=True)
        if not gcat_m.empty:
            figm = px.bar(
                gcat_m,
                x="Categoria",
                y="Total",
                color="Categoria",           # cores por categoria
                hover_data={"Total":":.2f"},
            )
            figm.update_layout(xaxis_title="", yaxis_title="Total (R$)", bargap=0.2)
            st.plotly_chart(figm, use_container_width=True, key="gcat_mes")
            st.dataframe(gcat_m.assign(Total_fmt=gcat_m["Total"].map(fmt_brl)),
                         use_container_width=True, hide_index=True)
        else:
            st.info("Sem dados de categorias no mês.")

        st.markdown("#### Gastos por Origem (mês)")
        gorig_m = df_gastos_por_origem(period_yyyymm=period_yyyymm)
        if not gorig_m.empty:
            figm2 = px.bar(gorig_m, x="Origem", y="Total")
            figm2.update_layout(xaxis_title="", yaxis_title="Total (R$)")
            st.plotly_chart(figm2, use_container_width=True, key="gorig_mes")
            st.dataframe(gorig_m.assign(Total_fmt=gorig_m["Total"].map(fmt_brl)),
                         use_container_width=True, hide_index=True)
        else:
            st.info("Sem dados por origem no mês.")

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
        } for r in rows]
        df_tx = pd.DataFrame(data)

        # categorias cadastradas
        all_cats = [c.name for c in db.session.query(Category).order_by(Category.name.asc()).all()]
        cats = [c for c in all_cats if c.strip().lower() != "receita"] if is_cc else all_cats

        if is_cc:
            st.info("Lote de **Cartão de Crédito**: valores positivos são **créditos** (nunca Receita). A categoria 'Receita' foi ocultada neste lote.")

        st.caption("Edite a coluna **categoria** (dropdown) e clique em **Aplicar alterações**.")
        df_edit = st.data_editor(
            df_tx,
            num_rows="dynamic",
            use_container_width=True,
            hide_index=True,
            column_config={
                "id": st.column_config.NumberColumn("id", disabled=True),
                "mês": st.column_config.TextColumn("mês", disabled=True),
                "descrição": st.column_config.TextColumn("descrição", disabled=True),
                "valor": st.column_config.NumberColumn("valor", disabled=True),
                "conta": st.column_config.TextColumn("conta", disabled=True),
                "categoria": st.column_config.SelectboxColumn("categoria", options=cats, required=False),
            }
        )

        col_e1, col_e2 = st.columns([1,2])
        with col_e1:
            if st.button("Aplicar alterações", type="primary"):
                try:
                    updated = apply_category_bulk(df_edit[["id","categoria"]])
                    st.success(f"Categorias atualizadas em {updated} linhas.")
                except Exception as e:
                    st.error(f"Erro ao aplicar categorias: {e}")
        with col_e2:
            csv = df_tx.to_csv(index=False).encode("utf-8")
            st.download_button("Baixar CSV (snapshot)", data=csv, file_name=f"lote_{lote_id}.csv", mime="text/csv")

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

                for f in files:
                    tmp = save_temp(f)
                    backup_arquivo_local(tmp, prefix=account or "upload")
                    df_raw = detect_and_load(tmp)
                    df_std = to_standard_df(df_raw, account, f.name.split('.')[-1], path=tmp, origin_hint=None)

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

                    total_created += created
                    total_skipped += skipped
                    lotes.append(stmnt_id)

                st.success(
                    f"Importação concluída: {total_created} novas linhas (+{total_skipped} duplicadas). "
                    f"Lotes: {', '.join(map(str, lotes))} — {account} {period_label}. "
                    f"Regras inteligentes: {total_rules_updates} atualizações ({total_rules_matches} matches, {total_rules} regras ativas)."
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
    st.dataframe(df_lotes, use_container_width=True, hide_index=True)

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
            st.success(f"Excluídos {n} lançamentos e o lote {int(lote_id)}.")
            df_lotes = list_statements(origem_f, periodo_f)
            st.dataframe(df_lotes, use_container_width=True, hide_index=True)

# ===== CATEGORIAS =====
elif page == "Categorias":
    st.header("Categorias")

    # Toggle simples (sem cor)
    if "cat_view" not in st.session_state:
        st.session_state.cat_view = "Categorias"

    b1, b2, info = st.columns([1,1,2])
    with b1:
        if st.button("Categorias", use_container_width=True):
            st.session_state.cat_view = "Categorias"
    with b2:
        if st.button("Categorias Inteligentes", use_container_width=True):
            st.session_state.cat_view = "Categorias Inteligentes"
    with info:
        st.caption(f"Visão atual: **{st.session_state.cat_view}**")

    st.divider()

    # ---------- Visão: Categorias ----------
    if st.session_state.cat_view == "Categorias":
        cats = db.session.query(Category).order_by(Category.name.asc()).all()
        df_c = pd.DataFrame([{"id": c.id, "categoria": c.name} for c in cats])
        st.subheader("Lista")
        st.dataframe(df_c, use_container_width=True, hide_index=True)

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
    else:
        from engine.storage import SmartCategoryRule
        rules = db.session.query(SmartCategoryRule).order_by(SmartCategoryRule.id.desc()).all()
        df_r = pd.DataFrame([{"id": r.id, "keyword": r.keyword, "category": r.category, "active": r.active} for r in rules])

        st.subheader("Regras cadastradas")
        st.dataframe(df_r, use_container_width=True, hide_index=True)

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
