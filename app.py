
import os
from pathlib import Path
from datetime import date
import tempfile
import pandas as pd
import streamlit as st

st.set_page_config(page_title="NAS Finance", layout="wide")

st.sidebar.title("Menu")
pagina = st.sidebar.radio("", ["Painel de Controle", "Meu Dinheiro", "Despesas"], index=0)

st.title(pagina)

def _save_temp(uploaded_file):
    p = Path(tempfile.gettempdir()) / uploaded_file.name
    p.write_bytes(uploaded_file.read())
    return p

if pagina == "Painel de Controle":
    st.subheader("Sincronização e Importação")

    c1, c2 = st.columns([1,1])
    with c1:
        if st.button("Sincronizar via Open Finance", use_container_width=True):
            try:
                from engine.pluggy_import import sync_from_pluggy
                with st.spinner("Sincronizando com bancos (Open Finance)..."):
                    res = sync_from_pluggy(account_label_mapping={})
                st.success(f"Open Finance: {res['new']} novos, {res['skipped']} ignorados, {res['accounts']} contas.")
            except Exception as e:
                st.error(f"Falha na sincronização: {e}")
    with c2:
        st.caption("Última sincronização: —")

    st.markdown("---")
    st.subheader("Importar Arquivos Manualmente")

    files = st.file_uploader('Arquivos (OFX/CSV/XLS/XLSX/PDF)', accept_multiple_files=True,
                             type=['ofx','csv','xls','xlsx','pdf'])
    account = st.text_input('Conta/Cartão (ID)', value='default',
                            help='Ex.: itau_cc, nubank_master, santander_visa')

    if st.button('Processar Importação', type='primary'):
        from engine.importador import detect_and_load, to_standard_df, upsert_transactions

        total_new = 0
        total_skipped = 0
        report_rows = []
        last_df_std = None

        for f in files or []:
            try:
                tmp = _save_temp(f)
                df_raw = detect_and_load(tmp)
            except Exception as e:
                st.error(f"{f.name}: erro ao ler arquivo ({e})")
                continue

            raw_n = 0 if df_raw is None else len(df_raw)

            df_std = to_standard_df(df_raw, account, f.name.split('.')[-1])
            std_n = 0 if df_std is None else len(df_std)

            created = skipped = 0
            if df_std is not None and not df_std.empty:
                created, skipped = upsert_transactions(df_std)
                last_df_std = df_std

            total_new += created
            total_skipped += skipped
            report_rows.append({
                "arquivo": f.name,
                "linhas_brutas": raw_n,
                "linhas_pos_parse": std_n,
                "inseridas_novas": created,
                "duplicadas_skip": skipped,
            })

        st.success(f'Importação manual concluída. Novos: {total_new} | Duplicados ignorados: {total_skipped}')

        if report_rows:
            st.markdown("**Resumo por arquivo**")
            st.dataframe(pd.DataFrame(report_rows), use_container_width=True)

        if last_df_std is not None and not last_df_std.empty:
            st.markdown("**Prévia dos dados normalizados (top 10)**")
            st.dataframe(last_df_std.head(10), use_container_width=True)
        else:
            st.info("Nenhuma linha utilizável após a normalização.")

elif pagina == "Meu Dinheiro":
    from engine.budgets import (totais_consolidados,
                                df_despesas_por_categoria,
                                df_categoria_x_mes,
                                df_gastos_por_origem)

    ano_atual = date.today().year
    anos = list(range(ano_atual-2, ano_atual+1))
    colsel1, _ = st.columns([1,9])
    with colsel1:
        ano = st.selectbox("Ano", anos, index=len(anos)-1)

    try:
        tot_desp, tot_rec = totais_consolidados(ano)
    except Exception as e:
        tot_desp, tot_rec = 0.0, 0.0
        st.warning(f"Não foi possível calcular os totais: {e}")

    k1, k2 = st.columns(2)
    with k1: st.metric("Total de despesas (consolidado)", f"R$ {tot_desp:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
    with k2: st.metric("Total de receitas (consolidado)", f"R$ {tot_rec:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))

    st.markdown("---")

    g1, g2 = st.columns([1,1])
    with g1:
        st.subheader("Despesas por Categoria")
        try:
            df_cat = df_despesas_por_categoria(ano)
            st.dataframe(df_cat, use_container_width=True)
        except Exception as e:
            st.info(f"(sem dados) {e}")
    with g2:
        st.subheader("Gastos por Origem (banco/cartão)")
        try:
            df_org = df_gastos_por_origem(ano)
            st.dataframe(df_org, use_container_width=True)
        except Exception as e:
            st.info(f"(sem dados) {e}")

    st.subheader("Tabela principal: Categoria × Mês")
    try:
        df_cxm = df_categoria_x_mes(ano)
        st.dataframe(df_cxm, use_container_width=True)
    except Exception as e:
        st.info(f"(sem dados) {e}")

else:
    st.subheader("Atualizar despesas")
    if st.button("Atualizar despesas (classificar e reconciliar)", type='primary'):
        try:
            from engine.classificador import classify_batch
            classify_batch()
            st.success("Despesas atualizadas")
        except Exception as e:
            st.error(f"Falha ao atualizar: {e}")

    st.markdown("---")

    st.subheader("Todas as despesas (edição) — (placeholder)")
    st.caption("Edição inline e aplicar em lote chegarão aqui.")
