# engine/budgets.py
# -*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import select
from engine.storage import db, Transaction

M_MAP = {1:"jan",2:"fev",3:"mar",4:"abr",5:"mai",6:"jun",7:"jul",8:"ago",9:"set",10:"out",11:"nov",12:"dez"}

def _to_df():
    rows = db.session.execute(select(
        Transaction.id,
        Transaction.date,
        Transaction.description,
        Transaction.amount,
        Transaction.account_id,
        Transaction.category,
        Transaction.origin_label,
        Transaction.period_yyyymm
    )).all()
    if not rows:
        return pd.DataFrame(columns=[
            "id","date","description","amount","account_id","category","origin_label","period_yyyymm"
        ])
    return pd.DataFrame(rows, columns=[
        "id","date","description","amount","account_id","category","origin_label","period_yyyymm"
    ])

def _apply_filters(df, year=None, origin=None, period_yyyymm=None):
    if df.empty:
        return df
    out = df.copy()
    if year:
        out = out[out["period_yyyymm"].fillna(0).astype(int).floordiv(100) == int(year)]
    if origin:
        out = out[out["origin_label"].astype(str).str.contains(str(origin), case=False, na=False)]
    if period_yyyymm:
        out = out[out["period_yyyymm"] == int(period_yyyymm)]
    return out

def _exclude_nulo(df):
    if df.empty:
        return df
    mask = df["category"].fillna("").str.strip().str.lower().eq("nulo")
    return df.loc[~mask].copy()

def _is_receita_series(cat: pd.Series, origin: pd.Series) -> pd.Series:
    return cat.fillna("").str.strip().str.lower().eq("receitas")

def totais_consolidados(year=None, origin=None, period_yyyymm=None):
    df = _exclude_nulo(_apply_filters(_to_df(), year=year, origin=origin, period_yyyymm=period_yyyymm))
    if df.empty:
        return 0.0, 0.0
    is_receita = _is_receita_series(df["category"], df["origin_label"])
    total_despesas = float(df.loc[~is_receita, "amount"].sum())
    total_receitas = float(df.loc[is_receita,  "amount"].sum())
    return total_despesas, total_receitas

def df_despesas_por_categoria(year=None, excluir_positivas=True, origin=None, period_yyyymm=None):
    df = _exclude_nulo(_apply_filters(_to_df(), year=year, origin=origin, period_yyyymm=period_yyyymm))
    if df.empty:
        return pd.DataFrame(columns=["Categoria","Total"])
    df = df.copy()
    is_receita = _is_receita_series(df["category"], df["origin_label"])
    df = df.loc[~is_receita]
    df["Categoria"] = df["category"].fillna("Não classificado")
    agg = (df.groupby("Categoria", as_index=False)["amount"].sum()
             .rename(columns={"amount":"Total"})
             .sort_values("Total"))
    if excluir_positivas:
        agg = agg[agg["Total"] < 0]
    return agg

def _label_mes(yyyymm: int) -> str:
    if pd.isna(yyyymm): return "??/??"
    yyyymm = int(yyyymm); y = yyyymm // 100; m = yyyymm % 100
    return f"{M_MAP.get(m, '???')}/{str(y)[-2:]}"

def df_categoria_x_mes(year=None, origin=None, period_yyyymm=None):
    df = _exclude_nulo(_apply_filters(_to_df(), year=year, origin=origin, period_yyyymm=period_yyyymm))
    if df.empty:
        return pd.DataFrame(columns=["Categoria"])
    df = df.copy()
    is_receita = _is_receita_series(df["category"], df["origin_label"])
    df = df.loc[~is_receita]
    df = df.dropna(subset=["period_yyyymm"])
    if df.empty:
        return pd.DataFrame(columns=["Categoria"])

    df["Categoria"] = df["category"].fillna("Não classificado")
    df["MesAno"] = df["period_yyyymm"].astype(int).apply(_label_mes)

    tabela = df.pivot_table(index="Categoria", columns="MesAno", values="amount", aggfunc="sum", fill_value=0.0)

    if not tabela.empty:
        labels = list(tabela.columns)
        def parse_col(lbl):
            if isinstance(lbl, tuple): lbl = lbl[-1]
            s = str(lbl).strip()
            if "/" in s: m, y = s.split("/", 1)
            else:
                parts = s.replace("  "," ").split(" ")
                m, y = (parts[0], parts[1]) if len(parts)>=2 else (s, "00")
            m = m.lower()[:3]
            try: y2 = int(str(y)[-2:])
            except: y2 = -1
            ordem_map = {"jan":1,"fev":2,"mar":3,"abr":4,"mai":5,"jun":6,"jul":7,"ago":8,"set":9,"out":10,"nov":11,"dez":12}
            mi = ordem_map.get(m, 0)
            return m, y2, mi
        ord_df = pd.DataFrame([{"col":c,"m":parse_col(c)[0],"y":parse_col(c)[1],"mi":parse_col(c)[2]} for c in labels]).sort_values(["y","mi"])
        tabela = tabela[ord_df["col"].tolist()]
        tabela = tabela.reindex(tabela.sum(axis=1).sort_values().index)
    return tabela.reset_index()

def df_gastos_por_origem(year=None, period_yyyymm=None):
    df = _exclude_nulo(_apply_filters(_to_df(), year=year, period_yyyymm=period_yyyymm))
    if df.empty:
        return pd.DataFrame(columns=["Origem","Total"])
    df = df.copy()
    is_receita = _is_receita_series(df["category"], df["origin_label"])
    df = df.loc[~is_receita]
    g = (df.groupby("origin_label", as_index=False)["amount"].sum()
           .rename(columns={"origin_label":"Origem","amount":"Total"})
           .sort_values("Total"))
    return g

def transactions_for_category_month(category: str, period_yyyymm: int) -> pd.DataFrame:
    df = _exclude_nulo(_apply_filters(_to_df(), period_yyyymm=period_yyyymm))
    if df.empty:
        return pd.DataFrame(columns=["date","description","amount","account_id","category"])
    cat_norm = df["category"].fillna("Não classificado").str.strip()
    target = (category or "").strip() or "Não classificado"
    detail = df.loc[cat_norm.eq(target), ["date","description","amount","account_id","category"]].copy()
    return detail.sort_values("date")
