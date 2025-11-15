# engine/importador.py
import pandas as pd
from pathlib import Path
from sqlalchemy import select
from engine.storage import db, Transaction, Statement
from engine.pdf_parser import parse_pdf


def detect_and_load(path: str) -> pd.DataFrame:
    ext = Path(path).suffix.lower()
    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(path, sheet_name=0)
    if ext == ".csv":
        return pd.read_csv(path)
    if ext == ".pdf":
        return parse_pdf(Path(path))
    if ext == ".ofx":
        # opcional: implementar OFX no futuro
        raise ValueError("OFX ainda não suportado neste parser.")
    raise ValueError(f"Formato não suportado: {ext}")


def is_aadvantage_xlsx(df: pd.DataFrame) -> bool:
    cols = {c.strip().lower() for c in df.columns}
    return {"data", "descrição", "valor (r$)"} <= cols


def parse_aadvantage_xlsx(path: str, account_id: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=0)
    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns={"Data": "date", "Descrição": "description", "Valor (R$)": "amount"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["description"] = df["description"].astype(str).str.strip()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    out = pd.DataFrame({
        "date": df["date"],
        "description": df["description"],
        "amount": df["amount"],
        "account_id": account_id,
        "source_name": Path(path).name
    })
    return out.dropna(subset=["date"]).query("description != ''")


def to_standard_df(df_raw: pd.DataFrame, account_id: str, ext: str, path: str = None, origin_hint: str | None = None) -> pd.DataFrame:
    if is_aadvantage_xlsx(df_raw):
        return parse_aadvantage_xlsx(path, account_id)
    # fallback genérico
    df = df_raw.copy()
    cols = {c.lower(): c for c in df.columns}
    date_col = cols.get("date") or cols.get("data")
    desc_col = cols.get("description") or cols.get("descrição") or cols.get("descricao")
    amt_col = cols.get("amount") or cols.get("valor") or cols.get("valor (r$)")
    if not (date_col and desc_col and amt_col):
        raise ValueError("Não foi possível mapear colunas padrão (date/description/amount).")
    df["date"] = pd.to_datetime(df[date_col], errors="coerce").dt.date
    df["description"] = df[desc_col].astype(str).str.strip()
    df["amount"] = pd.to_numeric(df[amt_col], errors="coerce").fillna(0.0)
    out = pd.DataFrame({
        "date": df["date"],
        "description": df["description"],
        "amount": df["amount"],
        "account_id": account_id,
        "source_name": Path(path).name if path else "upload"
    })
    return out.dropna(subset=["date"]).query("description != ''")


def upsert_transactions(df_std, statement_id=None, origin_label=None, period_yyyymm=None):
    created, skipped = 0, 0
    for _, r in df_std.iterrows():
        q = select(Transaction).where(
            Transaction.date == r["date"],
            Transaction.amount == float(r["amount"]),
            Transaction.description == r["description"],
            Transaction.account_id == r["account_id"]
        )
        if db.session.execute(q).first():
            skipped += 1
            continue
        tx = Transaction(
            date=r["date"],
            description=r["description"],
            amount=float(r["amount"]),
            account_id=r["account_id"],
            statement_id=statement_id,
            origin_label=origin_label,  # aqui virá a CONTA (ex.: "AAdvantage CC")
            period_yyyymm=period_yyyymm
        )
        db.session.add(tx)
        created += 1
    db.commit()
    return created, skipped


def import_file_as_statement(path, account_id, df_std, origin_label: str, period_yyyymm: int, period_label: str):
    stmnt = Statement(
        source_name=Path(path).name,
        account_id=account_id,
        origin_label=origin_label,  # igual à conta
        period_yyyymm=period_yyyymm,
        period_label=period_label,
        rows=0,
    )
    db.session.add(stmnt)
    db.commit()
    created, skipped = upsert_transactions(df_std, statement_id=stmnt.id, origin_label=origin_label, period_yyyymm=period_yyyymm)
    stmnt.rows = created
    db.commit()
    return stmnt.id, created, skipped
