# engine/importador.py
import subprocess
import sys
import tempfile
import re
import pandas as pd
from pathlib import Path
from sqlalchemy import select
from engine.storage import db, Transaction, Statement
from engine.pdf_parser import parse_pdf
from datetime import datetime
from decimal import Decimal, InvalidOperation


def _norm_name(name: str | None) -> str:
    if not name:
        return ""
    return str(name).replace("\ufeff", "").strip().lower()


def _looks_like_personnalite(name: str | None) -> bool:
    if not name:
        return False
    n = _norm_name(name)
    return n.startswith("fatura-") and n.endswith(".pdf")


def _run_personnalite_converter(path: str) -> pd.DataFrame:
    script = Path(__file__).resolve().parent.parent / "python" / "main.py"
    if not script.exists():
        raise ValueError("Conversor python/main.py não encontrado para PDF Personnalité.")
    with tempfile.TemporaryDirectory() as tmpdir:
        cmd = [sys.executable, str(script), str(path)]
        res = subprocess.run(cmd, cwd=tmpdir, capture_output=True, text=True)
        if res.returncode != 0:
            raise ValueError(f"Falha ao converter PDF (exit {res.returncode}): {res.stderr or res.stdout}")
        csv_path = Path(tmpdir) / "output.csv"
        if not csv_path.exists():
            raise ValueError("Conversor Personnalité não gerou output.csv.")
        return pd.read_csv(csv_path, sep=";")


def _month_pt_to_int(token: str) -> int | None:
    if not token:
        return None
    t = str(token).strip().lower()[:3]
    mapa = {"jan":1,"fev":2,"mar":3,"abr":4,"mai":5,"jun":6,"jul":7,"ago":8,"set":9,"out":10,"nov":11,"dez":12}
    return mapa.get(t)


def _normalize_personnalite_csv(df_raw: pd.DataFrame, account_id: str, period_yyyymm: int | None, source_name: str) -> pd.DataFrame:
    df = df_raw.copy()
    rename_pt = {
        "nome do arquivo": "filename",
        "data": "date",
        "descricao": "description",
        "descrição": "description",
        "parcela": "parc",
        "total de parcelas": "total_parc",
        "categoria": "category",
        "cidade": "city",
        "valor": "value",
    }
    df.columns = [rename_pt.get(str(c).strip().lower(), c) for c in df.columns]
    required = {"date", "description", "value"}
    if not required.issubset({c.lower() for c in df.columns}):
        raise ValueError("CSV de fatura Personnalité faltando colunas obrigatórias.")

    y_hint = int(period_yyyymm // 100) if period_yyyymm else None

    def parse_date(txt):
        if pd.isna(txt):
            return pd.NaT
        s = str(txt).strip().lower()
        parts = s.split("/")
        if len(parts) >= 2:
            try:
                day = int(parts[0])
            except Exception:
                day = 1
            mi = _month_pt_to_int(parts[1])
            if mi is None:
                try:
                    mi = int(parts[1])
                except Exception:
                    mi = None
        else:
            return pd.NaT
        year = y_hint or datetime.now().year
        if not mi:
            return pd.NaT
        try:
            return datetime(year, mi, max(1, min(28 if mi == 2 else 31, day))).date()
        except Exception:
            return pd.NaT

    def parse_amount(val):
        s = str(val).replace(".", "").replace(",", ".")
        try:
            return -abs(float(s))
        except Exception:
            return 0.0

    out = pd.DataFrame({
        "date": df["date"].apply(parse_date),
        "description": df["description"].astype(str).str.strip(),
        "amount": df["value"].apply(parse_amount),
        "account_id": account_id,
        "source_name": source_name,
    })
    out = out.dropna(subset=["date", "description"])
    return out


def _is_xp_cc_csv(df_raw: pd.DataFrame, account_id: str) -> bool:
    if _norm_name(account_id) != "xp cc":
        return False
    cols = {_norm_name(c) for c in df_raw.columns}
    return {"data", "estabelecimento", "valor"}.issubset(cols)


def _parse_xp_cc_csv(df_raw: pd.DataFrame, account_id: str, source_name: str) -> pd.DataFrame:
    rename_map = {
        "data": "date",
        "estabelecimento": "description",
        "valor": "amount",
        "parcela": "installment",
    }
    df = df_raw.copy()
    df.columns = [rename_map.get(_norm_name(c), c) for c in df.columns]
    df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True).dt.date
    df["description"] = df["description"].astype(str).str.strip()
    df["amount"] = df["amount"].apply(_parse_brl_value)
    if "installment" in df.columns:
        df["description"] = df.apply(
            lambda r: f"{r['description']} ({r['installment']})" if pd.notna(r.get("installment")) and str(r["installment"]).strip() else r["description"],
            axis=1
        )
    out = pd.DataFrame({
        "date": df["date"],
        "description": df["description"],
        "amount": df["amount"],
        "account_id": account_id,
        "source_name": source_name,
    })
    return out.dropna(subset=["date"]).query("description != ''")


def detect_and_load(path: str, original_name: str | None = None) -> pd.DataFrame:
    ext = Path(path).suffix.lower()
    if ext in [".xlsx", ".xls"]:
        engine = "xlrd" if ext == ".xls" else None
        try:
            return pd.read_excel(path, engine=engine, sheet_name=0)
        except Exception:
            return pd.read_html(path)[0]
    if ext == ".csv":
        delim = None
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as fh:
                header = fh.readline()
                for cand in [",",";","\t","|"]:
                    if cand in header:
                        delim = cand
                        break
        except Exception:
            delim = None
        try:
            if delim:
                return pd.read_csv(path, sep=delim)
            return pd.read_csv(path)
        except Exception:
            try:
                return pd.read_csv(path, sep=";")
            except Exception:
                try:
                    return pd.read_csv(path, sep="\t")
                except Exception:
                    return pd.read_csv(path, sep=None, engine="python")
    if ext == ".pdf":
        if _looks_like_personnalite(original_name or Path(path).name):
            return _run_personnalite_converter(path)
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


def to_standard_df(
    df_raw: pd.DataFrame,
    account_id: str,
    ext: str,
    path: str = None,
    origin_hint: str | None = None,
    original_name: str | None = None,
    period_yyyymm: int | None = None,
) -> pd.DataFrame:
    if _is_xp_cc_csv(df_raw, account_id):
        return _parse_xp_cc_csv(df_raw, account_id, Path(original_name or path or "upload").name)
    if _looks_like_personnalite_conta(original_name) or (_norm_name(account_id) == "personnalite conta" and {"data","lançamento","valor (r$)"} <= {_norm_name(c) for c in df_raw.columns}):
        return _parse_personnalite_conta_xls(path, account_id)
    if (_looks_like_santander_conta(original_name) or _norm_name(account_id) == "santander conta") and {"data","descrição","valor"} <= {_norm_name(c) for c in df_raw.columns}:
        return _parse_santander_conta_csv(df_raw, account_id, Path(original_name or path or "upload").name)
    if _looks_like_santander_conta(original_name) or (_norm_name(account_id) == "santander conta" and {"data","descrição","valor"} <= {_norm_name(c) for c in df_raw.columns}):
        return _parse_santander_conta_xls(path, account_id)
    if _looks_like_personnalite(original_name or (Path(path).name if path else "")) and {"date","description","value"}.issubset({_norm_name(c) for c in df_raw.columns}):
        return _normalize_personnalite_csv(df_raw, account_id, period_yyyymm, Path(original_name or path or "upload").name)
    if is_aadvantage_xlsx(df_raw):
        return parse_aadvantage_xlsx(path, account_id)
    # fallback genérico
    df = df_raw.copy()
    cols = {_norm_name(c): c for c in df.columns}
    date_col = cols.get("date") or cols.get("data")
    desc_col = cols.get("description") or cols.get("descrição") or cols.get("descricao")
    amt_col = cols.get("amount") or cols.get("valor") or cols.get("valor (r$)") or cols.get("value")
    if not (date_col and desc_col and amt_col):
        raise ValueError("Não foi possível mapear colunas padrão (date/description/amount).")
    df["date"] = pd.to_datetime(df[date_col], errors="coerce").dt.date
    df["description"] = df[desc_col].astype(str).str.strip()
    df["amount"] = pd.to_numeric(
        df[amt_col].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
        errors="coerce"
    ).fillna(0.0)
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
def _looks_like_personnalite_conta(name: str | None) -> bool:
    if not name:
        return False
    n = _norm_name(name)
    return n.startswith("personnalite conta") and n.endswith(".xls")

def _looks_like_santander_conta(name: str | None) -> bool:
    if not name:
        return False
    n = _norm_name(name)
    return "santander" in n and "conta" in n and (n.endswith(".xls") or n.endswith(".csv"))
def _parse_personnalite_conta_xls(path: str, account_id: str) -> pd.DataFrame:
    df = pd.read_html(path)[0]
    cols = {_norm_name(c): c for c in df.columns}
    date_col = cols.get("data")
    desc_col = cols.get("lançamento") or cols.get("lancamento")
    amt_col = cols.get("valor (r$)") or cols.get("valor")
    if not (date_col and desc_col and amt_col):
        raise ValueError("Planilha Personnalité conta sem colunas data/lançamento/valor.")
    df["date"] = pd.to_datetime(df[date_col], errors="coerce", dayfirst=True).dt.date
    df["description"] = df[desc_col].astype(str).str.strip()
    df["amount"] = df[amt_col].apply(_parse_brl_value)
    out = pd.DataFrame({
        "date": df["date"],
        "description": df["description"],
        "amount": df["amount"],
        "account_id": account_id,
        "source_name": Path(path).name,
    })
    return out.dropna(subset=["date"]).query("description != ''")


def _parse_santander_conta_xls(path: str, account_id: str) -> pd.DataFrame:
    import pandas as pd
    df = pd.read_html(path)[0]
    cols = {_norm_name(c): c for c in df.columns}
    date_col = cols.get("data")
    desc_col = cols.get("descrição") or cols.get("descricao")
    amt_col = cols.get("valor") or cols.get("valor (r$)")
    if not (date_col and desc_col and amt_col):
        raise ValueError("Planilha Santander conta sem colunas data/descrição/valor.")
    df["date"] = pd.to_datetime(df[date_col], errors="coerce", dayfirst=True).dt.date
    df["description"] = df[desc_col].astype(str).str.strip()
    df["amount"] = df[amt_col].apply(_parse_brl_value)
    out = pd.DataFrame({
        "date": df["date"],
        "description": df["description"],
        "amount": df["amount"],
        "account_id": account_id,
        "source_name": Path(path).name,
    })
    return out.dropna(subset=["date"]).query("description != ''")


def _parse_santander_conta_csv(df_raw: pd.DataFrame, account_id: str, source_name: str) -> pd.DataFrame:
    rename_map = {"data":"date","descrição":"description","descricao":"description","valor":"amount","valor (r$)":"amount"}
    df = df_raw.copy()
    df.columns = [rename_map.get(_norm_name(c), c) for c in df.columns]
    if not {"date","description","amount"}.issubset({_norm_name(c) for c in df.columns}):
        raise ValueError("CSV Santander conta sem colunas data/descrição/valor.")
    df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True).dt.date
    df["description"] = df["description"].astype(str).str.strip()
    df["amount"] = df["amount"].apply(_parse_brl_value)
    out = pd.DataFrame({
        "date": df["date"],
        "description": df["description"],
        "amount": df["amount"],
        "account_id": account_id,
        "source_name": source_name,
    })
    return out.dropna(subset=["date"]).query("description != ''")


def _parse_brl_value(val) -> float:
    if pd.isna(val):
        return 0.0
    if isinstance(val, (int, float)):
        return float(round(float(val), 2))
    s = str(val).strip()
    s = s.replace("R$", "").replace("\u00a0", "").replace(" ", "")
    s = s.replace("'", "")
    if s.count(",") > 1 and s.count(".") == 0:
        parts = s.split(",")
        s = "".join(parts[:-1]) + "." + parts[-1]
    else:
        s = s.replace(".", "").replace(",", ".")
    s = re.sub(r"[^0-9\-.]", "", s)
    try:
        return float(Decimal(s))
    except InvalidOperation:
        return 0.0
