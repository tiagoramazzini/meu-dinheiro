# engine/importador.py
from __future__ import annotations

import re
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy.exc import IntegrityError

# Dependências opcionais por tipo de arquivo
try:
    import pdfplumber
except Exception:
    pdfplumber = None

try:
    from ofxparse import OfxParser
except Exception:
    OfxParser = None

from .storage import db, Transaction
from .normalizar import norm_desc

# ===================== Helpers / Consts =====================
_BR_CURRENCY = "BRL"

_RE_DATA_DDMM = re.compile(r"\b(\d{2})/(\d{2})\b")                      # 26/09
_RE_DATA_DDMMYYYY = re.compile(r"\b(\d{2})/(\d{2})/(\d{4})\b")          # 26/09/2025
_RE_VALOR_FIM = re.compile(r"(-?\d{1,3}(?:\.\d{3})*,\d{2})\s*$")        # 1.090,86 (no fim)
_RE_TOKEN_PARC = re.compile(r"\b(\d{1,2})/(\d{1,2})\b")                 # 03/12 etc.

_DROP_PREFIXES_PDF = (
    "BANCO SANTANDER", "Olá,", "Opções de Pagamento", "Resumo da Fatura", "Histórico de Faturas",
    "Posição do seu Limite", "Orientações para Pagamento", "Beneficiário", "Beneficiária",
    "Agência", "Autenticação", "Ficha de Compensação", "Pagamento Mínimo", "Total desta Fatura",
    "Valor Pago", "CPF/CNPJ", "Programa AAdvantage", "Juros e Custo Efetivo Total",
    "Central de Atendimento", "SAC", "Ouvidoria", "Melhor Data", "Vencimento", "Total a Pagar",
    "Escaneie para", "Número do Documento", "Nosso Número", "Data Documento", "Data Process",
    "Carteira", "Espécie", "Uso Banco", "CET", "Parcelamento de Fatura", "Veja outras opções",
    "PARCELAMENTO DE FATURA",
)
_KEEP_SECTIONS = ("Detalhamento da Fatura", "Despesas", "Parcelamentos")


def _parse_brl(txt: str) -> float:
    s = (txt or "").strip().replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def _parse_date_ddmm(line: str, year_hint: int) -> Optional[datetime.date]:
    m = _RE_DATA_DDMM.search(line or "")
    if not m:
        return None
    dd, mm = m.group(1), m.group(2)
    try:
        return datetime.strptime(f"{dd}/{mm}/{year_hint}", "%d/%m/%Y").date()
    except Exception:
        return None


def _should_drop_pdf_line(line: str) -> bool:
    t = (line or "").strip()
    if not t:
        return True
    # Números de página tipo 2/3
    if re.fullmatch(r"\d+/\d+", t):
        return True
    if t.upper().startswith("VALOR TOTAL"):
        return True
    for p in _DROP_PREFIXES_PDF:
        if t.startswith(p):
            return True
    return False


# ===================== Detect & Load =====================
def detect_and_load(path: Path | str) -> pd.DataFrame:
    """
    Retorna DataFrame com colunas mínimas:
      - date, description, amount, currency
      - (opcional: external_uid)
    """
    p = Path(path)
    ext = p.suffix.lower()

    # OFX
    if ext == ".ofx":
        if OfxParser is None:
            raise RuntimeError("ofxparse não está instalado dentro do container.")
        with open(p, "rb") as fh:
            ofx = OfxParser.parse(fh)
        rows = []
        for acct in ofx.accounts or []:
            stmt = getattr(acct, "statement", None)
            txs = getattr(stmt, "transactions", None)
            for tx in (txs or []):
                dt = tx.date.date() if isinstance(tx.date, datetime) else tx.date
                rows.append({
                    "date": dt,
                    "description": (tx.memo or tx.payee or "").strip(),
                    "amount": float(tx.amount or 0.0),
                    "currency": _BR_CURRENCY,
                    "external_uid": getattr(tx, "id", None) or getattr(tx, "uniqueid", None),
                })
        return pd.DataFrame(rows)

    # CSV
    if ext in (".csv",):
        try:
            return pd.read_csv(p)
        except Exception:
            return pd.read_csv(p, encoding="latin1")

    # Excel
    if ext in (".xls", ".xlsx"):
        return pd.read_excel(p)

    # PDF (Santander-like)
    if ext == ".pdf":
        if pdfplumber is None:
            raise RuntimeError("pdfplumber não está instalado dentro do container.")
        return _parse_pdf_fatura_santander_like(p)

    raise ValueError(f"Extensão de arquivo não suportada: {ext}")


def _parse_pdf_fatura_santander_like(p: Path) -> pd.DataFrame:
    """
    Parser simples para faturas no estilo Santander (Way/AAdvantage).
    Extrai linhas com: <data DD/MM> ... <descrição> ... <valor no fim>.
    """
    rows = []
    year_hint = None
    current_section = None

    with pdfplumber.open(str(p)) as pdf:
        # tenta extrair ano explícito nas primeiras páginas (dd/mm/YYYY)
        for page in pdf.pages[:3]:
            text = page.extract_text() or ""
            m = _RE_DATA_DDMMYYYY.search(text)
            if m:
                year_hint = int(m.group(3))
                break
        if not year_hint:
            year_hint = datetime.now().year

        for page in pdf.pages:
            text = page.extract_text() or ""
            for raw in text.splitlines():
                # normaliza espaços/quebras
                line = " ".join((raw or "").split())
                if _should_drop_pdf_line(line):
                    continue

                # ativa seção de detalhe
                if any(h in line for h in _KEEP_SECTIONS):
                    current_section = "DETAIL"
                    continue
                if current_section != "DETAIL":
                    continue

                mval = _RE_VALOR_FIM.search(line)
                if not mval:
                    continue
                valor_txt = mval.group(1)
                amount = _parse_brl(valor_txt)

                d = _parse_date_ddmm(line, year_hint)
                if not d:
                    continue

                desc = line[: mval.start()].strip()
                desc = _RE_DATA_DDMM.sub("", desc).strip()

                # parcela "03/12"
                mparc = _RE_TOKEN_PARC.search(desc)
                if mparc:
                    # guardamos mas não usamos aqui; ficam no DF para quem quiser tratar
                    try:
                        parc_n = int(mparc.group(1))
                        parc_total = int(mparc.group(2))
                    except Exception:
                        parc_n = parc_total = None
                    desc = _RE_TOKEN_PARC.sub("", desc).strip()
                else:
                    parc_n = parc_total = None

                # pagamentos (crédito)
                if "PAGAMENTO DE FATURA" in desc.upper():
                    amount = -abs(amount)

                rows.append({
                    "date": d,
                    "description": desc,
                    "amount": amount,
                    "currency": _BR_CURRENCY,
                    "parcela_n": parc_n,
                    "parcela_total": parc_total,
                })

    return pd.DataFrame(rows)


# ===================== Normalização & Upsert =====================
def to_standard_df(df: pd.DataFrame, account_id: str, source: str) -> pd.DataFrame:
    """
    Normaliza antes de inserir:
    - garante colunas básicas
    - normaliza descrição
    - gera hash_uni por (account|date|amount|description_norm)
    - remove linhas lixo (descrição vazia E valor 0)
    - remove linhas SEM data válida (NaT)
    - dedup dentro do lote
    """
    # início seguro (evita "truth value of a DataFrame is ambiguous")
    if df is None:
        out = pd.DataFrame()
    elif isinstance(df, pd.DataFrame):
        out = df.copy()
    else:
        out = pd.DataFrame(df)

    # description
    if "description" not in out.columns:
        out["description"] = ""
    out["description"] = (
        out["description"]
        .fillna("")
        .astype(str)
        .str.replace(r"\s+", " ", regex=True)  # remove quebras de linha / múltiplos espaços
        .str.strip()
    )

    # amount
    if "amount" not in out.columns:
        out["amount"] = 0.0
    out["amount"] = pd.to_numeric(out["amount"], errors="coerce").fillna(0.0)

    # currency
    if "currency" not in out.columns:
        out["currency"] = _BR_CURRENCY
    out["currency"] = out["currency"].fillna(_BR_CURRENCY).astype(str)

    # date
    if "date" not in out.columns:
        out["date"] = datetime.utcnow().date()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date

    # descarta SEM data válida
    out = out[~pd.isna(out["date"])]

    # normaliza descrição
    out["description_norm"] = out["description"].apply(norm_desc)

    # hash determinístico
    def _mkhash(r):
        key = f"{account_id}|{r['date']}|{r['amount']:.2f}|{r['description_norm']}"
        return hashlib.md5(key.encode("utf-8")).hexdigest()[:16]
    out["hash_uni"] = out.apply(_mkhash, axis=1)

    # remove lixo de PDF: descrição vazia E 0.0
    out = out[~((out["description"] == "") & (out["amount"] == 0.0))]

    # dedup no lote
    out = out.drop_duplicates(subset=["hash_uni"])

    # campos finais esperados
    out["account_id"] = account_id
    out["source"] = source
    if "external_uid" not in out.columns:
        out["external_uid"] = None
    out["status"] = "confirmed"

    cols = [
        "date", "description", "description_norm", "amount", "currency",
        "account_id", "source", "external_uid", "hash_uni", "status"
    ]
    if out.empty:
        return pd.DataFrame(columns=cols)
    return out[cols]


def upsert_transactions(df: pd.DataFrame) -> tuple[int, int]:
    """
    Insere linha a linha com tratamento de duplicidade.
    - Em conflito (UNIQUE), faz rollback e conta como 'skipped'
    - Commit no final
    Retorna (created, skipped)
    """
    created = skipped = 0
    if df is None or df.empty:
        return 0, 0

    for rec in df.to_dict(orient="records"):
        try:
            db.session.add(Transaction(**rec))
            db.session.flush()  # força INSERT agora; se duplicado, cai no except
            created += 1
        except IntegrityError:
            db.session.rollback()
            skipped += 1

    db.session.commit()
    return created, skipped
