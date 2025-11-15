from __future__ import annotations

import re

from datetime import datetime, date
from typing import Iterable, Optional
import calendar

import pandas as pd
from sqlalchemy import func

from .storage import (
    db,
    Transaction,
    Category,
    Invoice,
    InvoiceTransaction,
    CategoryHint,
)

_BR_NUMBER_RE = re.compile(r'-?\d{1,3}(?:\.\d{3})*,\d{2}')

MONTH_LABELS = [
    "Janeiro",
    "Fevereiro",
    "Marco",
    "Abril",
    "Maio",
    "Junho",
    "Julho",
    "Agosto",
    "Setembro",
    "Outubro",
    "Novembro",
    "Dezembro",
]
_MONTH_TO_LABEL = {idx + 1: label for idx, label in enumerate(MONTH_LABELS)}
_LABEL_TO_MONTH = {label: idx + 1 for idx, label in enumerate(MONTH_LABELS)}


ORIGIN_RULES = {
    "aadvantage": {
        "flip_positive_to_negative": True,
        "drop_descriptions": [
            "pagamento de fatura",
            "pagamento de fatura-internet",
        ],
        "amount_from_description": True,
    },
}


def month_number_to_label(month: int) -> str:
    return _MONTH_TO_LABEL.get(month, str(month))


def month_label_to_number(label: str) -> Optional[int]:
    return _LABEL_TO_MONTH.get(label)


def _normalize_text(value: Optional[str]) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _apply_amount_rules(df_out: pd.DataFrame) -> pd.DataFrame:
    def _normalize_amount(val: float) -> float:
        try:
            v = float(val or 0.0)
        except (TypeError, ValueError):
            return 0.0
        if v > 0:
            return -abs(v)
        return v

    df_out["amount"] = df_out["amount"].apply(_normalize_amount)
    return df_out


def _apply_amount_from_description(df_out: pd.DataFrame) -> pd.DataFrame:
    def _extract(row):
        text = str(row.get('description') or '')
        matches = _BR_NUMBER_RE.findall(text)
        if not matches:
            return row['amount']
        token = matches[-1]
        cleaned = token.replace('.', '').replace(',', '.').replace('-', '')
        try:
            value = float(cleaned)
        except ValueError:
            return row['amount']
        raw_amount = row.get('amount', 0.0)
        if raw_amount > 0:
            sign = -1
        elif raw_amount < 0:
            sign = 1
        else:
            sign = -1
        if token.strip().startswith('-'):
            sign = 1
        return value * sign

    df_out['amount'] = df_out.apply(_extract, axis=1)
    return df_out


def _apply_drop_rules(df_out: pd.DataFrame, keywords: list[str]) -> pd.DataFrame:
    if not keywords:
        return df_out
    norm_keywords = {_normalize_text(k) for k in keywords}
    if "description_norm" in df_out.columns:
        desc_series = df_out["description_norm"].apply(_normalize_text)
    else:
        desc_series = df_out["description"].fillna("").astype(str).apply(_normalize_text)
    mask = desc_series.apply(lambda txt: any(keyword in txt for keyword in norm_keywords))
    return df_out[~mask]


def _apply_reference_date(df_out: pd.DataFrame, reference_year: int, reference_month: int) -> pd.DataFrame:
    last_day = calendar.monthrange(reference_year, reference_month)[1]

    def _adjust_date(dt_value):
        if isinstance(dt_value, date):
            day = min(getattr(dt_value, "day", 1) or 1, last_day)
        else:
            try:
                parsed = pd.to_datetime(dt_value, errors="coerce")
                if pd.isna(parsed):
                    day = 1
                else:
                    day = min(parsed.day, last_day)
            except Exception:
                day = 1
        return date(reference_year, reference_month, day)

    df_out["date"] = df_out["date"].apply(_adjust_date)
    return df_out


def apply_origin_rules(
    df: pd.DataFrame,
    origin: Optional[str],
    reference_year: Optional[int] = None,
    reference_month: Optional[int] = None,
) -> pd.DataFrame:
    if df is None or df.empty or not origin:
        return df

    rule = ORIGIN_RULES.get(origin.strip().lower())
    if not rule:
        return df

    df_out = df.copy()

    if rule.get("amount_from_description"):
        df_out = _apply_amount_from_description(df_out)
    elif rule.get("flip_positive_to_negative"):
        df_out = _apply_amount_rules(df_out)

    df_out = _apply_drop_rules(df_out, rule.get("drop_descriptions") or [])

    if reference_year and reference_month:
        df_out = _apply_reference_date(df_out, reference_year, reference_month)

    return df_out


def apply_category_hints_to_df(
    df: pd.DataFrame, origin: Optional[str] = None, account_id: Optional[str] = None
) -> pd.DataFrame:
    hints = (
        db.session.query(CategoryHint)
        .filter(CategoryHint.active.is_(True))
        .all()
    )
    if not hints or df.empty:
        if "category_id" not in df.columns:
            df = df.copy()
            df["category_id"] = None
        return df

    df_out = df.copy()
    if "description_norm" not in df_out.columns:
        df_out["description_norm"] = df_out["description"].fillna("").astype(str)
    if "category_id" not in df_out.columns:
        df_out["category_id"] = None

    def _normalize(value: Optional[str]) -> str:
        return (value or "").strip().lower()

    origin_norm = _normalize(origin)
    account_norm = _normalize(account_id)

    for hint in hints:
        if hint.match_type != "contains":
            continue
        keyword = _normalize(hint.keyword)
        if not keyword:
            continue

        if hint.origin:
            if origin_norm != _normalize(hint.origin):
                continue
        if hint.account_id:
            if account_norm != _normalize(hint.account_id):
                continue

        mask = df_out["description_norm"].str.contains(keyword, case=False, na=False)
        if not mask.any():
            continue
        df_out.loc[mask & df_out["category_id"].isna(), "category_id"] = hint.category_id

    return df_out


def register_invoice_with_hashes(
    origin: str,
    reference_year: int,
    reference_month: int,
    note: Optional[str],
    hash_list: Iterable[str],
) -> Optional[Invoice]:
    hashes = list({h for h in hash_list if h})
    if not hashes:
        return None

    session = db.session

    invoice = (
        session.query(Invoice)
        .filter(
            Invoice.origin == origin,
            Invoice.reference_year == reference_year,
            Invoice.reference_month == reference_month,
            Invoice.notes == note,
        )
        .first()
    )
    if not invoice:
        invoice = Invoice(
            origin=origin,
            reference_year=reference_year,
            reference_month=reference_month,
            notes=note,
        )
        session.add(invoice)
        session.flush()

    transactions = (
        session.query(Transaction)
        .filter(Transaction.hash_uni.in_(hashes))
        .all()
    )
    if not transactions:
        return invoice

    existing_tx_ids = {tx.id for tx in invoice.transactions}
    for tx in transactions:
        if tx.id in existing_tx_ids:
            continue
        invoice.transactions.append(tx)

    session.commit()
    return invoice


def list_invoices() -> pd.DataFrame:
    session = db.session
    rows = (
        session.query(
            Invoice.id.label("invoice_id"),
            Invoice.origin,
            Invoice.reference_year,
            Invoice.reference_month,
            Invoice.created_at,
            func.count(InvoiceTransaction.transaction_id).label("qtd"),
            func.sum(Transaction.amount).label("valor"),
        )
        .outerjoin(InvoiceTransaction, InvoiceTransaction.invoice_id == Invoice.id)
        .outerjoin(Transaction, Transaction.id == InvoiceTransaction.transaction_id)
        .group_by(Invoice.id)
        .order_by(Invoice.created_at.desc())
        .all()
    )
    data = []
    for row in rows:
        total = abs(row.valor or 0.0)
        data.append(
            {
                "InvoiceID": row.invoice_id,
                "Origem": row.origin,
                "Mes": month_number_to_label(row.reference_month),
                "Ano": row.reference_year,
                "Lancamentos": row.qtd or 0,
                "ValorTotal": total,
                "CriadoEm": row.created_at,
            }
        )
    return pd.DataFrame(data)


def get_invoice_transactions(invoice_id: int) -> pd.DataFrame:
    session = db.session
    rows = (
        session.query(
            Transaction.id,
            Transaction.date,
            Transaction.description,
            Transaction.amount,
            Transaction.account_id,
            Transaction.subaccount,
            Transaction.category_id,
            Category.name.label("category_name"),
        )
        .join(
            InvoiceTransaction,
            InvoiceTransaction.transaction_id == Transaction.id,
        )
        .join(Invoice, Invoice.id == InvoiceTransaction.invoice_id)
        .outerjoin(Category, Category.id == Transaction.category_id)
        .filter(Invoice.id == invoice_id)
        .order_by(Transaction.date.desc(), Transaction.id.desc())
        .all()
    )
    data = []
    for row in rows:
        data.append(
            {
                "ID": row.id,
                "Data": row.date,
                "Descricao": row.description or "",
                "Valor": abs(row.amount or 0.0),
                "Conta": row.account_id or "",
                "Subconta": row.subaccount or "",
                "CategoriaID": row.category_id,
                "Categoria": row.category_name or "Sem categoria",
            }
        )
    return pd.DataFrame(data)


def update_invoice_transactions(invoice_id: int, updates: list[dict]) -> int:
    if not updates:
        return 0

    session = db.session
    updated = 0
    for upd in updates:
        tx_id = upd.get("ID")
        if not tx_id:
            continue
        tx = session.get(Transaction, tx_id)
        if not tx:
            continue
        if invoice_id not in {inv.id for inv in tx.invoices}:
            continue

        changed = False
        if "Data" in upd and upd["Data"] is not None:
            new_date = upd["Data"]
            if isinstance(new_date, str):
                try:
                    new_date = datetime.fromisoformat(new_date).date()
                except ValueError:
                    new_date = tx.date
            if new_date != tx.date:
                tx.date = new_date
                changed = True

        if "Descricao" in upd and upd["Descricao"] is not None:
            new_desc = str(upd["Descricao"])
            if new_desc != (tx.description or ""):
                tx.description = new_desc
                changed = True

        if "Valor" in upd and upd["Valor"] is not None:
            try:
                new_val = float(upd["Valor"])
            except (TypeError, ValueError):
                new_val = abs(tx.amount or 0.0)
            if abs(tx.amount or 0.0) != new_val:
                tx.amount = -abs(new_val)
                changed = True

        if "Conta" in upd:
            new_acc = (upd["Conta"] or "").strip()
            if new_acc != (tx.account_id or ""):
                tx.account_id = new_acc or None
                changed = True

        if "Subconta" in upd:
            new_sub = (upd["Subconta"] or "").strip()
            if new_sub != (tx.subaccount or ""):
                tx.subaccount = new_sub or None
                changed = True

        if "CategoriaID" in upd:
            cat_id = upd["CategoriaID"]
            if cat_id == "":
                cat_id = None
            try:
                cat_id = int(cat_id) if cat_id is not None else None
            except (TypeError, ValueError):
                cat_id = None
            if cat_id != tx.category_id:
                tx.category_id = cat_id
                changed = True

        if changed:
            updated += 1

    if updated:
        session.commit()
    return updated


def list_category_hints() -> pd.DataFrame:
    hints = (
        db.session.query(CategoryHint, Category)
        .join(Category, Category.id == CategoryHint.category_id)
        .order_by(CategoryHint.created_at.desc())
        .all()
    )
    rows = []
    for hint, cat in hints:
        rows.append(
            {
                "ID": hint.id,
                "Palavra": hint.keyword,
                "Categoria": cat.name,
                "CategoriaID": hint.category_id,
                "Origem": hint.origin or "",
                "Conta": hint.account_id or "",
                "Ativo": bool(hint.active),
            }
        )
    return pd.DataFrame(rows)


def create_category_hint(
    keyword: str,
    category_id: int,
    origin: Optional[str] = None,
    account_id: Optional[str] = None,
    match_type: str = "contains",
) -> CategoryHint:
    hint = CategoryHint(
        keyword=keyword.strip(),
        category_id=category_id,
        origin=(origin or "").strip() or None,
        account_id=(account_id or "").strip() or None,
        match_type=match_type,
        active=True,
    )
    db.session.add(hint)
    db.session.commit()
    return hint


def toggle_category_hint(hint_id: int, active: bool) -> None:
    hint = db.session.get(CategoryHint, hint_id)
    if not hint:
        return
    hint.active = active
    db.session.commit()


def delete_category_hint(hint_id: int) -> None:
    hint = db.session.get(CategoryHint, hint_id)
    if not hint:
        return
    db.session.delete(hint)
    db.session.commit()
