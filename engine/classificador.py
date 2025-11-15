# engine/classificador.py
# -*- coding: utf-8 -*-
from typing import Any, Dict, Iterable
import pandas as pd
from sqlalchemy import select, update
from engine.storage import db, Transaction, SmartCategoryRule

def apply_category_bulk(rows: Any) -> int:
    if rows is None:
        return 0
    if isinstance(rows, pd.DataFrame):
        df = rows.copy()
    elif isinstance(rows, Iterable):
        df = pd.DataFrame(list(rows))
    else:
        return 0
    if df.empty:
        return 0
    cols = {c.lower(): c for c in df.columns}
    id_col = cols.get("id")
    cat_col = cols.get("categoria") or cols.get("category")
    if not id_col or not cat_col:
        return 0
    df = df[[id_col, cat_col]].rename(columns={id_col: "id", cat_col: "category"})
    df = df.dropna(subset=["id"]).copy()
    df["id"] = df["id"].astype(int)

    updated = 0
    for _, r in df.iterrows():
        db.session.execute(
            update(Transaction)
            .where(Transaction.id == int(r["id"]))
            .values(category=(None if pd.isna(r["category"]) else str(r["category"]).strip()))
        )
        updated += 1
    db.commit()
    return updated

def classify_batch(*args, **kwargs) -> Dict[str, int]:
    ids = [x[0] for x in db.session.execute(select(Transaction.id).where(Transaction.category.is_(None))).all()]
    if ids:
        db.session.execute(update(Transaction).where(Transaction.id.in_(ids)).values(category="Não classificado"))
        db.commit()
    return {"classified": len(ids), "skipped": 0, "rules_used": 0}

def apply_smart_rules_to_statement(statement_id: int) -> dict:
    rules = db.session.query(SmartCategoryRule).filter(SmartCategoryRule.active == 1).all()
    if not rules:
        return {"updated": 0, "matched": 0, "rules": 0}
    total_updated, total_matched = 0, 0
    for rule in rules:
        ids = [
            x[0] for x in db.session.query(Transaction.id)
            .filter(
                Transaction.statement_id == statement_id,
                Transaction.category.is_(None),
                Transaction.description.ilike(f"%{rule.keyword}%")
            ).all()
        ]
        total_matched += len(ids)
        if ids:
            db.session.execute(
                update(Transaction).where(Transaction.id.in_(ids)).values(category=rule.category)
            )
            total_updated += len(ids)
    db.commit()
    return {"updated": total_updated, "matched": total_matched, "rules": len(rules)}
