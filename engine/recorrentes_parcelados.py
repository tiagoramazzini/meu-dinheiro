from datetime import timedelta
from .storage import db, Transaction, PlannedTransaction, InstallmentGroup

def next_month(d, n=1):
    y, m = d.year, d.month
    m += n
    while m > 12:
        m -= 12
        y += 1
    from datetime import date
    return date(y, m, d.day if d.day <= 28 else 28)

def generate_future_installments(group: InstallmentGroup):
    existing = {p.installment_n for p in db.session.query(PlannedTransaction).filter_by(installment_group_id=group.id).all()}
    for n in range(2, group.total_parcels + 1):
        if n in existing:
            continue
        due = next_month(group.first_date, n-1)
        p = PlannedTransaction(
            due_date=due,
            description=f"{group.description_root} Parc {n}/{group.total_parcels}",
            description_norm=group.description_root,
            amount=-group.amount_per_parcel,
            account_id=group.account_id,
            installment_group_id=group.id,
            installment_n=n,
            installment_total=group.total_parcels,
            status='open')
        db.session.add(p)
    db.session.commit()

def reconcile_imported_with_planned(new_tx: Transaction):
    if not new_tx.installment_group_id or not new_tx.installment_n:
        return
    q = db.session.query(PlannedTransaction).filter_by(
        installment_group_id=new_tx.installment_group_id,
        installment_n=new_tx.installment_n,
        status='open')
    p = q.first()
    if p and abs(p.amount - new_tx.amount) < 0.01:
        p.status = 'matched'
        new_tx.status = 'confirmed'
        db.session.commit()
