import sys
import os
# ensure project root is on sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from engine.storage import db, Transaction

q = (db.session.query(Transaction)
     .order_by(Transaction.id.desc())
     .limit(50)
     .all())

if not q:
    print('No transactions found')
else:
    print(f"{'ID':>4}  {'Date':10}  {'Amount':12}  {'Subacct':8}  Description")
    print('-'*100)
    for t in q:
        print(f"{t.id:4}  {str(t.date):10}  {t.amount:12.2f}  {str(t.subaccount) if t.subaccount else '-':8}  {t.description}")
