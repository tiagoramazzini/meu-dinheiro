import sys, os
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from engine.storage import db, Transaction

q = db.session.query(Transaction.account_id).distinct().all()
if not q:
    print('No accounts found')
else:
    print('Distinct account_id values:')
    for (a,) in q:
        print('-', a)
