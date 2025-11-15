import traceback, sys

try:
    import engine.storage as s
    print('Imported engine.storage OK')
    print('DB path:', s.DB_PATH)
    print('Engine URL:', s.engine.url)
    from engine.storage import db, Transaction
    print('Got db and Transaction classes')
except Exception:
    traceback.print_exc()
    sys.exit(1)
