# engine/storage.py
import os
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, DateTime, Text, inspect, text
from sqlalchemy.orm import declarative_base, sessionmaker

FIN_DATA_DIR = os.getenv("FIN_DATA_DIR", "./data")
os.makedirs(FIN_DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(FIN_DATA_DIR, "finance.db")

Base = declarative_base()

class Transaction(Base):
    __tablename__ = "transactions"
    id            = Column(Integer, primary_key=True)
    date          = Column(Date, nullable=False)
    description   = Column(Text, nullable=False)
    amount        = Column(Float, nullable=False)
    account_id    = Column(String(100), nullable=True)
    category      = Column(String(120), nullable=True)
    statement_id  = Column(Integer, nullable=True)
    origin_label  = Column(String(100), nullable=True)
    period_yyyymm = Column(Integer, nullable=True)

class Category(Base):
    __tablename__ = "categories"
    id          = Column(Integer, primary_key=True)
    name        = Column(String(120), unique=True, nullable=False)
    created_at  = Column(DateTime, default=datetime.utcnow)
    radar       = Column(Integer, default=0)
    budget_meta = Column(Float, nullable=True)

class Statement(Base):
    __tablename__ = "statements"
    id            = Column(Integer, primary_key=True)
    source_name   = Column(String(255), nullable=True)
    account_id    = Column(String(100), nullable=True)
    imported_at   = Column(DateTime, default=datetime.utcnow)
    rows          = Column(Integer, default=0)
    origin_label  = Column(String(100), nullable=True)   # usamos a Conta aqui
    period_yyyymm = Column(Integer, nullable=True)
    period_label  = Column(String(16), nullable=True)

class SmartCategoryRule(Base):
    __tablename__ = "smart_category_rules"
    id           = Column(Integer, primary_key=True)
    keyword      = Column(String(200), nullable=False)
    category     = Column(String(120), nullable=False)
    active       = Column(Integer, default=1)
    created_at   = Column(DateTime, default=datetime.utcnow)

class RadarKeyword(Base):
    __tablename__ = "radar_keywords"
    id         = Column(Integer, primary_key=True)
    keyword    = Column(String(200), nullable=False)
    label      = Column(String(200), nullable=True)
    active     = Column(Integer, default=1)
    created_at = Column(DateTime, default=datetime.utcnow)

class _DB:
    def __init__(self, url):
        self.engine = create_engine(url, future=True)
        self.Session = sessionmaker(bind=self.engine, future=True)
        self.session = self.Session()
    def commit(self):
        self.session.commit()

_db_url = f"sqlite:///{DB_PATH}"
db = _DB(_db_url)

def _ensure_cols():
    insp = inspect(db.engine)
    with db.engine.connect() as con:
        if not insp.has_table("statements"):
            Statement.__table__.create(db.engine)
        if not insp.has_table("transactions"):
            Transaction.__table__.create(db.engine)
        else:
            cols = [c["name"] for c in insp.get_columns("transactions")]
            if "statement_id" not in cols:
                con.execute(text("ALTER TABLE transactions ADD COLUMN statement_id INTEGER"))
            if "origin_label" not in cols:
                con.execute(text("ALTER TABLE transactions ADD COLUMN origin_label VARCHAR(100)"))
            if "period_yyyymm" not in cols:
                con.execute(text("ALTER TABLE transactions ADD COLUMN period_yyyymm INTEGER"))
        if not insp.has_table("categories"):
            Category.__table__.create(db.engine)
        else:
            cols = [c["name"] for c in insp.get_columns("categories")]
            if "radar" not in cols:
                con.execute(text("ALTER TABLE categories ADD COLUMN radar INTEGER DEFAULT 0"))
            if "budget_meta" not in cols:
                con.execute(text("ALTER TABLE categories ADD COLUMN budget_meta FLOAT"))
        if not insp.has_table("smart_category_rules"):
            SmartCategoryRule.__table__.create(db.engine)
        if not insp.has_table("radar_keywords"):
            RadarKeyword.__table__.create(db.engine)
        con.execute(text("CREATE INDEX IF NOT EXISTS ix_transactions_period ON transactions (period_yyyymm)"))
        con.execute(text("CREATE INDEX IF NOT EXISTS ix_transactions_origin ON transactions (origin_label)"))
        con.execute(text("CREATE INDEX IF NOT EXISTS ix_statements_period ON statements (period_yyyymm)"))

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    Base.metadata.create_all(db.engine)
    _ensure_cols()
