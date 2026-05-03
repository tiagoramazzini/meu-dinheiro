from sqlalchemy import create_engine, Column, Integer, String, Float, Date, Text, text
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from datetime import date
from typing import Optional

import os
DB_PATH = os.path.join(os.getenv("FIN_DATA_DIR", "./data"), "portfolio.db")
DATABASE_URL = f"sqlite:///{DB_PATH}"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


class InvestmentSale(Base):
    __tablename__ = "investment_sales"

    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String, nullable=False)
    name = Column(String, nullable=False)
    asset_type = Column(String, nullable=False)
    quantity_sold = Column(Float, nullable=False)
    sale_price = Column(Float, nullable=False)
    sale_date = Column(Date, nullable=False)
    broker = Column(String, nullable=True)
    notes = Column(Text, nullable=True)
    trade_type = Column(String, nullable=True)   # "swing" | "day" (ações BR only)
    avg_cost_at_sale = Column(Float, nullable=False)
    total_cost = Column(Float, nullable=False)
    total_revenue = Column(Float, nullable=False)
    gross_profit = Column(Float, nullable=False)
    ir_rate = Column(Float, nullable=False)       # e.g. 0.15 = 15%
    ir_due = Column(Float, nullable=False)        # max(0, gross_profit * ir_rate)


class InvestmentLot(Base):
    __tablename__ = "investment_lots"

    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String, nullable=False)
    name = Column(String, nullable=False)
    asset_type = Column(String, nullable=False)
    quantity = Column(Float, nullable=False)
    avg_price = Column(Float, nullable=False)
    purchase_date = Column(Date, nullable=False)
    broker = Column(String, nullable=True)
    notes = Column(Text, nullable=True)


class Dividend(Base):
    __tablename__ = "dividends"

    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String, nullable=False)
    payment_date = Column(Date, nullable=False)
    amount_per_unit = Column(Float, nullable=False)
    total_amount = Column(Float, nullable=False)


class PortfolioSnapshot(Base):
    __tablename__ = "portfolio_snapshots"

    id = Column(Integer, primary_key=True, index=True)
    snapshot_date = Column(Date, nullable=False, unique=True)
    total_value = Column(Float, nullable=False)
    total_invested = Column(Float, nullable=False)


def _migrate_legacy():
    """Migrate from old investments table and fix dividends schema."""
    try:
        with engine.connect() as conn:
            # 1. Migrate investments → investment_lots
            investments_exists = conn.execute(text(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='investments'"
            )).fetchone()

            if investments_exists:
                lots_count = conn.execute(text("SELECT COUNT(*) FROM investment_lots")).scalar()
                if lots_count == 0:
                    old_rows = conn.execute(text(
                        "SELECT ticker, name, asset_type, quantity, avg_price, purchase_date, broker, notes FROM investments"
                    )).fetchall()
                    for row in old_rows:
                        conn.execute(text(
                            "INSERT INTO investment_lots (ticker, name, asset_type, quantity, avg_price, purchase_date, broker, notes) "
                            "VALUES (:t, :n, :at, :q, :ap, :pd, :b, :no)"
                        ), {"t": row[0], "n": row[1], "at": row[2], "q": row[3],
                            "ap": row[4], "pd": row[5], "b": row[6], "no": row[7]})
                conn.execute(text("DROP TABLE IF EXISTS investments"))
                conn.commit()

            # 2. Fix dividends schema: remove investment_id if present
            divs_exists = conn.execute(text(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='dividends'"
            )).fetchone()

            if divs_exists:
                cols = conn.execute(text("PRAGMA table_info(dividends)")).fetchall()
                col_names = [c[1] for c in cols]
                if "investment_id" in col_names:
                    div_count = conn.execute(text("SELECT COUNT(*) FROM dividends")).scalar()
                    conn.execute(text("ALTER TABLE dividends RENAME TO dividends_legacy"))
                    conn.execute(text("""
                        CREATE TABLE dividends (
                            id INTEGER PRIMARY KEY,
                            ticker VARCHAR NOT NULL,
                            payment_date DATE NOT NULL,
                            amount_per_unit FLOAT NOT NULL,
                            total_amount FLOAT NOT NULL
                        )
                    """))
                    if div_count > 0:
                        conn.execute(text(
                            "INSERT INTO dividends (ticker, payment_date, amount_per_unit, total_amount) "
                            "SELECT ticker, payment_date, amount_per_unit, total_amount FROM dividends_legacy"
                        ))
                    conn.execute(text("DROP TABLE dividends_legacy"))
                    conn.commit()
    except Exception:
        pass


def init_db():
    Base.metadata.create_all(bind=engine)
    _migrate_legacy()


def get_db() -> Session:
    return SessionLocal()


# ─── Investment Lots CRUD ──────────────────────────────────────────────────────

def add_lot(
    ticker: str,
    name: str,
    asset_type: str,
    quantity: float,
    avg_price: float,
    purchase_date: date,
    broker: Optional[str] = None,
    notes: Optional[str] = None,
) -> InvestmentLot:
    db = get_db()
    try:
        lot = InvestmentLot(
            ticker=ticker.upper(),
            name=name,
            asset_type=asset_type,
            quantity=quantity,
            avg_price=avg_price,
            purchase_date=purchase_date,
            broker=broker,
            notes=notes,
        )
        db.add(lot)
        db.commit()
        db.refresh(lot)
        return lot
    finally:
        db.close()


def update_lot(
    lot_id: int,
    ticker: str,
    name: str,
    asset_type: str,
    quantity: float,
    avg_price: float,
    purchase_date: date,
    broker: Optional[str] = None,
    notes: Optional[str] = None,
) -> Optional[InvestmentLot]:
    db = get_db()
    try:
        lot = db.query(InvestmentLot).filter(InvestmentLot.id == lot_id).first()
        if not lot:
            return None
        lot.ticker = ticker.upper()
        lot.name = name
        lot.asset_type = asset_type
        lot.quantity = quantity
        lot.avg_price = avg_price
        lot.purchase_date = purchase_date
        lot.broker = broker
        lot.notes = notes
        db.commit()
        db.refresh(lot)
        return lot
    finally:
        db.close()


def delete_lot(lot_id: int) -> bool:
    db = get_db()
    try:
        lot = db.query(InvestmentLot).filter(InvestmentLot.id == lot_id).first()
        if not lot:
            return False
        db.delete(lot)
        db.commit()
        return True
    finally:
        db.close()


def get_all_lots() -> list[InvestmentLot]:
    db = get_db()
    try:
        return (
            db.query(InvestmentLot)
            .order_by(InvestmentLot.ticker, InvestmentLot.purchase_date)
            .all()
        )
    finally:
        db.close()


def get_lot_by_id(lot_id: int) -> Optional[InvestmentLot]:
    db = get_db()
    try:
        return db.query(InvestmentLot).filter(InvestmentLot.id == lot_id).first()
    finally:
        db.close()


def get_positions() -> list[dict]:
    """Return positions grouped by ticker with weighted avg price."""
    db = get_db()
    try:
        lots = (
            db.query(InvestmentLot)
            .order_by(InvestmentLot.ticker, InvestmentLot.purchase_date)
            .all()
        )
        positions: dict[str, dict] = {}
        for lot in lots:
            k = lot.ticker
            if k not in positions:
                positions[k] = {
                    "ticker": lot.ticker,
                    "name": lot.name,
                    "asset_type": lot.asset_type,
                    "total_quantity": 0.0,
                    "total_invested": 0.0,
                    "purchase_dates": [],
                    "oldest_purchase_date": lot.purchase_date,
                    "num_lots": 0,
                }
            positions[k]["total_quantity"] += lot.quantity
            positions[k]["total_invested"] += lot.quantity * lot.avg_price
            positions[k]["purchase_dates"].append(lot.purchase_date)
            positions[k]["num_lots"] += 1
            if lot.purchase_date < positions[k]["oldest_purchase_date"]:
                positions[k]["oldest_purchase_date"] = lot.purchase_date

        result = []
        for pos in positions.values():
            pos["weighted_avg_price"] = (
                pos["total_invested"] / pos["total_quantity"]
                if pos["total_quantity"] else 0.0
            )
            result.append(pos)
        return result
    finally:
        db.close()


def bulk_add_lots(lots_data: list[dict]) -> tuple[int, int]:
    """Add multiple lots at once. Returns (success_count, error_count)."""
    success = 0
    errors = 0
    for data in lots_data:
        try:
            add_lot(**data)
            success += 1
        except Exception:
            errors += 1
    return success, errors


# ─── Dividends ─────────────────────────────────────────────────────────────────

def upsert_dividend(
    ticker: str,
    payment_date: date,
    amount_per_unit: float,
    total_amount: float,
) -> bool:
    db = get_db()
    try:
        existing = (
            db.query(Dividend)
            .filter(Dividend.ticker == ticker, Dividend.payment_date == payment_date)
            .first()
        )
        if existing:
            return False
        div = Dividend(
            ticker=ticker,
            payment_date=payment_date,
            amount_per_unit=amount_per_unit,
            total_amount=total_amount,
        )
        db.add(div)
        db.commit()
        return True
    finally:
        db.close()


def get_all_dividends() -> list[Dividend]:
    db = get_db()
    try:
        return db.query(Dividend).order_by(Dividend.payment_date.desc()).all()
    finally:
        db.close()


# ─── Snapshots ─────────────────────────────────────────────────────────────────

def save_snapshot(snapshot_date: date, total_value: float, total_invested: float) -> bool:
    db = get_db()
    try:
        existing = (
            db.query(PortfolioSnapshot)
            .filter(PortfolioSnapshot.snapshot_date == snapshot_date)
            .first()
        )
        if existing:
            return False
        snap = PortfolioSnapshot(
            snapshot_date=snapshot_date,
            total_value=total_value,
            total_invested=total_invested,
        )
        db.add(snap)
        db.commit()
        return True
    finally:
        db.close()


def get_all_snapshots() -> list[PortfolioSnapshot]:
    db = get_db()
    try:
        return (
            db.query(PortfolioSnapshot)
            .order_by(PortfolioSnapshot.snapshot_date.asc())
            .all()
        )
    finally:
        db.close()


# ─── Sales ─────────────────────────────────────────────────────────────────────

def get_lots_for_ticker_fifo(ticker: str) -> list[InvestmentLot]:
    """Return lots for a ticker ordered oldest-first (FIFO)."""
    db = get_db()
    try:
        return (
            db.query(InvestmentLot)
            .filter(InvestmentLot.ticker == ticker.upper())
            .order_by(InvestmentLot.purchase_date, InvestmentLot.id)
            .all()
        )
    finally:
        db.close()


def get_weighted_avg_cost(ticker: str) -> tuple[float, float]:
    """Return (weighted_avg_price, total_quantity) for a ticker across all lots."""
    lots = get_lots_for_ticker_fifo(ticker)
    total_qty = sum(l.quantity for l in lots)
    total_cost = sum(l.quantity * l.avg_price for l in lots)
    avg = total_cost / total_qty if total_qty > 0 else 0.0
    return avg, total_qty


def deduct_lots_fifo(ticker: str, quantity_to_sell: float) -> float:
    """
    Deduct quantity_to_sell from lots using FIFO.
    Returns the weighted avg cost at time of sale.
    Raises ValueError if not enough quantity in portfolio.
    """
    db = get_db()
    try:
        lots = (
            db.query(InvestmentLot)
            .filter(InvestmentLot.ticker == ticker.upper())
            .order_by(InvestmentLot.purchase_date, InvestmentLot.id)
            .all()
        )
        total_available = sum(l.quantity for l in lots)
        if quantity_to_sell > total_available + 1e-8:
            raise ValueError(
                f"Quantidade insuficiente em carteira: disponível {total_available:.6f}, "
                f"tentativa de vender {quantity_to_sell:.6f}"
            )

        # Weighted avg cost for the sold portion (using overall avg before deduction)
        total_cost_all = sum(l.quantity * l.avg_price for l in lots)
        avg_cost = total_cost_all / total_available if total_available > 0 else 0.0

        # FIFO deduction
        remaining = quantity_to_sell
        for lot in lots:
            if remaining <= 1e-8:
                break
            if lot.quantity <= remaining:
                remaining -= lot.quantity
                db.delete(lot)
            else:
                lot.quantity -= remaining
                remaining = 0.0

        db.commit()
        return avg_cost
    except ValueError:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise ValueError(f"Erro ao deduzir lotes: {e}")
    finally:
        db.close()


def create_sale(
    ticker: str,
    name: str,
    asset_type: str,
    quantity_sold: float,
    sale_price: float,
    sale_date: date,
    avg_cost_at_sale: float,
    ir_rate: float,
    ir_due: float,
    broker: Optional[str] = None,
    notes: Optional[str] = None,
    trade_type: Optional[str] = None,
) -> InvestmentSale:
    total_cost = quantity_sold * avg_cost_at_sale
    total_revenue = quantity_sold * sale_price
    gross_profit = total_revenue - total_cost

    db = get_db()
    try:
        sale = InvestmentSale(
            ticker=ticker.upper(),
            name=name,
            asset_type=asset_type,
            quantity_sold=quantity_sold,
            sale_price=sale_price,
            sale_date=sale_date,
            broker=broker,
            notes=notes,
            trade_type=trade_type,
            avg_cost_at_sale=avg_cost_at_sale,
            total_cost=total_cost,
            total_revenue=total_revenue,
            gross_profit=gross_profit,
            ir_rate=ir_rate,
            ir_due=ir_due,
        )
        db.add(sale)
        db.commit()
        db.refresh(sale)
        return sale
    finally:
        db.close()


def get_all_sales() -> list[InvestmentSale]:
    db = get_db()
    try:
        return (
            db.query(InvestmentSale)
            .order_by(InvestmentSale.sale_date.desc(), InvestmentSale.id.desc())
            .all()
        )
    finally:
        db.close()


def get_sales_by_month(year: int, month: int) -> list[InvestmentSale]:
    db = get_db()
    try:
        return (
            db.query(InvestmentSale)
            .filter(
                InvestmentSale.sale_date >= date(year, month, 1),
                InvestmentSale.sale_date <= date(year, month, 28 if month == 2 else (30 if month in (4,6,9,11) else 31)),
            )
            .order_by(InvestmentSale.sale_date)
            .all()
        )
    finally:
        db.close()


def get_monthly_acao_br_revenue(year: int, month: int) -> float:
    """Sum of total_revenue for Ação BR sales in the given month (for exemption check)."""
    db = get_db()
    try:
        from sqlalchemy import extract
        sales = (
            db.query(InvestmentSale)
            .filter(
                InvestmentSale.asset_type == "Ação BR",
                InvestmentSale.sale_date >= date(year, month, 1),
                InvestmentSale.sale_date <= date(year, month, 28 if month == 2 else (30 if month in (4,6,9,11) else 31)),
            )
            .all()
        )
        return sum(s.total_revenue for s in sales)
    finally:
        db.close()


# ─── Portfolio Targets ─────────────────────────────────────────────────────────

class PortfolioTarget(Base):
    __tablename__ = "portfolio_targets"

    id = Column(Integer, primary_key=True, index=True)
    asset_type = Column(String, nullable=False, unique=True)
    target_pct = Column(Float, nullable=False, default=0.0)
    updated_at = Column(Date, nullable=False)


def get_targets() -> list[dict]:
    db = get_db()
    try:
        rows = (
            db.query(PortfolioTarget)
            .order_by(PortfolioTarget.asset_type)
            .all()
        )
        return [
            {
                "id": r.id,
                "asset_type": r.asset_type,
                "target_pct": r.target_pct,
                "updated_at": r.updated_at,
            }
            for r in rows
        ]
    finally:
        db.close()


def save_targets(targets: list[dict]) -> None:
    """Upsert targets. targets is list of {asset_type, target_pct}."""
    db = get_db()
    try:
        for t in targets:
            existing = (
                db.query(PortfolioTarget)
                .filter(PortfolioTarget.asset_type == t["asset_type"])
                .first()
            )
            if existing:
                existing.target_pct = t["target_pct"]
                existing.updated_at = date.today()
            else:
                db.add(
                    PortfolioTarget(
                        asset_type=t["asset_type"],
                        target_pct=t["target_pct"],
                        updated_at=date.today(),
                    )
                )
        db.commit()
    finally:
        db.close()


def get_monthly_crypto_profit(year: int, month: int) -> float:
    """Sum of gross_profit for Crypto sales in the given month (for R$35k threshold)."""
    db = get_db()
    try:
        sales = (
            db.query(InvestmentSale)
            .filter(
                InvestmentSale.asset_type == "Crypto",
                InvestmentSale.sale_date >= date(year, month, 1),
                InvestmentSale.sale_date <= date(year, month, 28 if month == 2 else (30 if month in (4,6,9,11) else 31)),
            )
            .all()
        )
        return sum(s.gross_profit for s in sales)
    finally:
        db.close()
