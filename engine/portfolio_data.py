import os
import json
import calendar
import requests
import yfinance as yf
import pandas as pd
import streamlit as st
from datetime import date, datetime, timedelta
from typing import Optional
import sys
sys.path.insert(0, "engine")
import portfolio_storage as storage

BRAPI_TOKEN = os.environ.get("BRAPI_TOKEN", "")
BRAPI_BASE = "https://brapi.dev/api"
BCB_CDI_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados"
BCB_CDI_LAST = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados/ultimos/1?formato=json"

BR_TYPES = {"Ação BR", "FII", "ETF BR"}

PALETTE = [
    "#00B4D8", "#0077B6", "#90E0EF", "#48CAE4", "#ADE8F4",
    "#023E8A", "#F77F00", "#FCBF49", "#EAE2B7", "#D62828",
]


# ─── Current Price ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_current_price(ticker: str, asset_type: str) -> Optional[float]:
    try:
        if asset_type in BR_TYPES:
            return _brapi_price(ticker)
        elif asset_type == "ETF Internacional":
            info = yf.Ticker(ticker).fast_info
            return float(info["lastPrice"])
        elif asset_type == "Crypto":
            info = yf.Ticker(f"{ticker}-USD").fast_info
            return float(info["lastPrice"])
        elif asset_type == "Renda Fixa":
            return None
        return None
    except Exception:
        return None


def _brapi_price(ticker: str) -> Optional[float]:
    url = f"{BRAPI_BASE}/quote/{ticker}"
    resp = requests.get(url, params={"token": BRAPI_TOKEN}, timeout=10)
    resp.raise_for_status()
    results = resp.json().get("results", [])
    if results:
        return results[0].get("regularMarketPrice")
    return None


# ─── CDI ───────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def get_cdi_accumulated(from_date: date, to_date: date) -> float:
    effective_to = min(to_date, date.today() - timedelta(days=1))
    if from_date >= effective_to:
        return 0.0
    fmt = "%d/%m/%Y"
    params = {
        "formato": "json",
        "dataInicial": from_date.strftime(fmt),
        "dataFinal": effective_to.strftime(fmt),
    }
    try:
        resp = requests.get(BCB_CDI_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            return 0.0
        accumulated = 1.0
        for entry in data:
            daily_rate = float(entry["valor"]) / 100
            accumulated *= (1 + daily_rate)
        return accumulated - 1.0
    except Exception:
        return 0.0


@st.cache_data(ttl=3600)
def get_cdi_daily_rate() -> float:
    try:
        resp = requests.get(BCB_CDI_LAST, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return float(data[0]["valor"]) / 100 if data else 0.0
    except Exception:
        return 0.0


def get_cdi_monthly_rate() -> float:
    daily = get_cdi_daily_rate()
    return (1 + daily) ** 21 - 1


# ─── Renda Fixa ────────────────────────────────────────────────────────────────

def get_renda_fixa_current_value(
    avg_price: float,
    purchase_date: date,
    pct_cdi: float = 100.0,
) -> float:
    today = date.today()
    if purchase_date >= today:
        return avg_price
    cdi_acc = get_cdi_accumulated(purchase_date, today)
    return avg_price * (1 + cdi_acc * pct_cdi / 100)


# ─── Dividend Sync ─────────────────────────────────────────────────────────────

def sync_dividends(ticker: str, asset_type: str, total_quantity: float):
    if asset_type not in BR_TYPES:
        return
    try:
        url = f"{BRAPI_BASE}/quote/{ticker}"
        resp = requests.get(
            url,
            params={"modules": "dividendsData", "token": BRAPI_TOKEN},
            timeout=10,
        )
        resp.raise_for_status()
        results = resp.json().get("results", [])
        if not results:
            return
        cash_dividends = results[0].get("dividendsData", {}).get("cashDividends", [])
        for div in cash_dividends:
            date_str = div.get("paymentDate") or div.get("approvedOn")
            if not date_str:
                continue
            try:
                payment_date = datetime.fromisoformat(date_str[:10]).date()
            except Exception:
                continue
            rate = float(div.get("rate", 0) or 0)
            if rate <= 0:
                continue
            storage.upsert_dividend(
                ticker=ticker,
                payment_date=payment_date,
                amount_per_unit=rate,
                total_amount=rate * total_quantity,
            )
    except Exception:
        pass


# ─── Upcoming Dividends (Agenda) ───────────────────────────────────────────────

@st.cache_data(ttl=3600)
def get_upcoming_dividends(positions_json: str) -> list[dict]:
    """Fetch upcoming dividends from brapi for all BR tickers in the portfolio."""
    positions = json.loads(positions_json)
    today = date.today()
    cutoff_past = today - timedelta(days=30)
    cutoff_future = today + timedelta(days=180)
    upcoming: list[dict] = []

    for pos in positions:
        if pos["asset_type"] not in list(BR_TYPES):
            continue
        ticker = pos["ticker"]
        try:
            url = f"{BRAPI_BASE}/quote/{ticker}"
            resp = requests.get(
                url,
                params={"modules": "dividendsData", "token": BRAPI_TOKEN},
                timeout=10,
            )
            resp.raise_for_status()
            results = resp.json().get("results", [])
            if not results:
                continue
            dividends_data = (results[0].get("dividendsData") or {})
            cash_dividends = dividends_data.get("cashDividends") or []

            for div in cash_dividends:
                pay_str = div.get("paymentDate") or div.get("approvedOn")
                ex_str = div.get("lastDatePrior") or div.get("approvedOn")
                if not pay_str:
                    continue
                try:
                    pay_date = datetime.fromisoformat(pay_str[:10]).date()
                except Exception:
                    continue
                try:
                    ex_date = datetime.fromisoformat(ex_str[:10]).date() if ex_str else None
                except Exception:
                    ex_date = None

                if pay_date < cutoff_past or pay_date > cutoff_future:
                    continue
                rate = float(div.get("rate") or 0)
                if rate <= 0:
                    continue

                upcoming.append({
                    "ticker": ticker,
                    "name": pos["name"],
                    "ex_date": ex_date,
                    "payment_date": pay_date,
                    "amount_per_unit": rate,
                    "total_estimated": rate * pos["quantity"],
                })
        except Exception:
            continue

    upcoming.sort(key=lambda x: x["payment_date"])
    return upcoming


# ─── P&L Calculation ───────────────────────────────────────────────────────────

def calculate_pl_position(pos: dict, current_price: Optional[float]) -> dict:
    valor_investido = pos["total_invested"]
    if current_price is not None:
        valor_atual = pos["total_quantity"] * current_price
        lucro_reais = valor_atual - valor_investido
        rentabilidade_pct = (
            (valor_atual / valor_investido - 1) * 100 if valor_investido else 0.0
        )
    else:
        valor_atual = None
        lucro_reais = None
        rentabilidade_pct = None

    return {
        "ticker": pos["ticker"],
        "name": pos["name"],
        "asset_type": pos["asset_type"],
        "quantity": pos["total_quantity"],
        "avg_price": pos["weighted_avg_price"],
        "current_price": current_price,
        "valor_investido": valor_investido,
        "valor_atual": valor_atual,
        "lucro_reais": lucro_reais,
        "rentabilidade_pct": rentabilidade_pct,
        "oldest_purchase_date": pos["oldest_purchase_date"],
        "num_lots": pos["num_lots"],
    }


@st.cache_data(ttl=300)
def build_portfolio_data() -> list[dict]:
    positions = storage.get_positions()
    rows = []
    for pos in positions:
        price = get_current_price(pos["ticker"], pos["asset_type"])
        if pos["asset_type"] == "Renda Fixa" and price is None:
            price = get_renda_fixa_current_value(
                pos["weighted_avg_price"], pos["oldest_purchase_date"]
            )
        rows.append(calculate_pl_position(pos, price))
    return rows


# ─── CDI Benchmark ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def get_cdi_benchmark() -> dict:
    positions = storage.get_positions()
    if not positions:
        return {}
    oldest_date = min(p["oldest_purchase_date"] for p in positions)
    total_invested = sum(p["total_invested"] for p in positions)
    today = date.today()
    cdi_acc = get_cdi_accumulated(oldest_date, today)
    return {
        "oldest_date": oldest_date,
        "total_invested": total_invested,
        "cdi_accumulated_pct": cdi_acc * 100,
        "cdi_value": total_invested * (1 + cdi_acc),
    }


# ─── Monthly Dividends ─────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_monthly_dividends_last12() -> list[dict]:
    dividends = storage.get_all_dividends()
    cutoff = date.today() - timedelta(days=365)
    result: dict[tuple, dict] = {}
    for div in dividends:
        if div.payment_date < cutoff:
            continue
        key = (div.payment_date.year, div.payment_date.month, div.ticker)
        if key not in result:
            result[key] = {
                "year": div.payment_date.year,
                "month": div.payment_date.month,
                "ticker": div.ticker,
                "total": 0.0,
            }
        result[key]["total"] += div.total_amount
    return list(result.values())


@st.cache_data(ttl=300)
def get_current_month_dividends() -> float:
    today = date.today()
    return sum(
        d.total_amount
        for d in storage.get_all_dividends()
        if d.payment_date.year == today.year and d.payment_date.month == today.month
    )


def get_dividends_for_month(year: int, month: int) -> list:
    return [
        d for d in storage.get_all_dividends()
        if d.payment_date.year == year and d.payment_date.month == month
    ]


def get_lots_added_in_month(year: int, month: int) -> list:
    return [
        l for l in storage.get_all_lots()
        if l.purchase_date.year == year and l.purchase_date.month == month
    ]


def get_snapshots_for_month(year: int, month: int):
    snapshots = storage.get_all_snapshots()
    first_day = date(year, month, 1)
    last_day = date(year, month, calendar.monthrange(year, month)[1])
    pre_snaps = [s for s in snapshots if s.snapshot_date < first_day]
    month_snaps = [s for s in snapshots if first_day <= s.snapshot_date <= last_day]
    start_snap = pre_snaps[-1] if pre_snaps else (month_snaps[0] if month_snaps else None)
    end_snap = month_snaps[-1] if month_snaps else None
    return start_snap, end_snap


# ─── Price History ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def get_price_history(ticker: str, asset_type: str) -> Optional[pd.DataFrame]:
    try:
        if asset_type in BR_TYPES:
            url = f"{BRAPI_BASE}/quote/{ticker}"
            resp = requests.get(
                url,
                params={"range": "1y", "interval": "1mo", "token": BRAPI_TOKEN},
                timeout=10,
            )
            resp.raise_for_status()
            results = resp.json().get("results", [])
            if not results:
                return None
            historical = results[0].get("historicalDataPrice", [])
            if not historical:
                return None
            rows = []
            for entry in historical:
                ts = entry.get("date")
                close = entry.get("close")
                if ts and close:
                    rows.append({
                        "date": datetime.fromtimestamp(ts).date(),
                        "close": float(close),
                    })
            if not rows:
                return None
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

        elif asset_type in {"ETF Internacional", "Crypto"}:
            t = f"{ticker}-USD" if asset_type == "Crypto" else ticker
            hist = yf.Ticker(t).history(period="1y", interval="1mo")
            if hist.empty:
                return None
            hist = hist.reset_index()
            hist["date"] = pd.to_datetime(hist["Date"]).dt.date
            return (
                hist[["date", "Close"]]
                .rename(columns={"Close": "close"})
                .sort_values("date")
                .reset_index(drop=True)
            )
        return None
    except Exception:
        return None


@st.cache_data(ttl=3600)
def get_comparison_history(tickers_json: str) -> dict:
    """Return price history normalized to 100 for multiple tickers."""
    tickers_types = json.loads(tickers_json)
    result: dict[str, list] = {}
    for item in tickers_types:
        ticker = item["ticker"]
        asset_type = item["asset_type"]
        hist = get_price_history(ticker, asset_type)
        if hist is None or hist.empty:
            continue
        base = hist["close"].iloc[0]
        if base <= 0:
            continue
        hist = hist.copy()
        hist["normalized"] = hist["close"] / base * 100
        result[ticker] = hist[["date", "close", "normalized"]].to_dict("records")
    return result


# ─── Fundamentals ─────────────────────────────────────────────────────────────

# ─── Sales / IR ────────────────────────────────────────────────────────────────

def calculate_ir_for_sale(
    asset_type: str,
    trade_type: str,
    sale_date: date,
    sale_price: float,
    quantity_sold: float,
    gross_profit: float,
    existing_monthly_acao_br_revenue: float = 0.0,
    existing_monthly_crypto_profit: float = 0.0,
) -> tuple[float, float]:
    """
    Returns (ir_rate, ir_due).
    - existing_monthly_acao_br_revenue: total BR stock revenue already in DB for the same month
    - existing_monthly_crypto_profit: total crypto gross_profit already in DB for same month
    """
    total_revenue = quantity_sold * sale_price

    if asset_type == "Ação BR":
        if trade_type == "day":
            ir_rate = 0.20
        else:
            month_total = existing_monthly_acao_br_revenue + total_revenue
            ir_rate = 0.0 if month_total <= 20_000.0 else 0.15
    elif asset_type == "FII":
        ir_rate = 0.20
    elif asset_type == "ETF BR":
        ir_rate = 0.15
    elif asset_type == "Crypto":
        month_profit = existing_monthly_crypto_profit + gross_profit
        ir_rate = 0.15 if month_profit > 35_000.0 else 0.0
    else:
        # Renda Fixa, ETF Internacional: IR retido na fonte pela corretora
        ir_rate = 0.0

    ir_due = max(0.0, gross_profit * ir_rate)
    return ir_rate, ir_due


def compute_sale_preview(
    ticker: str,
    quantity_sold: float,
    sale_price: float,
    sale_date: date,
    asset_type: str,
    trade_type: str,
) -> dict:
    """
    Returns a preview dict without persisting anything.
    Uses current lots to determine avg cost and applies IR rules.
    """
    try:
        avg_cost, total_qty = storage.get_weighted_avg_cost(ticker)
        if total_qty < quantity_sold - 1e-8:
            return {"error": f"Quantidade insuficiente: {total_qty:.6f} disponíveis"}

        total_cost = quantity_sold * avg_cost
        total_revenue = quantity_sold * sale_price
        gross_profit = total_revenue - total_cost

        year, month = sale_date.year, sale_date.month
        existing_acao = storage.get_monthly_acao_br_revenue(year, month)
        existing_crypto = storage.get_monthly_crypto_profit(year, month)

        ir_rate, ir_due = calculate_ir_for_sale(
            asset_type, trade_type, sale_date,
            sale_price, quantity_sold, gross_profit,
            existing_acao, existing_crypto,
        )

        return {
            "avg_cost": avg_cost,
            "total_cost": total_cost,
            "total_revenue": total_revenue,
            "gross_profit": gross_profit,
            "ir_rate": ir_rate,
            "ir_due": ir_due,
            "total_qty_available": total_qty,
        }
    except Exception as e:
        return {"error": str(e)}


def get_realized_profit_ytd() -> float:
    """Sum of gross_profit for all sales in the current calendar year."""
    try:
        today = date.today()
        sales = storage.get_all_sales()
        return sum(s.gross_profit for s in sales if s.sale_date.year == today.year)
    except Exception:
        return 0.0


def get_ir_due_current_month() -> float:
    """Sum of ir_due for all sales in the current month."""
    try:
        today = date.today()
        sales = storage.get_sales_by_month(today.year, today.month)
        return sum(s.ir_due for s in sales)
    except Exception:
        return 0.0


# ─── Rebalancing ───────────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def get_targets() -> list[dict]:
    return storage.get_targets()


def save_targets_and_clear(targets: list[dict]) -> None:
    storage.save_targets(targets)
    get_targets.clear()


def compute_rebalancing(
    portfolio_rows: list[dict],
    targets: list[dict],
    aporte_mensal: float = 0.0,
    tolerance_pct: float = 2.0,
) -> dict:
    """
    Compute rebalancing plan.
    Returns dict with by_class, by_ticker, aporte_distribution, and KPIs.
    """
    if not portfolio_rows or not targets:
        return {}

    total_value = sum(
        (r["valor_atual"] if r["valor_atual"] is not None else r["valor_investido"])
        for r in portfolio_rows
    )
    if total_value <= 0:
        return {}

    by_class_val: dict[str, float] = {}
    tickers_by_class: dict[str, list[dict]] = {}
    for r in portfolio_rows:
        at = r["asset_type"]
        val = r["valor_atual"] if r["valor_atual"] is not None else r["valor_investido"]
        by_class_val[at] = by_class_val.get(at, 0.0) + (val or 0.0)
        tickers_by_class.setdefault(at, []).append(r)

    target_map = {t["asset_type"]: t["target_pct"] for t in targets}

    classes = []
    for at, target_pct in target_map.items():
        valor_atual = by_class_val.get(at, 0.0)
        pct_atual = valor_atual / total_value * 100 if total_value > 0 else 0.0
        desvio = pct_atual - target_pct
        valor_alvo = total_value * target_pct / 100
        diferenca = valor_alvo - valor_atual

        abs_dev = abs(desvio)
        if abs_dev < tolerance_pct:
            acao = "✅ OK"
        elif diferenca > 0:
            acao = "🟢 Comprar"
        else:
            acao = "🔴 Vender"

        classes.append({
            "asset_type": at,
            "valor_atual": round(valor_atual, 2),
            "pct_atual": round(pct_atual, 2),
            "pct_alvo": round(target_pct, 2),
            "desvio": round(desvio, 2),
            "valor_alvo": round(valor_alvo, 2),
            "diferenca": round(diferenca, 2),
            "acao": acao,
        })

    # Aporte distribution — fill deficits proportionally, no sell required
    aporte_distribution = []
    if aporte_mensal > 0:
        total_deficit = sum(max(0.0, c["diferenca"]) for c in classes)
        for c in classes:
            deficit = max(0.0, c["diferenca"])
            if total_deficit > 0:
                val_ap = aporte_mensal * deficit / total_deficit
            else:
                val_ap = aporte_mensal * c["pct_alvo"] / 100
            val_ap = min(val_ap, deficit) if deficit > 0 else val_ap
            aporte_distribution.append({
                "asset_type": c["asset_type"],
                "valor_aporte": round(val_ap, 2),
                "pct_do_aporte": round(val_ap / aporte_mensal * 100, 1) if aporte_mensal > 0 else 0.0,
            })

    # Per-ticker suggestions
    by_ticker_rows = []
    for c in classes:
        at = c["asset_type"]
        tickers = tickers_by_class.get(at, [])
        class_valor = c["valor_atual"]
        compra_disponivel = max(0.0, c["diferenca"])
        for r in tickers:
            val = (r["valor_atual"] if r["valor_atual"] is not None else r["valor_investido"]) or 0.0
            peso = val / class_valor if class_valor > 0 else 0.0
            sugestao = round(compra_disponivel * peso, 2) if compra_disponivel > 0 else 0.0
            by_ticker_rows.append({
                "ticker": r["ticker"],
                "name": r["name"],
                "asset_type": at,
                "valor_atual": round(val, 2),
                "pct_na_classe": round(peso * 100, 1),
                "sugestao_compra": sugestao,
            })

    desvio_max = max((abs(c["desvio"]) for c in classes), default=0.0)
    classes_fora_alvo = sum(1 for c in classes if abs(c["desvio"]) >= tolerance_pct)
    valor_total_rebalancear = sum(abs(c["diferenca"]) for c in classes if c["acao"] != "✅ OK") / 2

    return {
        "by_class": classes,
        "by_ticker": by_ticker_rows,
        "aporte_distribution": aporte_distribution,
        "total_value": total_value,
        "desvio_max": round(desvio_max, 2),
        "classes_fora_alvo": classes_fora_alvo,
        "valor_total_rebalancear": round(valor_total_rebalancear, 2),
    }


@st.cache_data(ttl=3600)
def get_fundamentals(ticker: str, asset_type: str) -> dict:
    if asset_type not in {"Ação BR", "FII"}:
        return {}
    try:
        url = f"{BRAPI_BASE}/quote/{ticker}"
        resp = requests.get(
            url,
            params={"modules": "summaryProfile,defaultKeyStatistics", "token": BRAPI_TOKEN},
            timeout=10,
        )
        resp.raise_for_status()
        results = resp.json().get("results", [])
        return results[0] if results else {}
    except Exception:
        return {}
