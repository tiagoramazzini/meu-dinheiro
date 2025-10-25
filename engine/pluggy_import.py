
from __future__ import annotations
import os, time
from datetime import datetime, timedelta, timezone
from typing import Iterable, Dict
import requests
import pandas as pd

from .importador import to_standard_df, upsert_transactions

PLUGGY_BASE = "https://api.pluggy.ai"

class PluggyClient:
    def __init__(self, client_id: str | None = None, client_secret: str | None = None):
        self.client_id = client_id or os.getenv("PLUGGY_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("PLUGGY_CLIENT_SECRET")
        self._token = None
        self._token_exp = 0

    def _auth(self) -> str:
        if not self.client_id or not self.client_secret:
            raise RuntimeError("PLUGGY_CLIENT_ID/PLUGGY_CLIENT_SECRET não configurados.")
        now = time.time()
        if self._token and now < self._token_exp - 60:
            return self._token
        r = requests.post(f"{PLUGGY_BASE}/auth",
                          json={"clientId": self.client_id, "clientSecret": self.client_secret},
                          timeout=30)
        r.raise_for_status()
        data = r.json()
        token = data.get("accessToken") or data.get("apiKey") or data.get("access_token")
        if not token:
            raise RuntimeError(f"Auth ok mas sem token no corpo. Chaves disponíveis: {list(data.keys())}")
        self._token = token
        self._token_exp = now + 50*60
        return self._token

    def _headers(self):
        return {"X-API-KEY": self._auth()}

    def list_items(self) -> list[dict]:
        url = f"{PLUGGY_BASE}/items"
        items = []; page = 1
        while True:
            r = requests.get(url, headers=self._headers(), params={"page": page, "pageSize": 50}, timeout=30)
            if not r.ok:
                raise requests.HTTPError(f"GET /items falhou ({r.status_code}): {r.text}")
            data = r.json()
            items.extend(data.get("results", []))
            if not data.get("nextPage"):
                break
            page += 1
        return items

    def list_accounts(self, item_id: str) -> list[dict]:
        r = requests.get(f"{PLUGGY_BASE}/items/{item_id}/accounts", headers=self._headers(), timeout=30)
        if not r.ok:
            raise requests.HTTPError(f"GET /items/{item_id}/accounts falhou ({r.status_code}): {r.text}")
        return r.json().get("results", [])

    def list_transactions(self, item_id: str, account_id: str, since: datetime) -> list[dict]:
        url = f"{PLUGGY_BASE}/items/{item_id}/transactions"
        results = []; page = 1
        while True:
            params = {"page": page, "pageSize": 200, "accountId": account_id, "from": since.astimezone(timezone.utc).isoformat()}
            r = requests.get(url, headers=self._headers(), params=params, timeout=60)
            if not r.ok:
                raise requests.HTTPError(f"GET /items/{item_id}/transactions falhou ({r.status_code}): {r.text}")
            data = r.json()
            batch = data.get("results", [])
            results.extend(batch)
            if not data.get("nextPage") or not batch:
                break
            page += 1
        return results

def sync_from_pluggy(account_label_mapping: Dict[str,str] | None = None,
                     days: int | None = None,
                     allow_ids: Iterable[str] | None = None,
                     block_ids: Iterable[str] | None = None,
                     allow_last4: Iterable[str] | None = None) -> dict:
    days = days or int(os.getenv("PLUGGY_SYNC_DAYS", "90"))
    since = datetime.now(timezone.utc) - timedelta(days=days)
    client = PluggyClient()
    items = client.list_items()
    total_new = total_skipped = accounts_count = 0

    allow_ids = set(allow_ids or [])
    block_ids = set(block_ids or [])
    allow_last4 = set([str(x).strip() for x in (allow_last4 or []) if str(x).strip()])

    for item in items:
        item_id = item.get("id")
        accounts = client.list_accounts(item_id)
        for acc in accounts:
            acc_id = acc.get("id")
            last4 = str(acc.get("number") or acc.get("mask") or "")[-4:]

            if block_ids and acc_id in block_ids: continue
            if allow_ids and acc_id not in allow_ids: continue
            if allow_last4 and last4 and last4 not in allow_last4: continue

            accounts_count += 1
            label = (account_label_mapping or {}).get(acc_id) or (acc.get("name") or acc_id)
            txs = client.list_transactions(item_id=item_id, account_id=acc_id, since=since)
            if not txs:
                continue
            df = pd.DataFrame([{
                "date": t.get("date"),
                "description": t.get("description") or (t.get("merchant") or {}).get("name"),
                "amount": float(t.get("amount", 0.0)),
                "currency": t.get("currencyCode", "BRL")
            } for t in txs])
            if df.empty:
                continue
            df_std = to_standard_df(df, label, source="pluggy")
            created, skipped = upsert_transactions(df_std)
            total_new += created
            total_skipped += skipped
    return {"new": total_new, "skipped": total_skipped, "accounts": accounts_count}
