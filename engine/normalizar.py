import re
import pandas as pd

def norm_desc(s: str) -> str:
    s = (s or '').strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def parse_date(x):
    try:
        return pd.to_datetime(x, dayfirst=True).date()
    except Exception:
        return pd.to_datetime('today').date()
