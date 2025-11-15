from pathlib import Path
import pdfplumber
import pandas as pd

def parse_pdf(path: Path) -> pd.DataFrame:
    rows = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                continue
            header = [str(h).strip().lower() for h in table[0]]
            # heurística simples: assume 3 colunas [data, descrição, valor]
            for r in table[1:]:
                if not r or len(r) < 3:
                    continue
                rows.append({'date': r[0], 'description': r[1], 'amount': _to_float(r[2]), 'currency':'BRL'})
    if rows:
        return pd.DataFrame(rows)
    raise ValueError('PDF não reconhecido — precisa de mapeamento específico/ocr')

def _to_float(txt):
    try:
        return float(str(txt).replace('.', '').replace(',', '.'))
    except Exception:
        return 0.0
