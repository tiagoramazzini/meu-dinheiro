# -*- coding: utf-8 -*-
import os
import math
from tempfile import NamedTemporaryFile
from typing import Iterable, Optional, Union

Number = Union[int, float]

FIN_DATA_DIR = os.getenv("FIN_DATA_DIR", "./data")
TMP_DIR = os.path.join(FIN_DATA_DIR, "tmp")
os.makedirs(TMP_DIR, exist_ok=True)

def save_temp(uploaded_file) -> str:
    """
    Salva um arquivo do Streamlit (UploadedFile) em um temp dentro de FIN_DATA_DIR/tmp
    e retorna o caminho absoluto.
    """
    # nome base seguro
    fname = getattr(uploaded_file, "name", "upload.bin")
    base = os.path.basename(fname)
    # cria temp com mesmo sufixo
    _, ext = os.path.splitext(base)
    with NamedTemporaryFile(delete=False, dir=TMP_DIR, suffix=ext or ".bin") as tmp:
        # UploadedFile tem .getbuffer() ou .read()
        try:
            data = uploaded_file.getbuffer()
        except Exception:
            data = uploaded_file.read()
        if hasattr(data, "tobytes"):
            data = data.tobytes()
        tmp.write(data)
        return tmp.name

def _fmt_number_br(val: Number) -> str:
    """
    Formata número no padrão brasileiro, sem símbolo.
    1234.56 -> '1.234,56'
    -7 -> '-7,00'
    """
    if val is None or (isinstance(val, float) and (math.isnan(val) or math.isinf(val))):
        return "0,00"
    try:
        v = float(val)
    except Exception:
        # tenta limpar string tipo '1.234,56'
        s = str(val).strip()
        if s == "":
            return "0,00"
        # remove pontos de milhar e troca vírgula por ponto
        s = s.replace(".", "").replace(",", ".")
        v = float(s)
    sign = "-" if v < 0 else ""
    v = abs(v)
    # usa formatação US e troca separadores
    s = f"{v:,.2f}"            # ex: 1,234,567.89
    s = s.replace(",", "X")    # 1X234X567.89
    s = s.replace(".", ",")    # 1X234X567,89
    s = s.replace("X", ".")    # 1.234.567,89
    return f"{sign}{s}"

def fmt_brl(val: Number, with_symbol: bool=False) -> str:
    """
    Formata número no padrão brasileiro.
    with_symbol=True -> prefixa 'R$ '.
    """
    s = _fmt_number_br(val)
    return f"R$ {s}" if with_symbol else s

def add_total_row(df, label_col: Optional[str]=None, label: str="TOTAL"):
    """
    Adiciona uma linha TOTAL somando colunas numéricas, mantendo outras vazias.
    Retorna um novo DataFrame (não modifica o original in-place).
    """
    import pandas as pd
    if df is None or getattr(df, "empty", True):
        return df
    df2 = df.copy()
    # identifica numéricas
    num_cols = [c for c in df2.columns if pd.api.types.is_numeric_dtype(df2[c])]
    total_vals = {c: df2[c].sum(skipna=True) for c in num_cols}
    row = {c: "" for c in df2.columns}
    row.update(total_vals)
    # coluna do rótulo
    if label_col is None and len(df2.columns) > 0:
        label_col = df2.columns[0]
    if label_col in df2.columns:
        row[label_col] = label
    # append
    df2 = pd.concat([df2, pd.DataFrame([row])], ignore_index=True)
    return df2
