from __future__ import annotations
import re
import pdfplumber
import pandas as pd
from datetime import datetime

# Regex de linhas "úteis"
RE_DATA = re.compile(r"\b(\d{2}/\d{2})\b")  # DD/MM
RE_VALOR = re.compile(r"(-?\d{1,3}(\.\d{3})*,\d{2})\s*$")  # BRL no fim da linha
RE_PARC_TOKEN = re.compile(r"\b(\d{1,2})/(\d{1,2})\b")  # 03/12 etc.

# Linhas a descartar (cabeçalhos, totais, etc.)
DROP_PREFIXES = (
    "BANCO SANTANDER", "Olá, ", "Opções de Pagamento", "Resumo da Fatura", "Histórico de Faturas",
    "Posição do seu Limite", "Orientações para Pagamento", "Beneficiário", "Beneficiária",
    "Agência", "Autenticação", "Ficha de Compensação", "Pagamento Mínimo", "Total desta Fatura",
    "Valor Pago", "CPF/CNPJ", "Programa AAdvantage", "Juros e Custo Efetivo Total", "Central de Atendimento",
    "SAC", "Ouvidoria", "Melhor Data", "Vencimento", "Total a Pagar", "Escaneie para",
    "Número do Documento", "Nosso Número", "Data Documento", "Data Process", "Carteira",
    "Espécie", "Uso Banco", "CET", "Parcelamento de Fatura", "Veja outras opções",
    "Parcele esta fatura", "PARCELAMENTO DE FATURA",
)

KEEP_SECTIONS = ("Detalhamento da Fatura", "Despesas", "Parcelamentos")

def _parse_brl(s: str) -> float:
    s = s.strip().replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0

def _parse_date_ddmm(txt: str, year_hint: int) -> datetime.date | None:
    m = RE_DATA.search(txt)
    if not m:
        return None
    ddmm = m.group(1)
    try:
        d = datetime.strptime(f"{ddmm}/{year_hint}", "%d/%m/%Y").date()
        return d
    except Exception:
        return None

def _should_drop(line: str) -> bool:
    t = line.strip()
    if not t:
        return True
    # remove números de página tipo "2/3", "3/3"
    if re.fullmatch(r"\d+/\d+", t):
        return True
    # cabeçalhos comuns
    for p in DROP_PREFIXES:
        if t.startswith(p):
            return True
    # linhas resumo "VALOR TOTAL ..."
    if t.upper().startswith("VALOR TOTAL"):
        return True
    return False

def parse_pdf(path: str, account_id: str) -> pd.DataFrame:
    """
    Retorna DataFrame com colunas:
      date, description, amount, parcela_n, parcela_total
    """
    rows = []
    year_hint = None
    current_section = None

    with pdfplumber.open(path) as pdf:
        # Chuta o ano pela presença de "Vencimento dd/mm/YYYY" ou usa o ano atual
        for page in pdf.pages:
            text = page.extract_text() or ""
            m = re.search(r"\b(\d{2})/(\d{2})/(\d{4})\b", text)  # pega um ano explícito
            if m:
                year_hint = int(m.group(3))
                break
        if not year_hint:
            year_hint = datetime.now().year

        for page in pdf.pages:
            text = page.extract_text() or ""
            for raw in text.splitlines():
                line = " ".join(raw.split())  # colapsa espaços/quebras
                if _should_drop(line):
                    continue

                # Troca de seção
                if any(h in line for h in KEEP_SECTIONS):
                    current_section = "DETAIL"
                    continue

                if current_section != "DETAIL":
                    # ignorar fora das seções úteis
                    continue

                # Heurística básica:
                # buscamos a combinação: ... <data> ... <descrição> ... <valor final>
                mvalor = RE_VALOR.search(line)
                if not mvalor:
                    continue

                valor_txt = mvalor.group(1)
                amount = _parse_brl(valor_txt)

                # tenta achar a data (DD/MM)
                d = _parse_date_ddmm(line, year_hint)
                if not d:
                    continue

                # descrição = linha sem o valor do fim + remove a data isolada
                desc_part = line[: mvalor.start()].strip()
                desc_part = RE_DATA.sub("", desc_part).strip()

                # detecta token de parcela (03/12 etc)
                parc_n = parc_total = None
                mparc = RE_PARC_TOKEN.search(desc_part)
                if mparc:
                    parc_n = int(mparc.group(1))
                    parc_total = int(mparc.group(2))
                    # limpa o token da descrição
                    desc_part = RE_PARC_TOKEN.sub("", desc_part).strip()

                # Sinal do valor:
                # - "Pagamento de Fatura" tende a ser crédito (negativo no cartão)
                if "PAGAMENTO DE FATURA" in desc_part.upper
                    amount = -abs(amount)

                rows.append({
                    "da
                    "description": desc_part,
                    "amount": amount,
                  
                    "parcela_total": parc_total,
                    "account_id": account_id,
              
                })

    df = pd.DataFrame(rows)
    return df
