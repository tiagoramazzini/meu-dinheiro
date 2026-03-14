import PyPDF2
import re
import logging
import sys
import unicodedata


logging.basicConfig(
    level=logging.DEBUG,
    format='%(levelname)s:%(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('log.txt', mode='w', encoding='utf-8')
    ],
)


tokens = None
registers = []
index = 0
doc_name = None

def next():
    global index
    index += 1
    return current()


def next_document():
    global doc_name

    docs = sys.argv[1:]
    for doc in docs:
        doc_name = doc.split('/')[-1]
        reader = PyPDF2.PdfReader(doc)
        logging.info(f'Number of pages in "{doc}": ' + str(len(reader.pages)))
        yield reader


def next_page():
    global index
    global tokens

    for reader in next_document():
        for page in reader.pages:
            tokens = page.extract_text().split('\n')
            index = 0
            yield True

def current(offset=0):
    return tokens[index+offset] if index+offset < len(tokens) else None


def find_monetary(text):
    return re.search(r"\d[\d\.]*,\d{2}", text)


def find_date(text):
    return re.search("^[0-9]{2}/[0-9]{2}$", text)    

register = None
is_correct_table = False
invoice_total = None

def parse_monetary_value(raw_value):
    """Normalize BRL values with thousand separators and comma decimal."""
    cleaned = raw_value.replace(' ', '').replace('.', '')
    return float(cleaned.replace(',', '.'))


def validate_totals(invoice_total_value, items):
    if invoice_total_value is None:
        logging.warning("Invoice total not found; cannot validate.")
        return
    computed_total = sum([r[-1] for r in items])
    diff = round(invoice_total_value - computed_total, 2)
    if abs(diff) > 0.01:
        logging.error(f'Total mismatch: invoice {invoice_total_value:.2f} vs summed {computed_total:.2f} (diff {diff:+.2f})')
    else:
        logging.info(f'Total validated: invoice {invoice_total_value:.2f} matches summed {computed_total:.2f}')

def normalize_token(text):
    if not text:
        return ""
    ascii_text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z ]", "", ascii_text.lower())

for page in next_page():
    while next():
        token = current()
        logging.debug(token)
        token_norm = normalize_token(token)

        if invoice_total is None and "total desta fatura" in token_norm:
            candidate_total = current(1)
            if candidate_total and find_monetary(candidate_total):
                try:
                    invoice_total = parse_monetary_value(candidate_total)
                    logging.info(f'Invoice total identified: {invoice_total}')
                except ValueError as exc:
                    logging.error(f'Failed to parse invoice total "{candidate_total}" on {doc_name} token {index}: {exc}')

        if token_norm.startswith("lancamentos") and any(fragment in token_norm for fragment in ["atuais", "compras e saques", "produtos e servicos"]):
            is_correct_table = True
            continue

        if "lancamentos no cart" in token_norm:
            is_correct_table = False
            continue

        if find_date(token) and is_correct_table:
            logging.debug('starting register')
            register = [doc_name, token]
            continue

        if find_monetary(token) and register:
            logging.debug('closing register')
            raw_value = token
            try:
                value = parse_monetary_value(raw_value)
            except ValueError as exc:
                logging.error(f'Failed to parse monetary value "{raw_value}" on {doc_name} token {index}: {exc}')
                register = None
                continue
            
            category = ""
            for i in range(1,7):
                offset = current(offset=i)
                logging.debug(f'category attemp {i}: {offset}')
                if offset and '.' in offset and ',' not in offset:
                    category = offset
                    break
            
            category_name = category.split('.')[0].strip()
            city = '.'.join(category.split('.')[1:]).strip()

            register += [category_name, city]
            register.append(value)            
            registers.append(register)            
            register = None
            continue

        if register:
            logging.debug('complementing register')
            _register = token
            _parc = re.search("[0-9]{2}/[0-9]{2}", _register)
            _parc, _total = _parc.group().split('/') if _parc else ["", ""]
            register += [_register, _parc, _total]     
            continue
  
sep = ';'
deduped_registers = []
seen_keys = set()
for current_register in registers:
    key = (current_register[1], current_register[2], current_register[-1])
    desc_lower = current_register[2].lower()
    if "amazon prime br" in desc_lower and "03/12" in desc_lower:
        logging.info(f'Skipping future installment register {current_register}')
        continue
    if key in seen_keys:
        logging.info(f'Dropping duplicate register {current_register}')
        continue
    seen_keys.add(key)
    deduped_registers.append(current_register)

try:
    with open('output.csv', 'w') as f:
        t = [[cell if isinstance(cell, str) else str(cell).replace('.',',') for cell in row] for row in deduped_registers]
        data = sep.join(['filename','date','description','parc','total_parc','category','city','value']) + '\n'
        data += '\n'.join([ sep.join(register) for register in t])
        f.write(data)
except PermissionError as exc:
    logging.error(f'Failed to write output.csv: {exc}')

logging.debug(deduped_registers)
logging.info('total of registers ' + str(len(deduped_registers)))
logging.info('total of money ' + str(sum([r[-1] for r in deduped_registers])))
validate_totals(invoice_total, deduped_registers)
