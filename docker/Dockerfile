FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    FIN_DATA_DIR=/app/data \
    FIN_BACKUP_DIR=/app/backup \
    FIN_LOCALE=pt_BR \
    TZ=America/Sao_Paulo

WORKDIR /app

# Dependências do sistema (básicas) e Python
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Código da aplicação
COPY VERSION /app/VERSION
COPY app.py /app/app.py
COPY engine /app/engine
COPY modulos /app/modulos
COPY static /app/static
COPY python /app/python

# Pastas de dados/backup
RUN mkdir -p /app/data/tmp /app/backup

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
