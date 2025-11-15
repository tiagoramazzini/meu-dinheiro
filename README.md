# Meu Financeiro (NAS-Finance)

Aplicativo Streamlit para organizar finanças pessoais, consolidar extratos bancários e visualizar despesas/receitas de forma rápida. Os dados ficam em SQLite dentro de `FIN_DATA_DIR` (padrão `./data`), e os uploads são mantidos em `FIN_BACKUP_DIR` (padrão `./backup`).

## Principais recursos
- Dashboard **Meu Dinheiro** com visão anual ou mensal, totais de despesas/receitas, gastos por categoria e por origem (conta).
- Módulo **Importar Extratos** para carregar arquivos `.xlsx`, `.xls`, `.csv` e `.pdf` (OFX ainda não suportado), evitando duplicidades e gerando backup automático do upload.
- Tela **Despesas** para revisar um lote importado, editar categorias em massa e exportar um snapshot CSV.
- Gestão de **Categorias** e **Categorias Inteligentes** (regras por palavra-chave aplicadas automaticamente após cada importação).
- Base local em SQLite com índices para consultas rápidas e persistência dos lotes importados.

## Como rodar localmente (Python)
1) Requisitos: Python 3.11+ e pip.\
2) Instale dependências: `pip install -r requirements.txt`\
3) Opcional: defina pastas personalizadas (`FIN_DATA_DIR`, `FIN_BACKUP_DIR`).\
4) Suba a interface: `streamlit run app.py`\
5) Abra o navegador em `http://localhost:8501`.

## Como rodar com Docker Compose
- Ajuste (se quiser) os volumes `_local_data` e `_local_backup` para persistir dados/backup locais.
- Execute: `docker-compose up`\
- A aplicação sobe em `http://localhost:8501`. Variáveis `FIN_DATA_DIR` e `FIN_BACKUP_DIR` já mapeiam para `/app/data` e `/app/backup` dentro do contêiner.

## Fluxo recomendado de uso
1) **Importar extratos**: selecione mês/ano e conta, faça upload dos arquivos. O sistema normaliza colunas, evita duplicatas, cria um lote e aplica automaticamente as “Categorias Inteligentes”.\
2) **Revisar despesas**: na aba Despesas, escolha o lote e ajuste categorias manualmente (edição em tabela) ou baixe o CSV.\
3) **Análises**: acesse “Meu Dinheiro” para ver totais anuais/mensais, gastos por categoria e origem.\
4) **Afinar regras**: em Categorias > Categorias Inteligentes, adicione palavras‑chave para preencher categorias futuras automaticamente.

## Estrutura de pastas/arquivos
- `app.py`: UI Streamlit (navegação, dashboards, importação e edição).  
- `engine/storage.py`: modelos SQLAlchemy e inicialização do SQLite (`finance.db`).  
- `engine/importador.py`: leitura de arquivos, normalização de colunas e inserção em lote com proteção contra duplicidade.  
- `engine/budgets.py`: consolidação para os gráficos e tabelas.  
- `engine/classificador.py`: aplicação em massa de categorias e regras inteligentes.  
- `_local_data/`: volume padrão para `FIN_DATA_DIR` (banco e temporários).  
- `_local_backup/`: volume padrão para `FIN_BACKUP_DIR` (cópias dos uploads).

## Variáveis de ambiente
- `FIN_DATA_DIR`: diretório de dados e `finance.db` (padrão `./data`).  
- `FIN_BACKUP_DIR`: onde ficam os backups dos uploads (padrão `./backup`).  
- `FIN_LOCALE`: opcional; pode ser usado para definir localidade do contêiner (ex.: `pt_BR`).  
- `TZ`: fuso horário do contêiner/host (no compose: `America/Sao_Paulo`).

## Alterações recentes (20/10/2025)
- UI do Streamlit reestruturada nas abas **Meu Dinheiro**, **Despesas**, **Importar Extratos** e **Categorias**, com navegação por `st.radio` e seleção de modo anual/mensal.
- Novo pipeline de importação: detecção de formato (`.xlsx`, `.xls`, `.csv`, `.pdf`), normalização para colunas padrão, de-dup por lote, aplicação automática de backups em `FIN_BACKUP_DIR` e criação de lotes (`Statement`) e transações (`Transaction`) no SQLite.
- Consolidações para dashboards em `engine/budgets.py` (totais de despesas/receitas, pivô categoria x mês, gastos por origem) usadas nos gráficos do dashboard.
- CRUD de categorias e **Categorias Inteligentes** (regras por palavra-chave) com aplicação automática logo após cada importação.
- Conteinerização via `docker-compose.yml` usando `python:3.11-slim`, com volumes `_local_data`/`_local_backup` mapeando `FIN_DATA_DIR`/`FIN_BACKUP_DIR`.
- Utilitários adicionados para salvar uploads temporários em `data/tmp` e formatar valores em BRL (`engine/utils.py`); dependências atualizadas em `requirements.txt` (Streamlit, Pandas, SQLAlchemy, pdfplumber/pypdf).

## Limitações atuais
- OFX ainda não é suportado (`detect_and_load` lança erro nesse formato).  
- Receitas são reconhecidas quando `category == "Receita"` **e** a origem termina com “conta” (positivos em cartões de crédito continuam abatendo despesas).  
- Uploads ficam também em `FIN_DATA_DIR/tmp`; limpe periodicamente se gerar muitos arquivos.

## Pendências observadas
- Perfil de PDF para fatura Santander (`engine/pdf_profiles/santander_fatura.py`) ficou incompleto/corrompido (erro de sintaxe e dicionário truncado); não é chamado pelo fluxo principal, mas precisa ser corrigido antes de usar esse perfil específico.
- O módulo de faturas (`engine/invoices.py`) importa modelos (`Invoice`, `InvoiceTransaction`, `CategoryHint`) que ainda não existem em `engine/storage.py`, portanto qualquer import direto desse módulo falhará até os modelos serem adicionados ou as referências removidas.
