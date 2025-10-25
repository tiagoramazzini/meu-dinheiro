
# NAS Finance — snapshot

## Arquivos
- app.py — UI (Painel de Controle, Meu Dinheiro, Despesas)
- engine/importador.py — (cole a versão completa enviada na conversa)
- engine/pluggy_import.py — cliente Pluggy (X-API-KEY)
- requirements.txt — dependências
- docker-compose.yml — serviço `nas-finance` (porta 8503)

## Deploy
docker-compose down && docker-compose up -d

## .env (exemplo)
Veja `.env.sample` e ajuste PLUGGY_CLIENT_ID / PLUGGY_CLIENT_SECRET.
