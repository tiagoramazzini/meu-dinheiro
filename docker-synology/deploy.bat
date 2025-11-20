@echo off
setlocal
REM Copia app + compose para o NAS compartilhado (ajuste o caminho \\nas se precisar)

set "SCRIPT_DIR=%~dp0"
set "REPO_ROOT=%SCRIPT_DIR%.."
set "NAS_ROOT=\\nas\docker\nas-finance"

echo Criando pastas no NAS...
mkdir "%NAS_ROOT%\app" 2>nul
mkdir "%NAS_ROOT%\data" 2>nul
mkdir "%NAS_ROOT%\backup" 2>nul

echo Copiando docker-compose.yml...
copy /Y "%SCRIPT_DIR%docker-compose.yml" "%NAS_ROOT%\docker-compose.yml"

echo Copiando arquivos da aplicacao...
copy /Y "%REPO_ROOT%\app.py" "%NAS_ROOT%\app\app.py"
copy /Y "%REPO_ROOT%\requirements.txt" "%NAS_ROOT%\app\requirements.txt"

echo Copiando pasta engine...
robocopy "%REPO_ROOT%\engine" "%NAS_ROOT%\app\engine" /E /XD "__pycache__" ".git"

echo Copiando pasta python...
robocopy "%REPO_ROOT%\python" "%NAS_ROOT%\app\python" /E /XD "__pycache__" ".git"

echo Pronto. Para subir no NAS, conecte via SSH e rode:
echo    cd /volume1/docker/nas-finance
echo    docker compose up --build -d

endlocal
