@echo off
chcp 65001 >nul
title Jogo da Urna Eletrônica - TikTok LIVE

echo ============================================================
echo   JOGO DA URNA ELETRÔNICA - TIKTOK LIVE
echo ============================================================
echo.
echo Links Locais Disponíveis:
echo   - No seu Navegador (PC): http://localhost:8000/urna
echo   - No OBS / Navegador:    http://127.0.0.1:8000/urna
echo   - Na Rede Local / Wi-Fi: http://192.168.1.6:8000/urna
echo.

REM Verifica se o servidor já está ativo na porta 8000
netstat -ano | findstr :8000 >nul
if errorlevel 1 (
    echo [INFO] Iniciando servidor local...
    start /b .\.venv\Scripts\python.exe -m uvicorn server.main:app --host 0.0.0.0 --port 8000
    timeout /t 2 /nobreak >nul
)

echo Abrindo Jogo da Urna no seu navegador...
start http://localhost:8000/urna
