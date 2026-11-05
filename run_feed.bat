@echo off
REM ============================================================
REM  HTF feed - PC runner (Kiwoom source, full US scan)
REM  Called by Task Scheduler (install_task.bat) or by hand.
REM ============================================================
setlocal
cd /d "%~dp0"
if not exist logs mkdir logs
set LOG=logs\feed_%date:~0,4%%date:~5,2%%date:~8,2%.log

if not exist feed-config.txt (
  echo feed-config.txt missing - copy feed-config.example.txt to feed-config.txt and fill it in >> "%LOG%"
  echo feed-config.txt missing - copy feed-config.example.txt to feed-config.txt and fill it in
  exit /b 2
)
for /f "usebackq eol=# tokens=1,* delims==" %%A in ("feed-config.txt") do set "%%A=%%B"
findstr /C:"=PASTE_HERE" feed-config.txt >nul && (
  echo feed-config.txt still has PASTE_HERE placeholders - open it in Notepad and fill them in >> "%LOG%"
  echo feed-config.txt still has PASTE_HERE placeholders - open it in Notepad and fill them in
  exit /b 2
)

echo ===== %date% %time% start ===== >> "%LOG%"
python scripts\build_universe_kiwoom.py >> "%LOG%" 2>&1
if errorlevel 1 (
  echo build_universe_kiwoom FAILED, not publishing >> "%LOG%"
  exit /b 1
)
REM Insider collection must never take down the price feed.
python scripts\build_insiders.py >> "%LOG%" 2>&1
python scripts\publish.py %* >> "%LOG%" 2>&1
echo ===== %date% %time% done (exit %errorlevel%) ===== >> "%LOG%"
endlocal
