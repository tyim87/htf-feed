@echo off
REM Registers the daily feed run in Windows Task Scheduler.
REM US close: 06:00 KST (winter) / 05:00 KST (summer). 06:20 KST Tue-Sat
REM covers Mon-Fri US sessions; the 09:00 KST screen reads the result.
REM The PC must be on (or waking) at that time: power settings -> allow wake timers.
cd /d "%~dp0"
schtasks /Create /F /TN "HTF feed (Kiwoom)" /SC WEEKLY /D TUE,WED,THU,FRI,SAT /ST 06:20 ^
  /TR "\"%~dp0run_feed.bat\"" /RL LIMITED
if errorlevel 1 (
  echo Could not register the task. Run this file as administrator, or create it in Task Scheduler by hand pointing at run_feed.bat
  pause
  exit /b 1
)
echo Registered: "HTF feed (Kiwoom)" daily 06:20 Tue-Sat -> %~dp0run_feed.bat
echo To run it now:  schtasks /Run /TN "HTF feed (Kiwoom)"
pause
