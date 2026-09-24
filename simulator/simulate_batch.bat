@echo off
setlocal
cd /d "%~dp0" || exit /b 1
set "PYTHON_EXE=python"
if exist ".venv\Scripts\python.exe" set "PYTHON_EXE=.venv\Scripts\python.exe"
"%PYTHON_EXE%" -m data_simulator.simulate_batch --prepare --upload %*
exit /b %ERRORLEVEL%
