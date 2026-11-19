@echo off
rem Runs one dashboard-update step in the ArcGIS Pro clone environment.
rem   tests\run_dashboard.bat <step> [options]
rem   steps: preflight  next [--add] [--date YYYYMMDD]  build  compare  publish-dry  publish  verify  display
rem A .bat file, not PowerShell, so the PowerShell execution policy on the BLM computer does not apply.
rem Overrides (optional): set DQIMP_ENV=<env name>  /  set DQIMP_PRO_DIR=<ArcGIS Pro folder>\
setlocal
rem Windows' find.exe by full path: Git Bash (Claude Code's shell) puts its own find first on the PATH.
set "HERE=%~dp0"
set "ENV_NAME=arcgispro_clone"
if defined DQIMP_ENV set "ENV_NAME=%DQIMP_ENV%"

set "PRO_DIR="
if defined DQIMP_PRO_DIR set "PRO_DIR=%DQIMP_PRO_DIR%"
if not defined PRO_DIR for /f "tokens=2,*" %%A in ('reg query "HKLM\SOFTWARE\ESRI\ArcGISPro" /v InstallDir 2^>nul ^| "%SystemRoot%\System32\find.exe" "InstallDir"') do set "PRO_DIR=%%B"
if not defined PRO_DIR for /f "tokens=2,*" %%A in ('reg query "HKCU\SOFTWARE\ESRI\ArcGISPro" /v InstallDir 2^>nul ^| "%SystemRoot%\System32\find.exe" "InstallDir"') do set "PRO_DIR=%%B"
if not defined PRO_DIR set "PRO_DIR=C:\Program Files\ArcGIS\Pro\"
if not "%PRO_DIR:~-1%"=="\" set "PRO_DIR=%PRO_DIR%\"

if not exist "%PRO_DIR%bin\Python\Scripts\proenv.bat" (
  echo FAIL ArcGIS Pro's Python was not found under "%PRO_DIR%". Set DQIMP_PRO_DIR to the ArcGIS Pro folder.
  exit /b 1
)
call "%PRO_DIR%bin\Python\Scripts\proenv.bat" >nul 2>&1
call activate %ENV_NAME% >nul 2>&1
if errorlevel 1 (
  echo FAIL could not activate the environment "%ENV_NAME%". Check its name in ArcGIS Pro, Package Manager.
  exit /b 1
)
set "DQIMP_ENV_NAME=%ENV_NAME%"
set "PYTHONIOENCODING=utf-8"
python -u "%HERE%dashboard_agent.py" %*
exit /b %ERRORLEVEL%
