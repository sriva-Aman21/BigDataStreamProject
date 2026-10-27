@echo off
title Stop Supply Chain Project
cd /d "%~dp0"

echo ==========================================
echo   STOPPING SUPPLY CHAIN SERVICES
echo ==========================================
echo.

echo [1/4] Terminating background Spark jobs inside Docker...
docker exec -u root spark pkill -9 -f SparkSubmit >nul 2>&1
docker exec -u root spark pkill -9 -f spark-submit >nul 2>&1

echo.
echo [2/4] Clearing Spark checkpoint locks...
if exist ".\checkpoints\supply_chain" (
    rmdir /s /q ".\checkpoints\supply_chain" >nul 2>&1
)

echo.
echo [3/4] Resetting batch tracking in PostgreSQL...
docker exec -u postgres postgres psql -U postgres -d supply_chain_db -c "TRUNCATE TABLE processed_batches;" >nul 2>&1

echo.
echo [4/4] Terminating Producer, Dashboard, and terminal windows...
powershell -Command "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like '*producer.py*' -or $_.CommandLine -like '*streamlit*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }" >nul 2>&1
taskkill /F /FI "WINDOWTITLE eq Supply Chain Producer*" >nul 2>&1
taskkill /F /FI "WINDOWTITLE eq Supply Chain Dashboard*" >nul 2>&1
taskkill /F /FI "WINDOWTITLE eq Spark Streaming*" >nul 2>&1

echo.
echo ==========================================
echo All services stopped cleanly.
echo Safe to run start_project.bat again.
echo ==========================================
timeout /t 3 /nobreak >nul