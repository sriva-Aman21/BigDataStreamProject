@echo off
setlocal
title Supply Chain Project Launcher

cd /d "%~dp0"

echo ==========================================
echo   REAL-TIME SUPPLY CHAIN PROJECT
echo ==========================================
echo.

echo [1/6] Starting Docker containers...
docker compose up -d
if errorlevel 1 (
    echo ERROR: Docker containers could not start.
    echo Make sure Docker Desktop is running.
    pause
    exit /b 1
)

echo.
echo [2/6] Cleaning up old background jobs and checkpoints...
docker exec -u root spark pkill -9 -f SparkSubmit >nul 2>&1
docker exec -u root spark pkill -9 -f spark-submit >nul 2>&1
if exist ".\checkpoints\supply_chain" (
    rmdir /s /q ".\checkpoints\supply_chain" >nul 2>&1
)

echo.
echo [3/6] Waiting for Kafka to become ready...
set /a RETRIES=0

:WAIT_KAFKA
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >nul 2>&1
if not errorlevel 1 goto KAFKA_READY

set /a RETRIES+=1
if %RETRIES% GEQ 30 (
    echo ERROR: Kafka did not become ready within 60 seconds.
    echo Check Docker Desktop and Kafka logs.
    pause
    exit /b 1
)

timeout /t 2 /nobreak >nul
goto WAIT_KAFKA

:KAFKA_READY
echo Kafka is ready.

echo.
echo [4/6] Starting Spark Streaming...
start "Spark Streaming" powershell.exe -NoExit -Command "docker exec spark /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/spark-ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 /opt/spark/work-dir/streaming/streaming.py"

echo Waiting for Spark to initialize (20s)...
timeout /t 20 /nobreak >nul

echo.
echo [5/6] Starting Python Producer...
start "Supply Chain Producer" powershell.exe -NoExit -Command "Set-Location -LiteralPath '%~dp0'; & '.\.venv\Scripts\python.exe' 'producer\producer.py'"

echo.
echo [6/6] Starting Streamlit Dashboard...
start "Supply Chain Dashboard" powershell.exe -NoExit -Command "$envFile = Get-Content -LiteralPath '%~dp0.env'; foreach ($line in $envFile) { if ($line -match '^\s*([^#=]+)=(.*)$') { [System.Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim(), 'Process') } }; Set-Location -LiteralPath '%~dp0'; & '.\.venv\Scripts\python.exe' -m streamlit run 'dashboard\app.py'"

echo.
echo ==========================================
echo Startup commands have been sent.
echo Dashboard: http://localhost:8501
echo Keep all project terminals open.
echo ==========================================
pause