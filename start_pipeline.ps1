# start_pipeline.ps1
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
$env:HADOOP_HOME = "C:\hadoop"
$env:Path = "$env:JAVA_HOME\bin;C:\hadoop\bin;" + $env:Path
$env:JAVA_TOOL_OPTIONS = "--add-opens=java.base/javax.security.auth=ALL-UNNAMED"

Write-Host "Starting Docker services..."
docker-compose up -d

Start-Sleep -Seconds 15

Write-Host "Environment ready. Now run in separate terminals:"
Write-Host "  Terminal 1: python kafka/producer.py"
Write-Host "  Terminal 2: python spark/streaming_job.py"
Write-Host "  Terminal 3: python delta/lakehouse.py"
Write-Host "  Airflow UI: http://localhost:8081"