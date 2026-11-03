Set-Location (Split-Path $PSScriptRoot -Parent)
Write-Host "Resetting the fraud pipeline demo state..." -ForegroundColor Yellow

Write-Host "1/4 Stopping Spark jobs (restarting the spark container)"
docker restart spark | Out-Null

Write-Host "2/4 Deleting Delta tables and checkpoints (keeping data\config)"
foreach ($p in "data\bronze", "data\silver", "data\gold", "data\checkpoints") {
    if (Test-Path $p) { Remove-Item -Recurse -Force $p }
}

Write-Host "3/4 Recreating the Kafka topic"
docker exec kafka /opt/kafka/bin/kafka-topics.sh --delete --topic transactions --bootstrap-server kafka:9092 2>$null
for ($i = 0; $i -lt 10; $i++) {
    Start-Sleep -Seconds 3
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic transactions --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) { break }
}
docker exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic transactions --bootstrap-server kafka:9092 | Select-Object -First 1

Write-Host "4/4 Emptying the Postgres tables"
docker exec postgres psql -U fraud -d frauddb -c "TRUNCATE alerts, flagged_transactions, fraud_stats_by_minute RESTART IDENTITY CASCADE;"

Write-Host "Done. Start the pipeline, the alert service, then the generator." -ForegroundColor Green