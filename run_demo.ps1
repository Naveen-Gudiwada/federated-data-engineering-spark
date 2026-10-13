Write-Host "=== STAGE 1: Ingest three clouds (Parquet, CSV, JSON) ===" -ForegroundColor Cyan
python ingest.py 2>$null
Write-Host "=== STAGE 2: Governance (admin vs analyst + audit log) ===" -ForegroundColor Cyan
Remove-Item audit_log.csv -ErrorAction SilentlyContinue
python governance.py 2>$null
Get-Content audit_log.csv
Write-Host "=== STAGE 3: Federated vs centralized job ===" -ForegroundColor Cyan
python job.py 2>$null
Write-Host "=== STAGE 4: Saved benchmark results ===" -ForegroundColor Cyan
Get-Content benchmark_results.csv
Write-Host "=== DONE ===" -ForegroundColor Green
