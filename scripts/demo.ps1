$ErrorActionPreference = 'Stop'
$taskOrderId = [guid]::NewGuid().ToString()
$taskBody = @{orderId=$taskOrderId; sku='DEMO-SKU'; quantity=2} | ConvertTo-Json
Invoke-RestMethod http://localhost:8080/api/orders -Method Post -ContentType 'application/json' -Body $taskBody
for ($taskAttempt=0; $taskAttempt -lt 30; $taskAttempt++) {
    $taskOrder = Invoke-RestMethod "http://localhost:8080/api/orders/$taskOrderId"
    if ($taskOrder.status -ne 'PENDING') { $taskOrder; break }
    Start-Sleep -Seconds 1
}
if ($taskOrder.status -ne 'CONFIRMED') { throw "Expected CONFIRMED, received $($taskOrder.status)" }
Invoke-RestMethod http://localhost:8081/api/inventory/DEMO-SKU
