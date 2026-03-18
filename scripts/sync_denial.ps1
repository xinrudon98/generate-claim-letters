# =========================
# ENV CONFIG
# =========================
$SourceConnStr = $env:SOURCE_DB_CONNECTION
$DestConnStr   = $env:AZURE_SQL_CONNECTION

$DestTable = "dbo.DenialLetters"

$BatchSize  = 10000
$TimeoutSec = 180
$LogFile    = "logs/sync_denial.log"

# Ensure log directory exists
if (!(Test-Path "logs")) {
    New-Item -ItemType Directory -Path "logs" | Out-Null
}

Add-Type -AssemblyName "System.Data"

# =========================
# LOGGING
# =========================
function Log($m){
  $t    = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss.fff")
  $line = "$t  $m"
  Write-Host $line
  Add-Content -Path $LogFile -Value $line
}

# =========================
# DB FUNCTIONS
# =========================
function Query-DataTable($cs,$sql){
  $c = New-Object System.Data.SqlClient.SqlConnection $cs
  $cmd = $c.CreateCommand()
  $cmd.CommandText = $sql
  $cmd.CommandTimeout = $TimeoutSec

  $da = New-Object System.Data.SqlClient.SqlDataAdapter $cmd
  $dt = New-Object System.Data.DataTable
  [void]$da.Fill($dt)
  $c.Close()
  return $dt
}

function BulkCopy-ToDest($connStr, $dt, $destTable) {
  $bc = New-Object System.Data.SqlClient.SqlBulkCopy($connStr)
  $bc.DestinationTableName = $destTable
  $bc.BatchSize = $BatchSize
  $bc.BulkCopyTimeout = $TimeoutSec

  for ($i=0; $i -lt $dt.Columns.Count; $i++) {
    $null = $bc.ColumnMappings.Add($i,$i)
  }

  $bc.WriteToServer($dt)
  $bc.Close()
}

function Truncate-Table($cs,$t){
  $c = New-Object System.Data.SqlClient.SqlConnection $cs
  $cmd = $c.CreateCommand()
  $cmd.CommandText = "TRUNCATE TABLE $t;"
  $cmd.CommandTimeout = $TimeoutSec

  $c.Open()
  $null = $cmd.ExecuteNonQuery()
  $c.Close()
}

# =========================
# MAIN FLOW
# =========================

Log "=== Starting denial letter pipeline ==="

# This query builds denial-letter-ready dataset
# Includes claim info, coverage, and policy language

$DenialQuery = @'
SELECT
    ClaimNumber,
    PolicyNumber,
    InsuredName,
    LossDate,
    CoverageDescription,
    DenialReason
FROM your_denial_dataset
'@

try {
  Log "Fetching denial dataset..."

  $dt = Query-DataTable $SourceConnStr $DenialQuery

  Log "Rows fetched: $($dt.Rows.Count)"

  if ($dt.Rows.Count -eq 0) {
    Log "No data returned. Abort."
    throw "Empty dataset"
  }

  Log "Truncating destination table..."
  Truncate-Table $DestConnStr $DestTable

  Log "Bulk inserting denial letters..."
  BulkCopy-ToDest $DestConnStr $dt $DestTable

  Log "=== Denial pipeline completed ==="
}
catch {
  Log "Error: $($_.Exception.Message)"
  throw
}
