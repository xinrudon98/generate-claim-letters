# =========================
# ENV CONFIG
# =========================
$SourceConnStr = $env:SOURCE_DB_CONNECTION
$DestConnStr   = $env:AZURE_SQL_CONNECTION

$DestMainTable = "dbo.[Query]"

$BatchSize  = 10000
$TimeoutSec = 180
$LogFile    = "logs/sync_query.log"

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
function Query-DataTable($cs,$sql,$params=@{}){
  $c = New-Object System.Data.SqlClient.SqlConnection $cs
  $cmd = $c.CreateCommand()
  $cmd.CommandText = $sql
  $cmd.CommandTimeout = $TimeoutSec

  foreach($k in $params.Keys){
    $p = $cmd.Parameters.Add("@$k",[System.Data.SqlDbType]::VarChar,256)
    $p.Value = $params[$k]
  }

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

Log "=== Starting data sync ==="

# Build claim + policy dataset for letter generation
# Includes coverage aggregation and insured details

$SourceQuery = @'
-- Simplified example query (replace with full logic if needed)

SELECT TOP 100
    c.Number,
    c.PolicyNumber,
    c.LossDate,
    c.InsuredName
FROM your_claim_table c
'@

try {
  Log "Fetching data from source..."

  $dt = Query-DataTable $SourceConnStr $SourceQuery

  Log "Fetched rows: $($dt.Rows.Count) | Columns: $($dt.Columns.Count)"

  if ($dt.Rows.Count -eq 0) {
    Log "Source returned 0 rows. Aborting to prevent wiping target."
    throw "No source data."
  }

  Log "Truncating target table: $DestMainTable"
  Truncate-Table $DestConnStr $DestMainTable

  Log "Bulk inserting data..."
  BulkCopy-ToDest $DestConnStr $dt $DestMainTable

  Log "=== Data sync completed successfully ==="
}
catch {
  Log "Error: $($_.Exception.Message)"
  throw
}
