$SourceConnStr = "Server=10.222.19.164,1433;Database=WH_ONWARD_claimsystem;User Id=jLin2@knightcompany.com;Password=WfkEpnFIUY!pb7hN1jT1w;Encrypt=False;"

$DestConnStr    = "Server=tcp:onward.database.windows.net,1433;Database=Onward;User Id=Onward;Password=Knight20250801;Encrypt=True;"
$DestMainTable  = "dbo.[Query]"

$BatchSize   = 10000
$TimeoutSec  = 180
$LogFile     = "C:\Users\XDong\OneDrive - Hankey Group\sync\sync_query.log"

Add-Type -AssemblyName "System.Data"

function Log($m){
  $t   = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss.fff")
  $line= "$t  $m"
  Write-Host $line
  Add-Content -Path $LogFile -Value $line -Encoding UTF8
}

function Query-DataTable($cs,$sql,$params=@{}){
  $c=New-Object System.Data.SqlClient.SqlConnection $cs
  $cmd=$c.CreateCommand(); $cmd.CommandText=$sql; $cmd.CommandTimeout=$TimeoutSec
  foreach($k in $params.Keys){ $p=$cmd.Parameters.Add("@$k",[System.Data.SqlDbType]::VarChar,256); $p.Value=$params[$k] }
  $da=New-Object System.Data.SqlClient.SqlDataAdapter $cmd
  $dt=New-Object System.Data.DataTable
  [void]$da.Fill($dt)
  $c.Close(); return $dt
}

function BulkCopy-ToDest($connStr, $dt, $destTable) {
  $expectedCount = 27
  if ($dt.Columns.Count -lt $expectedCount) {
    throw "Source returned only $($dt.Columns.Count) columns, but at least $expectedCount are required to load into $destTable."
  }
  $bc = New-Object System.Data.SqlClient.SqlBulkCopy($connStr)
  $bc.DestinationTableName = $destTable
  $bc.BatchSize = $BatchSize
  $bc.BulkCopyTimeout = $TimeoutSec
  for ($i=0; $i -lt $expectedCount; $i++) { $null = $bc.ColumnMappings.Add($i,$i) }
  $bc.WriteToServer($dt); $bc.Close()
}

function Truncate-Table($cs,$t){
  $c=New-Object System.Data.SqlClient.SqlConnection $cs
  $cmd=$c.CreateCommand(); $cmd.CommandText="TRUNCATE TABLE $t;"; $cmd.CommandTimeout=$TimeoutSec
  $c.Open(); $null=$cmd.ExecuteNonQuery(); $c.Close()
}

function Refresh-MainFromStage($cs,$stage,$main){
  $cols = "[LetterType],[PersonRole],[Number],[PolicyNumber],[VIN],[Vehicle],
           [BI Limit Per Person],[BI Limit Per Accident],[PD Limit],[COMP Ded],[COLL Ded],
           [LossDate],[LossTime],[ReportDate],[InsuredName],[PersonName],
           [InsuredStreetAddress],[InsuredCity],[InsuredState],[InsuredZipCode],
           [Date],[AdjusterMemberName],[StreetAddress],[City],[State],[Zip],[InsuredEmail]"
  $sql=@"
BEGIN TRY
  SET XACT_ABORT ON;
  SET NOCOUNT ON;
  BEGIN TRAN;

  TRUNCATE TABLE $main;

  INSERT INTO $main ($cols)
  SELECT $cols FROM $stage;

  COMMIT;
END TRY
BEGIN CATCH
  IF @@TRANCOUNT>0 ROLLBACK;
  THROW;
END CATCH
"@
  $c=New-Object System.Data.SqlClient.SqlConnection $cs
  $cmd=$c.CreateCommand(); $cmd.CommandText=$sql; $cmd.CommandTimeout=$TimeoutSec
  $c.Open(); $null=$cmd.ExecuteNonQuery(); $c.Close()
}

$SourceQuery = @'
WITH pol_limit AS (
  SELECT
    PolicyNumber,
    [Vehicle Identification Number (VIN)],
    MAX(CASE WHEN [Coverage Name] = 'BI'   THEN p.[Mailing Street Address] END) AS [InsuredStreetAddress],
    MAX(CASE WHEN [Coverage Name] = 'BI'   THEN p.[Mailing City] END)           AS [InsuredCity],
    MAX(CASE WHEN [Coverage Name] = 'BI'   THEN p.[Mailing State] END)          AS [InsuredState],
    MAX(CASE WHEN [Coverage Name] = 'BI'   THEN p.[Mailing Zip Code] END)       AS [InsuredZipCode],
    MAX(CASE WHEN [Coverage Name] = 'BI'   THEN p.[Limit of Coverage 1] END)    AS [BI Limit Per Person],
    MAX(CASE WHEN [Coverage Name] = 'BI'   THEN p.[Limit of Coverage 2] END)    AS [BI Limit Per Accident],
    MAX(CASE WHEN [Coverage Name] = 'PD'   THEN p.[Limit of Coverage 3] END)    AS [PD Limit],
    MAX(CASE WHEN [Coverage Name] = 'COMP' THEN p.[Deductible] END)             AS [COMP Ded],
    MAX(CASE WHEN [Coverage Name] = 'COLL' THEN p.[Deductible] END)             AS [COLL Ded]
  FROM [WH_ONWARD_pmsppadb].[dbo].[policy_report] p
  WHERE p.[Stat Seq #] IN (
    SELECT MAX([Stat Seq #])
    FROM [WH_ONWARD_pmsppadb].[dbo].[policy_report]
    WHERE [Coverage Name] <> 'Fees'
    GROUP BY PolicyNumber,
             [Vehicle Identification Number (VIN)],
             CASE WHEN [Coverage Name] IN ('UMPDV','CDW') THEN 'UMPD/CDW' ELSE [Coverage Name] END
  )
  GROUP BY PolicyNumber, [Vehicle Identification Number (VIN)]
),
email_cte AS (
  SELECT policy_nbr, email
  FROM (
    SELECT DISTINCT policy_nbr, MAX(submission_id) OVER (PARTITION BY policy_nbr) AS submission_id
    FROM [WH_ONWARD_pmsppadb].[dbo].[t_au_policy]
  ) p
  LEFT JOIN [WH_ONWARD_pmsppadb].[dbo].[t_ws_users] u
    ON p.submission_id = u.submission_id
  WHERE policy_nbr IS NOT NULL AND policy_nbr <> ''
    AND p.submission_id = u.submission_id
),
base AS (
  SELECT 
      CASE WHEN cp.Type = 1 THEN 'Insured'
           WHEN cp.Type = 2 THEN 'Claimant' END                      AS [LetterType],
      CASE WHEN cp.[IncidentRole] = 1 THEN 'Driver'
           WHEN cp.[IncidentRole] = 2 THEN 'Passenger'
           WHEN cp.[IncidentRole] = 3 THEN 'Pedestrian'
           WHEN cp.[IncidentRole] = 4 THEN 'Motorcyclist'
           WHEN cp.[IncidentRole] = 5 THEN 'Cyclist'
           WHEN cp.[IncidentRole] = 6 THEN 'Not Involved'
           WHEN cp.[IncidentRole] = 7 THEN 'Registered Owner'
           WHEN cp.[IncidentRole] = 8 THEN 'Other' END               AS [PersonRole],
      c.[Number],
      c.[PolicyNumber],
      v.[VIN],
      CONCAT(v.Year, ' ', v.Make, ' ', v.Model)                      AS [Vehicle],
      p.[BI Limit Per Person],
      p.[BI Limit Per Accident],
      p.[PD Limit],
      p.[COMP Ded],
      p.[COLL Ded],
      c.[LossDate],
      c.[LossTime],
      c.[ReportDate],
      c.[InsuredName],
      CONCAT(cp.FirstName, ' ', cp.LastName)                         AS [PersonName],
      p1.[InsuredStreetAddress],
      p1.[InsuredCity],
      p1.[InsuredState],
      p1.[InsuredZipCode],
      COALESCE(c.[ReportDate], c.[LossDate])                         AS [Date],
      h.[AdjusterMemberName]                                         AS [AdjusterMemberName],
      CONCAT(a.StreetAddress1, CASE WHEN a.StreetAddress2 <> '' THEN ' ' ELSE '' END, a.StreetAddress2) AS [StreetAddress],
      a.City,
      a.State,
      a.Zip,
      em.email                                                       AS [InsuredEmail]
  FROM [WH_ONWARD_claimsystem].[dbo].[Claims] c
  LEFT JOIN [WH_ONWARD_claimsystem].[dbo].[ClaimPersons]  cp ON c.ID = cp.ClaimID
  LEFT JOIN [WH_ONWARD_claimsystem].[dbo].[ClaimVehicles] v  ON c.ID = v.ClaimID AND v.ID = cp.VehicleID
  LEFT JOIN [WH_ONWARD_claimsystem].[dbo].[ClaimAdjusterHistory] h 
         ON c.ID = h.ClaimID
        AND h.ID IN (SELECT MAX(ID) FROM [WH_ONWARD_claimsystem].[dbo].[ClaimAdjusterHistory] GROUP BY [ClaimID])
  LEFT JOIN [WH_ONWARD_claimsystem].[dbo].[Address] a ON cp.AddressID = a.ID
  LEFT JOIN pol_limit p
         ON c.[PolicyNumber] = p.PolicyNumber
        AND v.[VIN] = p.[Vehicle Identification Number (VIN)]
  LEFT JOIN (
      SELECT pl.PolicyNumber,
             pl.[Vehicle Identification Number (VIN)],
             pl.[InsuredStreetAddress],
             pl.[InsuredCity],
             pl.[InsuredState],
             pl.[InsuredZipCode],
             v.ClaimID
      FROM pol_limit pl
      LEFT JOIN [WH_ONWARD_claimsystem].[dbo].[ClaimVehicles] v
        ON pl.[Vehicle Identification Number (VIN)] = v.VIN
      WHERE v.Type = 1
  ) p1
    ON (c.ID = p1.ClaimID AND c.PolicyNumber = p1.PolicyNumber AND v.[VIN] = p1.[Vehicle Identification Number (VIN)] AND cp.Type = 1)
    OR (c.ID = p1.ClaimID AND c.PolicyNumber = p1.PolicyNumber AND cp.Type = 2)
  CROSS APPLY (
    SELECT policy_base =
           LTRIM(RTRIM(
             CASE WHEN CHARINDEX('-', c.[PolicyNumber]) > 0
                  THEN LEFT(c.[PolicyNumber], CHARINDEX('-', c.[PolicyNumber]) - 1)
                  ELSE c.[PolicyNumber]
             END))
  ) AS pb
  LEFT JOIN email_cte em
         ON pb.policy_base = LTRIM(RTRIM(em.policy_nbr))
  WHERE c.[Number] IS NOT NULL
)
SELECT [LetterType]
      ,[PersonRole]
      ,[Number]
      ,[PolicyNumber]
      ,[VIN]
      ,[Vehicle]
      ,[BI Limit Per Person]
      ,[BI Limit Per Accident]
      ,[PD Limit]
      ,[COMP Ded]
      ,[COLL Ded]
      ,[LossDate]
      ,[LossTime]
      ,[ReportDate]
      ,[InsuredName]
      ,[PersonName]
      ,[InsuredStreetAddress]
      ,[InsuredCity]
      ,[InsuredState]
      ,[InsuredZipCode]
      ,[Date]
      ,[AdjusterMemberName]
      ,[StreetAddress]
      ,[City]
      ,[State]
      ,[Zip]
      ,[InsuredEmail]
FROM base;
'@


try {
  Log "=== Full refresh start ==="

  $dt = Query-DataTable $SourceConnStr $SourceQuery
  Log "Fetched rows: $($dt.Rows.Count) | Columns: $($dt.Columns.Count)"
  if ($dt.Rows.Count -eq 0) {
    Log "Source returned 0 rows. Aborting refresh to avoid clearing target with no data."
    throw "No source data."
  }

 Log "Truncating target: $DestMainTable"
Truncate-Table  $DestConnStr $DestMainTable

Log "Bulk copying directly to target..."
BulkCopy-ToDest $DestConnStr $dt $DestMainTable

  Log "=== Full refresh done ==="
}
catch {
  Log "Error: $($_.Exception.Message)"
  throw
}