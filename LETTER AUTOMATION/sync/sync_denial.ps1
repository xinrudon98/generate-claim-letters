$SourceConnStr = "Server=10.222.19.164,1433;Database=WH_ONWARD_claimsystem;User Id=jLin2@knightcompany.com;Password=WfkEpnFIUY!pb7hN1jT1w;Encrypt=False;"

$DestConnStr    = "Server=tcp:onward.database.windows.net,1433;Database=Onward;User Id=Onward;Password=Knight20250801;Encrypt=True;"
$DestMainTable  = "dbo.[Denial]"

$BatchSize   = 10000
$TimeoutSec  = 180
$LogFile     = "C:\Users\XDong\OneDrive - Hankey Group\sync\sync_denial.log"

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
  $expectedCount = 30
  if ($dt.Columns.Count -lt $expectedCount) {
    $firstCols = ($dt.Columns | Select-Object -First 10 -Expand ColumnName) -join ', '
    throw "Source returned only $($dt.Columns.Count) columns (expected $expectedCount). First cols: $firstCols"
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

$SourceQuery = @'
SET NOCOUNT ON;

WITH effective AS (
SELECT [PolicyNumber]
, [Vehicle Identification Number (VIN)]
      ,CASE WHEN [Coverage Name] IN ('UMPDV', 'CDW') THEN 'UMPD/CDW' ELSE [Coverage Name] END AS [Coverage Name]
      ,CONVERT(DATE, CASE WHEN [Coverage Effective Date] < [Transaction Effective Date] THEN CONVERT(DATE, [Coverage Effective Date]) 
            WHEN [Coverage Effective Date] = [Transaction Effective Date] THEN CONVERT(DATE, [Transaction Effective Date])
            END) AS [Coverage Effective Date]

FROM [WH_ONWARD_pmsppadb].[dbo].[policy_report] p

WHERE p.[Stat Seq #] IN (SELECT MIN(CONVERT(INT, [Stat Seq #])) FROM [WH_ONWARD_pmsppadb].[dbo].[policy_report] WHERE [Coverage Name] != 'Fees' AND [Transaction Type] NOT IN ('Cancel', 'Recalc Cancel')
GROUP BY PolicyNumber, [Vehicle Identification Number (VIN)], CASE WHEN [Coverage Name] IN ('UMPDV', 'CDW') THEN 'UMPD/CDW' ELSE [Coverage Name] END)
),

email_cte AS (
SELECT policy_nbr, email
  FROM (SELECT distinct policy_nbr, MAX(submission_id) OVER (PARTITION BY policy_nbr) AS submission_id
  FROM [WH_ONWARD_pmsppadb].[dbo].[t_au_policy]) p
  LEFT JOIN [WH_ONWARD_pmsppadb].[dbo].[t_ws_users] u
  ON p.submission_id = u.submission_id
  WHERE policy_nbr IS NOT NULL AND policy_nbr != ''
  AND p.submission_id = u.submission_id),

expiration AS (
  SELECT
      p.[PolicyNumber],
      p.[Vehicle Identification Number (VIN)],
      CASE WHEN p.[Coverage Name] IN ('UMPDV','CDW') THEN 'UMPD/CDW' ELSE p.[Coverage Name] END AS [Coverage Name],
      p.[Limit of Coverage 1],
      p.[Limit of Coverage 2],
      p.[Limit of Coverage 3],
      p.[Deductible],
      p.[Policy Effective Date],
      p.[Policy Expiration Date],
      p.[Coverage Effective Date],
      p.[Coverage Expiration Date],
      p.[Transaction Effective Date],
      p.[Accounting Date],
      p.[Written Premium],
      p.[Transaction Type],
      p.[Location Address],
      p.[Location City],
      p.[Location State],
      p.[Location Zip Code],
      p.[Policy Named Insured (First)],
      p.[Policy Named Insured (Last)],
      p.[Mailing Street Address],
      p.[Mailing City],
      p.[Mailing State],
      p.[Mailing Zip Code],
      CONVERT(int, p.[Stat Seq #]) AS StatSeqInt,
      CASE
        WHEN p.[Coverage Expiration Date] <  p.[Transaction Effective Date] THEN CONVERT(date, p.[Policy Expiration Date])
        WHEN p.[Coverage Expiration Date] =  p.[Transaction Effective Date] AND p.[Written Premium] >= 0 THEN CONVERT(date, p.[Policy Expiration Date])
        WHEN p.[Coverage Expiration Date] =  p.[Transaction Effective Date] AND p.[Written Premium] <  0 THEN CONVERT(date, p.[Transaction Effective Date])
        WHEN p.[Coverage Expiration Date] >  p.[Transaction Effective Date] THEN CONVERT(date, p.[Policy Expiration Date])
      END AS [Coverage Canc/Exp Date],
      CASE
        WHEN p.[Transaction Type] IN ('Cancel','Recalc Cancel','OOSE Rollback of -Reinstatement','OOSE Rollback of -Recalc Reinstatement')
          THEN CONVERT(date, p.[Transaction Effective Date])
        ELSE CONVERT(date, p.[Policy Expiration Date])
      END AS [Policy Canc/Exp Date]
  FROM [WH_ONWARD_pmsppadb].[dbo].[policy_report] p
  WHERE p.[Stat Seq #] IN (
    SELECT MAX([Stat Seq #])
    FROM [WH_ONWARD_pmsppadb].[dbo].[policy_report]
    WHERE [Coverage Name] <> 'Fees'
    GROUP BY PolicyNumber, [Vehicle Identification Number (VIN)],
             CASE WHEN [Coverage Name] IN ('UMPDV','CDW') THEN 'UMPD/CDW' ELSE [Coverage Name] END
  )
),

e_temp AS (
  SELECT
      PolicyNumber,
      [Mailing Street Address],
      [Mailing City],
      [Mailing State],
      [Mailing Zip Code],
      [Policy Effective Date],
      [Policy Expiration Date],
	  [Coverage Name],
	  [Limit of Coverage 1],
	  [Limit of Coverage 2],
	  [Limit of Coverage 3]

  FROM (
    SELECT
        e.*,
        ROW_NUMBER() OVER (PARTITION BY e.PolicyNumber, e.[Coverage Name] ORDER BY e.StatSeqInt DESC) AS rn
    FROM expiration e
    WHERE e.[Coverage Name] in ('BI', 'PD')
  ) x
  WHERE rn = 1
)

SELECT [IsPolicyDriver], [IsPolicyVehicle]
	  , [InsuredName]
	  , CONCAT(cp.FirstName, ' ', cp.LastName) AS [DriverName]
	  ,[LossDate]
      ,c.[PolicyNumber]
	  ,MAX(e.[Policy Effective Date])  AS [PolicyEffectiveDate]
      ,MAX(e.[Policy Expiration Date]) AS [PolicyExpirationDate]
	  ,[Number] AS [ClaimNumber]
	  ,[LocationDescription]
	  ,[LossTime]
	  ,[Make]
	  ,v.[Model]
	  ,[Year]
	  ,v.[VIN]
      ,CASE WHEN [Uber] = 1 THEN 'Yes' WHEN [Uber] = 0 THEN 'No' WHEN [Uber] = 3 THEN 'Unknown' END AS [BusinessActivity]
	  ,MAX(e.[Mailing Street Address]) AS [MailingStreetAddress]
      ,MAX(e.[Mailing City]) AS [MailingCity]
      ,MAX(e.[Mailing State]) AS [MailingState]
      ,MAX(e.[Mailing Zip Code]) AS [MailingZipCode]
	  ,MAX(CASE WHEN e.[Coverage Name] = 'BI' THEN e.[Limit of Coverage 1] END) AS [BIPerPerson]
	  ,MAX(CASE WHEN e.[Coverage Name] = 'BI' THEN e.[Limit of Coverage 2] END) AS [BIPerAccident]
	  ,MAX(CASE WHEN e.[Coverage Name] = 'PD' THEN e.[Limit of Coverage 3] END) AS [PD]
	  ,MAX(CASE WHEN expiration.[Coverage Name] = 'COMP' THEN expiration.[Deductible] END) AS [CompDed]
	  ,MAX(CASE WHEN expiration.[Coverage Name] = 'COLL' THEN expiration.[Deductible] END) AS [CollDed]
	  ,[AdjusterMemberName]
	  ,MAX(CASE WHEN expiration.[Coverage Name] = 'BI' THEN CONCAT(expiration.[Location Address], ', ', expiration.[Location City], ' ', expiration.[Location State], ' ', expiration.[Location Zip Code]) END) AS [GaragingAddress]
	  ,MAX(CASE WHEN expiration.[Policy Expiration Date] > expiration.[Policy Canc/Exp Date] THEN expiration.[Policy Canc/Exp Date] END) AS [CancellationDate]
	  ,CASE WHEN MAX(CASE WHEN expiration.[Coverage Name] = 'BI' THEN expiration.[Location State] END) = 'CA' THEN
	  CONCAT(CASE WHEN SUM(CASE WHEN expiration.[Coverage Name] = 'BI' THEN 1 ELSE 0 END) > 0 THEN
	  CONCAT('bodily injury liability of $', MAX(CASE WHEN e.[Coverage Name] = 'BI' THEN e.[Limit of Coverage 1] END), ' per person/$'
	  , MAX(CASE WHEN e.[Coverage Name] = 'BI' THEN e.[Limit of Coverage 2] END), ' per loss') ELSE '' END
	  ,CASE WHEN SUM(CASE WHEN expiration.[Coverage Name] = 'PD' THEN 1 ELSE 0 END) > 0 THEN
	  CONCAT(', property liability of $' , MAX(CASE WHEN e.[Coverage Name] = 'PD' THEN e.[Limit of Coverage 3] END), ' per loss') ELSE '' END
	  ,CASE WHEN SUM(CASE WHEN expiration.[Coverage Name] = 'MEDPM' THEN 1 ELSE 0 END) > 0 THEN
	  CONCAT(', medical payments of $', MAX(CASE WHEN expiration.[Coverage Name] = 'MEDPM' THEN expiration.[Limit of Coverage 3] END), ' per person') ELSE '' END
	  ,CASE WHEN SUM(CASE WHEN expiration.[Coverage Name] = 'UMBI' THEN 1 ELSE 0 END) > 0 THEN
	  CONCAT(', uninsured motorist bodily injury of $', MAX(CASE WHEN expiration.[Coverage Name] = 'UMBI' THEN expiration.[Limit of Coverage 1] END), ' per person/$'
	  , MAX(CASE WHEN expiration.[Coverage Name] = 'UMBI' THEN expiration.[Limit of Coverage 2] END), ' per loss') ELSE '' END
	  ,CASE WHEN SUM(CASE WHEN expiration.[Coverage Name] = 'UMPD/CDW' THEN 1 ELSE 0 END) > 0 THEN
	  CONCAT(', uninsured motorist property damage of $', MAX(CASE WHEN expiration.[Coverage Name] = 'UMPD/CDW' THEN format(CONVERT(INT,expiration.[Limit of Coverage 3]), '#,##0') END), ' or CDW') ELSE '' END
	  ,CASE WHEN SUM(CASE WHEN expiration.[Coverage Name] = 'COLL' THEN 1 ELSE 0 END) > 0 THEN
	  CONCAT(', collision and comprehensive coverage with $', MAX(CASE WHEN expiration.[Coverage Name] = 'COLL' THEN format(CONVERT(INT,expiration.[Deductible]), '#,##0') END), ' deductible each') ELSE '' END
	  ,CASE WHEN SUM(CASE WHEN expiration.[Coverage Name] = 'RREIM' THEN 1 ELSE 0 END) > 0 THEN
	  CONCAT(', rental reimbursement of $', MAX(CASE WHEN expiration.[Coverage Name] = 'RREIM' THEN expiration.[Limit of Coverage 3] END), ' per day for 30 days ') ELSE '' END
	  ,CASE WHEN SUM(CASE WHEN expiration.[Coverage Name] = 'GLASS' THEN 1 ELSE 0 END) > 0 THEN ', glass deductible waiver ' ELSE '' END
	  ,CASE WHEN SUM(CASE WHEN expiration.[Coverage Name] = 'SPEQ' THEN 1 ELSE 0 END) > 0 THEN
	  CONCAT(', special equipment of $', MAX(CASE WHEN expiration.[Coverage Name] = 'SPEQ' THEN expiration.[Limit of Coverage 3] END), '') ELSE '' END)
	  WHEN MAX(CASE WHEN expiration.[Coverage Name] = 'BI' THEN expiration.[Location State] END) = 'TX' THEN
	  CONCAT(CASE WHEN SUM(CASE WHEN expiration.[Coverage Name] = 'BI' THEN 1 ELSE 0 END) > 0 THEN
	  CONCAT('bodily injury liability of $', MAX(CASE WHEN e.[Coverage Name] = 'BI' THEN e.[Limit of Coverage 1] END), ' per person/$'
	  , MAX(CASE WHEN e.[Coverage Name] = 'BI' THEN e.[Limit of Coverage 2] END), ' per loss') ELSE '' END
	  ,CASE WHEN SUM(CASE WHEN expiration.[Coverage Name] = 'PD' THEN 1 ELSE 0 END) > 0 THEN
	  CONCAT(', property liability of $', MAX(CASE WHEN e.[Coverage Name] = 'PD' THEN e.[Limit of Coverage 3] END), ' per loss') ELSE '' END
	  ,CASE WHEN SUM(CASE WHEN expiration.[Coverage Name] = 'PIP' THEN 1 ELSE 0 END) > 0 THEN ', personal injury protection of $2,500 per person' ELSE '' END
	  ,CASE WHEN SUM(CASE WHEN expiration.[Coverage Name] = 'MEDPM' THEN 1 ELSE 0 END) > 0 THEN
	  CONCAT(', medical payments of $', MAX(CASE WHEN expiration.[Coverage Name] = 'MEDPM' THEN expiration.[Limit of Coverage 3] END), ' per person') ELSE '' END
	  ,CASE WHEN SUM(CASE WHEN expiration.[Coverage Name] = 'UMBI' THEN 1 ELSE 0 END) > 0 THEN
	  CONCAT(', uninsured/underinsured bodily injury of $', MAX(CASE WHEN expiration.[Coverage Name] = 'UMBI' THEN expiration.[Limit of Coverage 1] END), ' per person/$'
	  , MAX(CASE WHEN expiration.[Coverage Name] = 'UMBI' THEN expiration.[Limit of Coverage 2] END), ' per loss') ELSE '' END
	  ,CASE WHEN SUM(CASE WHEN expiration.[Coverage Name] = 'UMPD/CDW' THEN 1 ELSE 0 END) > 0 THEN
	  ', uninsured/underinsured property damage of $25,000' END
	  ,CASE WHEN SUM(CASE WHEN expiration.[Coverage Name] = 'COLL' THEN 1 ELSE 0 END) > 0 THEN
	  CONCAT(', collision and comprehensive coverage with $', MAX(CASE WHEN expiration.[Coverage Name] = 'COLL' THEN format(CONVERT(INT,expiration.[Deductible]), '#,##0') END), ' deductible each') ELSE '' END
	  ,CASE WHEN SUM(CASE WHEN expiration.[Coverage Name] = 'RREIM' THEN 1 ELSE 0 END) > 0 THEN 
	  CONCAT(', rental reimbursement of $', MAX(CASE WHEN expiration.[Coverage Name] = 'RREIM' THEN expiration.[Limit of Coverage 1] END), ' /day  '
	  , MAX(CASE WHEN expiration.[Coverage Name] = 'RREIM' THEN expiration.[Limit of Coverage 2] END), ' days') ELSE '' END
	  ,CASE WHEN SUM(CASE WHEN expiration.[Coverage Name] = 'GLASS' THEN 1 ELSE 0 END) > 0 THEN ', glass deductible waiver ' ELSE '' END
	  ,CASE WHEN SUM(CASE WHEN expiration.[Coverage Name] = 'SPEQ' THEN 1 ELSE 0 END) > 0 THEN
	  CONCAT(', special equipment of $', MAX(CASE WHEN expiration.[Coverage Name] = 'SPEQ' THEN expiration.[Limit of Coverage 3] END), '') ELSE '' END)
	  END AS [PolicyCoverages]
      ,MAX(em.email) AS [InsuredEmail]

  FROM [WH_ONWARD_claimsystem].[dbo].[Claims] c

  LEFT JOIN [WH_ONWARD_claimsystem].[dbo].[ClaimVehicles] v
  on c.ID = v.ClaimID

  LEFT JOIN [WH_ONWARD_claimsystem].[dbo].[ClaimPersons] cp 
  ON c.ID = cp.ClaimID

  LEFT JOIN [WH_ONWARD_claimsystem].[dbo].[ClaimAdjusterHistory] h 
  on c.ID = h.ClaimID AND h.ID IN (SELECT MAX(ID) FROM [WH_ONWARD_claimsystem].[dbo].[ClaimAdjusterHistory] GROUP BY [ClaimID])

  LEFT JOIN [WH_ONWARD_claimsystem].[dbo].[Address] a 
  ON cp.AddressID = a.ID
  
  LEFT JOIN expiration ON c.PolicyNumber = expiration.PolicyNumber AND v.VIN = expiration.[Vehicle Identification Number (VIN)]

  LEFT JOIN effective ON expiration.PolicyNumber = effective.PolicyNumber AND expiration.[Vehicle Identification Number (VIN)] = effective.[Vehicle Identification Number (VIN)] AND expiration.[Coverage Name] = effective.[Coverage Name]

  LEFT JOIN e_temp e ON c.PolicyNumber = e.PolicyNumber

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

  WHERE Number IS NOT NULL AND (cp.Type = v.Type OR cp.Type IS NULL) AND (cp.VehicleID = v.ID OR cp.VehicleID IS NULL)
  AND (cp.[IncidentRole] = 1 AND cp.Type = 1)

    GROUP BY [IsPolicyDriver], [IsPolicyVehicle]
,[InsuredName]
,CONCAT(cp.FirstName, ' ', cp.LastName)
,[LossDate]
      ,c.[PolicyNumber]
	  ,[Number]
	  ,[LocationDescription]
	  ,[LossTime]
	  ,[Make]
	  ,v.[Model]
	  ,[Year]
	  ,v.[VIN]
      ,CASE WHEN [Uber] = 1 THEN 'Yes' WHEN [Uber] = 0 THEN 'No' WHEN [Uber] = 3 THEN 'Unknown' END
	  ,[AdjusterMemberName]
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
  try {
    $firstCols = ($dt.Columns | Select-Object -First 10 -Expand ColumnName) -join ', '
    Log "First 10 source cols: $firstCols"
  } catch {}
  Log "Error: $($_.Exception.Message)"
  throw
}

