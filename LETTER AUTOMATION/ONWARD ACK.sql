WITH pol_limit AS (
SELECT PolicyNumber
, [Vehicle Identification Number (VIN)]
,MAX(CASE WHEN [Coverage Name] = 'BI' THEN p.[Mailing Street Address] END) AS [InsuredStreetAddress]
,MAX(CASE WHEN [Coverage Name] = 'BI' THEN p.[Mailing City] END) AS [InsuredCity]
,MAX(CASE WHEN [Coverage Name] = 'BI' THEN p.[Mailing State] END) AS [InsuredState]
,MAX(CASE WHEN [Coverage Name] = 'BI' THEN p.[Mailing Zip Code] END) AS [InsuredZipCode]
,MAX(CASE WHEN [Coverage Name] = 'BI' THEN p.[Limit of Coverage 1] END) AS [BI Limit Per Person]
,MAX(CASE WHEN [Coverage Name] = 'BI' THEN p.[Limit of Coverage 2] END) AS [BI Limit Per Accident]
,MAX(CASE WHEN [Coverage Name] = 'PD' THEN p.[Limit of Coverage 3] END) AS [PD Limit]
,MAX(CASE WHEN [Coverage Name] = 'COMP' THEN p.[Deductible] END) AS [COMP Ded]
,MAX(CASE WHEN [Coverage Name] = 'COLL' THEN p.[Deductible] END) AS [COLL Ded]

FROM [WH_ONWARD_pmsppadb].[dbo].[policy_report] p
WHERE p.[Stat Seq #] IN (SELECT MAX([Stat Seq #]) FROM [WH_ONWARD_pmsppadb].[dbo].[policy_report] WHERE [Coverage Name] != 'Fees' GROUP BY PolicyNumber, [Vehicle Identification Number (VIN)], CASE WHEN [Coverage Name] IN ('UMPDV', 'CDW') THEN 'UMPD/CDW' ELSE [Coverage Name] END)
GROUP BY PolicyNumber
, [Vehicle Identification Number (VIN)]
),

email_cte AS (
SELECT policy_nbr, email
  FROM (SELECT distinct policy_nbr, MAX(submission_id) OVER (PARTITION BY policy_nbr) AS submission_id
  FROM [WH_ONWARD_pmsppadb].[dbo].[t_au_policy]) p
  LEFT JOIN [WH_ONWARD_pmsppadb].[dbo].[t_ws_users] u
  ON p.submission_id = u.submission_id
  WHERE policy_nbr IS NOT NULL AND policy_nbr != ''
  AND p.submission_id = u.submission_id)

SELECT 
      CASE WHEN cp.Type = 1 THEN 'Insured' WHEN cp.Type = 2 THEN 'Claimant' END AS [LetterType]
       ,CASE WHEN cp.[IncidentRole] = 1 THEN 'Driver'
	WHEN cp.[IncidentRole] = 2 THEN 'Passenger'
	WHEN cp.[IncidentRole] = 3 THEN 'Pedestrian'
	WHEN cp.[IncidentRole] = 4 THEN 'Motorcyclist'
	WHEN cp.[IncidentRole] = 5 THEN 'Cyclist'
	WHEN cp.[IncidentRole] = 6 THEN 'Not Involved'
	WHEN cp.[IncidentRole] = 7 THEN 'Registered Owner'
	WHEN cp.[IncidentRole] = 8 THEN 'Other'
	END AS [PersonRole]
      ,[Number]
      ,c.[PolicyNumber]
      ,v.[VIN]
	  ,CONCAT(v.Year, ' ', v.Make, ' ', v.Model) AS [Vehicle]
      ,p.[BI Limit Per Person]
	  ,p.[BI Limit Per Accident]
      ,p.[PD Limit]
      ,p.[COMP Ded]
      ,p.[COLL Ded]
      ,[LossDate]
      ,[LossTime]
      ,[ReportDate]
      ,[InsuredName]
      ,CONCAT(cp.FirstName, ' ', cp.LastName) AS [PersonName]
      ,p1.[InsuredStreetAddress]
      ,p1.[InsuredCity]
      ,p1.[InsuredState]
      ,p1.[InsuredZipCode]
      ,[Date]
      ,[AdjusterMemberName]
      ,CONCAT(a.StreetAddress1, CASE WHEN a.StreetAddress2 != '' THEN ' ' ELSE '' END, a.StreetAddress2) AS [StreetAddress]
      ,a.City
      ,a.State
      ,a.Zip
	  ,em.email AS [InsuredEmail]

  FROM [WH_ONWARD_claimsystem].[dbo].[Claims] c

  LEFT JOIN [WH_ONWARD_claimsystem].[dbo].[ClaimPersons] cp 
  ON c.ID = cp.ClaimID

  LEFT JOIN [WH_ONWARD_claimsystem].[dbo].[ClaimVehicles] v
  on c.ID = v.ClaimID AND v.ID = cp.VehicleID

  LEFT JOIN [WH_ONWARD_claimsystem].[dbo].[ClaimAdjusterHistory] h 
  on c.ID = h.ClaimID AND h.ID IN (SELECT MIN(ID) FROM [WH_ONWARD_claimsystem].[dbo].[ClaimAdjusterHistory] GROUP BY [ClaimID])

  LEFT JOIN [WH_ONWARD_claimsystem].[dbo].[Address] a 
  ON cp.AddressID = a.ID

  LEFT JOIN pol_limit p
  ON c.[PolicyNumber] = p.PolicyNumber AND v.[VIN] = p.[Vehicle Identification Number (VIN)]

  LEFT JOIN (SELECT PolicyNumber
      , [Vehicle Identification Number (VIN)]
      ,[InsuredStreetAddress]
      ,[InsuredCity]
      ,[InsuredState]
      ,[InsuredZipCode]
	  ,[ClaimID]
	  FROM pol_limit LEFT JOIN [WH_ONWARD_claimsystem].[dbo].[ClaimVehicles] v ON [Vehicle Identification Number (VIN)] = VIN
	  WHERE Type = 1
	  ) p1
  ON (c.ID = p1.ClaimID AND c.PolicyNumber = p1.PolicyNumber AND v.[VIN] = p1.[Vehicle Identification Number (VIN)] AND cp.Type = 1) OR (c.ID = p1.ClaimID AND c.PolicyNumber = p1.PolicyNumber AND cp.Type = 2)

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

  WHERE Number IS NOT NULL; 