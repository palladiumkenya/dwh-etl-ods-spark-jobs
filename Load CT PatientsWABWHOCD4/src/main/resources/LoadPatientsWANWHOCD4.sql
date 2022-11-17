SELECT  P.[PatientCccNumber] AS PatientID,
    P.[PatientPID] AS PatientPK,
    F.Name AS FacilityName,
    F.Code AS SiteCode,
    PB.[eCD4]
     ,PB.[eCD4Date]
     ,PB.[eWHO]
     ,PB.[eWHODate]
     ,PB.[bCD4]
     ,PB.[bCD4Date]
     ,PB.[bWHO]
     ,PB.[bWHODate]
     ,PB.[lastWHO]
     ,PB.[lastWHODate]
     ,PB.[lastCD4]
     ,PB.[lastCD4Date]
     ,PB.[m12CD4]
     ,PB.[m12CD4Date]
     ,PB.[m6CD4]
     ,PB.[m6CD4Date]
     ,P.[Emr]
     ,CASE P.[Project]
          WHEN 'I-TECH' THEN 'Kenya HMIS II'
          WHEN 'HMIS' THEN 'Kenya HMIS II'
          ELSE P.[Project]
    END AS [Project]
								  ,PB.[Voided]
								  ,PB.[Processed]
								  ,PB.[bWAB]
								  ,PB.[bWABDate]
								  ,PB.[eWAB]
								  ,PB.[eWABDate]
								  ,PB.[lastWAB]
								  ,PB.[lastWABDate]
								  ,PB.[Created]
								  ,LTRIM(RTRIM(STR(F.Code)))+'-'+LTRIM(RTRIM(P.[PatientCccNumber])) +'-'+LTRIM(RTRIM(STR(P.[PatientPID]))) AS CKV
							  ,P.ID
							  ,PB.ID
FROM [Dwapicentral].[dbo].[PatientExtract](NoLock) P
    INNER JOIN [Dwapicentral].[dbo].[PatientArtExtract](NoLock) PA ON PA.[PatientId]= P.ID
    INNER JOIN [Dwapicentral].[dbo].[PatientBaselinesExtract](NoLock) PB ON PB.[PatientId]= P.ID AND PB.Voided=0
    INNER JOIN [Dwapicentral].[dbo].[Facility](NoLock) F ON P.[FacilityId] = F.Id AND F.Voided=0
WHERE p.gender!='Unknown'