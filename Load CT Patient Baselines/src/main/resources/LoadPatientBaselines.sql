SELECT  Distinct P.[PatientCccNumber] AS PatientID,P.[PatientPID] AS PatientPK,F.Code AS SiteCode,PB.ID
               ,PB.[eCD4],PB.[eCD4Date],PB.[eWHO],PB.[eWHODate],PB.[bCD4],PB.[bCD4Date]
               ,PB.[bWHO],PB.[bWHODate],PB.[lastWHO],PB.[lastWHODate],PB.[lastCD4],PB.[lastCD4Date],PB.[m12CD4]
               ,PB.[m12CD4Date],PB.[m6CD4],PB.[m6CD4Date],P.[Emr]
               ,CASE P.[Project]
                    WHEN 'I-TECH' THEN 'Kenya HMIS II'
                    WHEN 'HMIS' THEN 'Kenya HMIS II'
                    ELSE P.[Project]
    END AS [Project]
			  ,PB.[Voided],PB.[Processed],PB.[bWAB],PB.[bWABDate],PB.[eWAB],PB.[eWABDate],PB.[lastWAB]
			  ,PB.[lastWABDate],PB.[Date_Created],PB.[Date_Last_Modified]


FROM [DWAPICentral].[dbo].[PatientExtract](NoLock) P
    --INNER JOIN [DWAPICentral].[dbo].[PatientArtExtract](NoLock) PA ON PA.[PatientId]= P.ID ---- This table is not been used in this contest analysis done by Mugo and Mumo. It is causing duplicates
    INNER JOIN [DWAPICentral].[dbo].[PatientBaselinesExtract](NoLock) PB ON PB.[PatientId]= P.ID AND PB.Voided=0
    INNER JOIN [DWAPICentral].[dbo].[Facility](NoLock) F ON P.[FacilityId] = F.Id AND F.Voided=0
WHERE p.gender!='Unknown'