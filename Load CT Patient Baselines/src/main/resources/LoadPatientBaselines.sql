SELECT  Distinct P.[PatientCccNumber] AS PatientID,P.[PatientPID] AS PatientPK,F.Code AS SiteCode,PB.ID
               ,PB.[eCD4],PB.[eCD4Date],PB.[eWHO],PB.[eWHODate],PB.[bCD4],PB.[bCD4Date]
               ,PB.[bWHO],PB.[bWHODate],PB.[lastWHO],PB.[lastWHODate],PB.[lastCD4],PB.[lastCD4Date],PB.[m12CD4]
               ,PB.[m12CD4Date],PB.[m6CD4],PB.[m6CD4Date],P.[Emr]
               ,CASE P.[Project]
                    WHEN 'I-TECH' THEN 'Kenya HMIS II'
                    WHEN 'HMIS' THEN 'Kenya HMIS II'
                    ELSE P.[Project]
    END AS [Project]
			  ,PB.[Processed],PB.[bWAB],PB.[bWABDate],PB.[eWAB],PB.[eWABDate],PB.[lastWAB]
			  ,PB.[lastWABDate],PB.[Date_Created],PB.[Date_Last_Modified]
			  ,PB.RecordUUID,PB.voided

FROM [DWAPICentral].[dbo].[PatientExtract](NoLock) P
    INNER JOIN [DWAPICentral].[dbo].[PatientBaselinesExtract](NoLock) PB ON PB.[PatientId]= P.ID
    INNER JOIN [DWAPICentral].[dbo].[Facility](NoLock) F ON P.[FacilityId] = F.Id AND F.Voided=0
    INNER JOIN (
    SELECT F.code as SiteCode,p.[PatientPID] as PatientPK,InnerPB.voided, MAX(InnerPB.created) AS Maxdatecreated
    FROM [DWAPICentral].[dbo].[PatientExtract] P WITH (NoLock)
    INNER JOIN [DWAPICentral].[dbo].[PatientBaselinesExtract] InnerPB  WITH(NoLock)  ON InnerPB.[PatientId]= P.ID
    INNER JOIN [DWAPICentral].[dbo].[Facility] F WITH(NoLock)  ON P.[FacilityId] = F.Id AND F.Voided=0
    GROUP BY F.code,p.[PatientPID],InnerPB.voided
    ) tm
    ON f.code = tm.[SiteCode] and p.PatientPID=tm.PatientPK and PB.voided=tm.voided and  PB.created = tm.Maxdatecreated
WHERE p.gender!='Unknown' AND F.code > 0