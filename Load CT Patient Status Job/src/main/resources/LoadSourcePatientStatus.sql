SELECT P.[PatientCccNumber] AS PatientID,
    P.[PatientPID] AS PatientPK,
    F.Name AS FacilityName,
    F.Code AS SiteCode
     ,PS.[ExitDescription] ExitDescription
     ,PS.[ExitDate] ExitDate
     ,PS.[ExitReason] ExitReason
     ,P.[Emr] Emr
     ,CASE P.[Project]
          WHEN 'I-TECH' THEN 'Kenya HMIS II'
          WHEN 'HMIS' THEN 'Kenya HMIS II'
          ELSE P.[Project]
    END AS [Project]
							,LTRIM(RTRIM(STR(F.Code)))+'-'+LTRIM(RTRIM(P.[PatientCccNumber])) +'-'+LTRIM(RTRIM(STR(P.[PatientPID]))) AS CKV--Previously PKV
						  --,PS.[Voided] Voided
						  --,PS.[Processed] Processed
						  --,PS.[Created] Created
						  ,PS.TOVerified TOVerified
						,PS.TOVerifiedDate TOVerifiedDate
						,PS.ReEnrollmentDate ReEnrollmentDate

						,[ReasonForDeath]
						,[SpecificDeathReason]
						,Cast([DeathDate] as Date)[DeathDate]
						,EffectiveDiscontinuationDate

FROM [DWAPICentral].[dbo].[PatientExtract] P WITH (NoLock)
    INNER JOIN [DWAPICentral].[dbo].[PatientStatusExtract]PS WITH (NoLock)  ON PS.[PatientId]= P.ID AND PS.Voided=0
    INNER JOIN [DWAPICentral].[dbo].[Facility] F (NoLock)  ON P.[FacilityId] = F.Id AND F.Voided=0
    ---INNER JOIN FacilityManifest_MaxDateRecieved(NoLock) a ON F.Code = a.SiteCode and a.[End] is not null and a.[Session] is not null
WHERE p.gender!='Unknown'