SELECT  P.[PatientPID] AS PatientPK,P.[PatientCccNumber] AS PatientID, F.Name AS FacilityName, F.Code AS SiteCode
     ,PA.[DOB],PA.[AgeEnrollment],PA.[AgeARTStart],PA.[AgeLastVisit],PA.[RegistrationDate],PA.[PatientSource],PA.[Gender]
     ,PA.[StartARTDate],PA.[PreviousARTStartDate],PA.[PreviousARTRegimen],PA.[StartARTAtThisFacility]
     ,PA.[StartRegimen],PA.[StartRegimenLine],PA.[LastARTDate],PA.[LastRegimen],PA.[LastRegimenLine],PA.[Duration],PA.[ExpectedReturn]
     ,PA.[Provider],PA.[LastVisit],PA.[ExitReason],PA.[ExitDate],P.[Emr]
     ,CASE P.[Project]
          WHEN 'I-TECH' THEN 'Kenya HMIS II'
          WHEN 'HMIS' THEN 'Kenya HMIS II'
          ELSE P.[Project]
    END AS [Project]
						   ,LTRIM(RTRIM(STR(F.Code)))+'-'+LTRIM(RTRIM(P.[PatientCccNumber])) +'-'+LTRIM(RTRIM(STR(P.[PatientPID]))) AS CKV

					,PA.[PreviousARTUse]
					,PA.[PreviousARTPurpose]
					,PA.[DateLastUsed]
                    ,GETDATE () AS DateAsOf

FROM [DWAPICentral].[dbo].[PatientExtract](NoLock) P
    INNER JOIN [DWAPICentral].[dbo].[PatientArtExtract](NoLock) PA ON PA.[PatientId]= P.ID AND PA.Voided=0
    INNER JOIN [DWAPICentral].[dbo].[Facility](NoLock) F ON P.[FacilityId] = F.Id AND F.Voided=0
WHERE p.gender!='Unknown'