SELECT  DISTINCT PA.ID,
    P.[PatientCccNumber] AS PatientID,P.[PatientPID] AS PatientPK,F.Code AS SiteCode,F.Name AS FacilityName, PA.[AgeEnrollment]
               ,PA.[AgeARTStart],PA.[AgeLastVisit],PA.[RegistrationDate],PA.[PatientSource],PA.[Gender],PA.[StartARTDate],PA.[PreviousARTStartDate]
               ,PA.[PreviousARTRegimen],PA.[StartARTAtThisFacility],PA.[StartRegimen],PA.[StartRegimenLine],PA.[LastARTDate],PA.[LastRegimen]
               ,PA.[LastRegimenLine],PA.[Duration],PA.[ExpectedReturn],PA.[Provider],PA.[LastVisit],PA.[ExitReason],PA.[ExitDate],P.[Emr]
               ,CASE P.[Project]
                    WHEN 'I-TECH' THEN 'Kenya HMIS II'
                    WHEN 'HMIS' THEN 'Kenya HMIS II'
                    ELSE P.[Project]
    END AS [Project]
								,PA.[DOB]

						,PA.[PreviousARTUse]
						,PA.[PreviousARTPurpose]
						,PA.[DateLastUsed]
						,PA.[Date_Created],PA.[Date_Last_Modified]
						,GETDATE () AS DateAsOf,
						PA.RecordUUID,PA.voided
FROM [DWAPICentral].[dbo].[PatientExtract](NoLock) P
    INNER JOIN [DWAPICentral].[dbo].[PatientArtExtract](NoLock) PA ON PA.[PatientId]= P.ID
    INNER JOIN [DWAPICentral].[dbo].[Facility](NoLock) F ON P.[FacilityId] = F.Id AND F.Voided=0
    INNER JOIN (SELECT a.PatientPID,c.code,Max(cast(b.created as date))MaxCreated FROM [DWAPICentral].[dbo].[PatientExtract]  a  with (NoLock)
    INNER JOIN [DWAPICentral].[dbo].[PatientArtExtract] b with(NoLock) ON b.[PatientId]= a.ID
    INNER JOIN [DWAPICentral].[dbo].[Facility] c with (NoLock)  ON a.[FacilityId] = c.Id AND c.Voided=0
    GROUP BY  a.PatientPID,c.code)tn
    on P.PatientPID = tn.PatientPID and F.code = tn.code and cast(PA.Created as date) = tn.MaxCreated
WHERE p.gender!='Unknown' AND F.code >0