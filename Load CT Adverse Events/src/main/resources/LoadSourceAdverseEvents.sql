SELECT Distinct
    P.[PatientCccNumber] AS PatientID,
    P.[PatientPID] AS PatientPK,
    F.Name AS FacilityName,
    F.Code AS SiteCode,
    [AdverseEvent], [AdverseEventStartDate], [AdverseEventEndDate],
    CASE [Severity]
    WHEN '1' THEN 'Mild'
    WHEN '2' THEN 'Moderate'
    WHEN '3' THEN 'Severe'
    ELSE [Severity]
END AS [Severity] ,
							[VisitDate],
							PA.[EMR], PA.[Project], [AdverseEventCause], [AdverseEventRegimen],
							[AdverseEventActionTaken],[AdverseEventClinicalOutcome], [AdverseEventIsPregnant]
							,PA.ID
							,PA.[Date_Created]
						  ,PA.[Date_Last_Modified]

					FROM [DWAPICentral].[dbo].[PatientExtract](NoLock) P
					INNER JOIN [DWAPICentral].[dbo].PatientAdverseEventExtract(NoLock) PA ON PA.[PatientId]= P.ID AND PA.Voided=0
					INNER JOIN [DWAPICentral].[dbo].[Facility](NoLock) F ON P.[FacilityId] = F.Id AND F.Voided=0