SELECT
    P.[PatientCccNumber] AS PatientID,P.[PatientPID] AS PatientPK,F.Code AS SiteCode,F.Name AS FacilityName,
    PL.[VisitId],PL.[OrderedByDate],PL.[ReportedByDate],PL.[TestName],
    PL.[EnrollmentTest],PL.[TestResult],P.[Emr]
        ,CASE P.[Project]
             WHEN 'I-TECH' THEN 'Kenya HMIS II'
             WHEN 'HMIS' THEN 'Kenya HMIS II'
             ELSE P.[Project]
        END AS [Project] ,
						   Getdate() as DateImported,
						   CAST(null AS varchar) as Reason,
						   CAST(null AS date) as Created,
						   LTRIM(RTRIM(STR(F.Code)))+'-'+LTRIM(RTRIM(P.[PatientCccNumber]))+'-'+LTRIM(RTRIM(STR(P.[PatientPID]))) AS CKV

					-------------------- Added by Dennis as missing columns
						,PL.DateSampleTaken,
						PL.SampleType,
                        p.ID as PatientUnique_ID,
						PL.ID as PatientLabsUnique_ID

FROM [DWAPICentral].[dbo].[PatientExtract](NoLock) P
    INNER JOIN [DWAPICentral].[dbo].[PatientLaboratoryExtract](NoLock) PL ON PL.[PatientId]= P.ID AND PL.Voided=0
    INNER JOIN [DWAPICentral].[dbo].[Facility](NoLock) F ON P.[FacilityId] = F.Id AND F.Voided=0
WHERE p.gender!='Unknown'