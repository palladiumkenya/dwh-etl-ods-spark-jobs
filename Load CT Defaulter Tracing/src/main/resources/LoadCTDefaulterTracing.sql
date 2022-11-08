SELECT P.[PatientPID] AS PatientPK
     ,P.[PatientCccNumber] AS PatientID
     ,P.[Emr]
     ,P.[Project]
     ,F.Code AS SiteCode
     ,F.Name AS FacilityName
     ,[VisitID]
     ,Cast([VisitDate] As Date)[VisitDate]
     ,[EncounterId]
     ,[TracingType]
     ,[TracingOutcome]
     ,[AttemptNumber]
     ,[IsFinalTrace]
     ,[TrueStatus]
     ,[CauseOfDeath]
     ,[Comments]
     ,Cast([BookingDate] As Date)[BookingDate]
     ,LTRIM(RTRIM(STR(F.[Code])))+'-'+LTRIM(RTRIM(P.[PatientCccNumber]))+'-'+LTRIM(RTRIM(STR(P.[PatientPID]))) AS CKV
     ,getdate() as [DateImported]
     ,P.ID as PatientUnique_ID
     ,C.ID as DefaulterTracingUnique_ID
FROM [DWAPICentral].[dbo].[PatientExtract](NoLock) P
    INNER JOIN [DWAPICentral].[dbo].[DefaulterTracingExtract](NoLock) C ON C.[PatientId]= P.ID AND C.Voided=0
    INNER JOIN [DWAPICentral].[dbo].[Facility](NoLock) F ON P.[FacilityId] = F.Id AND F.Voided=0
WHERE P.gender != 'Unknown'