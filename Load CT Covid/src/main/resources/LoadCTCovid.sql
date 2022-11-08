SELECT P.[PatientPID] AS PatientPK
     ,P.[PatientCccNumber] AS PatientID
     ,P.[Emr]
     ,P.[Project]
     ,F.Code AS SiteCode
     ,F.Name AS FacilityName ,[VisitID]
     ,Cast([Covid19AssessmentDate] as Date)[Covid19AssessmentDate]
     ,[ReceivedCOVID19Vaccine]
     ,Cast([DateGivenFirstDose] as date) [DateGivenFirstDose]
     ,[FirstDoseVaccineAdministered]
     ,Cast([DateGivenSecondDose] as Date)[DateGivenSecondDose]
     ,[SecondDoseVaccineAdministered]
     ,[VaccinationStatus],[VaccineVerification],[BoosterGiven],[BoosterDose]
     ,Cast([BoosterDoseDate] as Date)[BoosterDoseDate]
     ,[EverCOVID19Positive]
     ,Cast([COVID19TestDate] as Date) [COVID19TestDate],[PatientStatus],[AdmissionStatus],[AdmissionUnit],[MissedAppointmentDueToCOVID19]
     ,[COVID19PositiveSinceLasVisit]
     ,Cast([COVID19TestDateSinceLastVisit] as Date)[COVID19TestDateSinceLastVisit]
     ,[PatientStatusSinceLastVisit]
     ,[AdmissionStatusSinceLastVisit]
     ,Cast([AdmissionStartDate] as Date)[AdmissionStartDate]
     ,Cast([AdmissionEndDate] as Date)[AdmissionEndDate]
     ,[AdmissionUnitSinceLastVisit]
     ,[SupplementalOxygenReceived]
     ,[PatientVentilated]
     ,[TracingFinalOutcome]
     ,[CauseOfDeath]
     ,LTRIM(RTRIM(STR(F.Code)))+'-'+LTRIM(RTRIM(P.[PatientCccNumber]))+'-'+LTRIM(RTRIM(STR(P.[PatientPID]))) AS CKV
     ,getdate() as [DateImported]
     ,BoosterDoseVerified
     ,[Sequence]
     ,COVID19TestResult
     ,P.ID as PatientUnique_ID
     ,C.ID as CovidUnique_ID
FROM [DWAPICentral].[dbo].[PatientExtract](NoLock) P
    INNER JOIN [DWAPICentral].[dbo].[CovidExtract](NoLock) C  ON C.[PatientId]= P.ID AND C.Voided=0
    INNER JOIN [DWAPICentral].[dbo].[Facility](NoLock) F ON P.[FacilityId] = F.Id  AND F.Voided=0
WHERE P.gender != 'Unknown'