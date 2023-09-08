SELECT distinct P.[PatientPID] AS PatientPK
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
              ,BoosterDoseVerified
              ,[Sequence]
              ,COVID19TestResult
              ,P.ID,C.[Date_Created],C.[Date_Last_Modified]
FROM [DWAPICentral].[dbo].[PatientExtract](NoLock) P
    INNER JOIN [DWAPICentral].[dbo].[CovidExtract](NoLock) C  ON C.[PatientId]= P.ID AND C.Voided=0
    INNER JOIN [DWAPICentral].[dbo].[Facility](NoLock) F ON P.[FacilityId] = F.Id  AND F.Voided=0
WHERE P.gender != 'Unknown'