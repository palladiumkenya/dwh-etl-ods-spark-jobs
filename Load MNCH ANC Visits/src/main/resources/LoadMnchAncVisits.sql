SELECT  Distinct P.[PatientMnchID],[ANCClinicNumber], P.[PatientPk],F.[SiteCode], P.[FacilityName],P.[EMR], P.[Project]
        ,[VisitID],cast(p.[VisitDate] as date)[VisitDate],[ANCVisitNo],[GestationWeeks],[Height],[Weight],[Temp],[PulseRate],[RespiratoryRate]
        ,[OxygenSaturation],[MUAC],[BP],[BreastExam],[AntenatalExercises],[FGM],[FGMComplications],[Haemoglobin],[DiabetesTest],[TBScreening]
        ,[CACxScreen],[CACxScreenMethod],[WHOStaging],[VLSampleTaken],[VLDate],[VLResult],[SyphilisTreatment],[HIVStatusBeforeANC]
        ,[HIVTestingDone],[HIVTestType],[HIVTest1],[HIVTest1Result],[HIVTest2],[HIVTest2Result],[HIVTestFinalResult],[SyphilisTestDone]
        ,[SyphilisTestType],[SyphilisTestResults],[SyphilisTreated],[MotherProphylaxisGiven],[MotherGivenHAART],[AZTBabyDispense]
        ,[NVPBabyDispense],[ChronicIllness],[CounselledOn],[PartnerHIVTestingANC],[PartnerHIVStatusANC],[PostParturmFP],[Deworming]
        ,[MalariaProphylaxis],[TetanusDose],[IronSupplementsGiven],[ReceivedMosquitoNet],[PreventiveServices],[UrinalysisVariables]
        ,[ReferredFrom],[ReferredTo],[ReferralReasons],cast([NextAppointmentANC] as date)[NextAppointmentANC]
        ,[ClinicalNotes] , P.[Date_Last_Modified],RecordUUID
FROM [MNCHCentral].[dbo].[AncVisits] (NoLock) P
    inner join (select tn.PatientPK,tn.SiteCode,tn.VisitDate,Max(ID) As MaxID,max(cast(tn.DateExtracted as date))MaxDateExtracted
    FROM [MNCHCentral].[dbo].[AncVisits] (NoLock)tn
    group by tn.PatientPK,tn.SiteCode,tn.VisitDate
    )tm
on P.PatientPk = tm.PatientPk and p.SiteCode = tm.SiteCode
    and p.VisitDate = tm.VisitDate  and cast(p.DateExtracted as date) = tm.MaxDateExtracted
    and p.ID = tm.MaxID
    INNER JOIN [MNCHCentral].[dbo].[Facilities](NoLock) F ON P.[FacilityId] = F.Id