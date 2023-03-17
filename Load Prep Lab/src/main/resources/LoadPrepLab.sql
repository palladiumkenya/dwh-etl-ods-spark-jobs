SELECT	DISTINCT

    a.[RefId]
                 ,a.[Created]
                 ,a.[PatientPk]
                 ,a.[SiteCode]
                 ,a.[Emr]
                 ,a.[Project]
                 ,a.[Processed]
                 ,a.[QueueId]
                 ,a.[Status]
                 ,a.[StatusDate]
                 ,a.[DateExtracted]
                 ,a.[FacilityId]
                 ,a.[FacilityName]
                 ,a.[PrepNumber]
                 ,a.[HtsNumber]
                 ,[VisitID]
                 ,[TestName]
                 ,[TestResult]
                 ,[SampleDate]
                 ,[TestResultDate]
                 ,[Reason]
                 ,a.[Date_Created]
                 ,a.[Date_Last_Modified]
FROM [PREPCentral].[dbo].[PrepLabs](NoLock) a
    inner join (select tn.PatientPK,tn.SiteCode,tn.PrepNumber,max(tn.Created)MaxCreated FROM [PREPCentral].[dbo].[PrepLabs](NoLock)tn
    GROUP BY tn.PatientPK,tn.SiteCode,tn.PrepNumber)tm
on a.PatientPk = tm.PatientPk and a.SiteCode =tm.SiteCode and a.Created = tm.MaxCreated
    INNER JOIN  [PREPCentral].[dbo].[PrepPatients](NoLock) b
    ON a.sitecode = b.sitecode
    and a.patientPK = b.patientPK
    and a.[PrepNumber] = b.[PrepNumber]