SELECT distinct a.[Id]
              ,a.[RefId]
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
              ,[ExitDate]
              ,[ExitReason]
              ,[DateOfLastPrepDose]
              ,a.[Date_Created]
              ,a.[Date_Last_Modified]
FROM [PREPCentral].[dbo].[PrepCareTerminations](NoLock)a
    inner join    [PREPCentral].[dbo].[PrepPatients](NoLock) c
on a.SiteCode = c.SiteCode
    and a.PatientPk =  c.PatientPk
    --and a.ID = c.ID
    INNER JOIN (SELECT PatientPk, SiteCode, max(Created) AS maxCreated from [PREPCentral].[dbo].[PrepCareTerminations]
    group by PatientPk,SiteCode) tn
    ON a.PatientPk = tn.PatientPk and a.SiteCode = tn.SiteCode and a.Created = tn.maxCreated

    INNER JOIN (SELECT PatientPk, SiteCode, max(DateExtracted) AS maxDateExtracted from [PREPCentral].[dbo].[PrepCareTerminations]
    group by PatientPk,SiteCode) tm
    ON a.PatientPk = tm.PatientPk and a.SiteCode = tm.SiteCode and a.DateExtracted = tm.maxDateExtracted