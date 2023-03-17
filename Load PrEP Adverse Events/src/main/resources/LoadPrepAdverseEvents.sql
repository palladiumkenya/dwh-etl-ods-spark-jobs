SELECT
    a.[Id]
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
     ,[AdverseEvent]
     ,[AdverseEventStartDate]
     ,[AdverseEventEndDate]
     ,[Severity]
     ,[VisitDate]
     ,[AdverseEventActionTaken]
     ,[AdverseEventClinicalOutcome]
     ,[AdverseEventIsPregnant]
     ,[AdverseEventCause]
     ,[AdverseEventRegimen]
     ,a.[Date_Created]
     ,a.[Date_Last_Modified]

FROM [PREPCentral].[dbo].[PrepAdverseEvents](NoLock) a
    inner join    [PREPCentral].[dbo].[PrepPatients](NoLock) b
on a.SiteCode = b.SiteCode
    and a.PatientPk =  b.PatientPk
    and a.[PrepNumber] = b.[PrepNumber]