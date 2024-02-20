SELECT distinct
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
              ,a.RecordUUID

FROM [PREPCentral].[dbo].[PrepAdverseEvents](NoLock) a
    INNER JOIN
    (SELECT patientPK,sitecode,max(ID) As MaxID,max(cast(created as date))as Maxcreated from [PREPCentral].[dbo].[PrepAdverseEvents](NoLock) group by patientPK,sitecode)tn
on a.patientPK = tn.patientPK
    and a.sitecode =tn.sitecode and cast(a.created as date) = tn.Maxcreated
    and a.ID  = tn.MaxID
    inner join    [PREPCentral].[dbo].[PrepPatients](NoLock) b
    on a.SiteCode = b.SiteCode
    and a.PatientPk =  b.PatientPk
    and a.[PrepNumber] = b.[PrepNumber]