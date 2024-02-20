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
              ,a.[HtsNumber]
              ,[VisitID]
              ,[RegimenPrescribed]
              ,[DispenseDate]
              ,[Duration]
              ,a.[Date_Created]
              ,a.[Date_Last_Modified]
              ,a.RecordUUID

FROM [PREPCentral].[dbo].[PrepPharmacys](NoLock) a
    INNER JOIN (SELECT PatientPk, SiteCode,Max(ID) As MaxID, max(cast(Created as date)) AS maxCreated from [PREPCentral].[dbo].[PrepPharmacys]
    group by PatientPk,SiteCode) tn
ON a.PatientPk = tn.PatientPk and a.SiteCode = tn.SiteCode and cast(a.Created as date) = tn.maxCreated and a.ID = tn.MaxID

    INNER JOIN (SELECT PatientPk, SiteCode,Max(ID) As MaxID, max(cast(DateExtracted as date)) AS maxDateExtracted from [PREPCentral].[dbo].[PrepPharmacys]
    group by PatientPk,SiteCode) tm
    ON a.PatientPk = tm.PatientPk and a.SiteCode = tm.SiteCode and cast(a.DateExtracted as date) = tm.maxDateExtracted and a.ID = tm.MaxID