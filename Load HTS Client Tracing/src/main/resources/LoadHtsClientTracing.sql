SELECT DISTINCT  a.[FacilityName]
              ,a.[SiteCode]
              ,a.[PatientPk]
              ,a.[HtsNumber]
              ,a.[Emr]
              ,a.[Project]
              ,[TracingType]
              ,[TracingDate]
              ,[TracingOutcome]
              ,a.RecordUUID
FROM [HTSCentral].[dbo].[HtsClientTracing] (NoLock)a
    INNER JOIN [HTSCentral].[dbo].Clients (NoLock) Cl
on a.PatientPk = Cl.PatientPk and a.SiteCode = Cl.SiteCode
where a.TracingType is not null and a.TracingOutcome is not null