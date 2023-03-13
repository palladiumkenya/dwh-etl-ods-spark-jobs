SELECT DISTINCT a.ID, a.[FacilityName]
              ,a.[SiteCode]
              ,a.[PatientPk]
              ,a.[HtsNumber]
              ,a.[Emr]
              ,a.[Project]
              ,[TraceType]
              ,[TraceDate]
              ,[TraceOutcome]
              ,[BookingDate]

FROM [HTSCentral].[dbo].[HtsPartnerTracings](NoLock) a
    INNER JOIN [HTSCentral].[dbo].Clients (NoLock) Cl
on a.PatientPk = Cl.PatientPk and a.SiteCode = Cl.SiteCode