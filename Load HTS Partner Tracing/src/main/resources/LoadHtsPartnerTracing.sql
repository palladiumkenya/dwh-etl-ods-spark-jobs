SELECT DISTINCT  a.[FacilityName]
              ,a.[SiteCode]
              ,a.[PatientPk]
              ,a.[HtsNumber]
              ,a.[Emr]
              ,a.[Project]
              ,a.[TraceType]
              ,a.[TraceDate]
              ,a.[TraceOutcome]
              ,a.[BookingDate]
              ,a.RecordUUID

FROM [HTSCentral].[dbo].[HtsPartnerTracings](NoLock) a
    inner join (select tn.[SiteCode],tn.[PatientPk],tn.[HtsNumber],tn.[TraceType],tn.[TraceDate],tn.BookingDate,tn.[TraceOutcome],
    max(ID) As MaxID,max(cast(DateExtracted as date))MaxDateExtracted from [HTSCentral].[dbo].[HtsPartnerTracings](NoLock) tn
    group by tn.[SiteCode],tn.[PatientPk],tn.[HtsNumber],tn.[TraceType],tn.BookingDate,tn.[TraceDate],tn.[TraceOutcome]
    )tm
on a.[SiteCode] =tm.[SiteCode] and a.[PatientPk] =tm.[PatientPk] and a.[TraceType] = tm.[TraceType] and a.BookingDate =tm.BookingDate and cast(a.DateExtracted as date) = MaxDateExtracted
    and a.ID = tm.MaxID
    INNER JOIN [HTSCentral].[dbo].Clients (NoLock) Cl
    on a.PatientPk = Cl.PatientPk and a.SiteCode = Cl.SiteCode