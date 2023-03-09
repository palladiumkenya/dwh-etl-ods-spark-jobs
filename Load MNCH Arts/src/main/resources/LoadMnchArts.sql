SELECT  distinct  P.[PatientPk],P.[SiteCode],P.[Emr], P.[Project], P.[Processed], P.[QueueId], P.[Status], P.[StatusDate], P.[DateExtracted]
               , P.[Pkv], P.[PatientMnchID], P.[PatientHeiID], P.[FacilityName],[RegistrationAtCCC],[StartARTDate],[StartRegimen]
               ,[StartRegimenLine],[StatusAtCCC],[LastARTDate],[LastRegimen],[LastRegimenLine], P.[Date_Created], P.[Date_Last_Modified]

FROM [MNCHCentral].[dbo].[MnchArts] P(NoLock)
    inner join (select tn.PatientPK,tn.SiteCode,max(tn.DateExtracted)MaxDateExtracted FROM [MNCHCentral].[dbo].[MnchArts] (NoLock)tn
    group by tn.PatientPK,tn.SiteCode)tm
on P.PatientPk = tm.PatientPk and p.SiteCode = tm.SiteCode and p.DateExtracted = tm.MaxDateExtracted
    INNER JOIN  [MNCHCentral].[dbo].[MnchPatients] MnchP(Nolock)
    on P.patientPK = MnchP.patientPK and P.Sitecode = MnchP.Sitecode
    INNER JOIN [MNCHCentral].[dbo].[Facilities] F ON P.[FacilityId] = F.Id