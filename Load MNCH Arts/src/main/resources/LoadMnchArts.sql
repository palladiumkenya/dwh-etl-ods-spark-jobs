SELECT  distinct  P.[PatientPk],P.[SiteCode],P.[Emr], P.[Project], P.[Processed], P.[QueueId], P.[Status], P.[StatusDate], P.[DateExtracted]
               , P.[Pkv], P.[PatientMnchID], P.[PatientHeiID], P.[FacilityName],[RegistrationAtCCC],[StartARTDate],[StartRegimen]
               ,[StartRegimenLine],[StatusAtCCC],[LastARTDate],[LastRegimen],[LastRegimenLine], P.[Date_Created], P.[Date_Last_Modified]
               ,[FacilityReceivingARTCare],RecordUUID
FROM [MNCHCentral].[dbo].[MnchArts] P(NoLock)
    inner join (select tn.PatientPK,tn.SiteCode,max(ID) As MaxID,max(cast(tn.DateExtracted as date))MaxDateExtracted FROM [MNCHCentral].[dbo].[MnchArts] (NoLock)tn
    group by tn.PatientPK,tn.SiteCode)tm
on P.PatientPk = tm.PatientPk and p.SiteCode = tm.SiteCode and cast(p.DateExtracted as date) = tm.MaxDateExtracted
    and p.ID = tm.MaxID
    INNER JOIN [MNCHCentral].[dbo].[Facilities] F ON P.[FacilityId] = F.Id