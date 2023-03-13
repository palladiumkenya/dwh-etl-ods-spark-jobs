SELECT  distinct P.[PatientPk],P.[SiteCode],P.[Emr],P.[Project],P.[Processed],P.[QueueId],P.[Status],P.[StatusDate],P.[DateExtracted]
               ,[PatientMNCH_ID],P.[FacilityName],[SatelliteName],[VisitID],[OrderedbyDate],[ReportedbyDate],[TestName],[TestResult]
               ,[LabReason],P.[Date_Last_Modified]
FROM [MNCHCentral].[dbo].[MnchLabs] P(NoLock)
    inner join (select tn.PatientPK,tn.SiteCode,max(tn.DateExtracted)MaxDateExtracted FROM [MNCHCentral].[dbo].[MnchLabs] (NoLock)tn
    group by tn.PatientPK,tn.SiteCode)tm
on P.PatientPk = tm.PatientPk and p.SiteCode = tm.SiteCode and p.DateExtracted = tm.MaxDateExtracted
    INNER JOIN  [MNCHCentral].[dbo].[MnchPatients] MnchP(Nolock)
    on P.patientPK = MnchP.patientPK and P.Sitecode = MnchP.Sitecode
    INNER JOIN [MNCHCentral].[dbo].[Facilities] F ON P.[FacilityId] = F.Id