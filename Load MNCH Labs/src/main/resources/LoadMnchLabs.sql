SELECT  distinct P.[PatientPk],P.[SiteCode],P.[Emr],P.[Project],P.[Processed],P.[QueueId],P.[Status],P.[StatusDate]
               ,[PatientMNCH_ID],P.[FacilityName],[SatelliteName],[VisitID],P.[OrderedbyDate],[ReportedbyDate],[TestName],[TestResult]
               ,[LabReason],P.[Date_Last_Modified],RecordUUID
FROM [MNCHCentral].[dbo].[MnchLabs] P(NoLock)
    inner join (select tn.PatientPK,tn.SiteCode,tn.[OrderedbyDate],Max(ID) As MaxID,max(tn.DateExtracted)MaxDateExtracted FROM [MNCHCentral].[dbo].[MnchLabs] (NoLock)tn
    group by tn.PatientPK,tn.SiteCode,tn.[OrderedbyDate])tm
on P.PatientPk = tm.PatientPk and p.SiteCode = tm.SiteCode and P.[OrderedbyDate] =tm.[OrderedbyDate] and p.DateExtracted = tm.MaxDateExtracted and p.ID = tm.MaxID
    INNER JOIN [MNCHCentral].[dbo].[Facilities] F ON P.[FacilityId] = F.Id