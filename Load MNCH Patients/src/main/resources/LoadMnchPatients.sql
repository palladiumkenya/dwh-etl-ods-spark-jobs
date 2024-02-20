SELECT distinct P.[PatientPk],P.[SiteCode],P.[Emr],[Project],[Processed],[QueueId],[Status],[StatusDate],[DateExtracted]
        ,[FacilityId],[FacilityName],[Pkv],[PatientMnchID],[PatientHeiID],[Gender],[DOB],[FirstEnrollmentAtMnch],[Occupation]
        ,[MaritalStatus],[EducationLevel],[PatientResidentCounty],[PatientResidentSubCounty],[PatientResidentWard],[InSchool]
        ,[Date_Created],[Date_Last_Modified],[NUPI],RecordUUID
FROM [MNCHCentral].[dbo].[MnchPatients] P(nolock)
    inner join (select tn.PatientPK,tn.SiteCode,Max(ID) As MaxID,max(cast(tn.DateExtracted as date))MaxDateExtracted FROM [MNCHCentral].[dbo].[MnchPatients] (NoLock)tn
    group by tn.PatientPK,tn.SiteCode)tm
on P.PatientPk = tm.PatientPk and p.SiteCode = tm.SiteCode and cast(p.DateExtracted as date) = tm.MaxDateExtracted and p.ID = tm.MaxID
    INNER JOIN [MNCHCentral].[dbo].[Facilities] F ON P.[FacilityId] = F.Id