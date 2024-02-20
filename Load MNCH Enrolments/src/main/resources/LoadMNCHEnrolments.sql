SELECT distinct p.[PatientMnchID],p.[PatientPk],P.[SiteCode],p.[FacilityName],P.EMR,p.[Project],cast(p.[DateExtracted] as date)[DateExtracted]
              ,[ServiceType],cast([EnrollmentDateAtMnch] as date) [EnrollmentDateAtMnch],[MnchNumber],[FirstVisitAnc],[Parity],[Gravidae]
              ,[LMP],[EDDFromLMP],[HIVStatusBeforeANC],cast([HIVTestDate]as date)[HIVTestDate],[PartnerHIVStatus]
              ,cast([PartnerHIVTestDate] as date)[PartnerHIVTestDate],[BloodGroup],[StatusAtMnch],p.[Date_Last_Modified],RecordUUID
FROM [MNCHCentral].[dbo].[MnchEnrolments]P (nolock)
    inner join (select tn.PatientPK,tn.SiteCode,max(ID) As MaxID,max(cast(tn.DateExtracted as date))MaxDateExtracted FROM [MNCHCentral].[dbo].[MnchEnrolments] (NoLock)tn
    group by tn.PatientPK,tn.SiteCode)tm
on P.PatientPk = tm.PatientPk and p.SiteCode = tm.SiteCode and cast(p.DateExtracted as date) = tm.MaxDateExtracted and p.ID = tm.MaxID
    INNER JOIN [MNCHCentral].[dbo].[Facilities] F ON P.[FacilityId] = F.Id