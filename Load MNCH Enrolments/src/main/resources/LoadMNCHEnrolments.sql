SELECT distinct p.[PatientMnchID],p.[PatientPk],P.[SiteCode],p.[FacilityName],P.EMR,p.[Project],cast(p.[DateExtracted] as date)[DateExtracted]
              ,[ServiceType],cast([EnrollmentDateAtMnch] as date) [EnrollmentDateAtMnch],[MnchNumber],[FirstVisitAnc],[Parity],[Gravidae]
              ,[LMP],[EDDFromLMP],[HIVStatusBeforeANC],cast([HIVTestDate]as date)[HIVTestDate],[PartnerHIVStatus]
              ,cast([PartnerHIVTestDate] as date)[PartnerHIVTestDate],[BloodGroup],[StatusAtMnch],p.[Date_Last_Modified]
FROM [MNCHCentral].[dbo].[MnchEnrolments]P (nolock)
    inner join (select tn.PatientPK,tn.SiteCode,max(tn.DateExtracted)MaxDateExtracted FROM [MNCHCentral].[dbo].[MnchEnrolments] (NoLock)tn
    group by tn.PatientPK,tn.SiteCode)tm
on P.PatientPk = tm.PatientPk and p.SiteCode = tm.SiteCode and p.DateExtracted = tm.MaxDateExtracted
    --  INNER JOIN  [MNCHCentral].[dbo].[MnchPatients] MnchP(Nolock)  -- to be reviwed later
    --on P.patientPK = MnchP.patientPK and P.Sitecode = MnchP.Sitecode
    INNER JOIN [MNCHCentral].[dbo].[Facilities] F ON P.[FacilityId] = F.Id