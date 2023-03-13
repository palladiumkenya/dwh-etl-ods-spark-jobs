SELECT distinct [PatientIDCCC],P.[PatientPk],[BabyPatientPK],[MotherPatientPK],[BabyPatientMncHeiID],[MotherPatientMncHeiID]
        ,P.[SiteCode],F.Name FacilityName,P.[EMR],P.[Project],cast(P.[DateExtracted] as date)[DateExtracted],P.[Date_Created]
        ,P.[Date_Last_Modified],
    convert(nvarchar(64), hashbytes('SHA2_256', cast(p.[PatientPk]  as nvarchar(36))), 2) PatientPKHash,
    convert(nvarchar(64), hashbytes('SHA2_256', cast(BabyPatientPK  as nvarchar(36))), 2)BabyPatientPKHash,
    convert(nvarchar(64), hashbytes('SHA2_256', cast(MotherPatientPK  as nvarchar(36))), 2)MotherPatientPKHash,
    convert(nvarchar(64), hashbytes('SHA2_256', cast(MotherPatientMncHeiID  as nvarchar(36))), 2)MotherPatientMncHeiIDHash
FROM [MNCHCentral].[dbo].[MotherBabyPairs] P (Nolock)
    inner join (select tn.PatientPK,tn.SiteCode,max(tn.DateExtracted)MaxDateExtracted FROM [MNCHCentral].[dbo].[MotherBabyPairs] (NoLock)tn
    group by tn.PatientPK,tn.SiteCode)tm
on P.PatientPk = tm.PatientPk and p.SiteCode = tm.SiteCode and p.DateExtracted = tm.MaxDateExtracted
    INNER JOIN  [MNCHCentral].[dbo].[MnchPatients] MnchP(Nolock)
    on P.patientPK = MnchP.patientPK and P.Sitecode = MnchP.Sitecode
    INNER JOIN [MNCHCentral].[dbo].[Facilities] F ON P.[FacilityId] = F.Id