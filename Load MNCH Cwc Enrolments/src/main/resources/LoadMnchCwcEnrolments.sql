SELECT Distinct [PatientIDCWC],[HEIID],P.[PatientPk],P.[SiteCode],P.[EMR],F.Name FacilityName,P.[Project],cast(P.[DateExtracted] as date)[DateExtracted]
        ,P.[PKV],[MothersPkv],cast([RegistrationAtCWC] as date) [RegistrationAtCWC],cast([RegistrationAtHEI] as date)[RegistrationAtHEI]
        ,[VisitID],[Gestation],[BirthWeight],[BirthLength],[BirthOrder],[BirthType],[PlaceOfDelivery],[ModeOfDelivery],[SpecialNeeds]
        ,[SpecialCare],[HEI],[MotherAlive],[MothersCCCNo],[TransferIn],[TransferInDate],[TransferredFrom],[HEIDate],[NVP]
        ,[BreastFeeding],[ReferredFrom],[ARTMother],[ARTRegimenMother]
        ,cast([ARTStartDateMother] as date) [ARTStartDateMother]
        ,P.[Date_Created]
        ,P.[Date_Last_Modified]
        ,RecordUUID
FROM [MNCHCentral].[dbo].[CwcEnrolments]P(Nolock)
    inner join (select tn.PatientPK,tn.SiteCode,Max(ID) As MaxID,max(cast(tn.DateExtracted as date))MaxDateExtracted FROM [MNCHCentral].[dbo].[CwcEnrolments] (NoLock)tn
    group by tn.PatientPK,tn.SiteCode)tm
on P.PatientPk = tm.PatientPk and p.SiteCode = tm.SiteCode and cast(p.DateExtracted as date) = tm.MaxDateExtracted and p.ID = tm.MaxID
    INNER JOIN [MNCHCentral].[dbo].[Facilities]F on F.Id=P.FacilityId