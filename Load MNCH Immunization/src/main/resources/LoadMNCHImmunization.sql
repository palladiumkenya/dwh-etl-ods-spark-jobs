SELECT DISTINCT i.[Id],i.[RefId],i.[PatientPk],i.[SiteCode],i.[Emr],[Project],[DateExtracted],[FacilityId],[FacilityName],[PatientMnchID],[BCG],[OPVatBirth]
        ,[OPV1],[OPV2],[OPV3],[IPV],[DPTHepBHIB1],[DPTHepBHIB2],[DPTHepBHIB3],[PCV101],[PCV102],[PCV103]
        ,[ROTA1],[MeaslesReubella1],[YellowFever],[MeaslesReubella2],[MeaslesAt6Months],[ROTA2],[DateOfNextVisit]
        ,[BCGScarChecked],[DateChecked],[DateBCGrepeated],[VitaminAAt6Months],[VitaminAAt1Yr],[VitaminAAt18Months]
        ,[VitaminAAt2Years],[VitaminAAt2To5Years],[FullyImmunizedChild],RecordUUID
FROM [MNCHCentral].[dbo].[MnchImmunizations]i (NoLock)
    inner join (select tn.PatientPK,tn.SiteCode,Max(ID)As MaxID,max(cast(tn.DateExtracted as date))MaxDateExtracted FROM [MNCHCentral].[dbo].[MnchImmunizations] (NoLock)tn
    group by tn.PatientPK,tn.SiteCode)tm
on i.PatientPk = tm.PatientPk and i.SiteCode = tm.SiteCode and cast(i.DateExtracted as date) = tm.MaxDateExtracted and i.ID = tm.MaxID