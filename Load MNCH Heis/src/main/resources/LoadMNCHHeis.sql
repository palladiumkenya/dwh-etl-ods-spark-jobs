SELECT distinct P.[PatientPk],P.[SiteCode],P.[Emr],P.[Project],P.[Processed],P.[QueueId],P.[Status],P.[StatusDate]/*,P.[DateExtracted]*/
              ,P.[FacilityId],P.[FacilityName],P.[PatientMnchID],[DNAPCR1Date],[DNAPCR2Date],[DNAPCR3Date],[ConfirmatoryPCRDate],[BasellineVLDate]
              ,[FinalyAntibodyDate],[DNAPCR1],[DNAPCR2],[DNAPCR3],[ConfirmatoryPCR],[BasellineVL],[FinalyAntibody]
              ,[HEIExitDate],[HEIHIVStatus],[HEIExitCritearia],P.[Date_Created],P.[Date_Last_Modified],RecordUUID
FROM [MNCHCentral].[dbo].[Heis] P
    INNER JOIN (SELECT tn.PatientPk,tn.SiteCode,Max(ID) As MaxID,max(cast(tn.DateExtracted as date))MaxDateExtracted FROM [MNCHCentral].[dbo].[Heis] (NOLOCK)tn
    GROUP BY  tn.PatientPk,tn.SiteCode)tm
on p.PatientPk = tm.PatientPk and p.SiteCode = tm.SiteCode and cast(p.DateExtracted as date) = tm.MaxDateExtracted and p.ID = tm.MaxID
    INNER JOIN [MNCHCentral].[dbo].[Facilities] F ON P.[FacilityId] = F.Id