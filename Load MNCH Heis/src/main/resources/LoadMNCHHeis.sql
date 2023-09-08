SELECT distinct P.[PatientPk],P.[SiteCode],P.[Emr],P.[Project],P.[Processed],P.[QueueId],P.[Status],P.[StatusDate]/*,P.[DateExtracted]*/
              ,P.[FacilityId],P.[FacilityName],P.[PatientMnchID],[DNAPCR1Date],[DNAPCR2Date],[DNAPCR3Date],[ConfirmatoryPCRDate],[BasellineVLDate]
              ,[FinalyAntibodyDate],[DNAPCR1],[DNAPCR2],[DNAPCR3],[ConfirmatoryPCR],[BasellineVL],[FinalyAntibody]
              ,[HEIExitDate],[HEIHIVStatus],[HEIExitCritearia],P.[Date_Created],P.[Date_Last_Modified]
FROM [MNCHCentral].[dbo].[Heis] P
    INNER JOIN (SELECT tn.PatientPk,tn.SiteCode,max(tn.DateExtracted)MaxDateExtracted FROM [MNCHCentral].[dbo].[Heis] (NOLOCK)tn
    GROUP BY  tn.PatientPk,tn.SiteCode)tm
on p.PatientPk = tm.PatientPk and p.SiteCode = tm.SiteCode and p.DateExtracted = tm.MaxDateExtracted
    --INNER JOIN  [MNCHCentral].[dbo].[MnchPatients](NOLOCK)  Mnchp  -- to be reviwed later
    --ON p.patientpk = Mnchp.PatientPK and p.Sitecode = Mnchp.sitecode
    INNER JOIN [MNCHCentral].[dbo].[Facilities] F ON P.[FacilityId] = F.Id