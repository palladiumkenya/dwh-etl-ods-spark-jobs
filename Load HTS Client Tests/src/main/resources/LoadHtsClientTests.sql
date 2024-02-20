SELECT distinct
-- a.ID
    a.[FacilityName]

              ,a.[SiteCode]
              ,a.[PatientPk]
              ,a.[Emr]
              ,a.[Project]
              ,coalesce(a.[EncounterId],-1)EncounterId
              ,a.[TestDate]
--,a.DateExtracted
              ,[EverTestedForHiv]
              ,[MonthsSinceLastTest]
              ,a.[ClientTestedAs]
--,a.[EntryPoint]
--,mm.target_name as Entrypoint
              ,coalesce(mm.target_name,NULL,a.[EntryPoint],null,'Empty') as Entrypoint
--,a.[TestStrategy]
-- ,mp.Target_htsStrategy as TestStrategy
              ,coalesce(mp.Target_htsStrategy,NULL,a.[TestStrategy],null,'Empty') as TestStrategy
              ,coalesce(a.[TestResult1],'empty')TestResult1
              ,coalesce(a.[TestResult2],'empty')TestResult2
              ,a.[FinalTestResult]
              ,[PatientGivenResult]
              ,[TbScreening]
              ,a.[ClientSelfTested]
              ,a.[CoupleDiscordant]
              ,a.[TestType]
              ,[Consent]
              ,Setting
              ,Approach
              ,HtsRiskCategory
              ,HtsRiskScore
              ,[OtherReferredServices]
              ,[ReferredForServices]
              ,[ReferredServices]

FROM [HTSCentral].[dbo].[HtsClientTests](NoLock) a
    LEFT JOIN ods.dbo.lkp_patient_source mm
on a.entryPoint =mm.source_name
    LEFT JOIN ods.dbo.lkp_htsStrategy mp
    on a.TestStrategy = mp.Source_htsStrategy
    INNER JOIN ( select  ct.sitecode,ct.patientPK,ct.FinalTestResult,ct.TestDate,ct.EncounterId
    ,max(cast(DateExtracted as date))MaxDateExtracted
    from [HTSCentral].[dbo].[HtsClientTests] ct
    LEFT JOIN ods.dbo.lkp_patient_source mn
    on ct.entryPoint = mn.source_name
    LEFT JOIN ods.dbo.lkp_htsStrategy mq
    on ct.TestStrategy = mq.Source_htsStrategy
    GROUP BY ct.sitecode,ct.patientPK,ct.FinalTestResult,ct.TestDate
    ,ct.EncounterId
    )tn
    on a.sitecode = tn.sitecode
    and a.patientPK = tn.patientPK
    and cast(a.DateExtracted as date) = tn.MaxDateExtracted
    and a.FinalTestResult = tn.FinalTestResult
    and a.TestDate = tn.TestDate
    and coalesce(a.EncounterId,-1) = coalesce(tn.EncounterId,-1)
    INNER JOIN  [HTSCentral].[dbo].Clients(NoLock) c
    ON a.[SiteCode] = c.[SiteCode] and a.PatientPK=c.PatientPK

where a.FinalTestResult is not null