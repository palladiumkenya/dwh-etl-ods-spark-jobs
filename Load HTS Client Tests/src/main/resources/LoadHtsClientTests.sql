SELECT distinct
-- a.ID
    a.[FacilityName]
              ,a.[SiteCode]
              ,a.[PatientPk]
              ,a.[Emr]
              ,a.[Project]
              ,coalesce(a.[EncounterId],-1)EncounterId
              ,a.[TestDate]
              ,[EverTestedForHiv]
              ,[MonthsSinceLastTest]
              ,a.[ClientTestedAs]
              ,coalesce(a.[EntryPoint],'Empty') as Entrypoint
              ,coalesce(a.[TestStrategy],'Empty') as TestStrategy
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
    INNER JOIN ( select  ct.sitecode,ct.patientPK,ct.FinalTestResult,ct.TestDate,ct.EncounterId
    ,max(DateExtracted)MaxDateExtracted
    from [HTSCentral].[dbo].[HtsClientTests] ct
    GROUP BY ct.sitecode,ct.patientPK,ct.FinalTestResult,ct.TestDate
    ,ct.EncounterId
    )tn
    on a.sitecode = tn.sitecode
    and a.patientPK = tn.patientPK
    and a.DateExtracted = tn.MaxDateExtracted
    and a.FinalTestResult = tn.FinalTestResult
    and a.TestDate = tn.TestDate
    and coalesce(a.EncounterId,-1) = coalesce(tn.EncounterId,-1)
    INNER JOIN  [HTSCentral].[dbo].Clients(NoLock) c
    ON a.[SiteCode] = c.[SiteCode] and a.PatientPK=c.PatientPK

where a.FinalTestResult is not null