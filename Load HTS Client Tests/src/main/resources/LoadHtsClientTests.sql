SELECT distinct
    a.ID
              ,a.[FacilityName]

              ,a.[SiteCode]
              ,a.[PatientPk]
              ,a.[Emr]
              ,a.[Project]
              ,a.[EncounterId]
              ,a.[TestDate]
--,a.DateExtracted
              ,[EverTestedForHiv]
              ,[MonthsSinceLastTest]
              ,a.[ClientTestedAs]
              ,[EntryPoint]
              ,[TestStrategy]
              ,a.[TestResult1]
              ,a.[TestResult2]
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

FROM [HTSCentral].[dbo].[HtsClientTests](NoLock) a
    Inner join ( select ct.sitecode,ct.patientPK,ct.TestResult1,ct.TestResult2,ct.FinalTestResult,ct.TestDate,ct.TestType,ct.ID,max(DateExtracted)MaxDateExtracted  from [HTSCentral].[dbo].[HtsClientTests] ct
    group by ct.sitecode,ct.patientPK,ct.TestResult1,ct.TestResult2,ct.FinalTestResult,ct.TestDate,ct.TestType,ct.ID)tn
on a.sitecode = tn.sitecode and a.patientPK = tn.patientPK and a.ID = tn.ID
    and a.DateExtracted = tn.MaxDateExtracted
    and a.TestResult1 = tn.TestResult1
    and a.TestResult2 = tn.TestResult2
    and a.FinalTestResult = tn.FinalTestResult
    and a.TestDate = tn.TestDate
    and a.TestType = tn.TestType
    inner JOIN  [HTSCentral].[dbo].Clients(NoLock) b
    ON a.[SiteCode] = b.[SiteCode] and a.PatientPK=b.PatientPK

where a.FinalTestResult is not null