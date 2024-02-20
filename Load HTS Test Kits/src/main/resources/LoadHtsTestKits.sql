SELECT DISTINCT a.[FacilityName]
              ,a.[SiteCode]
              ,a.[PatientPk]
              ,a.[HtsNumber]
              ,a.[Emr]
              ,a.[Project]
              ,a.[EncounterId]
              ,a.[TestKitName1]
              ,a.[TestKitLotNumber1]
              ,a.[TestKitExpiry1]
              ,a.[TestResult1]
              ,a.[TestKitName2]
              ,a.[TestKitLotNumber2]
              ,[TestKitExpiry2]
              ,a.[TestResult2]
              ,a.RecordUUID

FROM [HTSCentral].[dbo].[HtsTestKits](NoLock) a
    Inner join ( select ct.sitecode,ct.patientPK,ct.[EncounterId],ct.[TestKitName1],ct.[TestResult2],ct.[TestKitLotNumber1],
    max(ID) As MaxID,max(cast(DateExtracted as date))MaxDateExtracted  from [HTSCentral].[dbo].[HtsTestKits] ct
    group by ct.sitecode,ct.patientPK,ct.[EncounterId],ct.[TestKitName1],ct.[TestResult2],ct.[TestKitLotNumber1])tn
on a.sitecode = tn.sitecode and a.patientPK = tn.patientPK
    and cast(a.DateExtracted as date) = tn.MaxDateExtracted
    and a.[EncounterId] = tn.[EncounterId]
    and a.[TestKitName1] =tn.[TestKitName1]
    and a.[TestResult2] =tn.[TestResult2]
    and a.[TestKitLotNumber1] = tn.[TestKitLotNumber1]
    and a.ID = tn.MaxID

    INNER JOIN [HTSCentral].[dbo].Clients (NoLock) Cl
    on a.PatientPk = Cl.PatientPk and a.SiteCode = Cl.SiteCode