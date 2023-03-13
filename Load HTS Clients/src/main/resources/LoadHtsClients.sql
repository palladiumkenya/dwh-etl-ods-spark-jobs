SELECT  DISTINCT [HtsNumber]
        ,a.[Emr]
        ,a.PatientPK
        ,a.SiteCode
        ,a.[Project]
        ,[FacilityName]
        ,[Serial]
        ,CAST ([Dob] AS DATE) AS [Dob]
        ,LEFT([Gender],1) AS Gender
        ,[MaritalStatus]
        ,[KeyPopulationType]
        ,[PatientDisabled] AS [DisabilityType]
        ,PatientDisabled
        ,[County]
        ,[SubCounty]
        ,[Ward]
        ,NUPI
        ,HtsRecencyId
        ,Occupation
        ,PriorityPopulationType
FROM [HTSCentral].[dbo].[Clients](NoLock) a
    INNER JOIN (
    SELECT SiteCode,PatientPK, MAX(datecreated) AS Maxdatecreated
    FROM  [HTSCentral].[dbo].[Clients](NoLock)
    GROUP BY SiteCode,PatientPK
    ) tm
ON a.[SiteCode] = tm.[SiteCode] and a.PatientPK=tm.PatientPK and a.datecreated = tm.Maxdatecreated
WHERE a.DateExtracted > '2019-09-08'