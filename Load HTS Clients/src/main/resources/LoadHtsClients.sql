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
    --,coalesce([KeyPopulationType],'',null) AS [KeyPopulationType]
        ,null [KeyPopulationType]
        ,coalesce([PatientDisabled],'',null) AS [DisabilityType]
    -- ,PatientDisabled
        ,coalesce([PatientDisabled],'',null) as PatientDisabled
        ,[County]
        ,[SubCounty]
        ,[Ward]
        ,NUPI
        ,HtsRecencyId
        ,Occupation
        ,PriorityPopulationType
        ,pkv
        ,a.RecordUUID
FROM [HTSCentral].[dbo].[Clients](NoLock) a
    INNER JOIN (
    SELECT SiteCode,PatientPK,max(ID)As MaxID, MAX(cast(datecreated as date)) AS Maxdatecreated
    FROM  [HTSCentral].[dbo].[Clients](NoLock)
    GROUP BY SiteCode,PatientPK
    ) tm
ON a.[SiteCode] = tm.[SiteCode] and
    a.PatientPK=tm.PatientPK and
    cast(a.datecreated as date) = tm.Maxdatecreated
    and a.ID = tm.MaxID
WHERE a.DateExtracted > '2019-09-08'