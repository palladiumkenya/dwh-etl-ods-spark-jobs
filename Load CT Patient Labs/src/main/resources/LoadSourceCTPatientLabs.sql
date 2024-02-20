SELECT distinct top 10
						  P.[PatientCccNumber] AS PatientID,P.[PatientPID] AS PatientPK,F.Code AS SiteCode,F.Name AS FacilityName,
                PL.[VisitId],PL.[OrderedByDate],PL.[ReportedByDate],PL.[TestName],
                PL.[EnrollmentTest],PL.[TestResult],P.[Emr]
        ,CASE P.[Project]
             WHEN 'I-TECH' THEN 'Kenya HMIS II'
             WHEN 'HMIS' THEN 'Kenya HMIS II'
             ELSE P.[Project]
                    END AS [Project]
						,PL.DateSampleTaken,
						PL.SampleType,
						p.ID ,
						reason,PL.[Date_Created],PL.[Date_Last_Modified]
						,PL.RecordUUID,PL.voided
FROM [DWAPICentral].[dbo].[PatientExtract](NoLock) P
    INNER JOIN [DWAPICentral].[dbo].[PatientLaboratoryExtract](NoLock) PL ON PL.[PatientId]= P.ID
    INNER JOIN [DWAPICentral].[dbo].[Facility](NoLock) F ON P.[FacilityId] = F.Id AND F.Voided=0
    INNER JOIN (
    SELECT F.code as SiteCode,p.[PatientPID] as PatientPK,
    InnerPL.TestResult,InnerPL.TestName,InnerPL.OrderedbyDate,
    InnerPL.voided,
    max(InnerPL.ID) As Max_ID,
    MAX(cast(InnerPL.created as date)) AS Maxdatecreated
    FROM [DWAPICentral].[dbo].[PatientExtract] P WITH (NoLock)
    INNER JOIN [DWAPICentral].[dbo].[PatientLaboratoryExtract] InnerPL WITH(NoLock)  ON InnerPL.[PatientId]= P.ID
    INNER JOIN [DWAPICentral].[dbo].[Facility] F WITH(NoLock)  ON P.[FacilityId] = F.Id AND F.Voided=0
    GROUP BY F.code,p.[PatientPID],InnerPL.TestResult,InnerPL.TestName,InnerPL.OrderedbyDate,InnerPL.voided
    ) tm
    ON f.code = tm.[SiteCode] and p.PatientPID=tm.PatientPK and
    PL.TestResult = tm.TestResult and PL.TestName = tm.TestName and PL.OrderedbyDate = tm.OrderedbyDate and
    PL.voided = tm.voided and
    cast(PL.created as date) = tm.Maxdatecreated
    and PL.ID = tm.Max_ID
WHERE p.gender!='Unknown'