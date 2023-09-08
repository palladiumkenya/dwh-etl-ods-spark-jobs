SELECT distinct
    P.[PatientCccNumber] AS PatientID,P.[PatientPID] AS PatientPK,F.Code AS SiteCode,F.Name AS FacilityName,
    DS.[VisitId] AS VisitID,DS.[VisitDate] AS VisitDate,P.[Emr],
    CASE
        P.[Project]
        WHEN 'I-TECH' THEN 'Kenya HMIS II'
        WHEN 'HMIS' THEN 'Kenya HMIS II'
        ELSE P.[Project]
        END AS Project,
    DS.[PHQ9_1],DS.[PHQ9_2],DS.[PHQ9_3],DS.[PHQ9_4],DS.[PHQ9_5],DS.[PHQ9_6],DS.[PHQ9_7],
    DS.[PHQ9_8],DS.[PHQ9_9],DS.[PHQ_9_rating],DS.[DepressionAssesmentScore]
        ,P.ID,DS.[Date_Created],DS.[Date_Last_Modified]
FROM [DWAPICentral].[dbo].[PatientExtract](NoLock) P
    INNER JOIN [DWAPICentral].[dbo].[DepressionScreeningExtract](NoLock) DS ON DS.[PatientId] = P.ID AND DS.Voided = 0
    INNER JOIN [DWAPICentral].[dbo].[Facility](NoLock) F ON P.[FacilityId] = F.Id AND F.Voided = 0
WHERE P.gender != 'Unknown'