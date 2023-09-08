SELECT distinct
    P.[PatientCccNumber] AS PatientID,P.[PatientPID] AS PatientPK,F.Code AS SiteCode,F.Name AS FacilityName,
    DAS.[VisitId] AS VisitID,DAS.[VisitDate] AS VisitDate,P.[Emr] AS Emr,
    CASE
        P.[Project]
        WHEN 'I-TECH' THEN 'Kenya HMIS II'
        WHEN 'HMIS' THEN 'Kenya HMIS II'
        ELSE P.[Project]
        END AS Project,
    DAS.[DrinkingAlcohol] AS DrinkingAlcohol,DAS.[Smoking] AS Smoking,DAS.[DrugUse] AS DrugUse

        ,DAS.ID ,DAS.[Date_Created],DAS.[Date_Last_Modified]
FROM [DWAPICentral].[dbo].[PatientExtract](NoLock) P
    INNER JOIN [DWAPICentral].[dbo].[DrugAlcoholScreeningExtract](NoLock) DAS ON DAS.[PatientId] = P.ID AND DAS.Voided = 0
    INNER JOIN [DWAPICentral].[dbo].[Facility](NoLock) F ON P.[FacilityId] = F.Id AND F.Voided = 0
WHERE P.gender != 'Unknown'