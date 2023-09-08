SELECT Distinct
    P.[PatientCccNumber] AS PatientID,P.[PatientPID] AS PatientPK,F.Code AS SiteCode,F.Name AS FacilityName,
    IE.[VisitId] AS VisitID,IE.[VisitDate] AS VisitDate,P.[Emr] AS Emr,
    CASE
        P.[Project]
        WHEN 'I-TECH' THEN 'Kenya HMIS II'
        WHEN 'HMIS' THEN 'Kenya HMIS II'
        ELSE P.[Project]
        END AS Project,
    IE.[OnTBDrugs] AS OnTBDrugs,IE.[OnIPT] AS OnIPT,IE.[EverOnIPT] AS EverOnIPT,IE.[Cough] AS Cough,
    IE.[Fever] AS Fever,IE.[NoticeableWeightLoss] AS NoticeableWeightLoss,IE.[NightSweats] AS NightSweats,
    IE.[Lethargy] AS Lethargy,IE.[ICFActionTaken] AS ICFActionTaken,IE.[TestResult] AS TestResult,
    IE.[TBClinicalDiagnosis] AS TBClinicalDiagnosis,IE.[ContactsInvited] AS ContactsInvited,
    IE.[EvaluatedForIPT] AS EvaluatedForIPT,IE.[StartAntiTBs] AS StartAntiTBs,IE.[TBRxStartDate] AS TBRxStartDate,
    IE.[TBScreening] AS TBScreening,IE.[IPTClientWorkUp] AS IPTClientWorkUp,IE.[StartIPT] AS StartIPT,
    IE.[IndicationForIPT] AS IndicationForIPT
        ,P.ID,IE.[Date_Created],IE.[Date_Last_Modified]
FROM [DWAPICentral].[dbo].[PatientExtract](NoLock) P
    INNER JOIN [DWAPICentral].[dbo].[IptExtract](NoLock) IE ON IE.[PatientId] = P.ID AND IE.Voided = 0
    INNER JOIN [DWAPICentral].[dbo].[Facility](NoLock) F ON P.[FacilityId] = F.Id AND F.Voided = 0
WHERE P.gender != 'Unknown'