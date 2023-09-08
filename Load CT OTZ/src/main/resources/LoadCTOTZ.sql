SELECT Distinct
    P.[PatientCccNumber] AS PatientID,P.[PatientPID] AS PatientPK,F.Code AS SiteCode,F.Name AS FacilityName,OE.[VisitId] AS VisitID,
    OE.[VisitDate] AS VisitDate,P.[Emr] AS Emr,
    CASE
        P.[Project]
        WHEN 'I-TECH' THEN 'Kenya HMIS II'
        WHEN 'HMIS' THEN 'Kenya HMIS II'
        ELSE P.[Project]
        END AS Project,
    OE.[OTZEnrollmentDate],OE.[TransferInStatus],OE.[ModulesPreviouslyCovered],OE.[ModulesCompletedToday],OE.[SupportGroupInvolvement],OE.[Remarks],
    OE.[TransitionAttritionReason],
    OE.[OutcomeDate]
        ,P.ID,OE.[Date_Created],OE.[Date_Last_Modified]

FROM [DWAPICentral].[dbo].[PatientExtract](NoLock) P
    INNER JOIN [DWAPICentral].[dbo].[OtzExtract](NoLock) OE ON OE.[PatientId] = P.ID AND OE.Voided = 0
    INNER JOIN [DWAPICentral].[dbo].[Facility](NoLock) F ON P.[FacilityId] = F.Id AND F.Voided = 0
WHERE P.gender != 'Unknown'