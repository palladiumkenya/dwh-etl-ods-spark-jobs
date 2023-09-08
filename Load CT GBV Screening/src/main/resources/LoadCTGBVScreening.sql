SELECT Distinct
     P.[PatientCccNumber] AS PatientID,P.[PatientPID] AS PatientPK,F.Code AS SiteCode,F.Name AS FacilityName,
     GSE.[VisitId] AS VisitID,GSE.[VisitDate] AS VisitDate,P.[Emr],
     CASE
         P.[Project]
         WHEN 'I-TECH' THEN 'Kenya HMIS II'
         WHEN 'HMIS' THEN 'Kenya HMIS II'
         ELSE P.[Project]
         END AS Project,
     GSE.[IPV] AS IPV,GSE.[PhysicalIPV],GSE.[EmotionalIPV],GSE.[SexualIPV],GSE.[IPVRelationship]
         ,GSE.ID,GSE.[Date_Created],GSE.[Date_Last_Modified]
 FROM [DWAPICentral].[dbo].[PatientExtract](NoLock) P
     INNER JOIN [DWAPICentral].[dbo].[GbvScreeningExtract](NoLock) GSE ON GSE.[PatientId] = P.ID AND GSE.Voided = 0
     INNER JOIN [DWAPICentral].[dbo].[Facility](NoLock) F ON P.[FacilityId] = F.Id AND F.Voided = 0
 WHERE P.gender != 'Unknown'