SELECT distinct
     P.[PatientCccNumber] AS PatientID,P.[PatientPID] AS PatientPK,F.Code AS SiteCode,
     F.Name AS FacilityName,P.[Emr] AS Emr,
     CASE
         P.[Project]
         WHEN 'I-TECH' THEN 'Kenya HMIS II'
         WHEN 'HMIS' THEN 'Kenya HMIS II'
         ELSE P.[Project]
         END AS Project,
     CL.[PartnerPersonID] AS PartnerPersonID,CL.[ContactAge] AS ContactAge,CL.[ContactSex] AS ContactSex,
     CL.[ContactMaritalStatus] AS ContactMaritalStatus,CL.[RelationshipWithPatient] AS RelationshipWithPatient,
     CL.[ScreenedForIpv] AS ScreenedForIpv,CL.[IpvScreening] AS IpvScreening,
     CL.[IPVScreeningOutcome] AS IPVScreeningOutcome,
     CL.[CurrentlyLivingWithIndexClient] AS CurrentlyLivingWithIndexClient,
     CL.[KnowledgeOfHivStatus] AS KnowledgeOfHivStatus,CL.[PnsApproach] AS PnsApproach,
     ContactPatientPK,
     CL.Created as DateCreated
         ,CL.ID,CL.[Date_Created],CL.[Date_Last_Modified],
     CL.RecordUUID,CL.voided
 FROM [DWAPICentral].[dbo].[PatientExtract](NoLock) P
     INNER JOIN [DWAPICentral].[dbo].[ContactListingExtract](NoLock) CL ON CL.[PatientId] = P.ID
     INNER JOIN [DWAPICentral].[dbo].[Facility](NoLock) F ON P.[FacilityId] = F.Id AND F.Voided = 0
     INNER JOIN (SELECT p.[PatientPID],F.code,CL.Contactage,max(cast(cl.created as date))Maxcreated
     FROM [DWAPICentral].[dbo].[PatientExtract](NoLock) P
     INNER JOIN [DWAPICentral].[dbo].[ContactListingExtract](NoLock) CL ON CL.[PatientId] = P.ID
     INNER JOIN [DWAPICentral].[dbo].[Facility](NoLock) F ON P.[FacilityId] = F.Id AND F.Voided = 0
     GROUP BY p.[PatientPID],F.code,CL.Contactage)tn
     on p.[PatientPID] = tn.[PatientPID] and
     F.code = tn.code and
     cast(cl.created as date) = tn.Maxcreated and
     cl.Contactage = tn.Contactage
 WHERE P.gender != 'Unknown' AND F.code >0